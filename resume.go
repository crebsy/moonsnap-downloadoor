package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type Bitset []byte

type RangeResumeEntry struct {
	fileOffset  int64
	chunkBitset Bitset
	length      int
	done        int
}

type ResumeCtx struct {
	lock         sync.Mutex
	resumeFile   *os.File
	ranges       Bitset
	rangeEntries map[int]*RangeResumeEntry
	opCount      int
}

const RANGE_BITSET_SIZE = 1024 * 1024 / 8

const AUTOSAVE_AFTER_N_CHUNKS = 128

func (b Bitset) Set(idx int) {
	b[idx/8] |= 1 << (idx % 8)
}

func (b Bitset) Get(idx int) bool {
	return b[idx/8]&(1<<(idx%8)) > 0
}

func LoadResumeFile() *ResumeCtx {
	resumeFileName := filepath.Join(OUT_DIR, ".moonsnap_resume")
	_, err := os.Stat(resumeFileName)
	var resumeFile *os.File
	if err != nil && errors.Is(err, os.ErrNotExist) {
		resumeFile, err = os.Create(resumeFileName)
		if err != nil {
			panic(err)
		}
	} else if err != nil {
		panic(err)
	} else {
		resumeFile, err = os.OpenFile(resumeFileName, os.O_RDWR, 0)
		if err != nil {
			panic(err)
		}
	}

	ranges := make(Bitset, RANGE_BITSET_SIZE)
	n, err := io.ReadFull(resumeFile, ranges)
	if err != nil || n != RANGE_BITSET_SIZE {
		err = resumeFile.Truncate(0)
		if err != nil {
			panic(err)
		}
		ranges = make(Bitset, RANGE_BITSET_SIZE)
		resumeFile.Seek(0, io.SeekStart)
		_, err := io.Copy(resumeFile, bytes.NewReader(ranges))
		if err != nil {
			panic(err)
		}
	}
	resumeFile.Seek(RANGE_BITSET_SIZE, io.SeekStart)

	return &ResumeCtx{
		resumeFile:   resumeFile,
		ranges:       ranges,
		rangeEntries: map[int]*RangeResumeEntry{},
	}
}

func (r *ResumeCtx) loadNextRange(rangeIndex int, numChunksInRange int) {
	r.lock.Lock()
	defer r.lock.Unlock()

	numBytes := (numChunksInRange + 7) / 8
	rangeBitset := make(Bitset, numBytes)

	offset, err := r.resumeFile.Seek(0, io.SeekCurrent)
	if err != nil {
		panic(err)
	}

	n, err := io.ReadFull(r.resumeFile, rangeBitset)
	if err != nil || n != numBytes {
		err = r.resumeFile.Truncate(offset)
		if err != nil {
			panic(err)
		}
		rangeBitset = make(Bitset, numBytes)
		r.resumeFile.Seek(offset, io.SeekStart)
		_, err := io.Copy(r.resumeFile, bytes.NewReader(rangeBitset))
		if err != nil {
			panic(err)
		}
	}
	r.resumeFile.Seek(offset+int64(numBytes), io.SeekStart)

	done := 0
	for i := range numChunksInRange {
		if rangeBitset.Get(i) {
			done++
		}
	}

	r.rangeEntries[rangeIndex] = &RangeResumeEntry{
		fileOffset:  offset,
		chunkBitset: rangeBitset,
		length:      numChunksInRange,
		done:        done,
	}
}

func (r *ResumeCtx) isRangeDone(rangeIdx int) bool {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.ranges.Get(rangeIdx)
}

func (r *ResumeCtx) getNumChunksDoneInRange(rangeIdx int) int {
	r.lock.Lock()
	defer r.lock.Unlock()
	entry := r.rangeEntries[rangeIdx]

	consecutiveDone := 0
	for i := 0; i < entry.length; i++ {
		if entry.chunkBitset.Get(i) {
			consecutiveDone++
		} else {
			break
		}
	}

	return consecutiveDone
}

func (r *ResumeCtx) setChunkDone(rangeIdx int, chunkIdx int) {
	r.lock.Lock()
	defer r.lock.Unlock()

	entry := r.rangeEntries[rangeIdx]

	if !entry.chunkBitset.Get(chunkIdx) {
		entry.chunkBitset.Set(chunkIdx)
		entry.done++
	}

	if entry.done >= entry.length {
		r.ranges.Set(rangeIdx)
		go r.persist()
	} else {
		r.opCount++
		if r.opCount >= AUTOSAVE_AFTER_N_CHUNKS {
			r.opCount = 0
			go r.persist()
		}
	}
}

func (r *ResumeCtx) persist() {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.resumeFile == nil {
		return
	}

	offset, err := r.resumeFile.Seek(0, io.SeekCurrent)
	if err != nil {
		panic(err)
	}

	_, err = r.resumeFile.Seek(0, io.SeekStart)
	if err != nil {
		panic(err)
	}
	_, err = io.Copy(r.resumeFile, bytes.NewReader(r.ranges))
	if err != nil {
		panic(err)
	}

	dropRangeIndexes := []int{}
	for rangeIdx, entry := range r.rangeEntries {
		_, err = r.resumeFile.Seek(entry.fileOffset, io.SeekStart)
		if err != nil {
			panic(err)
		}
		_, err := io.Copy(r.resumeFile, bytes.NewReader(entry.chunkBitset))
		if err != nil {
			panic(err)
		}

		if entry.done >= entry.length {
			dropRangeIndexes = append(dropRangeIndexes, rangeIdx)
		}
	}

	for _, rangeIdx := range dropRangeIndexes {
		/*if rangeIdx == 0 {
			entry := r.rangeEntries[rangeIdx]
			fmt.Printf("Dropping rangeIdx %d, entry.done=%d, entry.length=%d\n", rangeIdx, entry.done, entry.length)
		}*/
		delete(r.rangeEntries, rangeIdx)
	}

	_, err = r.resumeFile.Seek(offset, io.SeekStart)
	if err != nil {
		panic(err)
	}
}

func (r *ResumeCtx) close() {
	r.persist()

	r.lock.Lock()
	err := r.resumeFile.Close()
	if err != nil {
		panic(err)
	}
	r.resumeFile = nil
	r.lock.Unlock()
}
