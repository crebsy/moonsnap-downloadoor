package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/crebsy/moonsnap-downloadoor/moonproto"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/pierrec/lz4"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/encoding/protodelim"
)

type Chunk struct {
	FileIndex  int
	FileOffset int
	Data       []byte
}

func main() {
	indexFileName := os.Args[1]
	outDir := os.Args[2]
	baseURL := os.Args[3]
	chunkSize := 8192
	file, err := os.Open(indexFileName)
	if err != nil {
		panic(err)
	}
	reader := bufio.NewReader(file)
	index := moonproto.Index{}
	err = protodelim.UnmarshalOptions{MaxSize: -1}.UnmarshalFrom(reader, &index)
	if err != nil {
		panic(err)
	}
	totalBytes := createFileStructure(&index, outDir)
	bar := progressbar.DefaultBytes(int64(totalBytes), "downloading files")
	bar.Close()
	fileCache, err := lru.NewWithEvict(4096, func(_ int, file *os.File) {
		err := file.Close()
		if err != nil {
			panic(err)
		}
	})
	if err != nil {
		panic(err)
	}
	numThreads := runtime.GOMAXPROCS(0)
	downloadChan := make(chan *moonproto.LibraryChunk)
	persistChan := make(chan Chunk, 1024)
	downloadWg := sync.WaitGroup{}
	persistWg := sync.WaitGroup{}
	for range numThreads {
		persistWg.Add(1)
		go func() {
			chunkSavor(&index, bar, fileCache, outDir, chunkSize, persistChan)
			persistWg.Done()
		}()
	}

	for range 4 {
		downloadWg.Add(1)
		go func() {
			downloadoor(&index, baseURL, persistChan, downloadChan)
			downloadWg.Done()
		}()
	}

	for {
		chunk := moonproto.LibraryChunk{}
		err = protodelim.UnmarshalOptions{MaxSize: -1}.UnmarshalFrom(reader, &chunk)
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		downloadChan <- &chunk
	}
	close(downloadChan)
	downloadWg.Wait()
	close(persistChan)
	persistWg.Wait()

	fileCache.Purge()

	bar.Close()
	verifyFiles(&index, outDir)
}

func verifyFiles(index *moonproto.Index, outDir string) {
	bar := progressbar.Default(int64(len(index.Files)), "verifying files")
	for _, file := range index.Files {
		if !fs.FileMode(file.FileMode).IsRegular() {
			bar.Add(1)
			continue
		}
		f, err := os.Open(path.Join(outDir, file.FilePath))
		if err != nil {
			panic(err)
		}
		hash := sha256.New()
		_, err = io.Copy(hash, f)
		if err != nil {
			panic(err)
		}
		fileHash := hash.Sum(nil)
		if !bytes.Equal(fileHash, file.FileHash) {
			panic(fmt.Errorf("fileHash mismatch for %s: fileHash: %s, expected: %s",
				file.FilePath, hex.EncodeToString(fileHash), hex.EncodeToString(file.FileHash)))
		}
		f.Close()
		bar.Add(1)
	}
}

func chunkSavor(index *moonproto.Index, bar *progressbar.ProgressBar, fileCache *lru.Cache[int, *os.File], outDir string, chunkSize int, chunkChan <-chan Chunk) {
	dst := make([]byte, chunkSize)
	for chunk := range chunkChan {
		f, ok := fileCache.Get(chunk.FileIndex)
		if !ok {
			var err error
			f, err = os.OpenFile(path.Join(outDir, index.Files[chunk.FileIndex].FilePath), os.O_WRONLY, 0)
			if err != nil {
				panic(err)
			}
			fileCache.Add(chunk.FileIndex, f)
		}
		decompressedLength, err := lz4.UncompressBlock(chunk.Data, dst)
		if err != nil {
			panic(err)
		}
		bar.Add(decompressedLength)
		n, err := f.WriteAt(dst[0:decompressedLength], int64(chunk.FileOffset*chunkSize))
		if err != nil || n != decompressedLength {
			panic(err)
		}
	}
}

func downloadoor(index *moonproto.Index, baseUrl string, chunkChan chan<- Chunk, downloadChan <-chan *moonproto.LibraryChunk) {
	client := http.Client{}
	for libChunk := range downloadChan {
		libraryName := index.Libraries[libChunk.LibraryIndex].Name
		urlPath := baseUrl + libraryName
		u, err := url.Parse(urlPath)
		if err != nil {
			panic(err)
		}
		res, err := client.Do(&http.Request{
			Method: "GET",
			Header: http.Header{
				"Range": []string{
					fmt.Sprintf(
						"bytes=%d-%d",
						libChunk.StartOffset,
						libChunk.StartOffset+libChunk.Length,
					),
				},
			},
			URL: u,
		})
		if err != nil {
			panic(err)
		}
		var chunkBytes []byte
		for i := range len(libChunk.FileIndex) {
			if libChunk.FileLibraryChunkLength[i] > 0 {

				chunkBytes = make([]byte, libChunk.FileLibraryChunkLength[i])
				offset := 0
				for offset < len(chunkBytes) {
					n, err := res.Body.Read(chunkBytes[offset:])
					if err != nil {
						panic(err)
					}
					offset += n
				}
			} else {
				fmt.Printf("DUPE! %d\n", i)
			}
			if libChunk.FileIndex[i] < 0 {
				continue
			}
			chunkChan <- Chunk{
				FileIndex:  int(libChunk.FileIndex[i]),
				FileOffset: int(libChunk.FileOffset[i]),
				Data:       chunkBytes,
			}
		}
	}
}

func createFileStructure(index *moonproto.Index, outDir string) int {
	totalBytes := 0
	for _, file := range index.Files {
		fmt.Println(file)
		totalBytes += int(file.FileSize)
		if file.FileMode&uint64(fs.ModeSymlink) > 0 {
			fmt.Println(file.FileLinkTarget)

			newLinkTarget, err := filepath.Abs(path.Join(outDir, *file.FileLinkTarget))
			if err != nil {
				panic(err)
			}
			os.Symlink(newLinkTarget, path.Join(outDir, file.FilePath))
			continue
		} else if file.FileMode&uint64(fs.ModeDir) > 0 {
			os.Mkdir(path.Join(outDir, file.FilePath), fs.FileMode(file.FileMode&uint64(fs.ModePerm)))
			continue
		}
		f, err := os.OpenFile(path.Join(outDir, file.FilePath), os.O_CREATE|os.O_RDWR, fs.FileMode(file.FileMode))
		if err != nil {
			panic(err)
		}

		//TODO handle 0-byte files (e.g. LOCK)
		if file.FileSize > 0 {
			err = unix.Fallocate(int(f.Fd()), 0, 0, int64(file.FileSize))
			if err != nil {
				fmt.Printf("path=%s, size=%d\n", file.FilePath, file.FileSize)
				panic(err)
			}
		}
		err = f.Close()
		if err != nil {
			panic(err)
		}
	}
	return totalBytes
}
