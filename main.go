package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/crebsy/moonsnap-downloadoor/moonproto"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/minio/sha256-simd"
	"github.com/pierrec/lz4"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/encoding/protodelim"
)

type Chunk struct {
	FileIndex  int
	FileOffset int
	Data       []byte
	RangeIndex int
	ChunkIndex int
}

type SnapUrlResponse struct {
	Url           string `json:"url"`
	Authorization string `json:"authorization"`
	FileName      string `json:"file_name"`
	Curl          string `json:"curl"`
}

type DownloadItem struct {
	chunk      *moonproto.LibraryChunk
	rangeIndex int
}

var API_BASE_URL = "https://api.moonsnap.xyz"

var SNAP_KEY = os.Getenv("MOONSNAP_SNAP_KEY")
var OUT_DIR = os.Getenv("MOONSNAP_OUT_DIR")

var CHUNK_SIZE = 8192
var MAX_RETRIES = _getMaxRetries()

func main() {
	fmt.Println(getBanner())

	apiBaseUrl := os.Getenv("MOONSNAP_API_BASE_URL")
	if len(apiBaseUrl) > 0 {
		API_BASE_URL = apiBaseUrl
	}
	if len(SNAP_KEY) == 0 && len(os.Args) > 1 {
		SNAP_KEY = os.Args[1]
	}

	if len(OUT_DIR) == 0 && len(os.Args) > 2 {
		OUT_DIR = os.Args[2]
	} else {
		OUT_DIR = "."
	}

	if len(SNAP_KEY) == 0 {
		panic("Please provide a MOONSNAP_SNAP_KEY")
	}

	err := os.MkdirAll(OUT_DIR, 0755)
	if err != nil {
		panic(err)
	}

	indexFileName := filepath.Join(OUT_DIR, ".moonsnap_index")
	_, err = os.Stat(indexFileName)
	if err != nil && errors.Is(err, os.ErrNotExist) {
		downloadIndexFile(indexFileName)
	}

	file, err := os.Open(indexFileName)
	if err != nil {
		panic(err)
	}

	resumeCtx := LoadResumeFile()

	reader := bufio.NewReader(file)
	index := moonproto.Index{}
	err = protodelim.UnmarshalOptions{MaxSize: -1}.UnmarshalFrom(reader, &index)
	if err != nil {
		panic(err)
	}
	totalBytes := createFileStructure(&index, OUT_DIR)
	bar := progressbar.DefaultBytes(int64(totalBytes), "downloading files")
	fileCache, err := lru.NewWithEvict(4096, func(_ int, file *os.File) {
		err := file.Close()
		if err != nil {
			panic(err)
		}
	})
	if err != nil {
		panic(err)
	}
	numThreads := 128
	downloadChan := make(chan DownloadItem)
	persistChan := make(chan Chunk, 1024)
	downloadWg := sync.WaitGroup{}
	persistWg := sync.WaitGroup{}
	for range numThreads {
		persistWg.Add(1)
		go func() {
			chunkSavor(&index, resumeCtx, bar, fileCache, OUT_DIR, CHUNK_SIZE, persistChan)
			persistWg.Done()
		}()
	}

	for range numThreads {
		downloadWg.Add(1)
		go func() {
			downloadoor(&index, resumeCtx, persistChan, downloadChan)
			downloadWg.Done()
		}()
	}

	for rangeIdx := 0; ; rangeIdx++ {
		chunk := moonproto.LibraryChunk{}
		err = protodelim.UnmarshalOptions{MaxSize: -1}.UnmarshalFrom(reader, &chunk)
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}

		resumeCtx.loadNextRange(rangeIdx, len(chunk.FileIndex))
		if resumeCtx.isRangeDone(rangeIdx) {
			sumBytes := int64(0)
			for i := 0; i < len(chunk.FileIndex); i++ {
				if chunk.FileIndex[i] < 0 {
					continue
				}
				file := index.Files[chunk.FileIndex[i]]

				remainingFileSizeAfterOffset := int64(file.FileSize - chunk.FileOffset[i])
				maxChunkSize := int64(CHUNK_SIZE)
				if maxChunkSize < remainingFileSizeAfterOffset {
					sumBytes += maxChunkSize
				} else {
					sumBytes += remainingFileSizeAfterOffset
				}
			}
			bar.Add64(sumBytes)
			go resumeCtx.persist()
			continue
		}
		downloadChan <- DownloadItem{&chunk, rangeIdx}
	}
	close(downloadChan)
	downloadWg.Wait()
	close(persistChan)
	persistWg.Wait()

	fileCache.Purge()
	resumeCtx.close()
	bar.Close()
	verifyFiles(&index, OUT_DIR)
}

func downloadIndexFile(indexFileName string) string {
	retries := 0
	bar := progressbar.Default(int64(100), "downloading index file")
retry_index:
	snapUrlCreds, err := getSnapUrlCreds("")
	if err != nil {
		retries += 1
		if retries <= MAX_RETRIES {
			time.Sleep(1 * time.Second)
			goto retry_index
		}
	}
	if snapUrlCreds == nil {
		panic(err)
	}

	client := http.Client{}
	u, err := url.Parse(snapUrlCreds.Url)
	if err != nil {
		panic(err)
	}
	bar.Add(25)
	res, err := client.Do(&http.Request{
		Method: "GET",
		Header: http.Header{
			"Authorization": []string{snapUrlCreds.Authorization},
		},
		URL: u,
	})
	if err != nil {
		retries += 1
		if retries <= MAX_RETRIES {
			time.Sleep(1 * time.Second)
			goto retry_index
		}
		panic(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		//fmt.Printf("%+v\n", snapUrlCreds)
		dfn := "/tmp/index.failed"
		df, _ := os.Create(dfn)
		defer df.Close()
		_, err = io.Copy(df, res.Body)
		if err != nil {
			panic(err)
		}

		//fmt.Printf("Bad status while downloading index: %s, dumped to %s\n", res.Status, dfn)
		retries += 1
		if retries <= MAX_RETRIES {
			time.Sleep(1 * time.Second)
			goto retry_index
		}
	}
	bar.Add(50)

	f, err := os.Create(indexFileName)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	_, err = io.Copy(f, res.Body)
	if err != nil {
		panic(err)
	}
	bar.Add(25)
	bar.Close()

	return f.Name()
}

func getSnapUrlCreds(fileName string) (*SnapUrlResponse, error) {
	client := http.Client{}
	u, err := url.Parse(API_BASE_URL)
	if err != nil {
		return nil, err
	}
	values := u.Query()
	values.Set("snapKey", SNAP_KEY)
	if len(fileName) > 0 {
		values.Set("fileName", fileName)
	}
	u.RawQuery = values.Encode()
	res, err := client.Do(&http.Request{
		Method: "GET",
		URL:    u,
	})
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	if res.StatusCode == http.StatusPaymentRequired {
		return nil, errors.New(string(body))
	}

	snapUrlResponse := SnapUrlResponse{}
	err = json.Unmarshal(body, &snapUrlResponse)
	if err != nil {
		return nil, err
	}
	return &snapUrlResponse, nil
}

func verifyFilesWorker(fileChan <-chan *moonproto.Index_File, bar *progressbar.ProgressBar, outDir string, wg *sync.WaitGroup) {
	for file := range fileChan {
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

	wg.Done()
}

func verifyFiles(index *moonproto.Index, outDir string) {
	bar := progressbar.Default(int64(len(index.Files)), "verifying files")
	fileChan := make(chan *moonproto.Index_File)
	wg := sync.WaitGroup{}

	for range 8 {
		wg.Add(1)
		go verifyFilesWorker(fileChan, bar, outDir, &wg)
	}

	for _, file := range index.Files {
		fileChan <- file
	}
	close(fileChan)
	wg.Wait()
}

func chunkSavor(index *moonproto.Index, resumeCtx *ResumeCtx, bar *progressbar.ProgressBar, fileCache *lru.Cache[int, *os.File], outDir string, chunkSize int, chunkChan <-chan Chunk) {
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
		bar.Add(len(chunk.Data))
		n, err := f.WriteAt(chunk.Data, int64(chunk.FileOffset*chunkSize))
		if err != nil || n != len(chunk.Data) {
			panic(err)
		}

		resumeCtx.setChunkDone(chunk.RangeIndex, chunk.ChunkIndex)
	}
}

func downloadoor(index *moonproto.Index, resumeCtx *ResumeCtx, chunkChan chan<- Chunk, downloadChan <-chan DownloadItem) {
	client := http.Client{}
	for item := range downloadChan {
		libChunk := item.chunk
		// contains already the url_prefix but missing the leading "/"
		libName := "/" + index.Libraries[libChunk.LibraryIndex].Name
		retries := 0
		//fmt.Printf("start=%s, startOffset=%d\n", u.Path, libChunk.StartOffset)
		localStartOffset := libChunk.StartOffset
		localLength := libChunk.Length
		localIdx := 0

		alreadyDone := resumeCtx.getNumChunksDoneInRange(item.rangeIndex)
		for ; localIdx < alreadyDone; localIdx++ {
			len := libChunk.FileLibraryChunkLength[localIdx]
			if len > 0 {
				localStartOffset += len
				localLength -= len
			}
		}

	retry:
		// get url for lib
		libUrlCreds, err := getSnapUrlCreds(libName)
		if err != nil {
			retries += 1
			if retries <= MAX_RETRIES {
				time.Sleep(1 * time.Second)
				goto retry
			}
			panic(err)
		}

		req, err := http.NewRequest(
			"GET",
			libUrlCreds.Url,
			nil,
		)
		if err != nil {
			retries += 1
			if retries <= MAX_RETRIES {
				time.Sleep(1 * time.Second)
				goto retry
			}
			panic(err)
		}

		req.Header["Authorization"] = []string{libUrlCreds.Authorization}
		req.Header["Range"] = []string{
			fmt.Sprintf(
				"bytes=%d-%d",
				localStartOffset,
				localStartOffset+localLength-1,
			),
		}

		res, err := client.Do(req)
		if err != nil {
			retries += 1
			if retries <= MAX_RETRIES {
				time.Sleep(1 * time.Second)
				goto retry
			}
			panic(err)
		}
		if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusPartialContent {
			//fmt.Printf("%+v\n", req)
			//fmt.Printf("%d, %d\n", localLength, localStartOffset)
			retries += 1
			if retries <= MAX_RETRIES {
				time.Sleep(1 * time.Second)
				goto retry
			}
			panic(fmt.Sprintf("Download failed with status %d %s", res.StatusCode, res.Status))
		}
		var chunkBytes []byte

		for ; localIdx < len(libChunk.FileIndex); localIdx++ {
			if libChunk.FileLibraryChunkLength[localIdx] > 0 {
				chunkBytes = make([]byte, libChunk.FileLibraryChunkLength[localIdx])
				offset := 0
				for offset < len(chunkBytes) {
					n, err := res.Body.Read(chunkBytes[offset:])
					if err != nil && err != io.EOF {
						retries += 1
						if retries <= MAX_RETRIES {
							time.Sleep(1 * time.Second)
							goto retry
						}
						panic(err)
					}
					offset += n
				}
			}
			if libChunk.FileIndex[localIdx] < 0 {
				localStartOffset += uint64(len(chunkBytes))
				localLength -= uint64(len(chunkBytes))
				continue
			}
			dst := make([]byte, CHUNK_SIZE)
			decompressedLength, err := lz4.UncompressBlock(chunkBytes, dst)
			if err != nil {
				//fmt.Printf("%+v\n", req)
				//fmt.Printf("%d, %d\n", localLength, localStartOffset)
				retries += 1
				if retries <= MAX_RETRIES {
					time.Sleep(1 * time.Second)
					goto retry
				}
				panic(err)
			}
			/* else {
				fmt.Printf("DUPE! %d\n", i)
			}*/

			chunkChan <- Chunk{
				FileIndex:  int(libChunk.FileIndex[localIdx]),
				FileOffset: int(libChunk.FileOffset[localIdx]),
				Data:       dst[0:decompressedLength],
				RangeIndex: item.rangeIndex,
				ChunkIndex: localIdx,
			}

			if libChunk.FileLibraryChunkLength[localIdx] > 0 {
				localStartOffset += uint64(len(chunkBytes))
				localLength -= uint64(len(chunkBytes))
			}
		}
		//fmt.Printf("end=%s, startOffset=%d\n", u.Path, libChunk.StartOffset)
	}
}

func createFileStructure(index *moonproto.Index, outDir string) int {
	totalBytes := 0
	for _, file := range index.Files {
		//fmt.Println(file)
		totalBytes += int(file.FileSize)
		if file.FileMode&uint64(fs.ModeSymlink) > 0 {
			//fmt.Println(file.FileLinkTarget)

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
			err := unix.Fallocate(int(f.Fd()), 0, 0, int64(file.FileSize))
			if err != nil {
				fmt.Printf("Error with: path=%s, size=%d\n", file.FilePath, file.FileSize)
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

func _getMaxRetries() int {
	retries := 16
	var err error
	retryStr := os.Getenv("MOONSNAP_MAX_RETRIES")
	if len(retryStr) > 0 {
		retries, err = strconv.Atoi(retryStr)
		if err != nil {
			fmt.Printf("Cannot convert string to int %s", retryStr)
		}
	}
	return retries
}
