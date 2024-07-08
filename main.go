package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
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
}

type SnapUrlResponse struct {
	Url       string `json:"url"`
	Mac       string `json:"mac"`
	Timestamp int    `json:"timestamp"`
	FileName  string `json:"file_name"`
	Curl      string `json:"curl"`
}

var API_BASE_URL = os.Getenv("MOONSNAP_API_BASE_URL")
var SNAP_KEY = os.Getenv("MOONSNAP_SNAP_KEY")
var OUT_DIR = os.Getenv("MOONSNAP_OUT_DIR")

var CHUNK_SIZE = 8192
var MAX_RETRIES = _getMaxRetries()

func main() {
	if len(API_BASE_URL) == 0 {
		panic("Please provide a MOONSNAP_API_BASE_URL")
	}

	if len(SNAP_KEY) == 0 {
		panic("Please provide a MOONSNAP_SNAP_KEY")
	}

	if len(OUT_DIR) == 0 {
		OUT_DIR = "/tmp"
	}

	err := os.MkdirAll(OUT_DIR, 0755)
	if err != nil {
		panic(err)
	}

	// get url for index
	indexFileName := downloadIndexFile()
	fmt.Println(indexFileName)

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
	downloadChan := make(chan *moonproto.LibraryChunk)
	persistChan := make(chan Chunk, 1024)
	downloadWg := sync.WaitGroup{}
	persistWg := sync.WaitGroup{}
	for range numThreads {
		persistWg.Add(1)
		go func() {
			chunkSavor(&index, bar, fileCache, OUT_DIR, CHUNK_SIZE, persistChan)
			persistWg.Done()
		}()
	}

	for range 128 {
		downloadWg.Add(1)
		go func() {
			downloadoor(&index, persistChan, downloadChan)
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
	verifyFiles(&index, OUT_DIR)
}

func downloadIndexFile() string {
retry_index:
	snapUrlCreds := getSnapUrlCreds("")
	client := http.Client{}
	u, err := url.Parse(snapUrlCreds.Url)
	if err != nil {
		panic(err)
	}
	res, err := client.Do(&http.Request{
		Method: "GET",
		Header: http.Header{
			"Mac":       []string{snapUrlCreds.Mac},
			"Timestamp": []string{fmt.Sprintf("%d", snapUrlCreds.Timestamp)},
		},
		URL: u,
	})
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		fmt.Printf("%+v\n", snapUrlCreds)
		dfn := "/tmp/index.failed"
		df, _ := os.Create(dfn)
		defer df.Close()
		_, err = io.Copy(df, res.Body)
		if err != nil {
			panic(err)
		}

		fmt.Printf("Bad status while downloading index: %s, dumped to %s\n", res.Status, dfn)
		goto retry_index
	}
	f, err := os.Create("/tmp/index")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	_, err = io.Copy(f, res.Body)
	if err != nil {
		panic(err)
	}
	return f.Name()
}

func getSnapUrlCreds(fileName string) SnapUrlResponse {
	client := http.Client{}
	u, err := url.Parse(API_BASE_URL)
	if err != nil {
		panic(err)
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
		panic(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		panic(err)
	}

	var snapUrlResponse SnapUrlResponse
	err = json.Unmarshal(body, &snapUrlResponse)
	if err != nil {
		panic(err)
	}
	return snapUrlResponse
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

func chunkSavor(index *moonproto.Index, bar *progressbar.ProgressBar, fileCache *lru.Cache[int, *os.File], outDir string, chunkSize int, chunkChan <-chan Chunk) {
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
	}
}

func downloadoor(index *moonproto.Index, chunkChan chan<- Chunk, downloadChan <-chan *moonproto.LibraryChunk) {
	client := http.Client{}
	for libChunk := range downloadChan {
		// contains already the url_prefix but missing the leading "/"
		libName := "/" + index.Libraries[libChunk.LibraryIndex].Name
		retries := 0
		//fmt.Printf("start=%s, startOffset=%d\n", u.Path, libChunk.StartOffset)
	retry:
		localStartOffset := libChunk.StartOffset
		localLength := libChunk.Length
		// get url for lib
		libUrlCreds := getSnapUrlCreds(libName)
		u, err := url.Parse(libUrlCreds.Url)
		if err != nil {
			retries += 1
			if retries <= MAX_RETRIES {
				time.Sleep(1 * time.Second)
				goto retry
			}
			panic(err)
		}

		res, err := client.Do(&http.Request{
			Method: "GET",
			Header: http.Header{
				"Mac":       []string{libUrlCreds.Mac},
				"Timestamp": []string{fmt.Sprintf("%d", libUrlCreds.Timestamp)},
				"Range": []string{
					fmt.Sprintf(
						"bytes=%d-%d",
						localStartOffset,
						localStartOffset+localLength-1,
					),
				},
			},
			URL: u,
		})
		if err != nil {
			retries += 1
			if retries <= MAX_RETRIES {
				time.Sleep(1 * time.Second)
				goto retry
			}
			panic(err)
		}
		if res.StatusCode != http.StatusOK && res.StatusCode != http.StatusPartialContent {
			retries += 1
			if retries <= MAX_RETRIES {
				time.Sleep(1 * time.Second)
				goto retry
			}
			panic(fmt.Sprintf("Download failed with status %d %s", res.StatusCode, res.Status))
		}
		var chunkBytes []byte
		for i := range len(libChunk.FileIndex) {
			if libChunk.FileLibraryChunkLength[i] > 0 {
				chunkBytes = make([]byte, libChunk.FileLibraryChunkLength[i])
				offset := 0
				for offset < len(chunkBytes) {
					n, err := res.Body.Read(chunkBytes[offset:])
					if err != nil {
						retries += 1
						if retries <= MAX_RETRIES {
							time.Sleep(1 * time.Second)
							goto retry
						}
						panic(err)
					}
					offset += n
				}
				localStartOffset += uint64(offset)
				localLength -= uint64(offset)
			}
			dst := make([]byte, CHUNK_SIZE)
			decompressedLength, err := lz4.UncompressBlock(chunkBytes, dst)
			if err != nil {
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
			if libChunk.FileIndex[i] < 0 {
				continue
			}
			chunkChan <- Chunk{
				FileIndex:  int(libChunk.FileIndex[i]),
				FileOffset: int(libChunk.FileOffset[i]),
				Data:       dst[0:decompressedLength],
			}
		}
		//fmt.Printf("end=%s, startOffset=%d\n", u.Path, libChunk.StartOffset)
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
