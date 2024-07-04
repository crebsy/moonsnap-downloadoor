package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
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
	"sync"
	"time"

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

type SnapUrlResponse struct {
	Url  string `json:"url"`
	Hmac string `json:"hmac"`
}

var BASE_URL = os.Getenv("MOONSNAP_BASE_URL")
var CHUNK_SIZE = 8192
var MAX_RETRIES = 16

func main() {
	snapKey := os.Args[1]
	outDir := os.Args[2]

	snapUrlCreds := getSnapUrlCreds(snapKey, "/")
	indexFileName := downloadIndexFile(snapUrlCreds)

	file, err := os.Open(indexFileName)
	if err != nil {
		panic(err)
	}
	libUrlCreds := getSnapUrlCreds(snapKey, "/libs")
	reader := bufio.NewReader(file)
	index := moonproto.Index{}
	err = protodelim.UnmarshalOptions{MaxSize: -1}.UnmarshalFrom(reader, &index)
	if err != nil {
		panic(err)
	}
	totalBytes := createFileStructure(&index, outDir)
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
			chunkSavor(&index, bar, fileCache, outDir, CHUNK_SIZE, persistChan)
			persistWg.Done()
		}()
	}

	for range 128 {
		downloadWg.Add(1)
		go func() {
			downloadoor(&index, libUrlCreds, persistChan, downloadChan)
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

func downloadIndexFile(snapUrlCreds SnapUrlResponse) string {
	client := http.Client{}
	u, err := url.Parse(snapUrlCreds.Url)
	if err != nil {
		panic(err)
	}
	res, err := client.Do(&http.Request{
		Method: "GET",
		Header: http.Header{
			"Authorization": []string{snapUrlCreds.Hmac},
		},
		URL: u,
	})
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()
	f, err := os.Create("/tmp/moonsnap.index")
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

func getSnapUrlCreds(snapKey string, path string) SnapUrlResponse {
	client := http.Client{}
	u, err := url.Parse(BASE_URL)
	if err != nil {
		panic(err)
	}
	u.Path = path
	values := u.Query()
	values.Set("snap_key", snapKey)
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
			debugF, _ := os.OpenFile(
				fmt.Sprintf(
					"/tmp/moonsnap-%d-%d.failed",
					chunk.FileIndex,
					chunk.FileOffset,
				),
				os.O_APPEND|os.O_WRONLY|os.O_CREATE,
				0644,
			)
			debugF.WriteString(string(chunk.Data))
			debugF.WriteString(
				fmt.Sprintf("\n\nindex=%d, offset=%d, file=%s\n",
					chunk.FileIndex,
					chunk.FileOffset,
					f.Name(),
				),
			)
			debugF.Close()
			panic(err)
		}
		bar.Add(decompressedLength)
		n, err := f.WriteAt(dst[0:decompressedLength], int64(chunk.FileOffset*chunkSize))
		if err != nil || n != decompressedLength {
			panic(err)
		}
	}
}

func downloadoor(index *moonproto.Index, libUrlCreds SnapUrlResponse, chunkChan chan<- Chunk, downloadChan <-chan *moonproto.LibraryChunk) {
	client := http.Client{}
	for libChunk := range downloadChan {
		libraryName := index.Libraries[libChunk.LibraryIndex].Name
		urlPath := libUrlCreds.Url + libraryName
		u, err := url.Parse(urlPath)
		if err != nil {
			panic(err)
		}
		retries := 0
		localStartOffset := libChunk.StartOffset
		localLength := libChunk.Length
		//fmt.Printf("start=%s, startOffset=%d\n", u.Path, libChunk.StartOffset)
	retry:
		res, err := client.Do(&http.Request{
			Method: "GET",
			Header: http.Header{
				"Authorization": []string{
					libUrlCreds.Hmac,
				},
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
			/* else {
				fmt.Printf("DUPE! %d\n", i)
			}*/
			if libChunk.FileIndex[i] < 0 {
				continue
			}
			chunkChan <- Chunk{
				FileIndex:  int(libChunk.FileIndex[i]),
				FileOffset: int(libChunk.FileOffset[i]),
				Data:       chunkBytes,
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
