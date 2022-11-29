package test

import (
	"context"
	"math"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"

	. "github.com/longhorn/sparse-tools/sparse"
	"github.com/longhorn/sparse-tools/sparse/rest"
)

const srcPrefix = "ssync-src"
const dstPrefix = "ssync-dst"

func TestRandomSyncSingleBlock(t *testing.T) {
	const seed = 1
	const size = 4 /*KB*/ << 10
	srcName := tempFilePath(srcPrefix)
	dstName := tempFilePath(dstPrefix)
	RandomSync(t, size, seed, srcName, dstName, true /*create dstFile*/, true /* directIO */, false /* fastSync */)
}

func TestRandomSyncSingleBlockNoDirectIO(t *testing.T) {
	const seed = 1
	const size = 4 /*KB*/ << 10
	srcName := tempFilePath(srcPrefix)
	dstName := tempFilePath(dstPrefix)
	RandomSync(t, size, seed, srcName, dstName, true /*create dstFile*/, false /* directIO */, false /* fastSync */)
}

func TestRandomSyncSingleBlockNoDst(t *testing.T) {
	const seed = 2
	const size = 4 /*KB*/ << 10
	srcName := tempFilePath(srcPrefix)
	dstName := tempFilePath(dstPrefix)
	RandomSync(t, size, seed, srcName, dstName, false /*no dstFile*/, true /* directIO */, false /* fastSync */)
}

func TestRandomSyncSingleBlockNoDstNoDirectIO(t *testing.T) {
	const seed = 2
	const size = 4 /*KB*/ << 10
	srcName := tempFilePath(srcPrefix)
	dstName := tempFilePath(dstPrefix)
	RandomSync(t, size, seed, srcName, dstName, false /*no dstFile*/, false /* directIO */, false /* fastSync */)
}

func TestRandomSync4MB(t *testing.T) {
	const seed = 1
	const size = 4 /*MB*/ << 20
	srcName := tempFilePath(srcPrefix)
	dstName := tempFilePath(dstPrefix)
	RandomSync(t, size, seed, srcName, dstName, true /*create dstFile*/, true /* directIO */, false /* fastSync */)
}

func TestRandomSync4MBNoDirectIO(t *testing.T) {
	const seed = 1
	const size = 4 /*MB*/ << 20
	srcName := tempFilePath(srcPrefix)
	dstName := tempFilePath(dstPrefix)
	RandomSync(t, size, seed, srcName, dstName, true /*create dstFile*/, false /* directIO */, false /* fastSync */)
}

func TestRandomSyncNoDst4MB(t *testing.T) {
	const seed = 2
	const size = 4 /*MB*/ << 20
	srcName := tempFilePath(srcPrefix)
	dstName := tempFilePath(dstPrefix)
	RandomSync(t, size, seed, srcName, dstName, false /*no dstFile*/, true /* directIO */, false /* fastSync */)
}

func TestRandomSyncNoDst4MBNoDirectIO(t *testing.T) {
	const seed = 2
	const size = 4 /*MB*/ << 20
	srcName := tempFilePath(srcPrefix)
	dstName := tempFilePath(dstPrefix)
	RandomSync(t, size, seed, srcName, dstName, false /*no dstFile*/, false /* directIO */, false /* fastSync */)
}

func TestRandomSync100MB(t *testing.T) {
	const seed = 1
	const size = 100 /*MB*/ << 20
	srcName := tempFilePath(srcPrefix)
	dstName := tempFilePath(dstPrefix)
	RandomSync(t, size, seed, srcName, dstName, true /*create dstFile*/, true /* directIO */, false /* fastSync */)
}

func TestRandomSync100MBNoDirectIO(t *testing.T) {
	const seed = 1
	const size = 100 /*MB*/ << 20
	srcName := tempFilePath(srcPrefix)
	dstName := tempFilePath(dstPrefix)
	RandomSync(t, size, seed, srcName, dstName, true /*create dstFile*/, false /* directIO */, false /* fastSync */)
}

func TestRandomSyncNoDst100MB(t *testing.T) {
	const seed = 2
	const size = 100 /*MB*/ << 20
	srcName := tempFilePath(srcPrefix)
	dstName := tempFilePath(dstPrefix)
	RandomSync(t, size, seed, srcName, dstName, false /*no dstFile*/, true /* directIO */, false /* fastSync */)
}

func TestRandomSyncNoDst100MBNoDirectIO(t *testing.T) {
	const seed = 2
	const size = 100 /*MB*/ << 20
	srcName := tempFilePath(srcPrefix)
	dstName := tempFilePath(dstPrefix)
	RandomSync(t, size, seed, srcName, dstName, false /*no dstFile*/, false /* directIO */, false /* fastSync */)
}

func TestRandomSyncCustomGB(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped custom random sync")
	}

	// random seed
	seed := time.Now().UnixNano()
	log.Info("seed=", seed)

	// default size
	var size = int64(100) /*MB*/ << 20
	arg := os.Args[len(os.Args)-1]
	sizeGB, err := strconv.Atoi(arg)
	if err != nil {
		log.Info("")
		log.Info("Using default 100MB size for random seed test")
		log.Info("For alternative size in GB and in current dir(vs tmp) use -timeout 10m -args <GB>")
		log.Info("Increase the optional -timeout value for 20GB and larger sizes")
		log.Info("")
		srcName := tempFilePath(srcPrefix)
		dstName := tempFilePath(dstPrefix)
		RandomSync(t, size, seed, srcName, dstName, true /*create dstFile*/, true /* directIO */, false /* fastSync */)
	} else {
		log.Info("Using ", sizeGB, "(GB) size for random seed test")
		size = int64(sizeGB) << 30
		srcName := tempBigFilePath(srcPrefix)
		dstName := tempBigFilePath(dstPrefix)
		RandomSync(t, size, seed, srcName, dstName, true /*create dstFile*/, true /* directIO */, false /* fastSync */)
	}
}

func TestRandomSyncCustomGBNoDirectIO(t *testing.T) {
	if testing.Short() {
		t.Skip("skipped custom random sync")
	}

	// random seed
	seed := time.Now().UnixNano()
	log.Info("seed=", seed)

	// default size
	var size = int64(100) /*MB*/ << 20
	arg := os.Args[len(os.Args)-1]
	sizeGB, err := strconv.Atoi(arg)
	if err != nil {
		log.Info("")
		log.Info("Using default 100MB size for random seed test")
		log.Info("For alternative size in GB and in current dir(vs tmp) use -timeout 10m -args <GB>")
		log.Info("Increase the optional -timeout value for 20GB and larger sizes")
		log.Info("")
		srcName := tempFilePath(srcPrefix)
		dstName := tempFilePath(dstPrefix)
		RandomSync(t, size, seed, srcName, dstName, true /*create dstFile*/, false /* directIO */, false /* fastSync */)
	} else {
		log.Info("Using ", sizeGB, "(GB) size for random seed test")
		size = int64(sizeGB) << 30
		srcName := tempBigFilePath(srcPrefix)
		dstName := tempBigFilePath(dstPrefix)
		RandomSync(t, size, seed, srcName, dstName, true /*create dstFile*/, false /* directIO */, false /* fastSync */)
	}
}

func RandomSync(t *testing.T, size, seed int64, srcPath, dstPath string, dstCreate bool, directIO, fastSync bool) {
	const (
		localhost = "127.0.0.1"
		timeout   = 10 //seconds
		port      = "5000"
	)

	defer filesCleanup(srcPath, dstPath)

	srcLayout := generateRandomDataLayout(srcPrefix, size, seed)
	dstLayout := generateRandomDataLayout(dstPrefix, size, seed+1)

	doCreateSparseFile(srcPath, size, srcLayout)
	if dstCreate {
		// Create destination with some data
		doCreateSparseFile(dstPath, size, dstLayout)
	}

	log.Infof("Syncing with directIO: %v size: %v", directIO, size)
	go rest.TestServer(context.Background(), port, dstPath, timeout)
	startTime := time.Now()
	err := SyncFile(srcPath, localhost+":"+port, timeout, directIO, fastSync)
	if err != nil {
		t.Fatal("sync error")
	}
	log.Infof("Syncing done, size: %v elapsed: %.2fs", size, time.Now().Sub(startTime).Seconds())

	startTime = time.Now()
	err = checkSparseFiles(srcPath, dstPath)
	if err != nil {
		t.Fatal(err)
	}
	log.Infof("Checking done, size: %v elapsed: %.2fs", size, time.Now().Sub(startTime).Seconds())
}

// generate random hole and data interval, but just return data interval slcie
func generateRandomDataLayout(prefix string, size, seed int64) []Interval {
	const maxInterval = 256 // number of blocks
	var layout []Interval
	r := rand.New(rand.NewSource(seed))
	offset := int64(0)
	for offset < size {
		blocks := int64(r.Intn(maxInterval)) + 1 // 1..maxInterval
		length := blocks * BlockSize
		if offset+length > size {
			// don't overshoot size
			length = size - offset
		}

		interval := Interval{Begin: offset, End: offset + length}
		offset += interval.Len()

		// 50% chance we have a data
		if r.Intn(2) == 0 {
			//mask = 0xAA * byte(r.Intn(10)/9) // 10%
			layout = append(layout, interval)
		}
	}

	return layout
}

func makeIntervalData(size int64, rand *rand.Rand) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(rand.Intn(math.MaxUint8 + 1))
	}
	return data
}

func TestSyncCancellation(t *testing.T) {
	const (
		localhost     = "127.0.0.1"
		timeout       = 10
		port          = "5000"
		retryCount    = 5
		retryInterval = 1 * time.Second
	)

	dstName := tempFilePath(dstPrefix)

	ctx, cancelFunc := context.WithCancel(context.Background())
	go rest.TestServer(ctx, port, dstName, timeout)

	client := http.Client{}

	// First few requests may fail since the test server has not started serving.
	// As long as the final request succeeds, we will consider the server as running.
	var httpErr error
	for i := 0; i < retryCount; i++ {
		req1, err := http.NewRequest("GET", "http://"+localhost+":"+port, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp1, err := client.Do(req1)
		httpErr = err
		if httpErr == nil {
			resp1.Body.Close()
			break
		}
		time.Sleep(retryInterval)
	}
	if httpErr != nil {
		t.Fatal(httpErr)
	}

	cancelFunc()

	for i := 0; i < retryCount; i++ {
		req2, err := http.NewRequest("GET", "http://"+localhost+":"+port, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp2, err := client.Do(req2)
		httpErr = err
		if httpErr == nil {
			resp2.Body.Close()
		}
		if httpErr != nil && strings.Contains(httpErr.Error(), "connection refused") {
			break
		}
		time.Sleep(retryInterval)
	}
	if httpErr == nil || !strings.Contains(httpErr.Error(), "connection refused") {
		t.Fatalf("Unexpected error: %v", httpErr)
	}
}
