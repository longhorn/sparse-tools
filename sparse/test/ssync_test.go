package test

import (
	"math"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"

	. "github.com/longhorn/sparse-tools/sparse"
	"github.com/longhorn/sparse-tools/sparse/rest"
)

const srcPrefix = "ssync-src"
const dstPrefix = "ssync-dst"

func TestRandomSync4MB(t *testing.T) {
	const seed = 1
	const size = 4 /*MB*/ << 10
	srcName := tempFilePath(srcPrefix)
	dstName := tempFilePath(dstPrefix)
	RandomSync(t, size, seed, srcName, dstName, true /*create dstFile*/, true /* directIO */)
}

func TestRandomSync4MBNoDirectIO(t *testing.T) {
	const seed = 1
	const size = 4 /*MB*/ << 10
	srcName := tempFilePath(srcPrefix)
	dstName := tempFilePath(dstPrefix)
	RandomSync(t, size, seed, srcName, dstName, true /*create dstFile*/, false /* directIO */)
}

func TestRandomSyncNoDst4MB(t *testing.T) {
	const seed = 2
	const size = 4 /*MB*/ << 10
	srcName := tempFilePath(srcPrefix)
	dstName := tempFilePath(dstPrefix)
	RandomSync(t, size, seed, srcName, dstName, false /*no dstFile*/, true /* directIO */)
}

func TestRandomSyncNoDst4MBNoDirectIO(t *testing.T) {
	const seed = 2
	const size = 4 /*MB*/ << 10
	srcName := tempFilePath(srcPrefix)
	dstName := tempFilePath(dstPrefix)
	RandomSync(t, size, seed, srcName, dstName, false /*no dstFile*/, false /* directIO */)
}

func TestRandomSync100MB(t *testing.T) {
	const seed = 1
	const size = 100 /*MB*/ << 20
	srcName := tempFilePath(srcPrefix)
	dstName := tempFilePath(dstPrefix)
	RandomSync(t, size, seed, srcName, dstName, true /*create dstFile*/, true /* directIO */)
}

func TestRandomSync100MBNoDirectIO(t *testing.T) {
	const seed = 1
	const size = 100 /*MB*/ << 20
	srcName := tempFilePath(srcPrefix)
	dstName := tempFilePath(dstPrefix)
	RandomSync(t, size, seed, srcName, dstName, true /*create dstFile*/, false /* directIO */)
}

func TestRandomSyncNoDst100MB(t *testing.T) {
	const seed = 2
	const size = 100 /*MB*/ << 20
	srcName := tempFilePath(srcPrefix)
	dstName := tempFilePath(dstPrefix)
	RandomSync(t, size, seed, srcName, dstName, false /*no dstFile*/, true /* directIO */)
}

func TestRandomSyncNoDst100MBNoDirectIO(t *testing.T) {
	const seed = 2
	const size = 100 /*MB*/ << 20
	srcName := tempFilePath(srcPrefix)
	dstName := tempFilePath(dstPrefix)
	RandomSync(t, size, seed, srcName, dstName, false /*no dstFile*/, false /* directIO */)
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
		RandomSync(t, size, seed, srcName, dstName, true /*create dstFile*/, true /* directIO */)
	} else {
		log.Info("Using ", sizeGB, "(GB) size for random seed test")
		size = int64(sizeGB) << 30
		srcName := tempBigFilePath(srcPrefix)
		dstName := tempBigFilePath(dstPrefix)
		RandomSync(t, size, seed, srcName, dstName, true /*create dstFile*/, true /* directIO */)
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
		RandomSync(t, size, seed, srcName, dstName, true /*create dstFile*/, false /* directIO */)
	} else {
		log.Info("Using ", sizeGB, "(GB) size for random seed test")
		size = int64(sizeGB) << 30
		srcName := tempBigFilePath(srcPrefix)
		dstName := tempBigFilePath(dstPrefix)
		RandomSync(t, size, seed, srcName, dstName, true /*create dstFile*/, false /* directIO */)
	}
}

func RandomSync(t *testing.T, size, seed int64, srcPath, dstPath string, dstCreate bool, directIO bool) {
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

	log.Infof("Syncing with directIO: %v", directIO)

	go rest.TestServer(port, dstPath, timeout)
	err := SyncFile(srcPath, localhost+":"+port, timeout, directIO)

	if err != nil {
		t.Fatal("sync error")
	}
	log.Info("...syncing done")

	log.Info("Checking...")
	err = checkSparseFiles(srcPath, dstPath)
	if err != nil {
		t.Fatal(err)
	}
}

// genereate random hole and data interval, but just return data interval slcie
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
