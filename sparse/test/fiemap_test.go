package test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	. "github.com/longhorn/sparse-tools/sparse"
	"github.com/longhorn/sparse-tools/sparse/rest"
)

// TestFileSync can be used for benchmarking by default it's skipped
// for the worst case sparse file, use 4k blocks / 4k holes
// for a denser sparse file increase the testDataBlockSize
func TestFileSync(t *testing.T) {
	const (
		localhost = "127.0.0.1"
		timeout   = 10 // seconds
		port      = "5000"

		MB = int64(1024 * 1024)
		GB = MB * 1024

		testFileName = srcPrefix + "-fiemap-1gb-file"
		testFileSize = 1 * GB

		testHoleBlockSize = int64(4096)
		testDataBlockSize = int64(4096)
		// testDataBlockSize = int64(128*MB - testHoleBlockSize)
	)

	t.Skip("skipped fiemap_test::TestFileSyncSparse")
	log.SetLevel(log.DebugLevel)
	srcPath := filepath.Join(os.TempDir(), testFileName)
	if info, err := os.Stat(srcPath); err != nil || info.Size() == 0 {
		// in case of error we just create a new test file
		if err = writeMultipleHolesData(srcPath, testFileSize, testDataBlockSize, testHoleBlockSize); err != nil {
			t.Fatalf("failed to create fiemap test file path: %v error: %v", srcPath, err)
		}
	}

	dstPath := filepath.Join(os.TempDir(), testFileName+"-dst")

	// NOTE: depending on scenario you might want to reuse the test files or clean them up
	// defer fileCleanup(srcPath)
	// defer fileCleanup(dstPath)
	log.Info("Syncing file...")
	startTime := time.Now()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err := rest.TestServer(context.Background(), port, dstPath, timeout)
		assert.True(t, err == nil || err.Error() == "http: Server closed", "Unexpected error: %v", err)
	}()
	time.Sleep(time.Second)
	err := SyncFile(srcPath, localhost+":"+port, timeout, true, false)
	if err != nil {
		t.Fatalf("sync error: %v", err)
	}
	log.Infof("Syncing done, size: %v elapsed: %.2fs", testFileSize, time.Since(startTime).Seconds())
	wg.Wait()

	startTime = time.Now()
	log.Info("Checking...")
	err = checkSparseFiles(srcPath, dstPath)
	if err != nil {
		t.Fatal(err)
	}
	log.Infof("Checking done, size: %v elapsed: %.2fs", testFileSize, time.Since(startTime).Seconds())
}

func writeMultipleHolesData(filePath string, fileSize int64, dataSize int64, holeSize int64) (err error) {
	if fileSize%(dataSize+holeSize) != 0 {
		return fmt.Errorf("fileSize %v needs to be a multiple of dataSize %v + holeSize %v", fileSize, dataSize, holeSize)
	}

	const GB = int64(1024 * 1024 * 1024)
	sizeInGB := fileSize / GB
	log.Infof("Start to create a %vGB file with multiple hole", sizeInGB)
	f, err := NewDirectFileIoProcessor(filePath, os.O_RDWR, 0666, true)
	if err != nil {
		return err
	}
	defer func() {
		if removeErr := os.Remove(filePath); removeErr != nil {
			if err != nil {
				err = errors.Wrapf(err, "failed to remove file %v: %v", filePath, removeErr)
			} else {
				err = removeErr
			}
		}
	}()
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			if err != nil {
				err = errors.Wrapf(err, "failed to close file %v: %v", filePath, closeErr)
			} else {
				err = closeErr
			}
		}
	}()
	defer func() {
		if syncErr := f.Sync(); syncErr != nil {
			if err != nil {
				err = errors.Wrapf(err, "failed to sync file %v: %v", filePath, syncErr)
			} else {
				err = syncErr
			}
		}
	}()
	if err := f.Truncate(fileSize); err != nil {
		return err
	}

	startTime := time.Now()
	deltaTime := time.Now()

	// random is pretty slow, if called in the loop below for each character
	// better to call it once per block, so we get a full block of a single random character
	for offset := int64(0); offset < fileSize; {
		blockData := RandomBlock(dataSize)
		if nw, err := f.WriteAt(blockData, offset); err != nil {
			return errors.Wrapf(err, "write at %v, number of write %v", offset, nw)
		}
		offset += dataSize
		if err := NewFiemapFile(f.GetFile()).PunchHole(offset, holeSize); err != nil {
			return errors.Wrapf(err, "punch hole at %v", offset)
		}
		offset += holeSize

		if offset%GB == 0 {
			writtenGB := offset / GB
			log.Infof("Wrote %vGB of %vGB time delta: %.2f time elapsed: %.2f",
				writtenGB, sizeInGB,
				time.Since(deltaTime).Seconds(),
				time.Since(startTime).Seconds())
			deltaTime = time.Now()
		}
	}

	log.Infof("Done creating a %vGB file with multiple hole, time elapsed: %.2f", sizeInGB, time.Since(startTime).Seconds())
	return nil
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandomBlock(n int64) []byte {
	char := letterBytes[rand.Intn(len(letterBytes))]
	b := make([]byte, n)
	for i := range b {
		b[i] = char
	}
	return b
}
