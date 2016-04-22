package dfile_test

import (
	"bytes"
	"os"
	"testing"

	"log"
	"time"

	"github.com/kp6/alphorn/dfile"
)

const path = "foo.bar" //TODO: use temp file

func fillData(data []byte, start int) {
	for i := 0; i < len(data); i++ {
		data[i] = byte(start + i)
	}
}

func TestDirectFileIO1(t *testing.T) {
	blocks := 4

	// Init data
	data1 := dfile.AllocateAligned(blocks * dfile.BlockSize)
	fillData(data1, 0)

	{
		// Write
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|dfile.O_DIRECT|dfile.O_DSYNC, 0644)
		if err != nil {
			t.Fatal("Failed to OpenFile for write", err)
		}
		defer f.Close()

		_, err = f.WriteAt(data1, int64(blocks*dfile.BlockSize))
		if err != nil {
			t.Fatal("Failed to write", err)
		}
	}

	data2 := dfile.AllocateAligned(blocks * dfile.BlockSize)
	{
		// Read
		f, err := os.OpenFile(path, os.O_RDONLY|dfile.O_DIRECT|dfile.O_DSYNC, 0)
		if err != nil {
			t.Fatal("Failed to OpenFile for read", err)
		}
		defer f.Close()

		_, err = f.ReadAt(data2, int64(blocks*dfile.BlockSize))
		if err != nil {
			t.Fatal("Failed to read", err)
		}
	}

	// Check
	if !bytes.Equal(data1, data2) {
		t.Fatal("Read not the same as written")
	}
}

func TestDirectFileIO2(t *testing.T) {
	// Cleanup
	err := os.Remove(path)
	if err != nil {
		t.Fatal("Failed to remove file", path, err)
	}
}

const fileSize = int64(1) /*GB*/ << 30

// const FileMode = os.O_WRONLY|dfile.O_DIRECT|dfile.O_DSYNC
const FileMode = os.O_WRONLY | dfile.O_DIRECT

func write(b *testing.B, done chan<- bool, batchSize int, offset, size int64) {
	data := dfile.AllocateAligned(batchSize)
	fillData(data, 0)

	f, err := os.OpenFile(path, FileMode, 0)
	if err != nil {
		b.Fatal("Failed to OpenFile for write", err)
	}
	defer f.Close()

	for pos := offset; pos < offset+size; pos += int64(batchSize) {
		_, err = f.WriteAt(data, pos)
		if err != nil {
			b.Fatal("Failed to write", err)
		}
	}
	done <- true
}

func writeTest(b *testing.B, writers, batch int) {
	done := make(chan bool, writers)
	chunkSize := fileSize / int64(writers)

	start := time.Now().Unix()
	ioSize := batch * dfile.BlockSize
	for i := 0; i < writers; i++ {
		go write(b, done, ioSize, int64(i)*chunkSize, chunkSize)
	}
	for i := 0; i < writers; i++ {
		<-done
	}
	stop := time.Now().Unix()
	log.Println("writers=", writers, "batch=", batch, "(blocks)", "thruput=", fileSize/(1<<20)/(stop-start), "(MB/s)")
}

func BenchmarkWrite8(b *testing.B) {

	f, err := os.OpenFile(path, os.O_CREATE|FileMode, 0644)
	if err != nil {
		b.Fatal("Failed to OpenFile for write", err)
	}
	f.Truncate(fileSize)
	defer f.Close()

	for batch := 32; batch >= 1; batch >>= 1 {
		log.Println("")
		for writers := 1; writers <= 8; writers <<= 1 {
			writeTest(b, writers, batch)
		}
	}
}
