package dfile_test

import (
	"bytes"
	"os"
	"testing"

	"github.com/kp6/alphorn/dfile"
)

const path = "foo.bar" //TODO: use temp file

func TestDirectFileIO1(t *testing.T) {
	blocks := 4

	// Init data
	data1 := dfile.AllocateAligned(blocks * dfile.BlockSize)
	for i := 0; i < len(data1); i++ {
		data1[i] = byte(i)
	}

	{
		// Write
		f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|dfile.O_DIRECT|dfile.O_DSYNC, 0644)
		if err != nil {
			t.Fatal("Failed to OpenFile for write", err)
		}
		_, err = f.WriteAt(data1, int64(blocks*dfile.BlockSize))
		if err != nil {
			t.Fatal("Failed to write", err)
		}
		err = f.Close()
		if err != nil {
			t.Fatal("Failed to close writer", err)
		}
	}

	data2 := dfile.AllocateAligned(blocks * dfile.BlockSize)
	{
		// Read
		f, err := os.OpenFile(path, os.O_RDONLY|dfile.O_DIRECT|dfile.O_DSYNC, 0)
		if err != nil {
			t.Fatal("Failed to OpenFile for read", err)
		}
		_, err = f.ReadAt(data2, int64(blocks*dfile.BlockSize))
		if err != nil {
			t.Fatal("Failed to read", err)
		}
		err = f.Close()
		if err != nil {
			t.Fatal("Failed to close reader", err)
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
