package test

import (
	"io/ioutil"
	"os"
	"os/exec"

	"bytes"
	"errors"
	"fmt"

	log "github.com/Sirupsen/logrus"
	. "github.com/rancher/sparse-tools/sparse"
)

func filesAreEqual(aPath, bPath string) bool {
	cmd := exec.Command("diff", aPath, bPath)
	err := cmd.Run()
	return nil == err
}

func filesCleanup(src, dst string) {
	fileCleanup(src)
	fileCleanup(dst)
}

func fileCleanup(path string) {
	os.Remove(path)
}

func tempFilePath(prefix string) string {
	// Make a temporary file path
	f, err := ioutil.TempFile("", "sparse-"+prefix)
	if err != nil {
		log.Fatal("Failed to make temp file", err)
	}
	defer f.Close()
	return f.Name()
}

// tempBigFileName is for files that are substantial in isze (for benchmark and stress tests)
// created in current directory
func tempBigFilePath(prefix string) string {
	// Make a temporary file path in current dir
	f, err := ioutil.TempFile(".", "sparse-"+prefix)
	if err != nil {
		log.Fatal("Failed to make temp file", err)
	}
	defer f.Close()
	return f.Name()
}

const batch = int64(32) // Blocks for single read/write

func createTestSparseFile(name string, layout []FileInterval) {
	f, err := os.Create(name)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	if 0 == len(layout) {
		return // empty file
	}

	// Fill up data
	for _, interval := range layout {
		if SparseData == interval.Kind {
			size := batch * Blocks
			for offset := interval.Begin; offset < interval.End; {
				if offset+size > interval.End {
					size = interval.End - offset
				}
				data := MakeData(FileInterval{SparseData, Interval{offset, offset + size}})
				f.WriteAt(data, offset)
				offset += size
			}
		}
	}

	// Resize the file to the last hole
	last := len(layout) - 1
	if SparseHole == layout[last].Kind {
		if err := f.Truncate(layout[last].End); err != nil {
			log.Fatal(err)
		}
	}

	f.Sync()
}

func checkTestSparseFile(name string, layout []FileInterval) error {
	f, err := os.Open(name)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	if 0 == len(layout) {
		return nil // empty file
	}

	// Read and check data
	for _, interval := range layout {
		if SparseData == interval.Kind {
			size := batch * Blocks
			for offset := interval.Begin; offset < interval.End; {
				if offset+size > interval.End {
					size = interval.End - offset
				}
				dataModel := MakeData(FileInterval{SparseData, Interval{offset, offset + size}})
				data := make([]byte, size)
				f.ReadAt(data, offset)
				offset += size

				if !bytes.Equal(data, dataModel) {
					return errors.New(fmt.Sprint("data equality check failure at", interval))
				}
			}
		} else if SparseHole == interval.Kind {
			layoutActual, err := RetrieveLayout(f, interval.Interval)
			if err != nil {
				return errors.New(fmt.Sprint("hole retrieval failure at", interval, err))
			}
			if len(layoutActual) != 1 {
				return errors.New(fmt.Sprint("hole check failure at", interval))
			}
			// actual hole must cover interval span
			if layoutActual[0].Kind != interval.Kind ||
				layoutActual[0].Begin > interval.Begin ||
				layoutActual[0].End < interval.End {
				return errors.New(fmt.Sprint("hole equality check failure at", interval))
			}
		}
	}
	return nil // success
}

func createTestSmallFile(name string, size int, pattern []byte) {
	f, err := os.Create(name)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	if size == 0 {
		return // empty file
	}

	// Fill up data
	for offset := 0; offset < size; offset += len(pattern) {
		if offset+len(pattern) > size {
			pattern = pattern[:size-len(pattern)]
		}
		_, err := f.Write(pattern)
		if err != nil {
			log.Fatal("File write error:", err)
		}
	}

	f.Sync()
}
