package test

import (
	"io/ioutil"
	"math/rand"
	"os"
	"os/exec"
	"time"

	"fmt"

	log "github.com/sirupsen/logrus"

	. "github.com/longhorn/sparse-tools/sparse"
)

const batch = int64(32) // number of blocks for single read/write

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

func createTestSparseFile(name string, layout []FileInterval) {
	// convert FileInterval with hole and data segments into pure data segments
	// at the same time merge adjacent data intervals
	fileSize, dataIntervals := getDataIntervalsFromFileIntervals(layout)

	// create sparse files with data segments
	doCreateSparseFile(name, fileSize, dataIntervals)
}

func getDataIntervalsFromFileIntervals(layout []FileInterval) (int64, []Interval) {
	var dataIntervals []Interval
	var previousData *Interval
	previousType := SparseHole
	fileSizeInBlocks := int64(0)
	for _, fileInterval := range layout {
		if SparseData == fileInterval.Kind {
			if previousType == SparseData {
				// merge the data
				previousData.End = fileInterval.End
			}
			previousType = fileInterval.Kind
			dataIntervals = append(dataIntervals, fileInterval.Interval)
			previousData = &(dataIntervals[len(dataIntervals)-1])
		} else {
			previousType = SparseHole
		}
		fileSizeInBlocks += (fileInterval.End - fileInterval.Begin)
	}

	return fileSizeInBlocks, dataIntervals
}

func doCreateSparseFile(name string, fileSize int64, layout []Interval) {
	// Fill up file with layout data
	f, err := os.Create(name)
	if err != nil {
		log.Fatal(err)
	}
	err = f.Truncate(fileSize)
	if err != nil {
		log.Fatal(err)
	}

	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	for _, interval := range layout {
		size := batch * BlockSize
		for offset := interval.Begin; offset < interval.End; {
			if offset+size > interval.End {
				size = interval.End - offset
			}
			data := makeIntervalData(size, rand)
			_, err = f.WriteAt(data, offset)
			if err != nil {
				log.Fatal(err)
			}
			offset += size
		}
	}
	f.Sync()
	f.Close()
}

func mergeExts(exts []Extent) []Interval {
	if len(exts) == 0 {
		return nil
	}
	var merged []Interval
	previous := Interval{Begin: int64(exts[0].Logical), End: int64(exts[0].Logical + exts[0].Length)}
	for i := 1; i < len(exts); i++ {
		if int64(exts[i].Logical) > previous.End {
			// current extent is not continuous of previous, so append previous
			merged = append(merged, previous)
			previous = Interval{Begin: int64(exts[i].Logical), End: int64(exts[i].Logical + exts[i].Length)}
		} else {
			previous.End += int64(exts[i].Length)
		}
	}
	// append the last separated interval
	merged = append(merged, previous)

	return merged
}

func checkSparseFiles(srcPath string, dstPath string) error {
	srcFileIo, err := NewDirectFileIoProcessor(srcPath, os.O_RDWR, 0666, true)
	if err != nil {
		log.Fatal(err)
	}
	defer srcFileIo.Close()

	dstFileIo, err := NewDirectFileIoProcessor(dstPath, os.O_RDWR, 0666, true)
	if err != nil {
		log.Fatal(err)
	}
	defer dstFileIo.Close()

	// ensure contents are equal
	equal := filesAreEqual(srcPath, dstPath)
	if !equal {
		return fmt.Errorf("files(src: %s, dst: %s) contents are different", srcPath, dstPath)
	}

	// ensure the layout are equal
	srcExts, err := GetFiemapExtents(srcFileIo)
	if err != nil {
		log.Fatalf("GetFiemapExtents of file: %s failed", srcPath)
	}
	dstExts, err := GetFiemapExtents(dstFileIo)
	if err != nil {
		log.Fatalf("GetFiemapExtents of file: %s failed", dstPath)
	}

	// physical exts need to be merged
	srcIntervals := mergeExts(srcExts)
	dstIntervals := mergeExts(dstExts)

	if len(srcIntervals) != len(dstIntervals) {
		return fmt.Errorf("files(src: %s, dst: %s) Intervals lengths(src: %d, dst: %d) are different", srcPath, dstPath, len(srcIntervals), len(dstIntervals))
	}
	for i, srcInterval := range srcIntervals {
		if srcInterval.Begin != dstIntervals[i].Begin ||
			srcInterval.End != dstIntervals[i].End {
			return fmt.Errorf("files(src: %s, dst: %s) Interval[%d] are different", srcPath, dstPath, i)
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
