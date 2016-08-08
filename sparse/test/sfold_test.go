package test

import (
	"os"
	"testing"

	log "github.com/Sirupsen/logrus"
	. "github.com/rancher/sparse-tools/sparse"
)

func TestFoldFile1(t *testing.T) {
	// D H D => D D H
	layoutFrom := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
		{SparseData, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutTo := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	testFoldFile(t, layoutFrom, layoutTo)
}

func TestFoldFile2(t *testing.T) {
	// H D H  => D H H
	layoutFrom := []FileInterval{
		{SparseHole, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutTo := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	testFoldFile(t, layoutFrom, layoutTo)
}

func TestFoldFile3(t *testing.T) {
	// H H H  => D H H
	layoutFrom := []FileInterval{
		{SparseHole, Interval{0, 1 * Blocks}},
		{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutTo := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	testFoldFile(t, layoutFrom, layoutTo)
}

func TestFoldFile4(t *testing.T) {
	// D D H  => H H H
	layoutFrom := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutTo := []FileInterval{
		{SparseHole, Interval{0, 1 * Blocks}},
		{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	testFoldFile(t, layoutFrom, layoutTo)
}

func TestFoldFile5(t *testing.T) {
	// D D D  => D H D
	layoutFrom := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
		{SparseData, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutTo := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
		{SparseData, Interval{2 * Blocks, 3 * Blocks}},
	}
	testFoldFile(t, layoutFrom, layoutTo)
}

func TestFoldFile6(t *testing.T) {
	// H H D  => D D D
	layoutFrom := []FileInterval{
		{SparseHole, Interval{0, 1 * Blocks}},
		{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
		{SparseData, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutTo := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
		{SparseData, Interval{2 * Blocks, 3 * Blocks}},
	}
	testFoldFile(t, layoutFrom, layoutTo)
}

func foldLayout(from []FileInterval, to []FileInterval, fromPath string, toPath string, expectedPath string) {
	if from[len(from)-1].End != to[len(to)-1].End {
		log.Fatal("foldLayout: non equal length not implemented")
	}

	// create expectedPath file
	expFile, err := os.Create(expectedPath)
	if err != nil {
		log.Fatal(err)
	}
	err = expFile.Truncate(from[len(from)-1].End)
	if err != nil {
		log.Fatal(err)
	}

	copySparseData(to, toPath, expFile)
	copySparseData(from, fromPath, expFile)
}

func copySparseData(fromInterval []FileInterval, fromPath string, toFile *os.File) {
	_, dataIntervals := getDataIntervalsFromFileIntervals(fromInterval)
	fromFile, err := os.Open(fromPath)
	if err != nil {
		log.Fatal(err)
	}

	// copy all file data
	for _, interval := range dataIntervals {
		// read from fromFile and write to toFile
		data := make([]byte, interval.End-interval.Begin)
		_, err := fromFile.ReadAt(data, interval.Begin)
		if err != nil {
			log.Fatal(err)
		}
		_, err = toFile.WriteAt(data, interval.Begin)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func testFoldFile(t *testing.T, layoutFrom, layoutTo []FileInterval) (hashLocal []byte) {
	fromPath := tempFilePath("sfold-src-")
	toPath := tempFilePath("sfold-dst-")
	expectedPath := tempFilePath("sfold-exp-")

	// Only log errors
	log.SetLevel(log.ErrorLevel)

	filesCleanup(fromPath, toPath)
	defer filesCleanup(fromPath, toPath)
	defer fileCleanup(expectedPath)

	// Create test files
	createTestSparseFile(fromPath, layoutFrom)
	createTestSparseFile(toPath, layoutTo)
	foldLayout(layoutFrom, layoutTo, fromPath, toPath, expectedPath)

	// Fold
	err := FoldFile(fromPath, toPath)

	// Verify
	if err != nil {
		t.Fatal("Fold error:", err)
	}

	err = checkSparseFiles(toPath, expectedPath)
	if err != nil {
		t.Fatal("Folded file is different from expected:", err)
	}
	return
}
