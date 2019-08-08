package test

import (
	"os"
	"testing"

	log "github.com/sirupsen/logrus"

	. "github.com/longhorn/sparse-tools/sparse"
)

type FoldFileTest struct {
	doneChan    chan error
	progress    int
	progressErr bool
}

func (f *FoldFileTest) UpdateFoldFileProgress(progress int, done bool, err error) {
	if progress < f.progress {
		f.progressErr = true
	}
	f.progress = progress
	if done {
		f.doneChan <- err
	}
}

func TestFoldFile1(t *testing.T) {
	// D H D => D D H
	layoutFrom := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	layoutTo := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	testFoldFile(t, layoutFrom, layoutTo)
}

func TestFoldFile2(t *testing.T) {
	// H D H  => D H H
	layoutFrom := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	layoutTo := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	testFoldFile(t, layoutFrom, layoutTo)
}

func TestFoldFile3(t *testing.T) {
	// H H H  => D H H
	layoutFrom := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	layoutTo := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	testFoldFile(t, layoutFrom, layoutTo)
}

func TestFoldFile4(t *testing.T) {
	// D D H  => H H H
	layoutFrom := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	layoutTo := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	testFoldFile(t, layoutFrom, layoutTo)
}

func TestFoldFile5(t *testing.T) {
	// D D D  => D H D
	layoutFrom := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	layoutTo := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	testFoldFile(t, layoutFrom, layoutTo)
}

func TestFoldFile6(t *testing.T) {
	// H H D  => D D D
	layoutFrom := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	layoutTo := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
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
	ops := &FoldFileTest{
		doneChan: make(chan error),
	}
	err := FoldFile(fromPath, toPath, ops)
	if err != nil {
		t.Fatal("Start fold error:", err)
	}

	if err := <-ops.doneChan; err != nil {
		t.Fatal("Fold error:", err)
	}

	if ops.progress != 100 {
		t.Fatal("Completed fold does not have progress of 100")
	}

	if ops.progressErr {
		t.Fatal("Progress went backwards during fold")
	}

	err = checkSparseFiles(toPath, expectedPath)
	if err != nil {
		t.Fatal("Folded file is different from expected:", err)
	}
	return
}
