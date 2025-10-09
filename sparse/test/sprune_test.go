package test

import (
	"os"
	"sync"
	"testing"

	log "github.com/sirupsen/logrus"

	. "github.com/longhorn/sparse-tools/sparse"
)

type PruneFileTest struct {
	progress    int
	progressErr bool
	sync.Mutex
}

func (f *PruneFileTest) UpdateFileHandlingProgress(progress int, done bool, err error) {
	f.Lock()
	defer f.Unlock()
	if progress < f.progress {
		f.progressErr = true
	}
	f.progress = progress
}

func TestPruneFileCase1(t *testing.T) {
	// D H D - D D H => H H D - D D H
	layoutParent := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	layoutChild := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	testPruneFile(t, layoutParent, layoutChild)
}

func TestPruneFileCase2(t *testing.T) {
	// H D H - H H H => H D H - H H H
	layoutParent := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	layoutChild := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	testPruneFile(t, layoutParent, layoutChild)
}

func TestPruneFileCase3(t *testing.T) {
	// D H D - H H H => D H D - H H H
	layoutParent := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	layoutChild := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	testPruneFile(t, layoutParent, layoutChild)
}

func TestPruneFileCase4(t *testing.T) {
	// D D D - D D D => H H H - D D D
	layoutParent := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	layoutChild := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	testPruneFile(t, layoutParent, layoutChild)
}

func TestPruneNonOverlappingFile100MB(t *testing.T) {
	const FileSize = 100 * MB
	layoutParent, layoutChild := createNonOverlappingTestIntervals(FileSize)
	testPruneFile(t, layoutParent, layoutChild)
}

func TestPruneOverlappingFile100MB(t *testing.T) {
	const FileSize = 100 * MB
	layoutParent, layoutChild := createOverlappingTestIntervals(FileSize)
	testPruneFile(t, layoutParent, layoutChild)
}

func testPruneFile(t *testing.T, layoutParent, layoutChild []FileInterval) (hashLocal []byte) {
	parentPath := tempFilePath("sprune-parent-")
	childPath := tempFilePath("sprune-child-")
	expectedPath := tempFilePath("sprune-exp-")

	// Only log errors
	log.SetLevel(log.ErrorLevel)

	filesCleanup(parentPath, childPath)
	defer filesCleanup(parentPath, childPath)
	defer fileCleanup(expectedPath)

	// Create test files
	createTestSparseFile(parentPath, layoutParent)
	createTestSparseFile(childPath, layoutChild)
	generatePrunedFile(layoutParent, layoutChild, parentPath, childPath, expectedPath)

	// Prune
	ops := &PruneFileTest{}
	err := PruneFile(parentPath, childPath, ops)
	if err != nil {
		t.Fatal("Prune error:", err)
	}

	if ops.progress != 100 {
		t.Fatal("Completed prune does not have progress of 100")
	}

	if ops.progressErr {
		t.Fatal("Progress went backwards during pruning")
	}

	err = checkSparseFiles(parentPath, expectedPath)
	if err != nil {
		t.Fatal("Prune parent file is different from expected:", err)
	}
	return
}

func generatePrunedFile(parent []FileInterval, child []FileInterval, parentPath string, childPath string, expectedPath string) {
	if len(parent) != len(child) || parent[len(parent)-1].End != child[len(child)-1].End {
		log.Fatal("generatePrunedFile: non equal length not implemented")
	}

	parentFile, err := os.OpenFile(parentPath, os.O_RDWR, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = parentFile.Close()
	}()

	childFile, err := os.OpenFile(childPath, os.O_RDONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = childFile.Close()
	}()

	// create expectedPath file
	expectedFile, err := os.Create(expectedPath)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = expectedFile.Close()
	}()

	if err = expectedFile.Truncate(parent[len(parent)-1].End); err != nil {
		log.Fatal(err)
	}

	var interval FileInterval
	for i := 0; i < len(parent); i++ {
		if child[i].Kind != SparseHole || parent[i].Kind != SparseData {
			continue
		}
		interval = parent[i]
		buffer := make([]byte, interval.End-interval.Begin)
		if _, err := parentFile.ReadAt(buffer, interval.Begin); err != nil {
			log.Fatal(err)
		}
		if _, err = expectedFile.WriteAt(buffer, interval.Begin); err != nil {
			log.Fatal(err)
		}
	}
}
