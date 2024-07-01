package test

import (
	"bytes"
	"fmt"
	"os"

	"testing"

	"github.com/longhorn/sparse-tools/util"

	. "github.com/longhorn/sparse-tools/sparse"
)

func TestSyncLocalFile(t *testing.T) {
	tests := map[string][]FileInterval{
		"SyncLocalFile(...): data-hole-data": {
			{Kind: SparseData, Interval: Interval{Begin: 0, End: 1 * Blocks}},
			{Kind: SparseHole, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
			{Kind: SparseData, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
		},
		"SyncLocalFile(...): hole-data-hole": {
			{Kind: SparseHole, Interval: Interval{Begin: 0, End: 1 * Blocks}},
			{Kind: SparseData, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
			{Kind: SparseHole, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
		},
		"SyncLocalFile(...): hole-data": {
			{Kind: SparseHole, Interval: Interval{Begin: 0, End: 1 * Blocks}},
			{Kind: SparseData, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		},
		"SyncLocalFile(...): data-hole": {
			{Kind: SparseData, Interval: Interval{Begin: 0, End: 1 * Blocks}},
			{Kind: SparseHole, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		},
	}
	for name, layoutFrom := range tests {
		t.Run(name, func(t *testing.T) {
			t.Logf("Testing %s", name)
			testSyncLocalFile(t, layoutFrom)
		})
	}
}

func testSyncLocalFile(t *testing.T, layoutFrom []FileInterval) {
	randomID := util.RandomID(8)
	sourceFilePath := tempFilePath(fmt.Sprintf("sync-local-src-%s", randomID))
	targetFilePath := tempFilePath(fmt.Sprintf("sync-local-dst-%s", randomID))

	createTestSparseFile(sourceFilePath, layoutFrom)

	err := SyncLocalFile(sourceFilePath, targetFilePath)
	if err != nil {
		t.Fatal("Failed to sync local file", err)
	}

	sourceData, err := os.ReadFile(sourceFilePath)
	if err != nil {
		t.Fatal("Failed to read source file", err)
	}

	targetData, err := os.ReadFile(targetFilePath)
	if err != nil {
		t.Fatal("Failed to read target file", err)
	}

	if !bytes.Equal(sourceData, targetData) {
		t.Fatal("Source and target files are different")
	}
}
