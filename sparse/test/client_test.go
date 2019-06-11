package test

import (
	"testing"

	. "github.com/longhorn/sparse-tools/sparse"
	"github.com/longhorn/sparse-tools/sparse/rest"
)

const (
	localhost = "127.0.0.1"
	timeout   = 5 //seconds
	port      = "5000"
)

func TestSyncSmallFile1(t *testing.T) {
	localPath := tempFilePath("ssync-small-src-")
	remotePath := tempFilePath("ssync-small-dst-")

	filesCleanup(localPath, remotePath)
	defer filesCleanup(localPath, remotePath)

	data := []byte("json-fault")
	createTestSmallFile(localPath, len(data), data)
	testSyncAnyFile(t, localPath, remotePath)
}

func TestSyncSmallFile2(t *testing.T) {
	localPath := tempFilePath("ssync-small-src-")
	remotePath := tempFilePath("ssync-small-dst-")

	filesCleanup(localPath, remotePath)
	defer filesCleanup(localPath, remotePath)

	data := []byte("json-fault")
	data1 := []byte("json")
	createTestSmallFile(localPath, len(data), data)
	createTestSmallFile(remotePath, len(data1), data1)
	testSyncAnyFile(t, localPath, remotePath)
}

func TestSyncSmallFile3(t *testing.T) {
	localPath := tempFilePath("ssync-small-src-")
	remotePath := tempFilePath("ssync-small-dst-")

	filesCleanup(localPath, remotePath)
	defer filesCleanup(localPath, remotePath)

	data := []byte("json-fault")
	createTestSmallFile(localPath, len(data), data)
	createTestSmallFile(remotePath, len(data), data)
	testSyncAnyFile(t, localPath, remotePath)
}

func TestSyncSmallFile4(t *testing.T) {
	localPath := tempFilePath("ssync-small-src-")
	remotePath := tempFilePath("ssync-small-dst-")

	filesCleanup(localPath, remotePath)
	defer filesCleanup(localPath, remotePath)

	data := []byte("json-fault")
	createTestSmallFile(localPath, 0, make([]byte, 0))
	createTestSmallFile(remotePath, len(data), data)
	testSyncAnyFile(t, localPath, remotePath)
}

func TestSyncAnyFile(t *testing.T) {
	src := "src.bar"
	dst := "dst.bar"
	run := false
	// ad hoc test for testing specific problematic files
	// disabled by default
	if run {
		testSyncAnyFile(t, src, dst)
	}
}

func testSyncAnyFile(t *testing.T, src, dst string) {
	// Sync
	go rest.TestServer(port, dst, timeout)
	err := SyncFile(src, localhost+":"+port, timeout)

	// Verify
	if err != nil {
		t.Fatal("sync error")
	}
	if !filesAreEqual(src, dst) {
		t.Fatal("file content diverged")
	}
}

func TestSyncFile1(t *testing.T) {
	// D H D => D D H
	layoutLocal := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncFile2(t *testing.T) {
	// H D H  => D H H
	layoutLocal := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncFile3(t *testing.T) {
	// D H D => D D
	layoutLocal := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncFile4(t *testing.T) {
	// H D H  => D H
	layoutLocal := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncFile5(t *testing.T) {
	// H D H  => H D
	layoutLocal := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncFile6(t *testing.T) {
	// H D H  => D
	layoutLocal := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncFile7(t *testing.T) {
	// H D H  => H
	layoutLocal := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncFile8(t *testing.T) {
	// D H D =>
	layoutLocal := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	layoutRemote := []FileInterval{}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncFile9(t *testing.T) {
	// H D H  =>
	layoutLocal := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	layoutRemote := []FileInterval{}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff1(t *testing.T) {
	layoutLocal := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 30 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 30 * Blocks, End: 34 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 34 * Blocks, End: 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff2(t *testing.T) {
	layoutLocal := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 30 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 30 * Blocks, End: 34 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 34 * Blocks, End: 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff3(t *testing.T) {
	layoutLocal := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 30 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 30 * Blocks, End: 34 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 34 * Blocks, End: 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff4(t *testing.T) {
	layoutLocal := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 30 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 30 * Blocks, End: 34 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 34 * Blocks, End: 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff5(t *testing.T) {
	layoutLocal := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 30 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 30 * Blocks, End: 34 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 34 * Blocks, End: 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff6(t *testing.T) {
	layoutLocal := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 30 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 30 * Blocks, End: 34 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 34 * Blocks, End: 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff7(t *testing.T) {
	layoutLocal := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 30 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 30 * Blocks, End: 34 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 34 * Blocks, End: 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff8(t *testing.T) {
	layoutLocal := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 30 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 30 * Blocks, End: 34 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 34 * Blocks, End: 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff9(t *testing.T) {
	layoutLocal := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 30 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 30 * Blocks, End: 34 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 34 * Blocks, End: 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff10(t *testing.T) {
	layoutLocal := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 30 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 30 * Blocks, End: 34 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 34 * Blocks, End: 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff11(t *testing.T) {
	layoutLocal := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 30 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 30 * Blocks, End: 34 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 34 * Blocks, End: 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff12(t *testing.T) {
	layoutLocal := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 30 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 30 * Blocks, End: 34 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 34 * Blocks, End: 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff13(t *testing.T) {
	layoutLocal := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 30 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 30 * Blocks, End: 34 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 34 * Blocks, End: 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff14(t *testing.T) {
	layoutLocal := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 30 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 30 * Blocks, End: 34 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 34 * Blocks, End: 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff15(t *testing.T) {
	layoutLocal := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 30 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 30 * Blocks, End: 34 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 34 * Blocks, End: 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff16(t *testing.T) {
	layoutLocal := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 30 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 30 * Blocks, End: 34 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 34 * Blocks, End: 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff17(t *testing.T) {
	layoutLocal := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 28 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 28 * Blocks, End: 32 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 32 * Blocks, End: 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 30 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 30 * Blocks, End: 34 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 34 * Blocks, End: 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff18(t *testing.T) {
	layoutLocal := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 28 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 28 * Blocks, End: 36 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 36 * Blocks, End: 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 30 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 30 * Blocks, End: 34 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 34 * Blocks, End: 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff19(t *testing.T) {
	layoutLocal := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 31 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 31 * Blocks, End: 33 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 33 * Blocks, End: 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 30 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 30 * Blocks, End: 34 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 34 * Blocks, End: 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff20(t *testing.T) {
	layoutLocal := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 32 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 32 * Blocks, End: 36 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 36 * Blocks, End: 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 30 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 30 * Blocks, End: 34 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 34 * Blocks, End: 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff21(t *testing.T) {
	layoutLocal := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 28 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 28 * Blocks, End: 32 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 32 * Blocks, End: 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 30 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 30 * Blocks, End: 34 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 34 * Blocks, End: 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff22(t *testing.T) {
	layoutLocal := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 28 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 28 * Blocks, End: 36 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 36 * Blocks, End: 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 30 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 30 * Blocks, End: 34 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 34 * Blocks, End: 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff23(t *testing.T) {
	layoutLocal := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 31 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 31 * Blocks, End: 33 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 33 * Blocks, End: 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 30 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 30 * Blocks, End: 34 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 34 * Blocks, End: 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff24(t *testing.T) {
	layoutLocal := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 32 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 32 * Blocks, End: 36 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 36 * Blocks, End: 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 30 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 30 * Blocks, End: 34 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 34 * Blocks, End: 100 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncFileHashRetry(t *testing.T) {
	layoutLocal := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
	}
	layoutRemote := []FileInterval{}

	// Simulate file hash mismatch
	SetFailPointFileHashMatch(true)
	testSyncFile(t, layoutLocal, layoutRemote)
}

func testSyncFile(t *testing.T, layoutLocal, layoutRemote []FileInterval) (hashLocal []byte) {
	localPath := tempFilePath("ssync-src-")
	remotePath := tempFilePath("ssync-dst-")

	filesCleanup(localPath, remotePath)
	defer filesCleanup(localPath, remotePath)

	// Create test files
	createTestSparseFile(localPath, layoutLocal)
	if len(layoutRemote) > 0 {
		// only create destination test file if layout is speciifed
		createTestSparseFile(remotePath, layoutRemote)
	}

	// Sync
	go rest.TestServer(port, remotePath, timeout)
	err := SyncFile(localPath, localhost+":"+port, timeout)

	// Verify
	if err != nil {
		t.Fatal("sync error")
	}
	if !filesAreEqual(localPath, remotePath) {
		t.Fatal("file content diverged")
	}
	return
}

// created in current dir for benchmark tests
var localBigPath = "ssync-src-file.bar"
var remoteBigPath = "ssync-dst-file.bar"

func Test_1G_cleanup(*testing.T) {
	// remove temporaries if the benchmarks below are not run
	filesCleanup(localBigPath, remoteBigPath)
}

func Benchmark_1G_InitFiles(b *testing.B) {
	// Setup files
	layoutLocal := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: (256 << 10) * Blocks}},
	}
	layoutRemote := []FileInterval{}

	filesCleanup(localBigPath, remoteBigPath)
	createTestSparseFile(localBigPath, layoutLocal)
	createTestSparseFile(remoteBigPath, layoutRemote)
}

func Benchmark_1G_SendFiles_Whole(b *testing.B) {
	go rest.TestServer(port, remoteBigPath, timeout)
	err := SyncFile(localBigPath, localhost+":"+port, timeout)

	if err != nil {
		b.Fatal("sync error")
	}
}

func Benchmark_1G_SendFiles_Diff(b *testing.B) {

	go rest.TestServer(port, remoteBigPath, timeout)
	err := SyncFile(localBigPath, localhost+":"+port, timeout)

	if err != nil {
		b.Fatal("sync error")
	}
}

func Benchmark_1G_CheckFiles(b *testing.B) {
	if !filesAreEqual(localBigPath, remoteBigPath) {
		b.Error("file content diverged")
		return
	}
	filesCleanup(localBigPath, remoteBigPath)
}
