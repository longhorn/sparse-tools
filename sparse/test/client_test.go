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
	testSyncAnyFile(t, localPath, remotePath, false /* directIO */)
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
	testSyncAnyFile(t, localPath, remotePath, false /* directIO */)
}

func TestSyncSmallFile3(t *testing.T) {
	localPath := tempFilePath("ssync-small-src-")
	remotePath := tempFilePath("ssync-small-dst-")

	filesCleanup(localPath, remotePath)
	defer filesCleanup(localPath, remotePath)

	data := []byte("json-fault")
	createTestSmallFile(localPath, len(data), data)
	createTestSmallFile(remotePath, len(data), data)
	testSyncAnyFile(t, localPath, remotePath, false /* directIO */)
}

func TestSyncSmallFile4(t *testing.T) {
	localPath := tempFilePath("ssync-small-src-")
	remotePath := tempFilePath("ssync-small-dst-")

	filesCleanup(localPath, remotePath)
	defer filesCleanup(localPath, remotePath)

	data := []byte("json-fault")
	createTestSmallFile(localPath, 0, make([]byte, 0))
	createTestSmallFile(remotePath, len(data), data)
	testSyncAnyFile(t, localPath, remotePath, false /* directIO */)
}

func TestSyncSmallFile256Byte(t *testing.T) {
	localPath := tempFilePath("ssync-small-src-")
	remotePath := tempFilePath("ssync-small-dst-")

	filesCleanup(localPath, remotePath)
	defer filesCleanup(localPath, remotePath)

	data := make([]byte, 256)
	data1 := make([]byte, 256)
	createTestSmallFile(localPath, len(data), data)
	createTestSmallFile(remotePath, len(data1), data1)
	testSyncAnyFileExpectFailure(t, localPath, remotePath, true /* directIO */)
}

func TestSyncSmallFile256ByteNoDirectIO(t *testing.T) {
	localPath := tempFilePath("ssync-small-src-")
	remotePath := tempFilePath("ssync-small-dst-")

	filesCleanup(localPath, remotePath)
	defer filesCleanup(localPath, remotePath)

	data := make([]byte, 256)
	data1 := make([]byte, 256)
	createTestSmallFile(localPath, len(data), data)
	createTestSmallFile(remotePath, len(data1), data1)
	testSyncAnyFile(t, localPath, remotePath, false /* directIO */)
}

func TestSyncSmallFile512Byte(t *testing.T) {
	localPath := tempFilePath("ssync-small-src-")
	remotePath := tempFilePath("ssync-small-dst-")

	filesCleanup(localPath, remotePath)
	defer filesCleanup(localPath, remotePath)

	data := make([]byte, 512)
	data1 := make([]byte, 512)
	createTestSmallFile(localPath, len(data), data)
	createTestSmallFile(remotePath, len(data1), data1)
	testSyncAnyFileExpectFailure(t, localPath, remotePath, true /* directIO */)
}

func TestSyncSmallFile512ByteNoDirectIO(t *testing.T) {
	localPath := tempFilePath("ssync-small-src-")
	remotePath := tempFilePath("ssync-small-dst-")

	filesCleanup(localPath, remotePath)
	defer filesCleanup(localPath, remotePath)

	data := make([]byte, 512)
	data1 := make([]byte, 512)
	createTestSmallFile(localPath, len(data), data)
	createTestSmallFile(remotePath, len(data1), data1)
	testSyncAnyFile(t, localPath, remotePath, false /* directIO */)
}

func TestSyncSmallFile4MB(t *testing.T) {
	localPath := tempFilePath("ssync-small-src-")
	remotePath := tempFilePath("ssync-small-dst-")

	filesCleanup(localPath, remotePath)
	defer filesCleanup(localPath, remotePath)

	data := make([]byte, 4096)
	data1 := make([]byte, 4096)
	createTestSmallFile(localPath, len(data), data)
	createTestSmallFile(remotePath, len(data1), data1)
	testSyncAnyFile(t, localPath, remotePath, true /* directIO */)
}

func TestSyncSmallFile4MBNoDirectIO(t *testing.T) {
	localPath := tempFilePath("ssync-small-src-")
	remotePath := tempFilePath("ssync-small-dst-")

	filesCleanup(localPath, remotePath)
	defer filesCleanup(localPath, remotePath)

	data := make([]byte, 4096)
	data1 := make([]byte, 4096)
	createTestSmallFile(localPath, len(data), data)
	createTestSmallFile(remotePath, len(data1), data1)
	testSyncAnyFile(t, localPath, remotePath, false /* directIO */)
}

func TestSyncSmallFile_8MB_DirectIO(t *testing.T) {
	localPath := tempFilePath("ssync-small-src-")
	remotePath := tempFilePath("ssync-small-dst-")

	filesCleanup(localPath, remotePath)
	defer filesCleanup(localPath, remotePath)

	data := make([]byte, 8192)
	data1 := make([]byte, 8192)
	createTestSmallFile(localPath, len(data), data)
	createTestSmallFile(remotePath, len(data1), data1)
	testSyncAnyFile(t, localPath, remotePath, true /* directIO */)
}

func TestSyncSmallFile_8MB_NoDirectIO(t *testing.T) {
	localPath := tempFilePath("ssync-small-src-")
	remotePath := tempFilePath("ssync-small-dst-")

	filesCleanup(localPath, remotePath)
	defer filesCleanup(localPath, remotePath)

	data := make([]byte, 8192)
	data1 := make([]byte, 8192)
	createTestSmallFile(localPath, len(data), data)
	createTestSmallFile(remotePath, len(data1), data1)
	testSyncAnyFile(t, localPath, remotePath, false /* directIO */)
}

func TestSyncSmallFile_8MB_Minus_512KB_DirectIO(t *testing.T) {
	localPath := tempFilePath("ssync-small-src-")
	remotePath := tempFilePath("ssync-small-dst-")

	filesCleanup(localPath, remotePath)
	defer filesCleanup(localPath, remotePath)

	data := make([]byte, 7680)
	data1 := make([]byte, 7680)
	createTestSmallFile(localPath, len(data), data)
	createTestSmallFile(remotePath, len(data1), data1)
	testSyncAnyFileExpectFailure(t, localPath, remotePath, true /* directIO */)
}

func TestSyncSmallFile_8MB_Plus_512KB_DirectIO(t *testing.T) {
	localPath := tempFilePath("ssync-small-src-")
	remotePath := tempFilePath("ssync-small-dst-")

	filesCleanup(localPath, remotePath)
	defer filesCleanup(localPath, remotePath)

	data := make([]byte, 7680)
	data1 := make([]byte, 7680)
	createTestSmallFile(localPath, len(data), data)
	createTestSmallFile(remotePath, len(data1), data1)
	testSyncAnyFileExpectFailure(t, localPath, remotePath, true /* directIO */)
}

func TestSyncSmallFile_8MB_Minus_512KB_NoDirectIO(t *testing.T) {
	localPath := tempFilePath("ssync-small-src-")
	remotePath := tempFilePath("ssync-small-dst-")

	filesCleanup(localPath, remotePath)
	defer filesCleanup(localPath, remotePath)

	data := make([]byte, 8704)
	data1 := make([]byte, 8704)
	createTestSmallFile(localPath, len(data), data)
	createTestSmallFile(remotePath, len(data1), data1)
	testSyncAnyFile(t, localPath, remotePath, false /* directIO */)
}

func TestSyncSmallFile_8MB_Plus_512KB_NoDirectIO(t *testing.T) {
	localPath := tempFilePath("ssync-small-src-")
	remotePath := tempFilePath("ssync-small-dst-")

	filesCleanup(localPath, remotePath)
	defer filesCleanup(localPath, remotePath)

	data := make([]byte, 8704)
	data1 := make([]byte, 8704)
	createTestSmallFile(localPath, len(data), data)
	createTestSmallFile(remotePath, len(data1), data1)
	testSyncAnyFile(t, localPath, remotePath, false /* directIO */)
}

func TestSyncAnyFile(t *testing.T) {
	src := "src.bar"
	dst := "dst.bar"
	run := false
	// ad hoc test for testing specific problematic files
	// disabled by default
	if run {
		testSyncAnyFile(t, src, dst, true /* directIO */)
	}
}

func testSyncAnyFile(t *testing.T, src, dst string, directIO bool) {
	// Sync
	go rest.TestServer(port, dst, timeout)
	err := SyncFile(src, localhost+":"+port, timeout, directIO)

	// Verify
	if err != nil {
		t.Fatal("sync error")
	}
	if !filesAreEqual(src, dst) {
		t.Fatal("file content diverged")
	}
	err = checkSparseFiles(src, dst)
	if err != nil {
		t.Fatal(err)
	}
}

func testSyncAnyFileExpectFailure(t *testing.T, src, dst string, directIO bool) {
	// Sync
	go rest.TestServer(port, dst, timeout)
	err := SyncFile(src, localhost+":"+port, timeout, directIO)

	// Verify
	if err == nil {
		t.Fatal("sync error")
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
}

func TestSyncFile8(t *testing.T) {
	// D H D =>
	layoutLocal := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	layoutRemote := []FileInterval{}
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
}

func TestSyncFile9(t *testing.T) {
	// H D H  =>
	layoutLocal := []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
	}
	layoutRemote := []FileInterval{}
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
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
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
}

func TestSyncFileHashRetry(t *testing.T) {
	layoutLocal := []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
	}
	layoutRemote := []FileInterval{}

	// Simulate file hash mismatch
	SetFailPointFileHashMatch(true)
	testSyncFile(t, layoutLocal, layoutRemote, true /* directIO */)
}

func testSyncFile(t *testing.T, layoutLocal, layoutRemote []FileInterval, directIO bool) (hashLocal []byte) {
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
	err := SyncFile(localPath, localhost+":"+port, timeout, true /* directIO */)

	// Verify
	if err != nil {
		t.Fatal("sync error")
	}
	if !filesAreEqual(localPath, remotePath) {
		t.Fatal("file content diverged")
	}
	err = checkSparseFiles(localPath, remotePath)
	if err != nil {
		t.Fatal(err)
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
	err := SyncFile(localBigPath, localhost+":"+port, timeout, true /* directIO */)

	if err != nil {
		b.Fatal("sync error")
	}
}

func Benchmark_1G_SendFiles_Whole_No_DirectIO(b *testing.B) {
	go rest.TestServer(port, remoteBigPath, timeout)
	err := SyncFile(localBigPath, localhost+":"+port, timeout, false /* directIO */)

	if err != nil {
		b.Fatal("sync error")
	}
}

func Benchmark_1G_SendFiles_Diff(b *testing.B) {

	go rest.TestServer(port, remoteBigPath, timeout)
	err := SyncFile(localBigPath, localhost+":"+port, timeout, true /* directIO */)

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
