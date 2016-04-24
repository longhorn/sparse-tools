package sparse

import (
	"os"
	"os/exec"
	"testing"

	"github.com/rancher/sparse-tools/log"
)

const localPath = "foo1.bar"
const remotePath = "foo2.bar"
const localhost = "127.0.0.1"

var remoteAddr = TCPEndPoint{localhost, 5000}

func TestSyncFile1(t *testing.T) {
    // D H D => D D H
	layoutLocal := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
		{SparseData, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncFile2(t *testing.T) {
    // H D H  => D H H
	layoutLocal := []FileInterval{
		{SparseHole, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncFile3(t *testing.T) {
    // D H D => D D
	layoutLocal := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
		{SparseData, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncFile4(t *testing.T) {
    // H D H  => D H
	layoutLocal := []FileInterval{
		{SparseHole, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncFile5(t *testing.T) {
    // H D H  => H D
	layoutLocal := []FileInterval{
		{SparseHole, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseHole, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncFile6(t *testing.T) {
    // H D H  => D 
	layoutLocal := []FileInterval{
		{SparseHole, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncFile7(t *testing.T) {
    // H D H  => H 
	layoutLocal := []FileInterval{
		{SparseHole, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
	}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncFile8(t *testing.T) {
    // D H D => 
	layoutLocal := []FileInterval{
		{SparseData, Interval{0, 1 * Blocks}},
		{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
		{SparseData, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutRemote := []FileInterval{}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncFile9(t *testing.T) {
    // H D H  => 
	layoutLocal := []FileInterval{
		{SparseHole, Interval{0, 1 * Blocks}},
		{SparseData, Interval{1 * Blocks, 2 * Blocks}},
		{SparseHole, Interval{2 * Blocks, 3 * Blocks}},
	}
	layoutRemote := []FileInterval{}
	testSyncFile(t, layoutLocal, layoutRemote)
}

func testSyncFile(t *testing.T, layoutLocal, layoutRemote []FileInterval) {
    // Only log errors
	log.LevelPush(log.LevelError)
	defer log.LevelPop()
    
    // Create test files
	filesCleanup()
	createTestSparseFile(localPath, layoutLocal)
	if len(layoutRemote) > 0 {
        // only create destination test file if layout is speciifed
		createTestSparseFile(remotePath, layoutRemote)
	}

    // Sync
	go TestServer(remoteAddr)
	err := SyncFile(localPath, remoteAddr, remotePath)

    // Verify
	if err != nil {
		t.Fatal("sync error")
	}
	if !filesAreEqual(localPath, remotePath) {
		t.Fatal("file content diverged")
	}
	filesCleanup()
}

func Benchmark_1G_InitFiles(b *testing.B) {
	// Setup files
	layoutLocal := []FileInterval{
		{SparseData, Interval{0, (256 << 10) * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseData, Interval{0, (256 << 10) * Blocks}},
	}

	filesCleanup()
	createTestSparseFile(localPath, layoutLocal)
	createTestSparseFile(remotePath, layoutRemote)
}

func Benchmark_1G_SendFiles(b *testing.B) {
	log.LevelPush(log.LevelInfo)
	defer log.LevelPop()

	go TestServer(remoteAddr)
	err := SyncFile(localPath, remoteAddr, remotePath)

	if err != nil {
		b.Fatal("sync error")
	}
}

func Benchmark_1G_CheckFiles(b *testing.B) {
	if !filesAreEqual(localPath, remotePath) {
		b.Error("file content diverged")
		return
	}
	filesCleanup()
}

func filesAreEqual(aPath, bPath string) bool {
	cmd := exec.Command("diff", aPath, bPath)
	err := cmd.Run()
	return nil == err
}

func filesCleanup() {
	os.Remove(localPath)
	os.Remove(remotePath)
}
