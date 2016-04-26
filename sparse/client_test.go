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
const timeout = 5 //seconds

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

func TestSyncDiff1(t *testing.T) {
	layoutLocal := []FileInterval{
		{SparseData, Interval{0, 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseData, Interval{0, 30 * Blocks}},
		{SparseData, Interval{30 * Blocks, 34 * Blocks}},
		{SparseData, Interval{34 * Blocks, 100 * Blocks}},
        
    }
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncDiff2(t *testing.T) {
	layoutLocal := []FileInterval{
		{SparseData, Interval{0, 30 * Blocks}},
		{SparseData, Interval{30 * Blocks, 34 * Blocks}},
		{SparseData, Interval{34 * Blocks, 100 * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseData, Interval{0, 100 * Blocks}},        
    }
	testSyncFile(t, layoutLocal, layoutRemote)
}

func TestSyncHash1(t *testing.T) {
	var hash1, hash2 []byte
	{
		layoutLocal := []FileInterval{
			{SparseData, Interval{0, 1 * Blocks}},
			{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
		}
		layoutRemote := layoutLocal
		hash1 = testSyncFile(t, layoutLocal, layoutRemote)
	}
	{

		layoutLocal := []FileInterval{
			{SparseData, Interval{0, 1 * Blocks}},
			{SparseHole, Interval{1 * Blocks, 3 * Blocks}},
		}
		layoutRemote := layoutLocal
		hash2 = testSyncFile(t, layoutLocal, layoutRemote)
	}
    if !isHashDifferent(hash1, hash2) {
        t.Fatal("Files with same data content but different layouts should have unique hashes")
    }
}

func TestSyncHash2(t *testing.T) {
	var hash1, hash2 []byte
	{
		layoutLocal := []FileInterval{
			{SparseData, Interval{0, 1 * Blocks}},
			{SparseHole, Interval{1 * Blocks, 2 * Blocks}},
		}
		layoutRemote := []FileInterval{}
		hash1 = testSyncFile(t, layoutLocal, layoutRemote)
	}
	{

		layoutLocal := []FileInterval{
			{SparseData, Interval{0, 1 * Blocks}},
			{SparseHole, Interval{1 * Blocks, 3 * Blocks}},
		}
		layoutRemote := []FileInterval{}
		hash2 = testSyncFile(t, layoutLocal, layoutRemote)
	}
    if !isHashDifferent(hash1, hash2) {
        t.Fatal("Files with same data content but different layouts should have unique hashes")
    }
}

func testSyncFile(t *testing.T, layoutLocal, layoutRemote []FileInterval) (hashLocal []byte) {
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
	go TestServer(remoteAddr, timeout)
	hashLocal, err := SyncFile(localPath, remoteAddr, remotePath, timeout)

	// Verify
	if err != nil {
		t.Fatal("sync error")
	}
	if !filesAreEqual(localPath, remotePath) {
		t.Fatal("file content diverged")
	}
	filesCleanup()
    return
}

func Benchmark_1G_InitFiles(b *testing.B) {
	// Setup files
	layoutLocal := []FileInterval{
		{SparseData, Interval{0, (256 << 10) * Blocks}},
	}
	layoutRemote := []FileInterval{}

	filesCleanup()
	createTestSparseFile(localPath, layoutLocal)
	createTestSparseFile(remotePath, layoutRemote)
}

func Benchmark_1G_SendFiles_Whole(b *testing.B) {
	log.LevelPush(log.LevelInfo)
	defer log.LevelPop()

	go TestServer(remoteAddr, timeout)
	_, err := SyncFile(localPath, remoteAddr, remotePath, timeout)

	if err != nil {
		b.Fatal("sync error")
	}
}

func Benchmark_1G_SendFiles_Diff(b *testing.B) {
	log.LevelPush(log.LevelInfo)
	defer log.LevelPop()

	go TestServer(remoteAddr, timeout)
	_, err := SyncFile(localPath, remoteAddr, remotePath, timeout)

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
