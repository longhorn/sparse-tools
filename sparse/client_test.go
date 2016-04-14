package sparse

import (
	"os"
	"os/exec"
	"testing"

	"github.com/kp6/alphorn/log"
)

const localPath = "foo1.bar"
const remotePath = "foo2.bar"
const localhost = "127.0.0.1"

var remoteAddr = TCPEndPoint{localhost, 5000}

func TestSendFile1(t *testing.T) {
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
	createTestSparseFile(localPath, layoutLocal)
	createTestSparseFile(remotePath, layoutRemote)

	go TestServer(remoteAddr)
	status := SyncFile(localPath, remoteAddr, remotePath)

	if !status {
		t.Error("sync error")
		return
	}
	if !filesEqual(localPath, remotePath) {
		t.Error("file content diverged")
		return
	}
	os.Remove(localPath)
	os.Remove(remotePath)
}

func Benchmark_1G_InitFiles(b *testing.B) {
	// Setup files
	layoutLocal := []FileInterval{
		{SparseData, Interval{0, (256 << 10) * Blocks}},
	}
	layoutRemote := []FileInterval{
		{SparseData, Interval{0, (256 << 10) * Blocks}},
	}
	createTestSparseFile(localPath, layoutLocal)
	createTestSparseFile(remotePath, layoutRemote)
}

func Benchmark_1G_SendFiles(b *testing.B) {
	log.LevelPush(log.LevelInfo)
	defer log.LevelPop()

	go TestServer(remoteAddr)
	status := SyncFile(localPath, remoteAddr, remotePath)

	if !status {
		b.Error("sync error")
	}
}

func Benchmark_1G_CheckFiles(b *testing.B) {
	if !filesEqual(localPath, remotePath) {
		b.Error("file content diverged")
		return
	}
	os.Remove(localPath)
	os.Remove(remotePath)
}

func filesEqual(aPath, bPath string) bool {
	cmd := exec.Command("diff", aPath, bPath)
	err := cmd.Run()
	return nil == err
}
