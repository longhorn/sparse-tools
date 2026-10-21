package test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	. "github.com/longhorn/sparse-tools/sparse"
	"github.com/longhorn/sparse-tools/sparse/rest"
	"github.com/longhorn/sparse-tools/types"
	"github.com/longhorn/sparse-tools/util"
)

const testChecksumMethod = "crc64"

func newTestSnapshotDiskPath(dir, name string) string {
	return filepath.Join(dir, "volume-snap-"+name+types.SnapshotDiskSuffix)
}

// writeTestChecksumFile records the checksum for the disk file. If stale is true, the recorded
// change time doesn't match the current change time of the disk file, which simulates the disk
// file being changed (e.g. coalesced during snapshot purge) after it was hashed.
func writeTestChecksumFile(t *testing.T, diskPath, checksum string, stale bool) {
	changeTime, err := util.GetFileChangeTime(diskPath)
	require.NoError(t, err)
	if stale {
		changeTime = "2006-01-02 15:04:05 +0000 UTC"
	}

	err = util.SetSnapshotHashInfoToChecksumFile(diskPath+types.DiskChecksumSuffix, &types.SnapshotHashInfo{
		Method:     testChecksumMethod,
		Checksum:   checksum,
		ChangeTime: changeTime,
	})
	require.NoError(t, err)
}

func readTestChecksumFile(t *testing.T, diskPath string) *types.SnapshotHashInfo {
	info, err := util.GetSnapshotHashInfoFromChecksumFile(diskPath + types.DiskChecksumSuffix)
	if os.IsNotExist(err) {
		return nil
	}
	require.NoError(t, err)
	return info
}

func waitForTestServer(t *testing.T) {
	address := net.JoinHostPort(localhost, port)
	require.Eventually(t, func() bool {
		conn, err := net.Dial("tcp", address)
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, timeout*time.Second, 10*time.Millisecond, "server %v is not ready", address)
}

func syncSnapshotDiskFile(t *testing.T, src, dst string, fastSync bool) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err := rest.TestServer(context.Background(), port, dst, timeout)
		assert.True(t, err == nil || err.Error() == "http: Server closed", "Unexpected error: %v", err)
	}()
	// The fast sync requires the server to be ready, since getting the recorded metadata is not retried.
	waitForTestServer(t)
	err := SyncFile(src, localhost+":"+port, timeout, false /* directIO */, fastSync)
	wg.Wait()
	require.NoError(t, err)
}

// TestFastSyncWithStaleChecksum reproduces https://github.com/longhorn/longhorn/issues/14096:
//  1. The disk file on replica C is coalesced during snapshot purge, so its recorded checksum is stale.
//  2. Replica B is rebuilt from replica C. The full sync is done since the checksum of C is stale.
//     The stale checksum must not be propagated to and recorded by B.
//  3. Replica A, which has the disk file before coalescing and the same recorded checksum, is rebuilt
//     from replica B. The fast sync must not be skipped and A must receive the coalesced content.
func TestFastSyncWithStaleChecksum(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "b"), 0755))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "a"), 0755))
	pathC := newTestSnapshotDiskPath(dir, "c")
	pathB := newTestSnapshotDiskPath(filepath.Join(dir, "b"), "c")
	pathA := newTestSnapshotDiskPath(filepath.Join(dir, "a"), "c")

	oldChecksum := "17f13af45475f134"

	// Replica A: the disk file before coalescing with a valid checksum
	createTestSparseFile(pathA, []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 2 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 2 * Blocks, End: 3 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 3 * Blocks, End: 4 * Blocks}},
	})
	writeTestChecksumFile(t, pathA, oldChecksum, false)

	// Replica C: the coalesced disk file with the stale checksum
	createTestSparseFile(pathC, []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 3 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 3 * Blocks, End: 4 * Blocks}},
	})
	writeTestChecksumFile(t, pathC, oldChecksum, true)

	assert.False(t, filesAreEqual(pathA, pathC))

	// Rebuild B from C
	syncSnapshotDiskFile(t, pathC, pathB, true /* fastSync */)
	assert.True(t, filesAreEqual(pathC, pathB))
	assert.Nil(t, readTestChecksumFile(t, pathB), "stale checksum should not be propagated")

	// Rebuild A from B
	syncSnapshotDiskFile(t, pathB, pathA, true /* fastSync */)
	assert.True(t, filesAreEqual(pathB, pathA), "the sync of the changed disk file should not be skipped")
	assert.Nil(t, readTestChecksumFile(t, pathA), "existing checksum of the changed disk file should be removed")
}

func TestFastSyncWithValidChecksum(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "dst"), 0755))
	pathSrc := newTestSnapshotDiskPath(dir, "src")
	pathDst := newTestSnapshotDiskPath(filepath.Join(dir, "dst"), "src")

	checksum := "91bc1af7fd79a73f"

	createTestSparseFile(pathSrc, []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
	})
	writeTestChecksumFile(t, pathSrc, checksum, false)

	// The valid checksum is propagated after the full sync
	syncSnapshotDiskFile(t, pathSrc, pathDst, true /* fastSync */)
	assert.True(t, filesAreEqual(pathSrc, pathDst))
	info := readTestChecksumFile(t, pathDst)
	require.NotNil(t, info)
	assert.Equal(t, checksum, info.Checksum)
	changeTime, err := util.GetFileChangeTime(pathDst)
	require.NoError(t, err)
	assert.Equal(t, changeTime, info.ChangeTime)

	// The fast sync is skipped since the files are identical, and the checksum is kept
	syncSnapshotDiskFile(t, pathSrc, pathDst, true /* fastSync */)
	assert.True(t, filesAreEqual(pathSrc, pathDst))
	newChangeTime, err := util.GetFileChangeTime(pathDst)
	require.NoError(t, err)
	assert.Equal(t, changeTime, newChangeTime, "the destination file should not be touched")
	info = readTestChecksumFile(t, pathDst)
	require.NotNil(t, info)
	assert.Equal(t, checksum, info.Checksum)
}

func TestSyncWithoutChecksumRemovesStaleDestinationChecksum(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "dst"), 0755))
	pathSrc := newTestSnapshotDiskPath(dir, "src")
	pathDst := newTestSnapshotDiskPath(filepath.Join(dir, "dst"), "src")

	createTestSparseFile(pathSrc, []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 2 * Blocks}},
	})
	createTestSparseFile(pathDst, []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
	})
	writeTestChecksumFile(t, pathDst, "0123456789abcdef", false)

	syncSnapshotDiskFile(t, pathSrc, pathDst, false /* fastSync */)
	assert.True(t, filesAreEqual(pathSrc, pathDst))
	assert.Nil(t, readTestChecksumFile(t, pathDst))
}

// hookedSourceReader wraps the source file and invokes the hook before the first data read.
type hookedSourceReader struct {
	FileIoProcessor

	once sync.Once
	hook func() error
}

func (r *hookedSourceReader) ReadAt(data []byte, offset int64) (int, error) {
	var err error
	r.once.Do(func() {
		err = r.hook()
	})
	if err != nil {
		return 0, err
	}
	return r.FileIoProcessor.ReadAt(data, offset)
}

func syncSnapshotDiskContentWithHook(t *testing.T, src, dst string, hook func() error) error {
	srcIo, err := NewBufferedFileIoProcessor(src, os.O_RDONLY, 0)
	require.NoError(t, err)
	defer func() {
		_ = srcIo.Close()
	}()
	stat, err := srcIo.Stat()
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		err := rest.TestServer(context.Background(), port, dst, timeout)
		assert.True(t, err == nil || err.Error() == "http: Server closed", "Unexpected error: %v", err)
	}()
	waitForTestServer(t)
	rw := &hookedSourceReader{FileIoProcessor: srcIo, hook: hook}
	err = SyncContent(src, rw, stat.Size(), localhost+":"+port, timeout, false /* directIO */, true /* fastSync */)
	wg.Wait()
	return err
}

// changeFileChangeTime changes the change time of the file without changing its content.
func changeFileChangeTime(path string) error {
	oldChangeTime, err := util.GetFileChangeTime(path)
	if err != nil {
		return err
	}
	// The change time granularity depends on the kernel clock tick, so retry until it is changed.
	for i := 0; i < 100; i++ {
		if err := os.Chmod(path, 0640+os.FileMode(i%2)*0004); err != nil {
			return err
		}
		newChangeTime, err := util.GetFileChangeTime(path)
		if err != nil {
			return err
		}
		if newChangeTime != oldChangeTime {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return errors.New("change time of the file is not changed")
}

func TestFailedSyncDoesNotPropagateChecksum(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "dst"), 0755))
	pathSrc := newTestSnapshotDiskPath(dir, "src")
	pathDst := newTestSnapshotDiskPath(filepath.Join(dir, "dst"), "src")

	createTestSparseFile(pathSrc, []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 2 * Blocks}},
	})
	writeTestChecksumFile(t, pathSrc, "91bc1af7fd79a73f", false)
	createTestSparseFile(pathDst, []FileInterval{
		{Kind: SparseHole, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseData, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
	})
	writeTestChecksumFile(t, pathDst, "0123456789abcdef", false)

	err := syncSnapshotDiskContentWithHook(t, pathSrc, pathDst, func() error {
		return errors.New("injected read failure")
	})
	require.Error(t, err)
	assert.Nil(t, readTestChecksumFile(t, pathDst), "checksum should not be recorded for the partially synced file")
}

func TestSyncWithSourceChangedDuringSyncDoesNotPropagateChecksum(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "dst"), 0755))
	pathSrc := newTestSnapshotDiskPath(dir, "src")
	pathDst := newTestSnapshotDiskPath(filepath.Join(dir, "dst"), "src")

	createTestSparseFile(pathSrc, []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 2 * Blocks}},
	})
	writeTestChecksumFile(t, pathSrc, "91bc1af7fd79a73f", false)

	err := syncSnapshotDiskContentWithHook(t, pathSrc, pathDst, func() error {
		return changeFileChangeTime(pathSrc)
	})
	require.NoError(t, err)
	assert.True(t, filesAreEqual(pathSrc, pathDst))
	assert.Nil(t, readTestChecksumFile(t, pathDst), "checksum of the source file changed during sync should not be propagated")
}

// canceledLayoutSource cancels the sync context when the data layout is requested, then reports
// an empty layout, which simulates the sync being interrupted after the receiver is opened.
type canceledLayoutSource struct {
	FileIoProcessor

	cancel context.CancelFunc
}

func (s *canceledLayoutSource) GetDataLayout(ctx context.Context) (<-chan FileInterval, <-chan error, error) {
	s.cancel()
	<-ctx.Done()

	intervals := make(chan FileInterval)
	errc := make(chan error)
	close(intervals)
	close(errc)
	return intervals, errc, nil
}

func TestCanceledSyncReturnsError(t *testing.T) {
	dir := t.TempDir()
	pathSrc := newTestSnapshotDiskPath(dir, "src")
	createTestSparseFile(pathSrc, []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 2 * Blocks}},
	})
	writeTestChecksumFile(t, pathSrc, "91bc1af7fd79a73f", false)

	srcIo, err := NewBufferedFileIoProcessor(pathSrc, os.O_RDONLY, 0)
	require.NoError(t, err)
	defer func() {
		_ = srcIo.Close()
	}()

	var propagatedChecksum atomic.Value
	propagatedChecksum.Store("")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/open"):
			_, _ = w.Write([]byte("true"))
		case strings.HasSuffix(r.URL.Path, "/close"):
			propagatedChecksum.Store(r.URL.Query().Get("checksum"))
		default:
			http.Error(w, "unexpected request", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rw := &canceledLayoutSource{FileIoProcessor: srcIo, cancel: cancel}
	err = SyncContentWithContext(ctx, pathSrc, rw, 2*Blocks, strings.TrimPrefix(server.URL, "http://"),
		timeout, false /* directIO */, false /* fastSync */)
	require.Error(t, err, "interrupted sync should not be treated as succeeded")
	assert.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, propagatedChecksum.Load(), "checksum should not be propagated for the interrupted sync")
}

// TestSyncWithHTTPFailureDoesNotPropagateChecksum simulates a network failure during the data transfer:
// the connection of every data request is dropped without a response. The close request is still sent
// as best-effort cleanup, but it must not carry the valid checksum of the source file.
func TestSyncWithHTTPFailureDoesNotPropagateChecksum(t *testing.T) {
	dir := t.TempDir()
	pathSrc := newTestSnapshotDiskPath(dir, "src")
	createTestSparseFile(pathSrc, []FileInterval{
		{Kind: SparseData, Interval: Interval{Begin: 0, End: 1 * Blocks}},
		{Kind: SparseHole, Interval: Interval{Begin: 1 * Blocks, End: 2 * Blocks}},
	})
	writeTestChecksumFile(t, pathSrc, "91bc1af7fd79a73f", false)

	var closeReceived atomic.Bool
	var propagatedChecksum atomic.Value
	propagatedChecksum.Store("")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/open"):
			_, _ = w.Write([]byte("false"))
		case strings.HasSuffix(r.URL.Path, "/close"):
			propagatedChecksum.Store(r.URL.Query().Get("checksum"))
			closeReceived.Store(true)
		default:
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close()
			}
		}
	}))
	defer server.Close()

	err := SyncFile(pathSrc, strings.TrimPrefix(server.URL, "http://"), timeout, false /* directIO */, false /* fastSync */)
	require.Error(t, err, "sync with failed HTTP requests should fail")
	assert.True(t, closeReceived.Load(), "close request should still be sent as best-effort cleanup")
	assert.Empty(t, propagatedChecksum.Load(), "checksum should not be propagated when the sync fails")
}
