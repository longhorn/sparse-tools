package sparse

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	retry "github.com/avast/retry-go/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockReaderWriterAt is a minimal implementation for testing
type mockReaderWriterAt struct{}

func (m *mockReaderWriterAt) ReadAt(p []byte, off int64) (n int, err error) {
	return len(p), nil
}

func (m *mockReaderWriterAt) WriteAt(p []byte, off int64) (n int, err error) {
	return len(p), nil
}

func (m *mockReaderWriterAt) GetDataLayout(ctx context.Context) (<-chan FileInterval, <-chan error, error) {
	return nil, nil, nil
}

// testRetryOpts returns fast retry options for tests
func testRetryOpts(maxRetries int, baseDelay, maxDelay time.Duration) []retry.Option {
	return []retry.Option{
		retry.Attempts(uint(maxRetries) + 1),
		retry.Delay(baseDelay),
		retry.MaxDelay(maxDelay),
		retry.DelayType(retry.BackOffDelay),
		retry.LastErrorOnly(true),
	}
}

// newTestSyncClient creates a syncClient for testing with custom retry options
func newTestSyncClient(serverAddr string, retryOpts []retry.Option) *syncClient {
	client := newSyncClient(
		serverAddr,
		"test-source",
		1024,
		&mockReaderWriterAt{},
		false,
		10,
		"", "", "",
		512*Blocks,
		4,
	)
	client.retryOpts = retryOpts
	client.ctx = context.Background()
	return client
}

// TestHTTPRetryClientErrorsNotRetried tests that 4xx client errors are NOT retried
func TestHTTPRetryClientErrorsNotRetried(t *testing.T) {
	var attempts atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	client := newTestSyncClient(server.Listener.Addr().String(),
		testRetryOpts(5, 10*time.Millisecond, 100*time.Millisecond))

	resp, err := client.sendHTTPRequestWithRetry("POST", "sendHole", nil, nil)
	assert.NotNil(t, err)
	assert.Nil(t, resp, "Response should be nil on error")

	count := attempts.Load()
	assert.Equal(t, int32(1), count, "Should NOT retry 4xx errors")
}

// TestHTTPRetryTransientServerErrors tests that 5xx server errors ARE retried
func TestHTTPRetryTransientServerErrors(t *testing.T) {
	var attempts atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		if attempt <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := newTestSyncClient(server.Listener.Addr().String(),
		testRetryOpts(5, 10*time.Millisecond, 100*time.Millisecond))

	resp, err := client.sendHTTPRequestWithRetry("POST", "sendHole", nil, nil)
	assert.Nil(t, err)
	require.NotNil(t, resp)
	_ = resp.Body.Close()

	count := attempts.Load()
	t.Logf("Request was attempted %d times (expected >= 3)", count)
	assert.GreaterOrEqual(t, int(count), 3, "Should have retried 5xx errors")
}

// TestHTTPRetryMaxRetries tests that retries stop after exhausting max attempts
func TestHTTPRetryMaxRetries(t *testing.T) {
	var attempts atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	maxRetries := 3
	client := newTestSyncClient(server.Listener.Addr().String(),
		testRetryOpts(maxRetries, 10*time.Millisecond, 100*time.Millisecond))

	resp, err := client.sendHTTPRequestWithRetry("POST", "test", nil, nil)
	assert.NotNil(t, err)
	assert.Nil(t, resp)

	count := attempts.Load()
	t.Logf("Request was attempted %d times (expected %d)", count, maxRetries+1)
	assert.Equal(t, int32(maxRetries+1), count, "Should make exactly maxRetries+1 attempts")
}

// TestHTTPRetryRedirectsNotRetried tests that 3xx redirects are NOT retried
func TestHTTPRetryRedirectsNotRetried(t *testing.T) {
	var attempts atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusMovedPermanently)
	}))
	defer server.Close()

	client := newTestSyncClient(server.Listener.Addr().String(),
		testRetryOpts(5, 10*time.Millisecond, 100*time.Millisecond))

	resp, err := client.sendHTTPRequestWithRetry("POST", "test", nil, nil)
	assert.NotNil(t, err)
	assert.Nil(t, resp)

	count := attempts.Load()
	assert.Equal(t, int32(1), count, "Should NOT retry 3xx redirects")
}

// TestHTTPRetryWithData tests retry logic with request body data
func TestHTTPRetryWithData(t *testing.T) {
	var attempts atomic.Int32
	testData := []byte("test data")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		assert.Equal(t, testData, body, "Request body should match")

		if attempt == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"success": true}`)
	}))
	defer server.Close()

	client := newTestSyncClient(server.Listener.Addr().String(),
		testRetryOpts(5, 10*time.Millisecond, 100*time.Millisecond))

	resp, err := client.sendHTTPRequestWithRetry("POST", "writeData", nil, testData)
	assert.Nil(t, err)
	require.NotNil(t, resp)

	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	assert.NoError(t, err)
	assert.Contains(t, string(body), "success")

	count := attempts.Load()
	assert.Equal(t, int32(2), count, "Should have retried once")
}

// TestHTTPRetryContextCancellation tests that retry respects context cancellation
func TestHTTPRetryContextCancellation(t *testing.T) {
	var attempts atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client := newTestSyncClient(server.Listener.Addr().String(),
		testRetryOpts(10, 100*time.Millisecond, 1*time.Second))

	ctx, cancel := context.WithCancel(context.Background())
	client.ctx = ctx

	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, err := client.sendHTTPRequestWithRetry("POST", "test", nil, nil)
	assert.NotNil(t, err)

	count := attempts.Load()
	t.Logf("Request was attempted %d times before context cancellation", count)
	assert.Less(t, int(count), 5, "Should stop retrying when context is cancelled")
}

// TestHTTPRetryExponentialBackoff tests that retry delays increase exponentially
func TestHTTPRetryExponentialBackoff(t *testing.T) {
	var requestTimes []time.Time
	var requestTimesMu sync.Mutex
	var attempts atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestTimesMu.Lock()
		requestTimes = append(requestTimes, time.Now())
		requestTimesMu.Unlock()

		attempt := attempts.Add(1)
		if attempt <= 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := newTestSyncClient(server.Listener.Addr().String(),
		testRetryOpts(5, 50*time.Millisecond, 500*time.Millisecond))

	resp, err := client.sendHTTPRequestWithRetry("POST", "test", nil, nil)
	assert.Nil(t, err)
	if resp != nil {
		_ = resp.Body.Close()
	}

	requestTimesMu.Lock()
	timesSnapshot := make([]time.Time, len(requestTimes))
	copy(timesSnapshot, requestTimes)
	requestTimesMu.Unlock()

	require.GreaterOrEqual(t, len(timesSnapshot), 4, "Should have at least 4 requests")

	baseDelay := 50 * time.Millisecond
	for i := 1; i < len(timesSnapshot) && i < 4; i++ {
		delay := timesSnapshot[i].Sub(timesSnapshot[i-1])
		expectedMinDelay := time.Duration(1<<uint(i-1)) * baseDelay

		t.Logf("Delay between request %d and %d: %v (expected min: %v)", i, i+1, delay, expectedMinDelay)

		tolerance := 30 * time.Millisecond
		assert.GreaterOrEqual(t, delay, expectedMinDelay-tolerance,
			"Retry delay should follow exponential backoff")
	}
}

// TestHTTPRetryMaxDelayCap tests that retry delay is capped at maxDelay
func TestHTTPRetryMaxDelayCap(t *testing.T) {
	var requestTimes []time.Time
	var requestTimesMu sync.Mutex
	var attempts atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestTimesMu.Lock()
		requestTimes = append(requestTimes, time.Now())
		requestTimesMu.Unlock()

		attempt := attempts.Add(1)
		if attempt <= 5 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := newTestSyncClient(server.Listener.Addr().String(),
		testRetryOpts(10, 50*time.Millisecond, 150*time.Millisecond))

	resp, err := client.sendHTTPRequestWithRetry("POST", "test", nil, nil)
	assert.Nil(t, err)
	if resp != nil {
		_ = resp.Body.Close()
	}

	requestTimesMu.Lock()
	timesSnapshot := make([]time.Time, len(requestTimes))
	copy(timesSnapshot, requestTimes)
	requestTimesMu.Unlock()

	require.GreaterOrEqual(t, len(timesSnapshot), 4)

	for i := 3; i < len(timesSnapshot) && i < 6; i++ {
		delay := timesSnapshot[i].Sub(timesSnapshot[i-1])
		maxDelay := 150 * time.Millisecond

		t.Logf("Delay between request %d and %d: %v (should be capped at %v)", i, i+1, delay, maxDelay)

		tolerance := 50 * time.Millisecond
		assert.LessOrEqual(t, delay, maxDelay+tolerance,
			"Retry delay should be capped at maxDelay")
	}
}

// TestDefaultRetryConfig verifies that new clients use default retry options
func TestDefaultRetryConfig(t *testing.T) {
	client := newSyncClient(
		"localhost:1234", "test", 1024,
		&mockReaderWriterAt{}, false, 10,
		"", "", "", 512*Blocks, 4,
	)
	client.ctx = context.Background()

	// Verify retryOpts is nil (uses defaultRetryOpts() internally)
	assert.Nil(t, client.retryOpts, "retryOpts should be nil to use defaults")

	// Verify that retry behavior works with a simple server test
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		if attempt <= 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client.remote = server.Listener.Addr().String()
	resp, err := client.sendHTTPRequestWithRetry("GET", "test", nil, nil)
	assert.Nil(t, err, "Should succeed with default retry options")
	if resp != nil {
		_ = resp.Body.Close()
	}
	assert.GreaterOrEqual(t, int(attempts.Load()), 2, "Should have retried at least once")
}

// TestHTTPRetry429TooManyRequests tests that 429 IS retried
func TestHTTPRetry429TooManyRequests(t *testing.T) {
	var attempts atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		if attempt <= 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := newTestSyncClient(server.Listener.Addr().String(),
		testRetryOpts(5, 10*time.Millisecond, 100*time.Millisecond))

	resp, err := client.sendHTTPRequestWithRetry("POST", "test", nil, nil)
	assert.Nil(t, err)
	require.NotNil(t, resp)
	_ = resp.Body.Close()

	count := attempts.Load()
	t.Logf("Request was attempted %d times (expected >= 3)", count)
	assert.GreaterOrEqual(t, int(count), 3, "Should retry 429 errors")
}

// TestHTTPRetry4xxErrorsNotRetried tests that other 4xx errors are NOT retried
func TestHTTPRetry4xxErrorsNotRetried(t *testing.T) {
	testCases := []struct {
		name       string
		statusCode int
	}{
		{"400 Bad Request", http.StatusBadRequest},
		{"401 Unauthorized", http.StatusUnauthorized},
		{"403 Forbidden", http.StatusForbidden},
		{"404 Not Found", http.StatusNotFound},
		{"405 Method Not Allowed", http.StatusMethodNotAllowed},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var attempts atomic.Int32

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts.Add(1)
				w.WriteHeader(tc.statusCode)
			}))
			defer server.Close()

			client := newTestSyncClient(server.Listener.Addr().String(),
				testRetryOpts(5, 10*time.Millisecond, 100*time.Millisecond))

			resp, err := client.sendHTTPRequestWithRetry("POST", "test", nil, nil)
			assert.NotNil(t, err)
			assert.Nil(t, resp)

			count := attempts.Load()
			assert.Equal(t, int32(1), count, "Should NOT retry %d errors", tc.statusCode)
		})
	}
}
