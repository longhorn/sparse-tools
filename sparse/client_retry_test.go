package sparse

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

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

// newTestSyncClient creates a syncClient for testing with custom retry config
func newTestSyncClient(serverAddr string, retryConfig retryConfig) *syncClient {
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
	client.retry = retryConfig
	client.ctx = context.Background()
	return client
}

// TestHTTPRetryClientErrorsNotRetried tests that 4xx client errors are NOT retried
func TestHTTPRetryClientErrorsNotRetried(t *testing.T) {
	var attempts atomic.Int32

	// Mock HTTP server that returns 404
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	// Create client with fast retry config
	client := newTestSyncClient(server.Listener.Addr().String(), retryConfig{
		maxRetries:     5,
		retryBaseDelay: 10 * time.Millisecond,
		retryMaxDelay:  100 * time.Millisecond,
	})

	// Try to send request
	resp, err := client.sendHTTPRequestWithRetry("POST", "sendHole", nil, nil)

	// Should fail with error
	assert.NotNil(t, err)
	if resp != nil {
		_ = resp.Body.Close()
	}

	// Should have tried exactly once (no retries for 4xx)
	count := attempts.Load()
	assert.Equal(t, int32(1), count, "Should NOT retry 4xx errors")
}

// TestHTTPRetryTransientServerErrors tests that 5xx server errors ARE retried
func TestHTTPRetryTransientServerErrors(t *testing.T) {
	var attempts atomic.Int32

	// Mock HTTP server that fails first 2 attempts, then succeeds
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		if attempt <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Create client with fast retry config
	client := newTestSyncClient(server.Listener.Addr().String(), retryConfig{
		maxRetries:     5,
		retryBaseDelay: 10 * time.Millisecond,
		retryMaxDelay:  100 * time.Millisecond,
	})

	// Try to send request
	resp, err := client.sendHTTPRequestWithRetry("POST", "sendHole", nil, nil)

	// Should succeed after retries
	assert.Nil(t, err)
	require.NotNil(t, resp)
	_ = resp.Body.Close()

	// Should have retried (at least 3 attempts: 2 failures + 1 success)
	count := attempts.Load()
	t.Logf("Request was attempted %d times (expected >= 3)", count)
	assert.GreaterOrEqual(t, int(count), 3, "Should have retried 5xx errors")
}

// TestHTTPRetryExponentialBackoff tests that retry delays increase exponentially
func TestHTTPRetryExponentialBackoff(t *testing.T) {
	var requestTimes []time.Time
	var attempts atomic.Int32

	// Mock HTTP server that fails first 3 attempts
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestTimes = append(requestTimes, time.Now())
		attempt := attempts.Add(1)
		if attempt <= 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Create client with fast retry config
	client := newTestSyncClient(server.Listener.Addr().String(), retryConfig{
		maxRetries:     5,
		retryBaseDelay: 50 * time.Millisecond,
		retryMaxDelay:  500 * time.Millisecond,
	})

	// Try to send request
	resp, err := client.sendHTTPRequestWithRetry("POST", "test", nil, nil)
	assert.Nil(t, err)
	if resp != nil {
		_ = resp.Body.Close()
	}

	// Verify exponential backoff
	require.GreaterOrEqual(t, len(requestTimes), 4, "Should have at least 4 requests")

	// Check delays between requests increase exponentially
	baseDelay := 50 * time.Millisecond
	for i := 1; i < len(requestTimes) && i < 4; i++ {
		delay := requestTimes[i].Sub(requestTimes[i-1])
		expectedMinDelay := time.Duration(1<<uint(i-1)) * baseDelay // 50ms, 100ms, 200ms

		t.Logf("Delay between request %d and %d: %v (expected min: %v)", i, i+1, delay, expectedMinDelay)

		// Allow 30ms tolerance for processing time
		tolerance := 30 * time.Millisecond
		assert.GreaterOrEqual(t, delay, expectedMinDelay-tolerance,
			"Retry delay should follow exponential backoff")
	}
}

// TestHTTPRetryMaxRetries tests that retries stop after exhausting max attempts
func TestHTTPRetryMaxRetries(t *testing.T) {
	var attempts atomic.Int32

	// Mock HTTP server that always fails
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	// Create client with fast retry config
	maxRetries := 3
	client := newTestSyncClient(server.Listener.Addr().String(), retryConfig{
		maxRetries:     maxRetries,
		retryBaseDelay: 10 * time.Millisecond,
		retryMaxDelay:  100 * time.Millisecond,
	})

	// Try to send request
	resp, err := client.sendHTTPRequestWithRetry("POST", "test", nil, nil)

	// Should fail after max retries
	assert.NotNil(t, err)
	if resp != nil {
		_ = resp.Body.Close()
	}

	// Should have made exactly maxRetries+1 attempts (initial + retries)
	count := attempts.Load()
	t.Logf("Request was attempted %d times (expected %d: initial + %d retries)",
		count, maxRetries+1, maxRetries)
	assert.Equal(t, int32(maxRetries+1), count, "Should make exactly maxRetries+1 attempts")
}

// TestHTTPRetryRedirectsNotRetried tests that 3xx redirects are NOT retried
func TestHTTPRetryRedirectsNotRetried(t *testing.T) {
	var attempts atomic.Int32

	// Mock HTTP server that returns 301 redirect
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusMovedPermanently)
	}))
	defer server.Close()

	// Create client with fast retry config
	client := newTestSyncClient(server.Listener.Addr().String(), retryConfig{
		maxRetries:     5,
		retryBaseDelay: 10 * time.Millisecond,
		retryMaxDelay:  100 * time.Millisecond,
	})

	// Try to send request
	resp, err := client.sendHTTPRequestWithRetry("POST", "test", nil, nil)

	// Should fail with error
	assert.NotNil(t, err)
	if resp != nil {
		_ = resp.Body.Close()
	}

	// Should have tried exactly once (no retries for 3xx)
	count := attempts.Load()
	assert.Equal(t, int32(1), count, "Should NOT retry 3xx redirects")
}

// TestHTTPRetryWithData tests retry logic with request body data
func TestHTTPRetryWithData(t *testing.T) {
	var attempts atomic.Int32
	testData := []byte("test data")

	// Mock HTTP server that fails first attempt, then succeeds
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)

		// Verify request body
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

	// Create client with fast retry config
	client := newTestSyncClient(server.Listener.Addr().String(), retryConfig{
		maxRetries:     5,
		retryBaseDelay: 10 * time.Millisecond,
		retryMaxDelay:  100 * time.Millisecond,
	})

	// Try to send request with data
	resp, err := client.sendHTTPRequestWithRetry("POST", "writeData", nil, testData)

	// Should succeed after retry
	assert.Nil(t, err)
	require.NotNil(t, resp)

	// Read response
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	assert.NoError(t, err)
	assert.Contains(t, string(body), "success")

	// Should have retried (2 attempts: 1 failure + 1 success)
	count := attempts.Load()
	assert.Equal(t, int32(2), count, "Should have retried once")
}

// TestHTTPRetryContextCancellation tests that retry respects context cancellation
func TestHTTPRetryContextCancellation(t *testing.T) {
	var attempts atomic.Int32

	// Mock HTTP server that always fails
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	// Create client with slow retry config
	client := newTestSyncClient(server.Listener.Addr().String(), retryConfig{
		maxRetries:     10,
		retryBaseDelay: 100 * time.Millisecond,
		retryMaxDelay:  1 * time.Second,
	})

	// Set up context that will be cancelled
	ctx, cancel := context.WithCancel(context.Background())
	client.ctx = ctx

	// Cancel context after short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	// Try to send request
	_, err := client.sendHTTPRequestWithRetry("POST", "test", nil, nil)

	// Should fail due to context cancellation
	assert.NotNil(t, err)

	// Should have tried only a few times before context was cancelled
	count := attempts.Load()
	t.Logf("Request was attempted %d times before context cancellation", count)
	assert.Less(t, int(count), 5, "Should stop retrying when context is cancelled")
}

// TestHTTPRetryMaxDelayCap tests that retry delay is capped at maxDelay
func TestHTTPRetryMaxDelayCap(t *testing.T) {
	var requestTimes []time.Time
	var attempts atomic.Int32

	// Mock HTTP server that fails many times
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestTimes = append(requestTimes, time.Now())
		attempt := attempts.Add(1)
		if attempt <= 5 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Create client with config that hits max delay quickly
	client := newTestSyncClient(server.Listener.Addr().String(), retryConfig{
		maxRetries:     10,
		retryBaseDelay: 50 * time.Millisecond,
		retryMaxDelay:  150 * time.Millisecond, // Cap at 150ms
	})

	// Try to send request
	resp, err := client.sendHTTPRequestWithRetry("POST", "test", nil, nil)
	assert.Nil(t, err)
	if resp != nil {
		_ = resp.Body.Close()
	}

	// Verify delays are capped
	require.GreaterOrEqual(t, len(requestTimes), 4)

	// After 3rd retry, delay should be capped at maxDelay
	// Delays: 50ms, 100ms, 200ms (capped to 150ms), 400ms (capped to 150ms)
	for i := 3; i < len(requestTimes) && i < 6; i++ {
		delay := requestTimes[i].Sub(requestTimes[i-1])
		maxDelay := 150 * time.Millisecond

		t.Logf("Delay between request %d and %d: %v (should be capped at %v)", i, i+1, delay, maxDelay)

		// Delay should not significantly exceed maxDelay
		tolerance := 50 * time.Millisecond
		assert.LessOrEqual(t, delay, maxDelay+tolerance,
			"Retry delay should be capped at maxDelay")
	}
}

// TestDefaultRetryConfig verifies that new clients get the default retry config
func TestDefaultRetryConfig(t *testing.T) {
	client := newSyncClient(
		"localhost:1234",
		"test",
		1024,
		&mockReaderWriterAt{},
		false,
		10,
		"", "", "",
		512*Blocks,
		4,
	)

	assert.Equal(t, defaultRetryConfig.maxRetries, client.retry.maxRetries)
	assert.Equal(t, defaultRetryConfig.retryBaseDelay, client.retry.retryBaseDelay)
	assert.Equal(t, defaultRetryConfig.retryMaxDelay, client.retry.retryMaxDelay)
}

// TestCustomRetryConfig verifies that retry config can be customized per client
func TestCustomRetryConfig(t *testing.T) {
	customConfig := retryConfig{
		maxRetries:     10,
		retryBaseDelay: 5 * time.Second,
		retryMaxDelay:  60 * time.Second,
	}

	client := newTestSyncClient("localhost:1234", customConfig)

	assert.Equal(t, customConfig.maxRetries, client.retry.maxRetries)
	assert.Equal(t, customConfig.retryBaseDelay, client.retry.retryBaseDelay)
	assert.Equal(t, customConfig.retryMaxDelay, client.retry.retryMaxDelay)
}

// TestHTTPRetry429TooManyRequests tests that 429 (Too Many Requests) IS retried
// unlike other 4xx errors, since it represents transient rate-limiting
func TestHTTPRetry429TooManyRequests(t *testing.T) {
	var attempts atomic.Int32

	// Mock HTTP server that returns 429 for first 2 attempts, then succeeds
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		if attempt <= 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Create client with fast retry config
	client := newTestSyncClient(server.Listener.Addr().String(), retryConfig{
		maxRetries:     5,
		retryBaseDelay: 10 * time.Millisecond,
		retryMaxDelay:  100 * time.Millisecond,
	})

	// Try to send request
	resp, err := client.sendHTTPRequestWithRetry("POST", "test", nil, nil)

	// Should succeed after retries
	assert.Nil(t, err)
	require.NotNil(t, resp)
	_ = resp.Body.Close()

	// Should have retried (at least 3 attempts: 2 failures + 1 success)
	count := attempts.Load()
	t.Logf("Request was attempted %d times (expected >= 3)", count)
	assert.GreaterOrEqual(t, int(count), 3, "Should retry 429 (Too Many Requests) errors")
}

// TestHTTPRetry4xxErrorsNotRetried tests that other 4xx errors (not 429) are NOT retried
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

			// Mock HTTP server that returns the specific 4xx error
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts.Add(1)
				w.WriteHeader(tc.statusCode)
			}))
			defer server.Close()

			// Create client with fast retry config
			client := newTestSyncClient(server.Listener.Addr().String(), retryConfig{
				maxRetries:     5,
				retryBaseDelay: 10 * time.Millisecond,
				retryMaxDelay:  100 * time.Millisecond,
			})

			// Try to send request
			resp, err := client.sendHTTPRequestWithRetry("POST", "test", nil, nil)

			// Should fail with error
			assert.NotNil(t, err)
			if resp != nil {
				_ = resp.Body.Close()
			}

			// Should have tried exactly once (no retries for 4xx errors except 429)
			count := attempts.Load()
			assert.Equal(t, int32(1), count, "Should NOT retry %d errors", tc.statusCode)
		})
	}
}
