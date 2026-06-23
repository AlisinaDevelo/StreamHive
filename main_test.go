package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// safeBuffer is an io.Writer safe for concurrent writes and reads from another goroutine (e.g. with require.Eventually).
type safeBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestRun_version(t *testing.T) {
	var out bytes.Buffer
	err := run(context.Background(), []string{"-version"}, &out, io.Discard)
	require.NoError(t, err)
	assert.NotEmpty(t, strings.TrimSpace(out.String()))
}

func TestRun_listenUntilCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var out safeBuffer
	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, []string{"-listen", "127.0.0.1:0"}, &out, io.Discard)
	}()

	require.Eventually(t, func() bool {
		return strings.Contains(out.String(), "listening on")
	}, 2*time.Second, 10*time.Millisecond)

	cancel()
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return")
	}
}

func TestRun_healthEndpoints(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var out, stderr safeBuffer
	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, []string{"-listen", "127.0.0.1:0", "-health", "127.0.0.1:0"}, &out, &stderr)
	}()

	require.Eventually(t, func() bool {
		return strings.Contains(out.String(), "listening on") &&
			strings.Contains(stderr.String(), "addr=")
	}, 3*time.Second, 20*time.Millisecond)

	re := regexp.MustCompile(`addr=([0-9a-fA-F.:]+)`)
	m := re.FindStringSubmatch(stderr.String())
	require.Len(t, m, 2, "stderr=%q", stderr.String())

	client := &http.Client{Timeout: 2 * time.Second}
	base := "http://" + m[1]

	resp, err := client.Get(base + "/livez")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp2, err := client.Get(base + "/readyz")
	require.NoError(t, err)
	defer func() { _ = resp2.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp2.StatusCode)

	resp3, err := client.Get(base + "/metrics")
	require.NoError(t, err)
	defer func() { _ = resp3.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp3.StatusCode)
	var metrics map[string]int64
	require.NoError(t, json.NewDecoder(resp3.Body).Decode(&metrics))
	assert.Contains(t, metrics, "active_peers")
	assert.Contains(t, metrics, "replication_blobs_stored")

	cancel()
	<-errCh
}

func TestRun_putRequiresDial(t *testing.T) {
	var out bytes.Buffer
	err := run(context.Background(), []string{"-put-key", "alpha", "-put-data", "hello"}, &out, io.Discard)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "-put-key requires -dial")
}

func TestRun_replicatesBlobPutToDialPeer(t *testing.T) {
	serverCtx, serverCancel := context.WithCancel(context.Background())
	defer serverCancel()
	var serverOut, serverErr safeBuffer
	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- run(serverCtx, []string{"-listen", "127.0.0.1:0", "-replicate"}, &serverOut, &serverErr)
	}()

	require.Eventually(t, func() bool {
		return strings.Contains(serverOut.String(), "listening on")
	}, 3*time.Second, 20*time.Millisecond)

	re := regexp.MustCompile(`listening on ([^\n]+)`)
	m := re.FindStringSubmatch(serverOut.String())
	require.Len(t, m, 2, "stdout=%q", serverOut.String())

	clientCtx, clientCancel := context.WithCancel(context.Background())
	defer clientCancel()
	var clientOut, clientErr safeBuffer
	clientErrCh := make(chan error, 1)
	go func() {
		clientErrCh <- run(clientCtx, []string{
			"-listen", "127.0.0.1:0",
			"-dial", m[1],
			"-put-key", "alpha",
			"-put-data", "hello",
		}, &clientOut, &clientErr)
	}()

	require.Eventually(t, func() bool {
		logs := serverErr.String()
		return strings.Contains(logs, "replicated blob stored") &&
			strings.Contains(logs, "key=alpha") &&
			strings.Contains(logs, "bytes=5")
	}, 3*time.Second, 20*time.Millisecond, "server logs=%q client logs=%q", serverErr.String(), clientErr.String())

	clientCancel()
	serverCancel()
	require.NoError(t, <-clientErrCh)
	require.NoError(t, <-serverErrCh)
}
