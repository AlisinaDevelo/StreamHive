package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AliSinaDevelo/StreamHive/p2p"
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
	assert.Contains(t, err.Error(), "-put-key requires -dial or -peers")
}

func TestRun_peerReconnectRequiresPeers(t *testing.T) {
	var out bytes.Buffer
	err := run(context.Background(), []string{"-peer-reconnect"}, &out, io.Discard)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "-peer-reconnect requires -peers")
}

func TestRun_peerReconnectRejectsOneShotPut(t *testing.T) {
	var out bytes.Buffer
	err := run(context.Background(), []string{
		"-peers", "127.0.0.1:1",
		"-peer-reconnect",
		"-put-key", "alpha",
		"-put-data", "hello",
	}, &out, io.Discard)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be combined with -put-key")
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
			"-exit-after-put",
		}, &clientOut, &clientErr)
	}()

	require.Eventually(t, func() bool {
		logs := serverErr.String()
		return strings.Contains(logs, "replicated blob stored") &&
			strings.Contains(logs, "key=alpha") &&
			strings.Contains(logs, "bytes=5")
	}, 3*time.Second, 20*time.Millisecond, "server logs=%q client logs=%q", serverErr.String(), clientErr.String())

	serverCancel()
	require.NoError(t, <-clientErrCh)
	require.NoError(t, <-serverErrCh)
}

func TestParsePeerTargets(t *testing.T) {
	tests := []struct {
		name    string
		dial    string
		peers   string
		want    []string
		wantErr bool
	}{
		{
			name: "single dial",
			dial: " 127.0.0.1:7070 ",
			want: []string{"127.0.0.1:7070"},
		},
		{
			name:  "peer list",
			peers: "127.0.0.1:7070, 127.0.0.1:7071",
			want:  []string{"127.0.0.1:7070", "127.0.0.1:7071"},
		},
		{
			name:  "dial plus peer list",
			dial:  "127.0.0.1:7070",
			peers: "127.0.0.1:7071",
			want:  []string{"127.0.0.1:7070", "127.0.0.1:7071"},
		},
		{
			name:    "empty peer entry",
			peers:   "127.0.0.1:7070,",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePeerTargets(tt.dial, tt.peers)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestValidateReconnectBackoff(t *testing.T) {
	assert.NoError(t, validateReconnectBackoff(10*time.Millisecond, 20*time.Millisecond))
	assert.Error(t, validateReconnectBackoff(0, 20*time.Millisecond))
	assert.Error(t, validateReconnectBackoff(20*time.Millisecond, 10*time.Millisecond))
}

func TestPeerReconnector_dialsWhenPeerAppears(t *testing.T) {
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := reserved.Addr().String()
	require.NoError(t, reserved.Close())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := p2p.NewTCPTransport("127.0.0.1:0")
	client.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	client.DialTimeout = 20 * time.Millisecond
	require.NoError(t, client.ListenAndAccept(ctx))
	defer func() { _ = client.Close() }()

	reconnector := newPeerReconnector(
		ctx,
		client,
		[]string{addr},
		10*time.Millisecond,
		20*time.Millisecond,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
	reconnector.Start()

	var seen atomic.Int32
	server := p2p.NewTCPTransport(addr)
	server.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	server.OnPeer = func(p2p.Peer) {
		seen.Add(1)
	}
	require.NoError(t, server.ListenAndAccept(ctx))
	defer func() { _ = server.Close() }()

	require.Eventually(t, func() bool {
		return seen.Load() == 1
	}, 3*time.Second, 20*time.Millisecond)
}
