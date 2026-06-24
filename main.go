package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/AliSinaDevelo/StreamHive/internal/version"
	"github.com/AliSinaDevelo/StreamHive/p2p"
	"github.com/AliSinaDevelo/StreamHive/replication"
	"github.com/AliSinaDevelo/StreamHive/storage"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("streamhive", flag.ContinueOnError)
	fs.SetOutput(stderr)

	listen := fs.String("listen", "127.0.0.1:0", "TCP listen address")
	dial := fs.String("dial", "", "optional peer host:port to dial after listen")
	peers := fs.String("peers", "", "comma-separated peer host:port list to dial after listen")
	peerReconnect := fs.Bool("peer-reconnect", false, "keep retrying -peers with exponential backoff")
	peerReconnectMin := fs.Duration("peer-reconnect-min", 500*time.Millisecond, "minimum reconnect backoff for -peer-reconnect")
	peerReconnectMax := fs.Duration("peer-reconnect-max", 30*time.Second, "maximum reconnect backoff for -peer-reconnect")
	health := fs.String("health", "", "optional HTTP listen addr for /livez /readyz /metrics (e.g. :8080)")
	maxPeers := fs.Int("max-peers", 0, "max simultaneous peers (0 = unlimited)")
	dialTimeout := fs.Duration("dial-timeout", 0, "default dial timeout (0 = use context only)")
	readIdle := fs.Duration("read-idle-timeout", 0, "TCP read deadline refresh for peer loops (0 = none for discard mode)")
	showVer := fs.Bool("version", false, "print version and exit")
	replicate := fs.Bool("replicate", false, "enable in-memory blob replication from framed peers")
	storeDir := fs.String("store-dir", "", "directory for durable replicated blobs (requires -replicate)")
	putKey := fs.String("put-key", "", "send one replicated blob key to -dial peer")
	putData := fs.String("put-data", "", "send one replicated blob value to -dial peer")
	exitAfterPut := fs.Bool("exit-after-put", false, "exit after sending -put-key to the dialed peer")
	maxBlobBytes := fs.Int("max-blob-bytes", replication.DefaultMaxDataBytes, "max replicated blob payload bytes")

	tlsCert := fs.String("tls-cert", "", "path to PEM certificate (enables TLS on listener)")
	tlsKey := fs.String("tls-key", "", "path to PEM private key for -tls-cert")
	tlsCA := fs.String("tls-ca", "", "optional path to PEM CA bundle for outbound TLS")
	tlsServerName := fs.String("tls-server-name", "", "SNI / cert verification name for outbound TLS")
	insecureSkip := fs.Bool("tls-insecure-skip-verify", false, "skip TLS verify on outbound (dev only)")

	if err := fs.Parse(args); err != nil {
		return err
	}

	log := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if *showVer {
		_, err := fmt.Fprintln(stdout, version.Version)
		return err
	}
	dialTarget := strings.TrimSpace(*dial)
	peerList, err := parsePeerList(*peers)
	if err != nil {
		return err
	}
	peerTargets := combinePeerTargets(dialTarget, peerList)
	if *putKey != "" && len(peerTargets) == 0 {
		return fmt.Errorf("replication: -put-key requires -dial or -peers")
	}
	if *peerReconnect {
		if len(peerList) == 0 {
			return fmt.Errorf("peers: -peer-reconnect requires -peers")
		}
		if *putKey != "" {
			return fmt.Errorf("replication: -peer-reconnect cannot be combined with -put-key")
		}
		if err := validateReconnectBackoff(*peerReconnectMin, *peerReconnectMax); err != nil {
			return err
		}
	}
	if *storeDir != "" && !*replicate {
		return fmt.Errorf("storage: -store-dir requires -replicate")
	}

	replLimits := replication.Limits{MaxDataBytes: *maxBlobBytes}
	var blobStore storage.BlobStore
	var memoryStore *storage.MemoryStore
	replMetrics := &replicationMetrics{}
	if *replicate {
		if *storeDir != "" {
			var err error
			blobStore, err = storage.NewFileStore(*storeDir)
			if err != nil {
				return fmt.Errorf("storage: open file store: %w", err)
			}
		} else {
			memoryStore = storage.NewMemoryStore()
			blobStore = memoryStore
		}
	}
	var putPayload []byte
	var putResult chan error
	if *putKey != "" {
		putPayload, err = replication.EncodeBlobPut([]byte(*putKey), []byte(*putData), replLimits)
		if err != nil {
			return err
		}
		putResult = make(chan error, len(peerTargets))
	}

	tr := p2p.NewTCPTransport(*listen)
	tr.Logger = log
	tr.MaxPeers = *maxPeers
	tr.DialTimeout = *dialTimeout
	tr.ReadIdleTimeout = *readIdle
	tr.OnPeer = func(peer p2p.Peer) {
		log.Info("peer", "remote", peer.RemoteAddr().String(), "outbound", peer.IsOutbound())
		if putPayload == nil || !peer.IsOutbound() {
			return
		}
		tcpPeer, ok := peer.(*p2p.TCPPeer)
		if !ok {
			err := errors.New("peer is not TCP-backed")
			reportPutResult(putResult, err)
			log.Error("replication send", "err", err)
			_ = peer.Close()
			return
		}
		if err := tcpPeer.WriteFrame(putPayload, tr.MaxFrameBytes); err != nil {
			replMetrics.SendErrors.Add(1)
			reportPutResult(putResult, err)
			log.Error("replication send", "remote", peer.RemoteAddr().String(), "err", err)
			_ = peer.Close()
			return
		}
		replMetrics.BlobsSent.Add(1)
		replMetrics.BytesSent.Add(uint64(len(*putData)))
		reportPutResult(putResult, nil)
		log.Info("replicated blob sent", "remote", peer.RemoteAddr().String(), "key", *putKey, "bytes", len(*putData))
	}
	if blobStore != nil {
		tr.FrameHandler = func(ctx context.Context, peer p2p.Peer, payload []byte) error {
			msg, err := replication.Decode(payload, replLimits)
			if err != nil {
				replMetrics.ApplyErrors.Add(1)
				return err
			}
			if err := blobStore.Put(ctx, msg.Key, msg.Data); err != nil {
				replMetrics.ApplyErrors.Add(1)
				return err
			}
			replMetrics.BlobsStored.Add(1)
			replMetrics.BytesStored.Add(uint64(len(msg.Data)))
			attrs := []any{"remote", peer.RemoteAddr().String(), "key", string(msg.Key), "bytes", len(msg.Data)}
			if memoryStore != nil {
				attrs = append(attrs, "blobs", memoryStore.Len())
			}
			log.Info("replicated blob stored", attrs...)
			return nil
		}
	}
	var reconnector *peerReconnector
	if *peerReconnect {
		reconnector = newPeerReconnector(ctx, tr, peerList, *peerReconnectMin, *peerReconnectMax, log)
		tr.OnPeerDisconnected = reconnector.OnPeerDisconnected
	}

	if *tlsCert != "" || *tlsKey != "" {
		if *tlsCert == "" || *tlsKey == "" {
			return fmt.Errorf("tls: both -tls-cert and -tls-key are required")
		}
		cert, err := tls.LoadX509KeyPair(*tlsCert, *tlsKey)
		if err != nil {
			return fmt.Errorf("tls: load server cert: %w", err)
		}
		tr.TLSServerConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		}
	}

	if len(peerTargets) > 0 && (*tlsCA != "" || *insecureSkip || *tlsServerName != "") {
		cfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if *insecureSkip {
			cfg.InsecureSkipVerify = true
		}
		if *tlsServerName != "" {
			cfg.ServerName = *tlsServerName
		}
		if *tlsCA != "" {
			pool := x509.NewCertPool()
			data, err := os.ReadFile(*tlsCA)
			if err != nil {
				return fmt.Errorf("tls: read ca: %w", err)
			}
			if !pool.AppendCertsFromPEM(data) {
				return fmt.Errorf("tls: no certificates parsed from -tls-ca")
			}
			cfg.RootCAs = pool
		}
		tr.TLSClientConfig = cfg
	}

	if err := tr.ListenAndAccept(ctx); err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer func() {
		_ = tr.Close()
	}()

	addr := tr.Addr()
	if addr == nil {
		return errors.New("no listen address")
	}
	if _, err := fmt.Fprintf(stdout, "listening on %s\n", addr.String()); err != nil {
		return err
	}

	if dialTarget != "" {
		if err := tr.Dial(ctx, dialTarget); err != nil {
			return fmt.Errorf("dial %s: %w", dialTarget, err)
		}
	}
	if reconnector != nil {
		reconnector.Start()
	} else {
		for _, target := range peerList {
			if err := tr.Dial(ctx, target); err != nil {
				return fmt.Errorf("dial %s: %w", target, err)
			}
		}
	}
	if *exitAfterPut && putResult != nil {
		for range peerTargets {
			select {
			case err := <-putResult:
				if err != nil {
					return fmt.Errorf("replication: send blob: %w", err)
				}
			case <-ctx.Done():
				if errors.Is(ctx.Err(), context.Canceled) {
					return nil
				}
				return ctx.Err()
			case <-time.After(5 * time.Second):
				return fmt.Errorf("replication: timed out waiting for blob send")
			}
		}
		return nil
	}

	var hsrv *http.Server
	if *health != "" {
		var err error
		hsrv, err = startHealth(*health, tr, replMetrics, log)
		if err != nil {
			return fmt.Errorf("health: %w", err)
		}
		defer func() {
			shctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_ = hsrv.Shutdown(shctx)
		}()
	}

	<-ctx.Done()
	if errors.Is(ctx.Err(), context.Canceled) {
		return nil
	}
	return ctx.Err()
}

func reportPutResult(ch chan<- error, err error) {
	if ch == nil {
		return
	}
	select {
	case ch <- err:
	default:
	}
}

type peerReconnector struct {
	ctx    context.Context
	tr     *p2p.TCPTransport
	min    time.Duration
	max    time.Duration
	log    *slog.Logger
	target map[string]struct{}

	mu      sync.Mutex
	dialing map[string]bool
}

func newPeerReconnector(ctx context.Context, tr *p2p.TCPTransport, targets []string, minBackoff, maxBackoff time.Duration, log *slog.Logger) *peerReconnector {
	targetMap := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		targetMap[target] = struct{}{}
	}
	return &peerReconnector{
		ctx:     ctx,
		tr:      tr,
		min:     minBackoff,
		max:     maxBackoff,
		log:     log,
		target:  targetMap,
		dialing: make(map[string]bool, len(targets)),
	}
}

func (r *peerReconnector) Start() {
	for target := range r.target {
		r.schedule(target, 0)
	}
}

func (r *peerReconnector) OnPeerDisconnected(peer p2p.Peer) {
	if !peer.IsOutbound() {
		return
	}
	target := peer.RemoteAddr().String()
	if _, ok := r.target[target]; !ok {
		return
	}
	r.schedule(target, r.min)
}

func (r *peerReconnector) schedule(target string, initialDelay time.Duration) {
	r.mu.Lock()
	if r.dialing[target] {
		r.mu.Unlock()
		return
	}
	r.dialing[target] = true
	r.mu.Unlock()

	go r.loop(target, initialDelay)
}

func (r *peerReconnector) loop(target string, initialDelay time.Duration) {
	defer func() {
		r.mu.Lock()
		delete(r.dialing, target)
		r.mu.Unlock()
	}()

	delay := r.min
	if initialDelay > 0 {
		if !sleepContext(r.ctx, initialDelay) {
			return
		}
	}

	for {
		if err := r.ctx.Err(); err != nil {
			return
		}
		err := r.tr.Dial(r.ctx, target)
		if err == nil {
			r.log.Info("peer reconnect established", "target", target)
			return
		}
		r.log.Warn("peer reconnect failed", "target", target, "err", err, "next", delay)
		if !sleepContext(r.ctx, delay) {
			return
		}
		delay = nextBackoff(delay, r.max)
	}
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextBackoff(current, maxDelay time.Duration) time.Duration {
	next := current * 2
	if next <= 0 || next > maxDelay {
		return maxDelay
	}
	return next
}

func parsePeerTargets(dial, peers string) ([]string, error) {
	peerList, err := parsePeerList(peers)
	if err != nil {
		return nil, err
	}
	return combinePeerTargets(strings.TrimSpace(dial), peerList), nil
}

func parsePeerList(peers string) ([]string, error) {
	if strings.TrimSpace(peers) == "" {
		return nil, nil
	}
	targets := make([]string, 0, 1)
	for _, part := range strings.Split(peers, ",") {
		target := strings.TrimSpace(part)
		if target == "" {
			return nil, fmt.Errorf("peers: empty peer address")
		}
		targets = append(targets, target)
	}
	return targets, nil
}

func combinePeerTargets(dial string, peers []string) []string {
	targets := make([]string, 0, len(peers)+1)
	if dial != "" {
		targets = append(targets, dial)
	}
	targets = append(targets, peers...)
	return targets
}

func validateReconnectBackoff(minBackoff, maxBackoff time.Duration) error {
	if minBackoff <= 0 {
		return fmt.Errorf("peers: -peer-reconnect-min must be greater than zero")
	}
	if maxBackoff < minBackoff {
		return fmt.Errorf("peers: -peer-reconnect-max must be greater than or equal to -peer-reconnect-min")
	}
	return nil
}

type replicationMetrics struct {
	BlobsStored atomic.Uint64
	BytesStored atomic.Uint64
	ApplyErrors atomic.Uint64
	BlobsSent   atomic.Uint64
	BytesSent   atomic.Uint64
	SendErrors  atomic.Uint64
}

func (m *replicationMetrics) Snapshot() map[string]int64 {
	if m == nil {
		return map[string]int64{}
	}
	return map[string]int64{
		"replication_blobs_stored": int64(m.BlobsStored.Load()),
		"replication_bytes_stored": int64(m.BytesStored.Load()),
		"replication_apply_errors": int64(m.ApplyErrors.Load()),
		"replication_blobs_sent":   int64(m.BlobsSent.Load()),
		"replication_bytes_sent":   int64(m.BytesSent.Load()),
		"replication_send_errors":  int64(m.SendErrors.Load()),
	}
}

func startHealth(addr string, tr *p2p.TCPTransport, replMetrics *replicationMetrics, log *slog.Logger) (*http.Server, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !tr.Ready() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		snapshot := tr.Metrics().Snapshot()
		for key, value := range replMetrics.Snapshot() {
			snapshot[key] = value
		}
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(snapshot)
	})

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Info("health", "addr", ln.Addr().String())
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("health server", "err", err)
		}
	}()
	return srv, nil
}
