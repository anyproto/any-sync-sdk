//go:build !js

package anysyncx

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/app/logger"
	"github.com/anyproto/any-sync/net/transport/quic"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/config"
)

const p2pServerCName = "sdk.p2p.server"

var p2pLog = logger.NewNamed("anysyncx.p2p")

// portFileName sits directly under DataDir, next to the anysync/ and
// files/ subdirs. Plain decimal text — same low-ceremony persistence as
// the nodeconf store path.
const portFileName = "p2p_port"

// quicListener is the slice of quic.Quic the server needs; an interface
// so tests can fake the transport without booting secureservice.
type quicListener interface {
	ListenAddrs(ctx context.Context, addrs ...string) ([]net.Addr, error)
}

// p2pServer owns the inbound QUIC listener for local-network peers.
// The SDK is otherwise dial-only; this is the one component that makes
// it reachable. Inbound connections need no extra plumbing — the quic
// accept loop hands them to peerservice.Accept which registers them in
// the pool.
//
// The listen port is sticky across restarts (persisted under DataDir)
// so already-distributed peer addresses stay valid as long as they can.
// A listen failure downgrades p2p instead of failing app start: Run
// logs and leaves started=false, discovery then never announces.
type p2pServer struct {
	cfg      config.P2P
	portFile string
	quic     quicListener

	mu      sync.Mutex
	port    int
	started bool
}

func newP2PServer(cfg config.P2P, dataDir string) *p2pServer {
	return &p2pServer{cfg: cfg, portFile: filepath.Join(dataDir, portFileName)}
}

func (s *p2pServer) Init(a *app.App) error {
	if s.quic == nil {
		s.quic = a.MustComponent(quic.CName).(quic.Quic)
	}
	return nil
}

func (s *p2pServer) Name() string { return p2pServerCName }

func (s *p2pServer) Run(ctx context.Context) error {
	if !s.cfg.IsEnabled() {
		return nil
	}
	if err := s.start(ctx); err != nil {
		p2pLog.InfoCtx(ctx, "p2p listener not started", zap.Error(err))
	}
	return nil
}

func (s *p2pServer) Close(_ context.Context) error { return nil }

// Port returns the bound listen port; zero when not started.
func (s *p2pServer) Port() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.port
}

// Started reports whether the QUIC listener is up.
func (s *p2pServer) Started() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.started
}

func (s *p2pServer) start(ctx context.Context) error {
	want := s.cfg.Port
	forced := want != 0
	if !forced {
		want = s.readSavedPort()
	}
	addrs, err := s.quic.ListenAddrs(ctx, "0.0.0.0:"+strconv.Itoa(want))
	if err != nil && !forced && want != 0 {
		// The persisted port is taken (another instance, another
		// process). Fall back to a fresh ephemeral port and persist
		// that instead.
		addrs, err = s.quic.ListenAddrs(ctx, "0.0.0.0:0")
	}
	if err != nil {
		return err
	}
	port, err := parseAddrPort(addrs[0].String())
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.port = port
	s.started = true
	s.mu.Unlock()
	if !forced && port != want {
		s.savePort(port)
	}
	return nil
}

func (s *p2pServer) readSavedPort() int {
	raw, err := os.ReadFile(s.portFile)
	if err != nil {
		return 0
	}
	port, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || port <= 0 || port > 65535 {
		return 0
	}
	return port
}

func (s *p2pServer) savePort(port int) {
	if err := os.WriteFile(s.portFile, []byte(strconv.Itoa(port)), 0o600); err != nil {
		p2pLog.Warn("persist p2p port", zap.Error(err))
	}
}

func parseAddrPort(addr string) (int, error) {
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(portStr)
}
