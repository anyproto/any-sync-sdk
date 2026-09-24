//go:build !js

package anysyncx

import (
	"context"
	"net"
	"strconv"
	"sync"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/net/transport/quic"
	"github.com/anyproto/any-sync/net/transport/yamux"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/config"
)

const p2pServerCName = "sdk.p2p.server"

// quicListener is the slice of quic.Quic the server needs; an interface
// so tests can fake the transport without booting secureservice.
type quicListener interface {
	ListenAddrs(ctx context.Context, addrs ...string) ([]net.Addr, error)
}

// yamuxRegistrar is the slice of yamux.Yamux the server needs; an
// interface so tests can fake the transport without booting
// secureservice. The registered listener's accept loop starts
// immediately and its lifetime is owned by the transport.
type yamuxRegistrar interface {
	AddListener(lis net.Listener)
}

// maxPortAttempts bounds the fresh-port retries when one side of a
// picked port pair (TCP or its UDP twin) is occupied by another process.
const maxPortAttempts = 5

// p2pServer owns the inbound listeners for local-network peers — TCP
// (yamux) and QUIC bound on the SAME port. The SDK is otherwise
// dial-only; this is the one component that makes it reachable. Inbound
// connections need no extra plumbing — both transports' accept loops
// hand them to peerservice.Accept which registers them in the pool.
//
// Both transports listen so peers can dial yamux first: a dead LAN peer
// answers a TCP dial with an RST in one RTT, while a QUIC dial has to
// wait out the whole handshake timeout (quic-go gets no ICMP feedback
// on its unconnected dial sockets). QUIC stays bound for peers that
// still dial quic-only.
//
// The listen port is sticky across restarts (persisted under DataDir)
// so already-distributed peer addresses stay valid as long as they can;
// when either side of the pair is taken, fresh pairs are retried. A
// listen failure downgrades p2p instead of failing app start: Run warns
// and leaves started=false, discovery then never announces.
type p2pServer struct {
	cfg      config.P2P
	portFile string
	quic     quicListener
	yamux    yamuxRegistrar

	mu      sync.Mutex
	port    int
	started bool
}

func newP2PServer(cfg config.P2P, dataDir string) *p2pServer {
	return &p2pServer{cfg: cfg, portFile: portFilePath(dataDir, portFileName)}
}

func (s *p2pServer) Init(a *app.App) error {
	if s.quic == nil {
		s.quic = a.MustComponent(quic.CName).(quic.Quic)
	}
	if s.yamux == nil {
		s.yamux = a.MustComponent(yamux.CName).(yamux.Yamux)
	}
	return nil
}

func (s *p2pServer) Name() string { return p2pServerCName }

func (s *p2pServer) Run(ctx context.Context) error {
	if !s.cfg.IsEnabled() {
		return nil
	}
	if err := s.start(ctx); err != nil {
		p2pLog.WarnCtx(ctx, "p2p listeners not started", zap.Error(err))
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

// Started reports whether the listeners are up.
func (s *p2pServer) Started() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.started
}

func (s *p2pServer) start(ctx context.Context) error {
	want := s.cfg.Port
	forced := want != 0
	if !forced {
		want = readPortFile(s.portFile)
	}
	// A forced port is config-owned: one attempt, no fallback. Sticky /
	// ephemeral ports retry fresh pairs when either side is occupied
	// (another instance holds the TCP port, or an unrelated process sits
	// on the UDP twin of a freshly-picked TCP port).
	attempts := maxPortAttempts
	if forced {
		attempts = 1
	}
	var lastErr error
	for i := 0; i < attempts; i++ {
		port, err := s.bindPair(ctx, want)
		if err != nil {
			lastErr = err
			want = 0
			continue
		}
		s.mu.Lock()
		s.port = port
		s.started = true
		s.mu.Unlock()
		if !forced {
			savePortFile(s.portFile, port)
		}
		return nil
	}
	return lastErr
}

// bindPair brings up both listeners on one port: the TCP listener picks
// it (want may be 0 = ephemeral), QUIC must then bind its UDP twin. On
// the QUIC side failing the TCP listener is released so the next
// attempt starts clean; on success the TCP listener is handed to the
// yamux transport, which owns it from then on.
func (s *p2pServer) bindPair(ctx context.Context, want int) (int, error) {
	tcp, err := net.Listen("tcp", "0.0.0.0:"+strconv.Itoa(want))
	if err != nil {
		return 0, err
	}
	port, err := parseAddrPort(tcp.Addr().String())
	if err != nil {
		_ = tcp.Close()
		return 0, err
	}
	if _, err = s.quic.ListenAddrs(ctx, "0.0.0.0:"+strconv.Itoa(port)); err != nil {
		_ = tcp.Close()
		return 0, err
	}
	s.yamux.AddListener(tcp)
	return port, nil
}

func parseAddrPort(addr string) (int, error) {
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(portStr)
}
