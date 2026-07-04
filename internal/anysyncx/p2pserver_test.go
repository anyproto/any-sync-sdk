//go:build !js

package anysyncx

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/config"
)

// fakeQuic binds real UDP sockets so port collisions behave like the
// live transport, but skips the QUIC handshake stack entirely.
type fakeQuic struct {
	conns []*net.UDPConn
}

func (f *fakeQuic) ListenAddrs(_ context.Context, addrs ...string) ([]net.Addr, error) {
	var out []net.Addr
	for _, a := range addrs {
		udpAddr, err := net.ResolveUDPAddr("udp4", a)
		if err != nil {
			return nil, err
		}
		conn, err := net.ListenUDP("udp4", udpAddr)
		if err != nil {
			return nil, err
		}
		f.conns = append(f.conns, conn)
		out = append(out, conn.LocalAddr())
	}
	return out, nil
}

func (f *fakeQuic) closeAll() {
	for _, c := range f.conns {
		_ = c.Close()
	}
	f.conns = nil
}

func newTestP2PServer(t *testing.T, dir string, cfg config.P2P) (*p2pServer, *fakeQuic) {
	t.Helper()
	fq := &fakeQuic{}
	t.Cleanup(fq.closeAll)
	s := newP2PServer(cfg, dir)
	s.quic = fq
	return s, fq
}

func TestP2PServerPersistsAndReusesPort(t *testing.T) {
	dir := t.TempDir()

	first, fq := newTestP2PServer(t, dir, config.P2P{})
	require.NoError(t, first.Run(context.Background()))
	require.True(t, first.Started())
	port := first.Port()
	require.Greater(t, port, 0)

	raw, err := os.ReadFile(filepath.Join(dir, portFileName))
	require.NoError(t, err)
	require.Equal(t, strconv.Itoa(port), string(raw))

	// Release the socket, then a second instance must come up on the
	// same persisted port.
	fq.closeAll()
	second, _ := newTestP2PServer(t, dir, config.P2P{})
	require.NoError(t, second.Run(context.Background()))
	require.True(t, second.Started())
	require.Equal(t, port, second.Port())
}

func TestP2PServerFallsBackWhenSavedPortTaken(t *testing.T) {
	dir := t.TempDir()

	first, _ := newTestP2PServer(t, dir, config.P2P{})
	require.NoError(t, first.Run(context.Background()))
	port := first.Port()

	// Saved port still held by the first instance: the second must pick
	// a fresh ephemeral port and persist it.
	second, _ := newTestP2PServer(t, dir, config.P2P{})
	require.NoError(t, second.Run(context.Background()))
	require.True(t, second.Started())
	require.NotEqual(t, port, second.Port())

	raw, err := os.ReadFile(filepath.Join(dir, portFileName))
	require.NoError(t, err)
	require.Equal(t, strconv.Itoa(second.Port()), string(raw))
}

func TestP2PServerForcedPort(t *testing.T) {
	dir := t.TempDir()

	// Find a free port, release it, then force it.
	probe, fq := newTestP2PServer(t, dir, config.P2P{})
	require.NoError(t, probe.Run(context.Background()))
	forced := probe.Port()
	fq.closeAll()

	s, _ := newTestP2PServer(t, dir, config.P2P{Port: forced})
	require.NoError(t, s.Run(context.Background()))
	require.True(t, s.Started())
	require.Equal(t, forced, s.Port())
	// Forced ports are config-owned: nothing is persisted on their
	// account. The probe's ephemeral port is what remains in the file.
	raw, err := os.ReadFile(filepath.Join(dir, portFileName))
	require.NoError(t, err)
	require.Equal(t, strconv.Itoa(forced), string(raw)) // probe persisted the same value

	// A forced port that cannot bind leaves the server down (no
	// ephemeral fallback) without failing Run.
	conflict, _ := newTestP2PServer(t, dir, config.P2P{Port: forced})
	require.NoError(t, conflict.Run(context.Background()))
	require.False(t, conflict.Started())
	require.Zero(t, conflict.Port())
}

func TestP2PServerDisabled(t *testing.T) {
	disabled := false
	s, fq := newTestP2PServer(t, t.TempDir(), config.P2P{Enabled: &disabled})
	require.NoError(t, s.Run(context.Background()))
	require.False(t, s.Started())
	require.Zero(t, s.Port())
	require.Empty(t, fq.conns)
}

func TestP2PServerIgnoresGarbagePortFile(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, portFileName), []byte("not-a-port"), 0o600))
	s, _ := newTestP2PServer(t, dir, config.P2P{})
	require.NoError(t, s.Run(context.Background()))
	require.True(t, s.Started())
	require.Greater(t, s.Port(), 0)
}
