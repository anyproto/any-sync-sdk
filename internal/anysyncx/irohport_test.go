//go:build !js

package anysyncx

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/anyproto/any-sync/net/transport"
	"github.com/anyproto/any-sync/net/transport/iroh"
	"github.com/anyproto/any-sync/nodeconf"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/config"
)

// fakeLocalAddr stands in for the iroh endpoint's bound address.
type fakeLocalAddr netip.AddrPort

func (f fakeLocalAddr) LocalAddr() netip.AddrPort { return netip.AddrPort(f) }

func boundAt(port uint16) fakeLocalAddr {
	return fakeLocalAddr(netip.AddrPortFrom(netip.IPv6Unspecified(), port))
}

func globalOnConfig(dataDir string, port int) config.Config {
	on := true
	return config.Config{
		Storage: config.Storage{DataDir: dataDir},
		P2P: config.P2P{Global: config.GlobalP2P{
			Enabled:   &on,
			RelayURLs: []string{"https://relay.invalid"},
			Port:      port,
		}},
	}
}

func readIrohPortFile(t *testing.T, dir string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, irohPortFileName))
	require.NoError(t, err)
	return string(raw)
}

func TestIrohPortPersistsAndIsPreferred(t *testing.T) {
	dir := t.TempDir()
	cfg := globalOnConfig(dir, 0)

	adapter := newConfig(cfg, nodeconf.Configuration{})
	conf := adapter.GetIroh()
	require.Empty(t, conf.BindAddr, "first start binds an ephemeral port")
	require.False(t, conf.BindFallback)

	adapter.keepIrohPort(boundAt(40001))
	require.Equal(t, "40001", readIrohPortFile(t, dir))

	conf = adapter.GetIroh()
	require.Equal(t, "[::]:40001", conf.BindAddr)
	require.True(t, conf.BindFallback, "a persisted port is a preference")

	// The preferred port was taken and the endpoint fell back: the
	// fresh port is what the next start prefers.
	adapter.keepIrohPort(boundAt(40002))
	require.Equal(t, "40002", readIrohPortFile(t, dir))
}

func TestIrohPortForcedIsNeverWritten(t *testing.T) {
	dir := t.TempDir()
	adapter := newConfig(globalOnConfig(dir, 4321), nodeconf.Configuration{})

	adapter.keepIrohPort(boundAt(4321))
	_, err := os.Stat(filepath.Join(dir, irohPortFileName))
	require.ErrorIs(t, err, os.ErrNotExist)

	// A persisted port never overrides a configured one.
	require.NoError(t, os.WriteFile(filepath.Join(dir, irohPortFileName), []byte("40001"), 0o600))
	conf := adapter.GetIroh()
	require.Equal(t, "[::]:4321", conf.BindAddr)
	require.False(t, conf.BindFallback, "a configured port gets one attempt")
	adapter.keepIrohPort(boundAt(4321))
	require.Equal(t, "40001", readIrohPortFile(t, dir))
}

// The LAN listener binds after the iroh endpoint and gets one attempt at
// a configured port, so a remembered iroh port equal to it is dropped.
func TestIrohPortLeavesTheLANPort(t *testing.T) {
	remembered := func(t *testing.T, dir string) {
		t.Helper()
		require.NoError(t, os.WriteFile(filepath.Join(dir, irohPortFileName), []byte("40001"), 0o600))
	}
	t.Run("configured LAN port", func(t *testing.T) {
		dir := t.TempDir()
		remembered(t, dir)
		cfg := globalOnConfig(dir, 0)
		cfg.P2P.Port = 40001
		conf := newConfig(cfg, nodeconf.Configuration{}).GetIroh()
		require.Empty(t, conf.BindAddr)
		require.False(t, conf.BindFallback)
	})
	t.Run("remembered LAN port", func(t *testing.T) {
		dir := t.TempDir()
		remembered(t, dir)
		require.NoError(t, os.WriteFile(filepath.Join(dir, portFileName), []byte("40001"), 0o600))
		conf := newConfig(globalOnConfig(dir, 0), nodeconf.Configuration{}).GetIroh()
		require.Empty(t, conf.BindAddr)
	})
	t.Run("LAN off", func(t *testing.T) {
		dir := t.TempDir()
		remembered(t, dir)
		cfg := globalOnConfig(dir, 0)
		off := false
		cfg.P2P.Enabled = &off
		cfg.P2P.Port = 40001
		conf := newConfig(cfg, nodeconf.Configuration{}).GetIroh()
		require.Equal(t, "[::]:40001", conf.BindAddr)
	})
}

func TestPortFilesWithoutDataDir(t *testing.T) {
	t.Chdir(t.TempDir())
	require.NoError(t, os.WriteFile(irohPortFileName, []byte("40001"), 0o600))
	adapter := newConfig(globalOnConfig("", 0), nodeconf.Configuration{})
	require.Empty(t, adapter.GetIroh().BindAddr)
	adapter.keepIrohPort(boundAt(40002))
	raw, err := os.ReadFile(irohPortFileName)
	require.NoError(t, err)
	require.Equal(t, "40001", string(raw))
	require.Empty(t, newP2PServer(config.P2P{}, "").portFile)
}

func TestIrohPortIgnoresGarbageFile(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, irohPortFileName), []byte("not-a-port"), 0o600))
	conf := newConfig(globalOnConfig(dir, 0), nodeconf.Configuration{}).GetIroh()
	require.Empty(t, conf.BindAddr)
	require.False(t, conf.BindFallback)
}

// The real transport end to end: the port survives a restart, and a
// taken port falls back to a fresh one that the next start prefers.
func TestIrohPortStickyAcrossRestarts(t *testing.T) {
	yaml, err := os.ReadFile(filepath.Join("..", "..", "e2e", "local.yml"))
	if err != nil {
		t.Skipf("nodeconf not available: %v", err)
	}
	dir := t.TempDir()
	_, accPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	_, devPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	keys := &staticKeys{account: accPriv, device: devPriv}

	lanOff := false
	cfg := globalOnConfig(dir, 0)
	cfg.Storage.Topology = config.StorageShared
	cfg.Network = config.Network{NodeConfYAML: yaml}
	cfg.P2P.Enabled = &lanOff
	cfg.P2P.Global.RelayURLs = []string{"http://127.0.0.1:1"}
	cfg.P2P.Global.InsecureRelay = true

	start := func() uint16 {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		a, err := New(ctx, cfg, keys)
		require.NoError(t, err)
		port := a.a.MustComponent(transport.IrohCName).(iroh.Iroh).LocalAddr().Port()
		require.NoError(t, a.Close(context.Background()))
		return port
	}

	first := start()
	require.NotZero(t, first)
	require.Equal(t, strconv.Itoa(int(first)), readIrohPortFile(t, dir))
	require.Equal(t, first, start(), "the persisted port is reused")

	hog, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv6unspecified, Port: int(first)})
	require.NoError(t, err)
	defer hog.Close()
	fallback := start()
	require.NotZero(t, fallback)
	require.NotEqual(t, first, fallback)
	require.Equal(t, strconv.Itoa(int(fallback)), readIrohPortFile(t, dir))
}

type staticKeys struct{ account, device []byte }

func (k *staticKeys) AccountKey(context.Context) ([]byte, error) { return k.account, nil }
func (k *staticKeys) DeviceKey(context.Context) ([]byte, error)  { return k.device, nil }
