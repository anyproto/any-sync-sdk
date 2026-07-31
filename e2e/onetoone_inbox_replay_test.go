package e2e

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"
	"github.com/anyproto/any-sync-sdk/space"
)

// loadLocalNetworkYAML reads the local-infra nodeconf from
// ANYSYNC_E2E_LOCAL_NETWORK (skips the test when unset/unreadable).
func loadLocalNetworkYAML(t *testing.T) []byte {
	t.Helper()
	path := os.Getenv("ANYSYNC_E2E_LOCAL_NETWORK")
	if path == "" {
		t.Skip("set ANYSYNC_E2E_LOCAL_NETWORK to a local-infra nodeconf yml")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("local network config not readable at %s: %v", path, err)
	}
	return data
}

// proxyNodeconf rewrites every tree-node's address list to a local proxy
// port (one per tree node), leaving coordinator/consensus/file nodes
// untouched. Returns the rewritten yaml plus proxyAddr → realAddr pairs.
func proxyNodeconf(t *testing.T, data []byte, basePort int) (out []byte, routes map[string]string) {
	t.Helper()
	var conf map[string]any
	require.NoError(t, yaml.Unmarshal(data, &conf))
	nodes, ok := conf["nodes"].([]any)
	require.True(t, ok)
	routes = map[string]string{}
	port := basePort
	for _, n := range nodes {
		m, ok := n.(map[string]any)
		if !ok {
			continue
		}
		types, _ := m["types"].([]any)
		isTree := false
		for _, ty := range types {
			if ty == "tree" {
				isTree = true
			}
		}
		if !isTree {
			continue
		}
		addrs, _ := m["addresses"].([]any)
		require.NotEmpty(t, addrs)
		real, _ := addrs[0].(string)
		proxy := fmt.Sprintf("127.0.0.1:%d", port)
		port++
		routes[proxy] = real
		m["addresses"] = []any{proxy}
	}
	require.NotEmpty(t, routes, "no tree nodes in nodeconf")
	res, err := yaml.Marshal(conf)
	require.NoError(t, err)
	return res, routes
}

// startTCPProxies brings up plain TCP forwarders for routes. Returns a
// stop func. Until this is called, dials to the proxy addrs are refused.
func startTCPProxies(t *testing.T, routes map[string]string) (stop func()) {
	t.Helper()
	var lns []net.Listener
	var wg sync.WaitGroup
	for proxy, real := range routes {
		ln, err := net.Listen("tcp", proxy)
		require.NoError(t, err, "listen %s", proxy)
		lns = append(lns, ln)
		wg.Add(1)
		go func(ln net.Listener, real string) {
			defer wg.Done()
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				go func(c net.Conn) {
					defer c.Close()
					up, err := net.Dial("tcp", real)
					if err != nil {
						return
					}
					defer up.Close()
					go func() { _, _ = io.Copy(up, c); up.(*net.TCPConn).CloseWrite() }()
					_, _ = io.Copy(c, up)
				}(c)
			}
		}(ln, real)
	}
	return func() {
		for _, ln := range lns {
			_ = ln.Close()
		}
		wg.Wait()
	}
}

// capturedServedNodeconf opens a throwaway SDK against yml so the boot
// nodeconf refresh pulls the coordinator's current configuration, then
// reads it back from the nodeconf store (<netId>.yml under the data dir).
// Falls back to the input yml when no stored config appears (config
// already current).
func capturedServedNodeconf(t *testing.T, ctx context.Context, yml []byte) []byte {
	t.Helper()
	dir := t.TempDir()
	p2pOff := false
	cfg := config.Config{
		Storage: config.Storage{DataDir: dir, Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yml},
		P2P:     config.P2P{Enabled: &p2pOff},
	}
	prov := newFixedSeedProvider(t)
	sdk, err := anysyncsdk.Open(ctx, cfg, prov)
	require.NoError(t, err, "prime: Open")
	time.Sleep(3 * time.Second) // let the boot refresh land
	require.NoError(t, sdk.Close())

	var stored []byte
	_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".yml") {
			return nil
		}
		data, rerr := os.ReadFile(path)
		if rerr == nil && strings.Contains(string(data), "networkId") {
			stored = data
		}
		return nil
	})
	if stored == nil {
		t.Log("prime: no stored nodeconf found — using input yml as-is")
		return yml
	}
	t.Log("prime: using coordinator-served nodeconf")
	return stored
}

// TestE2E_OneToOne_ColdRestoreInboxReplay pins the cold-restore contract:
// a 1-1 accepted on any of the account's devices must never resurface as
// an incoming pending request on a fresh device. The dangerous shape is a
// cold restore (same account, fresh data dir) where the coordinator is
// reachable but the tree nodes are not yet: the synced inbox cursor reads
// "" because the space-index tree hasn't applied, and an ungated notifier
// would replay the WHOLE coordinator inbox — RegisterIncoming then
// resurrects long-accepted 1-1s as pending join requests until the synced
// remote=active rows finally merge in. The notifier's replay guard defers
// empty-cursor passes until the tech space has completed one clean sync
// round, so nothing may surface while the nodes are down, and the row
// must come up Active — never pending — once they return.
//
// Network shape: tree nodes sit behind local TCP proxies that start DOWN
// (connection refused) and are brought up mid-test.
// Coordinator/consensus stay direct.
func TestE2E_OneToOne_ColdRestoreInboxReplay(t *testing.T) {
	if testing.Short() {
		t.Skip("cold-restore repro needs a live local network; rerun without -short")
	}
	netYAML := loadLocalNetworkYAML(t)

	restore := spaceimpl.SetOneToOneInboxIntervalsForTest(time.Second, time.Second)
	defer restore()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	aliceProv := newFixedSeedProvider(t)
	bobProv := newFixedSeedProvider(t)

	// LAN p2p would let bob2 cold-restore straight from alice's process on
	// this same host, bypassing the tree nodes entirely — the report's
	// devices are not on one LAN, so switch it off.
	p2pOff := false
	open := func(name string, prov *fixedSeedProvider, nodeconf []byte) *anysyncsdk.SDK {
		t.Helper()
		cfg := config.Config{
			Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: nodeconf},
			P2P:     config.P2P{Enabled: &p2pOff},
		}
		sdk, err := anysyncsdk.Open(ctx, cfg, prov)
		require.NoError(t, err, "%s: Open", name)
		return sdk
	}

	// --- Seed: alice initiates via inbox, bob accepts, both active.
	alice := open("alice", aliceProv, netYAML)
	defer alice.Close()
	bob := open("bob", bobProv, netYAML)

	aliceSp, err := alice.Spaces().OneToOne(ctx, bob.Account().Id())
	require.NoError(t, err, "alice: OneToOne")
	oneToOneId := aliceSp.Id()
	t.Logf("1-1 space id: %s", oneToOneId)

	require.Eventually(t, func() bool {
		si, ok := infoByID(t, ctx, bob, oneToOneId)
		return ok && si.Status == space.StatusOneToOnePending
	}, 90*time.Second, time.Second, "bob: pending row never surfaced (coordinator inbox required)")

	_, err = bob.Spaces().AcceptOneToOne(ctx, oneToOneId)
	require.NoError(t, err, "bob: AcceptOneToOne")

	// Let bob push the accepted row (remote=active) + inbox cursor to the
	// sync nodes before the device disappears.
	time.Sleep(15 * time.Second)
	require.NoError(t, bob.Close(), "bob: Close")

	// --- Cold restore behind down proxies.
	// The SDK refreshes the nodeconf from the coordinator at every boot and
	// the served config replaces a stale local one (different id), undoing
	// any address rewrite. Base the rewrite on the coordinator-SERVED
	// config instead: prime a throwaway SDK, grab the stored <netId>.yml,
	// rewrite that — same id ⇒ the refresh no-ops and the proxy addrs hold.
	servedYAML := capturedServedNodeconf(t, ctx, netYAML)
	proxYAML, routes := proxyNodeconf(t, servedYAML, 34430)
	t.Logf("proxy routes: %v (down for phase 1)", routes)

	bob2 := open("bob2-cold", bobProv, proxYAML)
	defer bob2.Close()
	start := time.Now()

	// Phase 1: tree nodes down. The replay guard must hold the inbox back:
	// the 1-1 row must not surface as pending at any point (with the
	// 1s test cadence an ungated replay lands within ~2s; watch 12s).
	var pendingAt time.Duration = -1
	phase1 := time.Now().Add(12 * time.Second)
	for time.Now().Before(phase1) {
		if si, ok := infoByID(t, ctx, bob2, oneToOneId); ok {
			if si.Status == space.StatusOneToOnePending && pendingAt < 0 {
				pendingAt = time.Since(start)
				t.Logf("STALE PENDING: previously-accepted 1-1 resurfaced %.2fs after open", pendingAt.Seconds())
			}
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Phase 2: bring the tree nodes up; the synced active row must land and
	// the space must surface Active — never passing through pending.
	stop := startTCPProxies(t, routes)
	defer stop()
	upAt := time.Since(start)
	t.Logf("tree-node proxies up at %.2fs", upAt.Seconds())

	var activeAt time.Duration = -1
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		if si, ok := infoByID(t, ctx, bob2, oneToOneId); ok {
			if si.Status == space.StatusOneToOnePending && pendingAt < 0 {
				pendingAt = time.Since(start)
				t.Logf("STALE PENDING during catch-up %.2fs after open", pendingAt.Seconds())
			}
			if si.Status == space.StatusActive {
				activeAt = time.Since(start)
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.GreaterOrEqual(t, activeAt, time.Duration(0), "row never converged to Active after tree nodes came up")
	t.Logf("Active at %.2fs (%.2fs after proxies up)", activeAt.Seconds(), (activeAt - upAt).Seconds())

	require.Negative(t, pendingAt,
		"previously-accepted 1-1 resurfaced as an incoming pending request on a cold-restored device")
}
