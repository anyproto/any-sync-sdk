package e2e

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/anyproto/any-sync/nodeconf"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"
	"github.com/anyproto/any-sync-sdk/space"
)

// nodeConfUnreachable rewrites the addresses of every node of the given
// types (all nodes when none are given) to a closed local port. The
// configuration id is kept, so the coordinator never hands back a newer
// configuration that would restore the real addresses.
func nodeConfUnreachable(t *testing.T, in []byte, types ...nodeconf.NodeType) []byte {
	t.Helper()
	var c nodeconf.Configuration
	require.NoError(t, yaml.Unmarshal(in, &c))
	for i, n := range c.Nodes {
		hit := len(types) == 0
		for _, nt := range types {
			hit = hit || n.HasType(nt)
		}
		if hit {
			c.Nodes[i].Addresses = []string{"127.0.0.1:1"}
		}
	}
	out, err := yaml.Marshal(c)
	require.NoError(t, err)
	return out
}

// forgetStoredNodeConf removes the configuration a previous session may
// have saved from the coordinator, so the next Open runs on the YAML it
// is given rather than on stored real addresses.
func forgetStoredNodeConf(t *testing.T, dir string) {
	t.Helper()
	require.NoError(t, os.RemoveAll(filepath.Join(dir, "nodeconf")))
}

// openAt opens the SDK for prov on dir against the nodeconf nc. The
// returned close is idempotent and registered as a cleanup, so a failed
// assertion never leaves a live SDK behind.
func openAt(t *testing.T, ctx context.Context, dir string, prov *fixedSeedProvider, nc []byte) (*anysyncsdk.SDK, func() error) {
	t.Helper()
	sdk, err := anysyncsdk.Open(ctx, config.Config{
		Storage: config.Storage{DataDir: dir, Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: nc},
	}, prov)
	require.NoError(t, err, "Open")
	var (
		once sync.Once
		cerr error
	)
	closeSDK := func() error {
		once.Do(func() { cerr = sdk.Close() })
		return cerr
	}
	t.Cleanup(func() { _ = closeSDK() })
	return sdk, closeSDK
}

// registeredSpaces creates n spaces and waits until the coordinator has
// registered them. The signal is a reconcile pass receiving statuses for
// the batch: the coordinator answers it only once the largest id is
// registered, and the remaining first pushes land within the same
// seconds. Synced is no signal: a space with no pending tree reads
// Synced at once.
func registeredSpaces(t *testing.T, ctx context.Context, sdk *anysyncsdk.SDK, n int) []string {
	t.Helper()
	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Registered"})
		require.NoError(t, err, "create registered space")
		ids = append(ids, sp.Id())
	}
	mark := spaceimpl.DeletionReconcilePassesForTest()
	require.True(t, waitFor(ctx, 90*time.Second, time.Second, func() bool {
		return spaceimpl.DeletionReconcilePassesForTest() > mark
	}), "no reconcile pass received coordinator statuses: coordinator unreachable, or the largest id never registered")
	time.Sleep(3 * time.Second)
	return ids
}

// unpushedSpaces creates a regular space and a 1-1, each with one object,
// and returns their ids.
func unpushedSpaces(t *testing.T, ctx context.Context, sdk *anysyncsdk.SDK) (regular, oneToOne string) {
	t.Helper()
	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Unpushed"})
	require.NoError(t, err, "create space")
	_, err = sp.Objects().Create(ctx, space.CreateObjectOpts{Type: markerTypeId(t, ctx, sp, "Note")})
	require.NoError(t, err, "create object in space")

	_, peerPub, err := anySyncCryptoGenerate()
	require.NoError(t, err)
	dm, err := sdk.Spaces().OneToOne(ctx, peerPub.Account())
	require.NoError(t, err, "create 1-1")
	_, err = dm.Objects().Create(ctx, space.CreateObjectOpts{Type: markerTypeId(t, ctx, dm, "Note")})
	require.NoError(t, err, "create object in 1-1")
	return sp.Id(), dm.Id()
}

// assertSurvives watches the spaces until `passes` reconcile passes have
// received coordinator statuses and judged them. It fails the first time
// a space is tombstoned or loses its local storage, and when no such
// pass happens within timeout: a pass the coordinator refused proves
// nothing.
func assertSurvives(t *testing.T, ctx context.Context, sdk *anysyncsdk.SDK, dataDir string, passes int64, timeout time.Duration, ids map[string]string) {
	t.Helper()
	mark := spaceimpl.DeletionReconcilePassesForTest()
	deadline := time.Now().Add(timeout)
	for {
		for name, id := range ids {
			si, ok := infoByID(t, ctx, sdk, id)
			require.True(t, ok, "%s: row missing", name)
			storage := filepath.Join(dataDir, "anysync", id+".db")
			if si.Status != space.StatusActive {
				_, statErr := os.Stat(storage)
				t.Fatalf("%s %s: status %q (local storage present: %v)", name, id, si.Status, statErr == nil)
			}
			require.FileExists(t, storage, "%s: local storage", name)
		}
		if spaceimpl.DeletionReconcilePassesForTest()-mark >= passes {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("no reconcile pass received coordinator statuses within %s", timeout)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// A space created while the sync nodes are unreachable but the
// coordinator answers has not been registered on the coordinator yet.
// It must survive the reconciler's passes until its first push lands.
//
// The coordinator fails the whole StatusCheckMany when the LAST id in the
// request is unknown (the SDK sends ids in space-id order), so an attempt
// only counts when a registered space holds the largest id. Survival is
// asserted across passes that received statuses, so a pass the
// coordinator refused cannot pass the test.
func TestE2E_Reconcile_UnpushedSpaceSurvivesNodeOutage(t *testing.T) {
	netYAML, _, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	defer spaceimpl.SetDeletionReconcileIntervalForTest(2 * time.Second)()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	for attempt := 1; attempt <= 4; attempt++ {
		dir := t.TempDir()
		prov := newFixedSeedProvider(t)

		// An account with spaces already on the network.
		online, closeOnline := openAt(t, ctx, dir, prov, netYAML)
		registered := registeredSpaces(t, ctx, online, 6)
		require.NoError(t, closeOnline())

		// Sync nodes unreachable, coordinator reachable.
		forgetStoredNodeConf(t, dir)
		sdk, closeSDK := openAt(t, ctx, dir, prov, nodeConfUnreachable(t, netYAML, nodeconf.NodeTypeTree))
		regular, oneToOne := unpushedSpaces(t, ctx, sdk)
		last := slices.Max(registered)
		if regular > last || oneToOne > last {
			require.NoError(t, closeSDK())
			t.Logf("attempt %d: an unpushed space holds the largest id, retrying with a fresh account", attempt)
			continue
		}
		t.Logf("attempt %d: space %s, 1-1 %s, largest registered %s", attempt, regular, oneToOne, last)
		assertSurvives(t, ctx, sdk, dir, 3, 60*time.Second, map[string]string{"space": regular, "1-1": oneToOne})
		require.NoError(t, closeSDK())
		return
	}
	t.Skip("an unpushed space held the largest id on every attempt")
}
