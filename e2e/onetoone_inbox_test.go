package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_OneToOne_InboxDiscovery is the Phase-2 happy path: Alice
// initiates a 1-1, the SDK posts an inbox notification, and Bob's inbox
// notifier surfaces the incoming request as a pending row WITHOUT any
// out-of-band identity exchange (no RegisterIncoming call on Bob's side).
// Bob then accepts.
//
// Requires a coordinator that implements the inbox RPCs. If the
// configured network doesn't (the notification never reaches Bob), the
// test skips rather than fails — Layer-2 discovery is optional, and
// Phase-1 (out-of-band) is covered separately.
func TestE2E_OneToOne_InboxDiscovery(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("inbox-discovery e2e needs a live coordinator (~30-60s); rerun without -short")
	}

	// Shorten the notifier poll + invite-retry cadences so the test
	// doesn't wait the production minute. Must be set before Open.
	restore := spaceimpl.SetOneToOneInboxIntervalsForTest(time.Second, time.Second)
	defer restore()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	openSDK := func(name string) *anysyncsdk.SDK {
		t.Helper()
		cfg := config.Config{
			Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: yaml},
		}
		sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
		require.NoError(t, err, "%s: Open", name)
		t.Cleanup(func() { _ = sdk.Close() })
		return sdk
	}

	alice := openSDK("alice")
	bob := openSDK("bob")
	require.NotEqual(t, alice.Account().Id(), bob.Account().Id())

	// Alice publishes a profile so the invite body carries a display name.
	require.NoError(t, alice.Account().UpdateMetadata(ctx, space.AccountMetadata{Name: "Alice"}))

	// Alice initiates — active locally + posts an inbox notification to Bob.
	aliceSp, err := alice.Spaces().OneToOne(ctx, bob.Account().Id())
	require.NoError(t, err, "alice: OneToOne")
	id := aliceSp.Id()

	// Bob does NOT RegisterIncoming. His inbox notifier should fetch the
	// invite and create the pending row on its own. Poll for it.
	var pending *space.SpaceInfo
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if si, ok := infoByID(t, ctx, bob, id); ok && si.Status == space.StatusOneToOnePending {
			pending = &si
			break
		}
		time.Sleep(time.Second)
	}
	if pending == nil {
		t.Skipf("inbox notification never reached bob — coordinator likely lacks inbox support; "+
			"Phase-1 out-of-band path is covered by TestE2E_OneToOne_ApproveIncoming (space %s)", id)
	}

	// Discovered via inbox alone. The pending row surfaces Alice's account
	// identity (as Author); her name is not in the invite (symkey-only).
	assert.Equal(t, space.SpaceTypeOneToOne, pending.Type)
	assert.Equal(t, alice.Account().Id(), pending.Author, "pending 1-1 must surface the friend identity")

	// Pending-name resolution: the invite carried Alice's metadata symkey,
	// which Bob's notifier cached; RegisterIncoming then resolves her
	// identityRepo profile and writes the name onto the still-pending row —
	// no accept or member watcher needed.
	require.Eventually(t, func() bool {
		si, ok := infoByID(t, ctx, bob, id)
		return ok && si.Name == "Alice"
	}, 60*time.Second, time.Second, "pending 1-1 should resolve Alice's name from identityRepo")

	// Bob accepts → active.
	bobSp, err := bob.Spaces().AcceptOneToOne(ctx, id)
	require.NoError(t, err, "bob: AcceptOneToOne")
	require.Equal(t, id, bobSp.Id())
	si, ok := infoByID(t, ctx, bob, id)
	require.True(t, ok)
	assert.Equal(t, space.StatusActive, si.Status)

	// End-to-end key distribution: the invite carried Alice's metadata
	// symkey, which Bob's notifier cached. Starting the members watcher
	// makes it fetch Alice's identityRepo profile and decrypt it with that
	// key, resolving her name. Subscribe starts the profile loop.
	cancelSub := bobSp.Members().Subscribe(func(space.MemberEvent) {})
	defer cancelSub()
	require.Eventually(t, func() bool {
		m, err := bobSp.Members().Get(ctx, alice.Account().Id())
		return err == nil && m.Name == "Alice"
	}, 60*time.Second, time.Second, "Alice's name should resolve from identityRepo via the cached invite symkey")
}
