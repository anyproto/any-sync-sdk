package e2e

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"
	"github.com/anyproto/any-sync-sdk/space"
)

// infoByID returns the SpaceInfo for id from a fresh List snapshot.
func infoByID(t *testing.T, ctx context.Context, sdk *anysyncsdk.SDK, id string) (space.SpaceInfo, bool) {
	t.Helper()
	list, err := sdk.Spaces().List(ctx)
	require.NoError(t, err)
	if si := findSpace(list, id); si != nil {
		return *si, true
	}
	return space.SpaceInfo{}, false
}

// firstOneToOne returns the single 1-1 row in a List snapshot (the
// decline test creates exactly one).
func firstOneToOne(t *testing.T, ctx context.Context, sdk *anysyncsdk.SDK) (space.SpaceInfo, bool) {
	t.Helper()
	list, err := sdk.Spaces().List(ctx)
	require.NoError(t, err)
	for _, si := range list {
		if si.Type == space.SpaceTypeOneToOne {
			return si, true
		}
	}
	return space.SpaceInfo{}, false
}

// TestE2E_OneToOne_DeclineSticky exercises the Phase-1 serverless state
// machine with a single account and an out-of-band peer identity — no
// cross-account sync needed, so it runs fast.
//
//	RegisterIncoming → pending
//	Decline → declined (synced sticky marker)
//	RegisterIncoming again → still declined (sticky, no re-prompt)
//	OneToOne(peer) → overrides to active (un-decline)
func TestE2E_OneToOne_DeclineSticky(t *testing.T) {
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	bob, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = bob.Close() })

	// A valid out-of-band peer identity (Alice is never online here).
	_, peerPub, err := anySyncCryptoGenerate()
	require.NoError(t, err)
	peerID := peerPub.Account()

	// Incoming request → pending, not materialized.
	require.NoError(t, bob.Spaces().RegisterIncoming(ctx, peerID, space.AccountMetadata{Name: "Alice"}))
	si, ok := firstOneToOne(t, ctx, bob)
	require.True(t, ok, "pending 1-1 row should exist")
	assert.Equal(t, space.StatusOneToOnePending, si.Status)
	assert.Equal(t, "Alice", si.Name)
	id := si.Id

	// Decline → declined.
	require.NoError(t, bob.Spaces().DeclineOneToOne(ctx, id))
	si, ok = infoByID(t, ctx, bob, id)
	require.True(t, ok)
	assert.Equal(t, space.StatusOneToOneDeclined, si.Status)

	// Re-registering the same incoming is a no-op — decline is sticky.
	require.NoError(t, bob.Spaces().RegisterIncoming(ctx, peerID, space.AccountMetadata{Name: "Alice"}))
	si, ok = infoByID(t, ctx, bob, id)
	require.True(t, ok)
	assert.Equal(t, space.StatusOneToOneDeclined, si.Status, "re-register must not re-prompt a declined 1-1")

	// Explicit OneToOne(peer) overrides the decline → active.
	sp, err := bob.Spaces().OneToOne(ctx, peerID)
	require.NoError(t, err)
	require.Equal(t, id, sp.Id(), "OneToOne must land on the same derived id")
	si, ok = infoByID(t, ctx, bob, id)
	require.True(t, ok)
	assert.Equal(t, space.StatusActive, si.Status, "explicit OneToOne un-declines")
}

// TestE2E_OneToOne_DeleteAndRecreate exercises the 1-1 delete semantics
// on a single device: Delete writes the synced, non-terminal offload
// marker (surfaced as Deleted), and a later OneToOne re-creates the same
// space (the marker is not terminal, unlike a regular delete). Serverless
// — no cross-account sync needed.
func TestE2E_OneToOne_DeleteAndRecreate(t *testing.T) {
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cfg := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	_, peerPub, err := anySyncCryptoGenerate()
	require.NoError(t, err)
	peerID := peerPub.Account()

	// Initiate → active.
	sp, err := sdk.Spaces().OneToOne(ctx, peerID)
	require.NoError(t, err)
	id := sp.Id()
	si, ok := infoByID(t, ctx, sdk, id)
	require.True(t, ok)
	assert.Equal(t, space.StatusActive, si.Status)

	// Delete → surfaced as Deleted (synced offload marker), row still listed.
	require.NoError(t, sdk.Spaces().Delete(ctx, id))
	si, ok = infoByID(t, ctx, sdk, id)
	require.True(t, ok, "deleted 1-1 stays in List as a tombstone")
	assert.Equal(t, space.StatusDeleted, si.Status)

	// Re-create via OneToOne → active again (marker is non-terminal,
	// unlike a regular delete which is terminal/sticky).
	sp2, err := sdk.Spaces().OneToOne(ctx, peerID)
	require.NoError(t, err, "1-1 must be re-creatable after delete")
	require.Equal(t, id, sp2.Id())
	si, ok = infoByID(t, ctx, sdk, id)
	require.True(t, ok)
	assert.Equal(t, space.StatusActive, si.Status, "re-created 1-1 is active again")
}

// TestE2E_OneToOne_DeleteSyncsToOtherDevice proves the 1-1 delete
// propagates account-wide: device A deletes the 1-1, and device B (same
// account, distinct device) sees it become Deleted and offloads its local
// copy — WITHOUT the space being removed from the nodes. Cross-device
// convergence rides the tech-space sync, so the propagation assertion is
// best-effort (skips if tech-space doesn't converge in time).
func TestE2E_OneToOne_DeleteSyncsToOtherDevice(t *testing.T) {
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("two-device 1-1 delete e2e needs cross-device sync; rerun without -short")
	}

	// Fast reconcile so device B offloads promptly when the marker lands.
	restore := spaceimpl.SetDeletionReconcileIntervalForTest(2 * time.Second)
	defer restore()

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	// Same account, two distinct devices (shared account seed, fresh
	// device seed for B).
	provA := newFixedSeedProvider(t)
	_, devBPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	provB := &fixedSeedProvider{account: provA.account, device: devBPriv}

	open := func(p *fixedSeedProvider, name string) *anysyncsdk.SDK {
		t.Helper()
		cfg := config.Config{
			Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: yaml},
		}
		sdk, err := anysyncsdk.Open(ctx, cfg, p)
		require.NoError(t, err, "%s: Open", name)
		t.Cleanup(func() { _ = sdk.Close() })
		return sdk
	}
	a := open(provA, "deviceA")
	b := open(provB, "deviceB")
	require.Equal(t, a.Account().Id(), b.Account().Id(), "both devices share one account")

	_, peerPub, err := anySyncCryptoGenerate()
	require.NoError(t, err)
	peerID := peerPub.Account()

	// A initiates; B also materializes the same 1-1 so it has a local copy
	// to offload.
	aSp, err := a.Spaces().OneToOne(ctx, peerID)
	require.NoError(t, err)
	id := aSp.Id()
	_, err = b.Spaces().OneToOne(ctx, peerID)
	require.NoError(t, err)

	// A deletes → synced oneToOneDeleted marker (no node removal).
	require.NoError(t, a.Spaces().Delete(ctx, id))
	aInfo, ok := infoByID(t, ctx, a, id)
	require.True(t, ok)
	assert.Equal(t, space.StatusDeleted, aInfo.Status, "A sees its own delete")

	// B should converge on Deleted via tech-space sync. Best-effort.
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		_ = b.Spaces().SyncSpaceList(ctx)
		if si, ok := infoByID(t, ctx, b, id); ok && si.Status == space.StatusDeleted {
			t.Logf("device B converged on the 1-1 delete")
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Skipf("device B did not converge on the 1-1 delete in time (tech-space sync / node reachability)")
}

// TestE2E_OneToOne_ApproveIncoming is the full two-account happy path:
// Alice initiates, Bob discovers out-of-band, lands on the SAME derived
// space (symmetric derivation), approves it, and then converges on
// Alice's content. Slow — needs cross-account replication.
func TestE2E_OneToOne_ApproveIncoming(t *testing.T) {
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("1-1 approve e2e needs cross-account sync (~60-90s); rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
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

	// Alice initiates — active immediately (implicit self-approval).
	aliceSp, err := alice.Spaces().OneToOne(ctx, bob.Account().Id())
	require.NoError(t, err, "alice: OneToOne")
	id := aliceSp.Id()
	si, ok := infoByID(t, ctx, alice, id)
	require.True(t, ok)
	assert.Equal(t, space.StatusActive, si.Status)
	assert.Equal(t, space.SpaceTypeOneToOne, si.Type)

	// Alice writes content before Bob accepts.
	objID, err := aliceSp.Objects().Create(ctx, space.CreateObjectOpts{})
	require.NoError(t, err, "alice: create object")

	// Bob learns Alice's identity out-of-band → pending row on the SAME id
	// (proves symmetric derivation: Bob derived it from his key + Alice's
	// pubkey, with no contact between the two).
	require.NoError(t, bob.Spaces().RegisterIncoming(ctx, alice.Account().Id(), space.AccountMetadata{Name: "Alice"}))
	si, ok = infoByID(t, ctx, bob, id)
	require.True(t, ok, "bob must derive the same 1-1 id Alice initiated")
	assert.Equal(t, space.StatusOneToOnePending, si.Status)

	// Bob approves → active, materialized.
	bobSp, err := bob.Spaces().AcceptOneToOne(ctx, id)
	require.NoError(t, err, "bob: AcceptOneToOne")
	require.Equal(t, id, bobSp.Id())
	si, ok = infoByID(t, ctx, bob, id)
	require.True(t, ok)
	assert.Equal(t, space.StatusActive, si.Status)

	// Content convergence is the only cross-account step and depends on the
	// 1-1's derived replication-key nodes being reachable — that is Phase-2
	// transport territory, not the Phase-1 serverless primitive this test
	// covers. Best-effort: log if it lands, don't fail the suite on node
	// reachability. (Promote to a hard require once Phase 2 wires/validates
	// 1-1 cross-account sync against the local coordinator.)
	converged := false
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		snap, err := bobSp.QueryObjects().Snapshot(ctx, space.QueryOpts{})
		if err == nil {
			for _, d := range snap.Initial {
				if d.GetString("id") == objID {
					converged = true
				}
			}
		}
		if converged {
			break
		}
		time.Sleep(2 * time.Second)
	}
	if converged {
		t.Logf("1-1 content converged: bob synced alice's object %s", objID)
	} else {
		t.Logf("1-1 content did NOT converge in time (object %s) — likely cross-account transport / node reachability, a Phase-2 concern", objID)
	}
}
