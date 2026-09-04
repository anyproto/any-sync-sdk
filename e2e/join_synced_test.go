package e2e

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_JoinLifecycleSynced covers the account-wide join lifecycle
// across two devices of the joiner's account (same mnemonic, fresh
// device key), neither of which acts on the other's behalf:
//
//  1. Device A requests to join; device B reads the row as StatusJoining
//     — not Active — refuses to materialize it, and pulls no storage.
//  2. The owner accepts; A and B both reach StatusActive and load.
//  3. On a second space, A requests and the owner declines; the ended
//     join converges to StatusDeleted on both devices, and B's space-list
//     subscription reports the row Removed.
//  4. A re-requests (the ended row revives); B withdraws it — the device
//     that never posted the request — and both read StatusDeleted; the
//     owner's pending list drains.
//  5. The owner adds the account directly (AddAccounts); the invite
//     registers over the ended join and both devices read
//     StatusInvitePending; B accepts and both reach StatusActive.
//
// Skips when no any-sync network is reachable; step 5 skips when the
// coordinator has no inbox support.
func TestE2E_JoinLifecycleSynced(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("synced-join e2e is slow (~3-5min); rerun without -short")
	}

	// Short controller and inbox cadences so a device that only synced a
	// row in reacts within seconds. Both are read at Open.
	restoreJoin := spaceimpl.SetJoinReconcileIntervalForTest(2 * time.Second)
	defer restoreJoin()
	restoreInbox := spaceimpl.SetOneToOneInboxIntervalsForTest(time.Second, time.Second)
	defer restoreInbox()

	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	openSDK := func(name string, prov *fixedSeedProvider) (*anysyncsdk.SDK, string) {
		t.Helper()
		dir := t.TempDir()
		cfg := config.Config{
			Storage: config.Storage{DataDir: dir, Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: yaml},
		}
		sdk, err := anysyncsdk.Open(ctx, cfg, prov)
		require.NoError(t, err, "%s: Open", name)
		t.Cleanup(func() { _ = sdk.Close() })
		return sdk, dir
	}

	alice, _ := openSDK("alice", newFixedSeedProvider(t))
	bobProv := newFixedSeedProvider(t)
	bobA, _ := openSDK("bobA", bobProv)
	_, devB, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	bobB, bobBDir := openSDK("bobB", &fixedSeedProvider{account: bobProv.account, device: devB})
	require.NotEqual(t, alice.Account().Id(), bobA.Account().Id())
	require.Equal(t, bobA.Account().Id(), bobB.Account().Id(), "same account on both devices")
	bob := bobA.Account().Id()

	// B's space-list stream, attached before any row exists so every
	// transition of both spaces is observed.
	var mu sync.Mutex
	removedSeen := map[string]int{}
	cancelSub := bobB.Spaces().Subscribe(func(ev space.SpaceListEvent) {
		mu.Lock()
		defer mu.Unlock()
		for _, id := range ev.Removed {
			removedSeen[id]++
		}
	})
	defer cancelSub()
	removedOn := func(id string) int {
		mu.Lock()
		defer mu.Unlock()
		return removedSeen[id]
	}

	createInvited := func(name string) (space.Space, string) {
		t.Helper()
		sp, err := alice.Spaces().Create(ctx, space.CreateRequest{Name: name})
		require.NoError(t, err, "alice: Spaces().Create")
		var inv space.Invite
		if !waitFor(ctx, 30*time.Second, 1*time.Second, func() bool {
			inv, err = sp.ACL().CreateInvite(ctx)
			return err == nil
		}) {
			if isNoNetworkErr(err) {
				t.Skipf("network unreachable on CreateInvite: %v", err)
			}
			t.Fatalf("alice: CreateInvite: %v", err)
		}
		token, err := space.EncodeInvite(inv)
		require.NoError(t, err)
		return sp, token
	}
	// statusOn polls one device's space list — forcing a tech-space sync
	// round each tick — until spaceId reads want.
	statusOn := func(dev *anysyncsdk.SDK, name, spaceId string, want space.Status, within time.Duration) {
		t.Helper()
		var last space.Status
		if !waitFor(ctx, within, time.Second, func() bool {
			_ = dev.Spaces().SyncSpaceList(ctx)
			last = bobStatus(t, ctx, dev, spaceId)
			return last == want
		}) {
			t.Fatalf("%s: space %s never reached %v (last %v)", name, spaceId, want, last)
		}
	}
	requestsDrained := func(sp space.Space) {
		t.Helper()
		if !waitFor(ctx, 90*time.Second, 1*time.Second, func() bool {
			_ = sp.SyncHeads(ctx)
			reqs, err := sp.Members().JoinRequests(ctx)
			return err == nil && len(reqs) == 0
		}) {
			t.Fatalf("alice's join requests never drained")
		}
	}

	// 1. A requests; B reads joining and materializes nothing.
	sp1, token1 := createInvited("SyncedJoin1")
	_, err = bobA.Spaces().Join(ctx, space.JoinRequest{Invite: token1})
	require.ErrorIs(t, err, spaceimpl.ErrJoinPending, "bobA: Join")
	require.Equal(t, space.StatusJoining, bobStatus(t, ctx, bobA, sp1.Id()))
	statusOn(bobB, "bobB", sp1.Id(), space.StatusJoining, 90*time.Second)
	_, err = bobB.Spaces().Get(ctx, sp1.Id())
	require.ErrorIs(t, err, space.ErrSpaceNotAccepted, "bobB: Get on a join pending elsewhere must refuse to materialize")
	require.NoFileExists(t, filepath.Join(bobBDir, "anysync", sp1.Id()+".db"),
		"a join pending on another device must not create any-sync storage here")
	// Long enough for B's controller to have run its passes; still no pull.
	time.Sleep(6 * time.Second)
	require.NoFileExists(t, filepath.Join(bobBDir, "anysync", sp1.Id()+".db"),
		"the join controller must not pull a pending join")

	// 2. The owner accepts; both devices load.
	req := awaitJoinRequest(t, ctx, sp1, bob)
	require.NoError(t, sp1.ACL().AcceptRequest(ctx, req.RecordId, space.PermissionWriter), "alice: AcceptRequest")
	statusOn(bobA, "bobA", sp1.Id(), space.StatusActive, 120*time.Second)
	statusOn(bobB, "bobB", sp1.Id(), space.StatusActive, 120*time.Second)
	gotA, err := bobA.Spaces().Get(ctx, sp1.Id())
	require.NoError(t, err, "bobA: Get after accept")
	require.Equal(t, sp1.Id(), gotA.Id())
	gotB, err := bobB.Spaces().Get(ctx, sp1.Id())
	require.NoError(t, err, "bobB: Get after accept")
	require.Equal(t, sp1.Id(), gotB.Id())

	// 3. Decline observed on the requesting device converges the other.
	sp2, token2 := createInvited("SyncedJoin2")
	_, err = bobA.Spaces().Join(ctx, space.JoinRequest{Invite: token2})
	require.ErrorIs(t, err, spaceimpl.ErrJoinPending, "bobA: Join (space 2)")
	statusOn(bobB, "bobB", sp2.Id(), space.StatusJoining, 90*time.Second)
	awaitJoinRequest(t, ctx, sp2, bob)
	require.NoError(t, sp2.ACL().DeclineRequest(ctx, bob), "alice: DeclineRequest")
	statusOn(bobA, "bobA", sp2.Id(), space.StatusDeleted, 120*time.Second)
	statusOn(bobB, "bobB", sp2.Id(), space.StatusDeleted, 90*time.Second)
	if !waitFor(ctx, 30*time.Second, time.Second, func() bool { return removedOn(sp2.Id()) > 0 }) {
		t.Fatalf("bobB's space-list stream never reported the ended join as Removed")
	}
	_, err = bobB.Spaces().Get(ctx, sp2.Id())
	require.ErrorIs(t, err, space.ErrSpaceDeleted, "bobB: Get must refuse the ended row")

	// 4. Re-request from A, withdrawal from B — the device that never
	// posted the request.
	_, err = bobA.Spaces().Join(ctx, space.JoinRequest{Invite: token2})
	require.ErrorIs(t, err, spaceimpl.ErrJoinPending, "bobA: re-request revives the ended row")
	require.Equal(t, space.StatusJoining, bobStatus(t, ctx, bobA, sp2.Id()))
	statusOn(bobB, "bobB", sp2.Id(), space.StatusJoining, 90*time.Second)
	awaitJoinRequest(t, ctx, sp2, bob)
	require.NoError(t, bobB.Spaces().CancelJoin(ctx, sp2.Id()), "bobB: CancelJoin from the non-requesting device")
	require.Equal(t, space.StatusDeleted, bobStatus(t, ctx, bobB, sp2.Id()), "the withdrawal reads back synchronously")
	statusOn(bobA, "bobA", sp2.Id(), space.StatusDeleted, 120*time.Second)
	requestsDrained(sp2)

	// 5. A direct add over the ended join registers the invite on the
	// account, and an accept on either device loads it everywhere.
	err = sp2.ACL().AddAccounts(ctx, []space.MemberAdd{{Identity: bob, Permission: space.PermissionWriter}})
	require.NoError(t, err, "alice: AddAccounts after the withdrawn join")
	pendingSeen := waitFor(ctx, 90*time.Second, time.Second, func() bool {
		_ = bobA.Spaces().SyncSpaceList(ctx)
		_ = bobB.Spaces().SyncSpaceList(ctx)
		return bobStatus(t, ctx, bobA, sp2.Id()) == space.StatusInvitePending ||
			bobStatus(t, ctx, bobB, sp2.Id()) == space.StatusInvitePending
	})
	if !pendingSeen {
		t.Skipf("inbox notification never reached the account — coordinator likely lacks inbox support (space %s)", sp2.Id())
	}
	statusOn(bobA, "bobA", sp2.Id(), space.StatusInvitePending, 60*time.Second)
	statusOn(bobB, "bobB", sp2.Id(), space.StatusInvitePending, 60*time.Second)
	if _, err := bobB.Spaces().AcceptInvite(ctx, sp2.Id()); err != nil {
		require.ErrorIs(t, err, space.ErrInviteAcceptPending, "bobB: AcceptInvite must succeed or report pending")
	}
	statusOn(bobB, "bobB", sp2.Id(), space.StatusActive, 120*time.Second)
	statusOn(bobA, "bobA", sp2.Id(), space.StatusActive, 120*time.Second)
	gotB2, err := bobB.Spaces().Get(ctx, sp2.Id())
	require.NoError(t, err, "bobB: Get after accept")
	require.Equal(t, sp2.Id(), gotB2.Id())
	gotA2, err := bobA.Spaces().Get(ctx, sp2.Id())
	require.NoError(t, err, "bobA: Get after the accept converged")
	require.Equal(t, sp2.Id(), gotA2.Id())
}
