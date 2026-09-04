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
//  3. On a second space, A requests and goes OFFLINE; the owner declines.
//     B — which never posted the request — detects the decline itself
//     (a head resolved from the chain), reads StatusDeleted, and its
//     space-list subscription reports the row Removed. A CancelJoin on B
//     racing the verdict either settles the request-less row or finds
//     it settled; never an error of another kind.
//  4. A comes back and re-requests (the ended row revives); B withdraws
//     it and both read StatusDeleted; the owner's pending list drains.
//  5. The owner adds the account directly (AddAccounts); the invite
//     registers over the ended join and both devices read
//     StatusInvitePending; B accepts and both reach StatusActive.
//  6. On a third space, a declined join followed by a direct add is
//     healed by Join on the device whose row is still ended: the ACL
//     already grants membership, so the space loads with no new request.
//
// Skips when no any-sync network is reachable; steps 5-6 skip when the
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

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	// A device handle whose Close is idempotent, so a test step can shut a
	// device down and the cleanup still runs safely.
	type device struct {
		sdk   *anysyncsdk.SDK
		dir   string
		close func()
	}
	openSDK := func(name string, prov *fixedSeedProvider, dir string) *device {
		t.Helper()
		if dir == "" {
			dir = t.TempDir()
		}
		cfg := config.Config{
			Storage: config.Storage{DataDir: dir, Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: yaml},
		}
		sdk, err := anysyncsdk.Open(ctx, cfg, prov)
		require.NoError(t, err, "%s: Open", name)
		var once sync.Once
		d := &device{sdk: sdk, dir: dir}
		d.close = func() { once.Do(func() { _ = sdk.Close() }) }
		t.Cleanup(d.close)
		return d
	}

	alice := openSDK("alice", newFixedSeedProvider(t), "")
	bobProv := newFixedSeedProvider(t)
	bobA := openSDK("bobA", bobProv, "")
	_, devB, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	bobBProv := &fixedSeedProvider{account: bobProv.account, device: devB}
	bobB := openSDK("bobB", bobBProv, "")
	require.NotEqual(t, alice.sdk.Account().Id(), bobA.sdk.Account().Id())
	require.Equal(t, bobA.sdk.Account().Id(), bobB.sdk.Account().Id(), "same account on both devices")
	bob := bobA.sdk.Account().Id()

	// B's space-list stream, attached before any row exists so every
	// transition of both spaces is observed.
	var mu sync.Mutex
	removedSeen := map[string]int{}
	cancelSub := bobB.sdk.Spaces().Subscribe(func(ev space.SpaceListEvent) {
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
		sp, err := alice.sdk.Spaces().Create(ctx, space.CreateRequest{Name: name})
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
	statusOn := func(dev *device, name, spaceId string, want space.Status, within time.Duration) {
		t.Helper()
		var last space.Status
		if !waitFor(ctx, within, time.Second, func() bool {
			_ = dev.sdk.Spaces().SyncSpaceList(ctx)
			last = bobStatus(t, ctx, dev.sdk, spaceId)
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
	// pendingOnEither reports whether the direct-add invite reached the
	// account (the inbox is processed by ONE device, the row syncs); false
	// means the coordinator has no inbox support and the step skips.
	pendingOnEither := func(spaceId string) bool {
		return waitFor(ctx, 90*time.Second, time.Second, func() bool {
			_ = bobA.sdk.Spaces().SyncSpaceList(ctx)
			_ = bobB.sdk.Spaces().SyncSpaceList(ctx)
			return bobStatus(t, ctx, bobA.sdk, spaceId) == space.StatusInvitePending ||
				bobStatus(t, ctx, bobB.sdk, spaceId) == space.StatusInvitePending
		})
	}

	// 1. A requests; B reads joining and materializes nothing.
	sp1, token1 := createInvited("SyncedJoin1")
	_, err = bobA.sdk.Spaces().Join(ctx, space.JoinRequest{Invite: token1})
	require.ErrorIs(t, err, spaceimpl.ErrJoinPending, "bobA: Join")
	require.Equal(t, space.StatusJoining, bobStatus(t, ctx, bobA.sdk, sp1.Id()))
	statusOn(bobB, "bobB", sp1.Id(), space.StatusJoining, 90*time.Second)
	_, err = bobB.sdk.Spaces().Get(ctx, sp1.Id())
	require.ErrorIs(t, err, space.ErrSpaceNotAccepted, "bobB: Get on a join pending elsewhere must refuse to materialize")
	require.NoFileExists(t, filepath.Join(bobB.dir, "anysync", sp1.Id()+".db"),
		"a join pending on another device must not create any-sync storage here")
	// Long enough for B's controller to have run its passes; still no pull.
	time.Sleep(6 * time.Second)
	require.NoFileExists(t, filepath.Join(bobB.dir, "anysync", sp1.Id()+".db"),
		"the join controller must not pull a pending join")

	// 2. The owner accepts; both devices load.
	req := awaitJoinRequest(t, ctx, sp1, bob)
	require.NoError(t, sp1.ACL().AcceptRequest(ctx, req.RecordId, space.PermissionWriter), "alice: AcceptRequest")
	statusOn(bobA, "bobA", sp1.Id(), space.StatusActive, 120*time.Second)
	statusOn(bobB, "bobB", sp1.Id(), space.StatusActive, 120*time.Second)
	gotA, err := bobA.sdk.Spaces().Get(ctx, sp1.Id())
	require.NoError(t, err, "bobA: Get after accept")
	require.Equal(t, sp1.Id(), gotA.Id())
	gotB, err := bobB.sdk.Spaces().Get(ctx, sp1.Id())
	require.NoError(t, err, "bobB: Get after accept")
	require.Equal(t, sp1.Id(), gotB.Id())

	// 3. Decline detected by the device that never requested, with the
	// requester offline.
	sp2, token2 := createInvited("SyncedJoin2")
	_, err = bobA.sdk.Spaces().Join(ctx, space.JoinRequest{Invite: token2})
	require.ErrorIs(t, err, spaceimpl.ErrJoinPending, "bobA: Join (space 2)")
	statusOn(bobB, "bobB", sp2.Id(), space.StatusJoining, 90*time.Second)
	awaitJoinRequest(t, ctx, sp2, bob)
	bobA.close()
	require.NoError(t, sp2.ACL().DeclineRequest(ctx, bob), "alice: DeclineRequest")
	// A withdrawal racing the verdict: the chain holds no request any
	// more, so the call either settles the row itself (nil) or finds B's
	// waiter already did (ErrJoinNotPending).
	if err := bobB.sdk.Spaces().CancelJoin(ctx, sp2.Id()); err != nil {
		require.ErrorIs(t, err, space.ErrJoinNotPending, "bobB: CancelJoin after the decline")
	}
	statusOn(bobB, "bobB", sp2.Id(), space.StatusDeleted, 120*time.Second)
	if !waitFor(ctx, 30*time.Second, time.Second, func() bool { return removedOn(sp2.Id()) > 0 }) {
		t.Fatalf("bobB's space-list stream never reported the ended join as Removed")
	}
	_, err = bobB.sdk.Spaces().Get(ctx, sp2.Id())
	require.ErrorIs(t, err, space.ErrSpaceDeleted, "bobB: Get must refuse the ended row")

	// 4. A comes back (same device, same data dir), converges on the
	// ended row, re-requests; B — the device that never posted the
	// request — withdraws it.
	bobA = openSDK("bobA (restarted)", bobProv, bobA.dir)
	statusOn(bobA, "bobA", sp2.Id(), space.StatusDeleted, 90*time.Second)
	_, err = bobA.sdk.Spaces().Join(ctx, space.JoinRequest{Invite: token2})
	require.ErrorIs(t, err, spaceimpl.ErrJoinPending, "bobA: re-request revives the ended row")
	require.Equal(t, space.StatusJoining, bobStatus(t, ctx, bobA.sdk, sp2.Id()))
	statusOn(bobB, "bobB", sp2.Id(), space.StatusJoining, 90*time.Second)
	awaitJoinRequest(t, ctx, sp2, bob)
	require.NoError(t, bobB.sdk.Spaces().CancelJoin(ctx, sp2.Id()), "bobB: CancelJoin from the non-requesting device")
	require.Equal(t, space.StatusDeleted, bobStatus(t, ctx, bobB.sdk, sp2.Id()), "the withdrawal reads back synchronously")
	statusOn(bobA, "bobA", sp2.Id(), space.StatusDeleted, 120*time.Second)
	requestsDrained(sp2)

	// 5. A direct add over the ended join registers the invite on the
	// account, and an accept on either device loads it everywhere.
	err = sp2.ACL().AddAccounts(ctx, []space.MemberAdd{{Identity: bob, Permission: space.PermissionWriter}})
	require.NoError(t, err, "alice: AddAccounts after the withdrawn join")
	if !pendingOnEither(sp2.Id()) {
		t.Skipf("inbox notification never reached the account — coordinator likely lacks inbox support (space %s)", sp2.Id())
	}
	statusOn(bobA, "bobA", sp2.Id(), space.StatusInvitePending, 60*time.Second)
	statusOn(bobB, "bobB", sp2.Id(), space.StatusInvitePending, 60*time.Second)
	if _, err := bobB.sdk.Spaces().AcceptInvite(ctx, sp2.Id()); err != nil {
		require.ErrorIs(t, err, space.ErrInviteAcceptPending, "bobB: AcceptInvite must succeed or report pending")
	}
	statusOn(bobB, "bobB", sp2.Id(), space.StatusActive, 120*time.Second)
	statusOn(bobA, "bobA", sp2.Id(), space.StatusActive, 120*time.Second)
	gotB2, err := bobB.sdk.Spaces().Get(ctx, sp2.Id())
	require.NoError(t, err, "bobB: Get after accept")
	require.Equal(t, sp2.Id(), gotB2.Id())
	gotA2, err := bobA.sdk.Spaces().Get(ctx, sp2.Id())
	require.NoError(t, err, "bobA: Get after the accept converged")
	require.Equal(t, sp2.Id(), gotA2.Id())

	// 6. Join on a row the ACL already grants: a declined join, then a
	// direct add, then Join with the token on A — no request lands, the
	// space loads, both devices converge. Whether the inbox registered
	// the invite before or after the Join does not matter: either row
	// shape is non-active, so the refusal is taken as membership.
	sp3, token3 := createInvited("SyncedJoin3")
	_, err = bobA.sdk.Spaces().Join(ctx, space.JoinRequest{Invite: token3})
	require.ErrorIs(t, err, spaceimpl.ErrJoinPending, "bobA: Join (space 3)")
	awaitJoinRequest(t, ctx, sp3, bob)
	require.NoError(t, sp3.ACL().DeclineRequest(ctx, bob), "alice: DeclineRequest (space 3)")
	statusOn(bobA, "bobA", sp3.Id(), space.StatusDeleted, 120*time.Second)
	statusOn(bobB, "bobB", sp3.Id(), space.StatusDeleted, 90*time.Second)
	requestsDrained(sp3)
	require.NoError(t, sp3.ACL().AddAccounts(ctx, []space.MemberAdd{{Identity: bob, Permission: space.PermissionWriter}}),
		"alice: AddAccounts (space 3)")
	_, err = bobA.sdk.Spaces().Join(ctx, space.JoinRequest{Invite: token3})
	require.ErrorIs(t, err, spaceimpl.ErrJoinPending, "bobA: Join on a granted identity is taken as accepted")
	statusOn(bobA, "bobA", sp3.Id(), space.StatusActive, 120*time.Second)
	statusOn(bobB, "bobB", sp3.Id(), space.StatusActive, 120*time.Second)
	gotA3, err := bobA.sdk.Spaces().Get(ctx, sp3.Id())
	require.NoError(t, err, "bobA: Get after the heal")
	require.Equal(t, sp3.Id(), gotA3.Id())
	reqs, err := sp3.Members().JoinRequests(ctx)
	require.NoError(t, err)
	require.Empty(t, reqs, "the heal must not post a new join request")
}
