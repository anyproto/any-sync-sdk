package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_JoinCancelRejoin covers the joiner-side withdrawal of a
// pending join and the re-request after it:
//
//  1. Alice creates a space and mints a RequestToJoin invite.
//  2. CancelJoin refuses an unknown id (ErrSpaceUnknown) and a row that
//     is not joining (ErrJoinNotPending) without touching the network.
//  3. Bob calls Join — ErrJoinPending, row StatusJoining; Alice sees the
//     request.
//  4. Bob calls CancelJoin — the row is StatusDeleted synchronously, Get
//     refuses it, a second CancelJoin is ErrJoinNotPending, and Alice's
//     pending list drains.
//  5. Bob calls Join again with the same invite — the ended row revives
//     to StatusJoining, Alice sees a fresh request, accepts it, and Bob's
//     controller flips the row to StatusActive.
//
// Skips when no any-sync network is reachable.
func TestE2E_JoinCancelRejoin(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("join-cancel e2e is slow (~60-120s); rerun without -short")
	}

	// Wider than the sum of the step budgets below, so a late step never
	// reports a failure that happened earlier.
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
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
	require.NoError(t, bob.Account().UpdateMetadata(ctx, space.AccountMetadata{Name: "Bob"}))

	sp, err := alice.Spaces().Create(ctx, space.CreateRequest{Name: "AliceLib"})
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

	// Row-state gate, before any request exists.
	err = bob.Spaces().CancelJoin(ctx, "bafyunknown.space")
	require.ErrorIs(t, err, space.ErrSpaceUnknown, "cancel on an unknown id")
	err = alice.Spaces().CancelJoin(ctx, sp.Id())
	require.ErrorIs(t, err, space.ErrJoinNotPending, "cancel on an active (owned) row")

	// Bob requests to join.
	_, err = bob.Spaces().Join(ctx, space.JoinRequest{Invite: token})
	require.ErrorIs(t, err, spaceimpl.ErrJoinPending, "bob: first Join")
	require.Equal(t, space.StatusJoining, bobStatus(t, ctx, bob, sp.Id()))

	first := awaitJoinRequest(t, ctx, sp, bob.Account().Id())

	// Bob withdraws. The end state is observable on return — no polling.
	require.NoError(t, bob.Spaces().CancelJoin(ctx, sp.Id()), "bob: CancelJoin")
	require.Equal(t, space.StatusDeleted, bobStatus(t, ctx, bob, sp.Id()),
		"a withdrawn join reads as deleted on the joiner")
	_, err = bob.Spaces().Get(ctx, sp.Id())
	require.ErrorIs(t, err, space.ErrSpaceDeleted, "Get must refuse the ended row")
	err = bob.Spaces().CancelJoin(ctx, sp.Id())
	require.ErrorIs(t, err, space.ErrJoinNotPending, "second cancel has nothing to withdraw")

	// Alice's pending list drains once the cancel record pulls.
	if !waitFor(ctx, 90*time.Second, 1*time.Second, func() bool {
		_ = sp.SyncHeads(ctx)
		reqs, err := sp.Members().JoinRequests(ctx)
		return err == nil && len(reqs) == 0
	}) {
		t.Fatalf("alice's join requests never drained after bob's cancel")
	}
	// The drain proves the cancel record applied, so the owner-side
	// account state is deterministic here.
	m, err := sp.Members().Get(ctx, bob.Account().Id())
	require.NoError(t, err, "owner-side member row after cancel")
	require.Equal(t, space.MemberStatusCanceled, m.Status, "owner-side member row after cancel")

	// Bob re-requests with the same invite: the ended row revives.
	_, err = bob.Spaces().Join(ctx, space.JoinRequest{Invite: token})
	require.ErrorIs(t, err, spaceimpl.ErrJoinPending, "bob: second Join")
	require.Equal(t, space.StatusJoining, bobStatus(t, ctx, bob, sp.Id()))

	second := awaitJoinRequest(t, ctx, sp, bob.Account().Id())
	require.NotEqual(t, first.RecordId, second.RecordId, "re-request must be a fresh ACL record")

	require.NoError(t, sp.ACL().AcceptRequest(ctx, second.RecordId, space.PermissionWriter),
		"alice: AcceptRequest")

	// Bob's controller loads the space and flips the row on its own.
	if !waitFor(ctx, 120*time.Second, 1*time.Second, func() bool {
		return bobStatus(t, ctx, bob, sp.Id()) == space.StatusActive
	}) {
		t.Fatalf("bob's re-requested join never reached active after accept")
	}
	got, err := bob.Spaces().Get(ctx, sp.Id())
	require.NoError(t, err, "bob: Get after accept")
	require.Equal(t, sp.Id(), got.Id())
	err = bob.Spaces().CancelJoin(ctx, sp.Id())
	require.ErrorIs(t, err, space.ErrJoinNotPending, "cancel on a joined row")
}

// bobStatus reads spaceId's status off sdk's space list; StatusUnknown
// when the row is absent.
func bobStatus(t *testing.T, ctx context.Context, sdk *anysyncsdk.SDK, spaceId string) space.Status {
	t.Helper()
	infos, err := sdk.Spaces().List(ctx)
	require.NoError(t, err)
	for _, si := range infos {
		if si.Id == spaceId {
			return si.Status
		}
	}
	return space.StatusUnknown
}

// awaitJoinRequest polls the owner's pending join requests until one
// from identity lands, kicking a head-sync round per tick so the ACL
// record pulls ahead of the periodic timer.
func awaitJoinRequest(t *testing.T, ctx context.Context, sp space.Space, identity string) space.JoinRequestInfo {
	t.Helper()
	var out space.JoinRequestInfo
	if !waitFor(ctx, 90*time.Second, 1*time.Second, func() bool {
		_ = sp.SyncHeads(ctx)
		reqs, err := sp.Members().JoinRequests(ctx)
		if err != nil {
			return false
		}
		for _, r := range reqs {
			if r.Identity == identity {
				out = r
				return true
			}
		}
		return false
	}) {
		t.Fatalf("owner never saw a join request from %s", identity)
	}
	return out
}
