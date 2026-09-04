package e2e

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_JoinEndedHealsOnMembership covers the device left behind by
// a withdrawn join when the account becomes a member through a request
// from another device:
//
//  1. Bob's device A requests to join Alice's space and withdraws
//     (CancelJoin) — the row is an ended join, StatusDeleted.
//  2. Bob's device B (same account, fresh device key) opens after the
//     withdrawal, syncs the ended row in, and requests with the same
//     token: the row revives to joining account-wide. Alice accepts; B
//     reaches StatusActive.
//  3. A never acts. The synced re-request gave A's controller a joining
//     row to watch (its head from step 1 was dropped with the
//     withdrawal; it resolves a fresh one from the chain), the
//     acceptance is observed on both devices, and the synced active
//     converges them — A reaches StatusActive and loads the space with
//     no new request landing on Alice's side.
//
// Skips when no any-sync network is reachable.
func TestE2E_JoinEndedHealsOnMembership(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("join-ended heal e2e is slow (~60-120s); rerun without -short")
	}

	restore := spaceimpl.SetJoinReconcileIntervalForTest(2 * time.Second)
	defer restore()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	openSDK := func(name string, prov *fixedSeedProvider) *anysyncsdk.SDK {
		t.Helper()
		cfg := config.Config{
			Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: yaml},
		}
		sdk, err := anysyncsdk.Open(ctx, cfg, prov)
		require.NoError(t, err, "%s: Open", name)
		t.Cleanup(func() { _ = sdk.Close() })
		return sdk
	}

	alice := openSDK("alice", newFixedSeedProvider(t))
	bobProv := newFixedSeedProvider(t)
	bobA := openSDK("bobA", bobProv)
	require.NotEqual(t, alice.Account().Id(), bobA.Account().Id())

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

	// 1. Device A requests and withdraws.
	_, err = bobA.Spaces().Join(ctx, space.JoinRequest{Invite: token})
	require.ErrorIs(t, err, spaceimpl.ErrJoinPending, "bobA: Join")
	awaitJoinRequest(t, ctx, sp, bobA.Account().Id())
	require.NoError(t, bobA.Spaces().CancelJoin(ctx, sp.Id()), "bobA: CancelJoin")
	require.Equal(t, space.StatusDeleted, bobStatus(t, ctx, bobA, sp.Id()))
	if !waitFor(ctx, 90*time.Second, 1*time.Second, func() bool {
		_ = sp.SyncHeads(ctx)
		reqs, err := sp.Members().JoinRequests(ctx)
		return err == nil && len(reqs) == 0
	}) {
		t.Fatalf("alice's join requests never drained after bobA's cancel")
	}

	// 2. Device B (same account) requests; Alice accepts; B goes active.
	_, devB, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	bobB := openSDK("bobB", &fixedSeedProvider{account: bobProv.account, device: devB})
	require.Equal(t, bobA.Account().Id(), bobB.Account().Id(), "same account on both devices")
	_, err = bobB.Spaces().Join(ctx, space.JoinRequest{Invite: token})
	require.ErrorIs(t, err, spaceimpl.ErrJoinPending, "bobB: Join")
	req := awaitJoinRequest(t, ctx, sp, bobB.Account().Id())
	require.NoError(t, sp.ACL().AcceptRequest(ctx, req.RecordId, space.PermissionWriter), "alice: AcceptRequest")
	if !waitFor(ctx, 120*time.Second, 1*time.Second, func() bool {
		return bobStatus(t, ctx, bobB, sp.Id()) == space.StatusActive
	}) {
		t.Fatalf("bobB never reached active after accept")
	}

	// 3. Device A converges without acting: the re-request and the
	// acceptance both reached it through the synced row.
	if !waitFor(ctx, 120*time.Second, 1*time.Second, func() bool {
		_ = bobA.Spaces().SyncSpaceList(ctx)
		return bobStatus(t, ctx, bobA, sp.Id()) == space.StatusActive
	}) {
		t.Fatalf("bobA never converged to active after the account became a member")
	}
	got, err := bobA.Spaces().Get(ctx, sp.Id())
	require.NoError(t, err, "bobA: Get after convergence")
	require.Equal(t, sp.Id(), got.Id())
	reqs, err := sp.Members().JoinRequests(ctx)
	require.NoError(t, err)
	require.Empty(t, reqs, "convergence must not post a new join request")
}
