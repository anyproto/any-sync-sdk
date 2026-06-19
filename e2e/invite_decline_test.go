package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_InviteDeclineAutonomous covers the reject branch of the
// joiner-side post-acceptance controller:
//
//  1. Alice creates a space and mints a RequestToJoin invite.
//  2. Bob calls Join — returns ErrJoinPending; his join controller
//     starts an ACL waiter.
//  3. Alice declines the request.
//  4. Bob's waiter detects the decline (the join record removed) and
//     autonomously flips his tech-space row to StatusDeleted — no
//     caller-side polling of Get().
//
// Skips when no any-sync network is reachable.
func TestE2E_InviteDeclineAutonomous(t *testing.T) {
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("invite-decline e2e is slow (~30-60s); rerun without -short")
	}

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

	// Bob requests to join.
	_, err = bob.Spaces().Join(ctx, space.JoinRequest{
		Invite:   token,
		Metadata: space.AccountMetadata{Name: "Bob"},
	})
	require.True(t, errors.Is(err, spaceimpl.ErrJoinPending),
		"bob: Join must return ErrJoinPending, got %v", err)

	// Alice waits for the request to land, then declines it.
	var joinReq space.JoinRequestInfo
	if !waitFor(ctx, 90*time.Second, 1*time.Second, func() bool {
		_ = sp.SyncHeads(ctx)
		reqs, _ := sp.Members().JoinRequests(ctx)
		if len(reqs) > 0 {
			joinReq = reqs[0]
			return true
		}
		return false
	}) {
		t.Fatalf("alice never saw bob's join request")
	}
	require.Equal(t, bob.Account().Id(), joinReq.Identity)
	require.NoError(t, sp.ACL().DeclineRequest(ctx, joinReq.Identity),
		"alice: DeclineRequest")

	// Bob's controller flips the row to deleted on its own.
	if !waitFor(ctx, 90*time.Second, 1*time.Second, func() bool {
		_ = bob.Spaces().SyncSpaceList(ctx)
		infos, listErr := bob.Spaces().List(ctx)
		if listErr != nil {
			return false
		}
		for _, si := range infos {
			if si.Id == sp.Id() {
				return si.Status == space.StatusDeleted
			}
		}
		return false
	}) {
		t.Fatalf("bob's declined space never flipped to deleted autonomously")
	}
}
