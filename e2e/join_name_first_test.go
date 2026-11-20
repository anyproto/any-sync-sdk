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

// TestE2E_JoinerSeesSpaceNameFirst: a joiner of a space with many
// objects reads the space name off its space list while most of the
// space is still to sync. A sync round fetches the missing trees one at
// a time in the diff's hash order; the spaceIndex, which carries the
// name, goes first instead of landing at an arbitrary point of that
// order.
//
// Skips when no any-sync network is reachable.
func TestE2E_JoinerSeesSpaceNameFirst(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("join e2e is slow; rerun without -short")
	}

	const (
		spaceName = "AliceBigLib"
		objects   = 300
	)
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
	require.NoError(t, bob.Account().UpdateMetadata(ctx, space.AccountMetadata{Name: "Bob"}))

	sp, err := alice.Spaces().Create(ctx, space.CreateRequest{Name: spaceName})
	require.NoError(t, err, "alice: Spaces().Create")
	typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Note"})
	require.NoError(t, err)
	created := make(map[string]struct{}, objects)
	for i := 0; i < objects; i++ {
		id, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId})
		require.NoError(t, err)
		created[id] = struct{}{}
	}

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

	_, err = bob.Spaces().Join(ctx, space.JoinRequest{Invite: token})
	require.ErrorIs(t, err, spaceimpl.ErrJoinPending, "bob: Join")
	req := awaitJoinRequest(t, ctx, sp, bob.Account().Id())

	// Bob's first round must find the whole space on the network, or
	// the order under test covers only a part of it.
	if !waitFor(ctx, 2*time.Minute, 500*time.Millisecond, func() bool {
		_ = sp.SyncHeads(ctx)
		return alice.Spaces().Status(sp.Id()).State == space.SyncStateSynced
	}) {
		t.Fatalf("alice's space never reached synced")
	}

	require.NoError(t, sp.ACL().AcceptRequest(ctx, req.RecordId, space.PermissionWriter), "alice: AcceptRequest")

	// The name is read off the space list — the row every client
	// renders — without loading the space.
	if !waitFor(ctx, 2*time.Minute, 5*time.Millisecond, func() bool {
		infos, err := bob.Spaces().List(ctx)
		if err != nil {
			return false
		}
		for _, si := range infos {
			if si.Id == sp.Id() {
				return si.Name == spaceName
			}
		}
		return false
	}) {
		t.Fatalf("bob never saw the space name")
	}

	var bobSpace space.Space
	if !waitFor(ctx, time.Minute, 5*time.Millisecond, func() bool {
		bobSpace, err = bob.Spaces().Get(ctx, sp.Id())
		return err == nil
	}) {
		t.Fatalf("bob: Get after the name arrived: %v", err)
	}
	local := func() int {
		docs, err := bobSpace.QueryObjects().All(ctx)
		require.NoError(t, err)
		n := 0
		for _, d := range docs {
			if _, ok := created[d.GetString("id")]; ok {
				n++
			}
		}
		return n
	}
	atName := local()
	t.Logf("bob: %d of %d objects local when the space name arrived", atName, objects)
	require.Less(t, atName, objects/4, "the space name must arrive ahead of the bulk of the space")

	// The rest of the space still syncs.
	if !waitFor(ctx, 3*time.Minute, 500*time.Millisecond, func() bool {
		return local() == objects
	}) {
		t.Fatalf("bob: only %d of %d objects synced", local(), objects)
	}
}
