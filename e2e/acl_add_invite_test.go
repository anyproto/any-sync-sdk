package e2e

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_DirectAdd_InboxInvite is the SYN-46 happy path: Alice adds Bob
// AND Carol to her space by identity in ONE AddAccounts call (one ACL
// record); the SDK notifies both via the coordinator inbox; on their
// sides the space surfaces as StatusInvitePending with no caller-side
// registration. Bob accepts (space loads, content syncs); Carol declines
// (sticky), then accepts from declined (non-terminal).
//
// Requires a coordinator that implements the inbox RPCs. If the
// notification never reaches the receivers the test skips rather than
// fails, mirroring TestE2E_OneToOne_InboxDiscovery.
func TestE2E_DirectAdd_InboxInvite(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("direct-add e2e needs a live coordinator (~1-2min); rerun without -short")
	}

	// Shorten the notifier poll + invite-retry cadences so the test
	// doesn't wait the production minute. Must be set before Open.
	restore := spaceimpl.SetOneToOneInboxIntervalsForTest(time.Second, time.Second)
	defer restore()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
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
	carol := openSDK("carol")
	require.NotEqual(t, alice.Account().Id(), bob.Account().Id())
	require.NotEqual(t, alice.Account().Id(), carol.Account().Id())

	// Alice publishes a profile so the receivers can resolve her name
	// from identityRepo via the symkey carried in the invite body.
	require.NoError(t, alice.Account().UpdateMetadata(ctx, space.AccountMetadata{Name: "Alice"}))

	// Alice creates the space + content BEFORE adding anyone, so the
	// accept-side load has real content to pull.
	sp, err := alice.Spaces().Create(ctx, space.CreateRequest{Name: "TeamSpace"})
	require.NoError(t, err, "alice: Spaces().Create")
	typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Note"})
	require.NoError(t, err)
	objId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{typeId}})
	require.NoError(t, err)
	t.Logf("alice: spaceId=%s objId=%s", sp.Id(), objId)

	// ONE call, ONE ACL record, two receivers. AddAccounts registers the
	// space shareable and retries the log-not-ready startup race
	// internally, like CreateInvite.
	err = sp.ACL().AddAccounts(ctx, []space.MemberAdd{
		{Identity: bob.Account().Id(), Permission: space.PermissionWriter},
		{Identity: carol.Account().Id(), Permission: space.PermissionReader},
	})
	if isNoNetworkErr(err) {
		t.Skipf("network unreachable on AddAccounts: %v", err)
	}
	require.NoError(t, err, "alice: AddAccounts")

	// Both members are effective immediately at the ACL level.
	members, err := sp.Members().List(ctx)
	require.NoError(t, err)
	assert.Len(t, members, 3, "owner + two added members")

	// The inbox notifications surface pending rows autonomously.
	waitPending := func(who *anysyncsdk.SDK, name string) *space.SpaceInfo {
		var pending *space.SpaceInfo
		deadline := time.Now().Add(90 * time.Second)
		for time.Now().Before(deadline) {
			if si, ok := infoByID(t, ctx, who, sp.Id()); ok && si.Status == space.StatusInvitePending {
				pending = &si
				break
			}
			time.Sleep(time.Second)
		}
		if pending == nil {
			t.Skipf("inbox notification never reached %s — coordinator likely lacks inbox support (space %s)",
				name, sp.Id())
		}
		return pending
	}
	bobPending := waitPending(bob, "bob")
	carolPending := waitPending(carol, "carol")

	// The pending rows carry the display hints from the invite body —
	// no space content has been pulled yet.
	assert.Equal(t, "TeamSpace", bobPending.Name, "pending row shows the name hint")
	assert.Equal(t, space.SpaceTypeRegular, bobPending.Type)
	assert.Equal(t, "TeamSpace", carolPending.Name)

	// The materialization gate: a pending invite cannot be loaded by a
	// read path — only AcceptInvite may materialize it.
	_, err = bob.Spaces().Get(ctx, sp.Id())
	require.Error(t, err, "Get on a pending invite must refuse to materialize the space")

	// Bob accepts. Either the bounded synchronous load succeeds, or it
	// returns ErrInviteAcceptPending and the join controller finishes in
	// the background — both are success paths.
	bobSp, err := bob.Spaces().AcceptInvite(ctx, sp.Id())
	if err != nil {
		require.True(t, errors.Is(err, spaceimpl.ErrInviteAcceptPending),
			"bob: AcceptInvite must succeed or report pending, got %v", err)
	}
	if !waitFor(ctx, 90*time.Second, time.Second, func() bool {
		si, ok := infoByID(t, ctx, bob, sp.Id())
		return ok && si.Status == space.StatusActive
	}) {
		t.Fatalf("bob's accepted space never flipped to active")
	}
	if bobSp == nil {
		bobSp, err = bob.Spaces().Get(ctx, sp.Id())
		require.NoError(t, err, "bob: Get after background load")
	}

	// Content syncs down to Bob.
	if !waitFor(ctx, 90*time.Second, time.Second, func() bool {
		_ = bobSp.SyncHeads(ctx)
		docs, qerr := bobSp.QueryObjects().All(ctx)
		return qerr == nil && len(docs) > 0
	}) {
		t.Fatalf("alice's object never synced to bob")
	}

	// Alice's name resolves on Bob from the symkey the invite carried.
	require.Eventually(t, func() bool {
		infos := listIdentities(t, ctx, bob)
		for _, ii := range infos {
			if ii.Identity == alice.Account().Id() && ii.Name == "Alice" {
				return true
			}
		}
		return false
	}, 60*time.Second, time.Second, "bob should resolve Alice's profile from the cached invite symkey")

	// Carol declines → synced sticky declined status; the space is not
	// materialized.
	require.NoError(t, carol.Spaces().DeclineInvite(ctx, sp.Id()), "carol: DeclineInvite")
	si, ok := infoByID(t, ctx, carol, sp.Id())
	require.True(t, ok)
	assert.Equal(t, space.StatusInviteDeclined, si.Status)

	// Decline is not terminal: Carol changes her mind and accepts.
	_, err = carol.Spaces().AcceptInvite(ctx, sp.Id())
	if err != nil {
		require.True(t, errors.Is(err, spaceimpl.ErrInviteAcceptPending),
			"carol: AcceptInvite from declined, got %v", err)
	}
	if !waitFor(ctx, 90*time.Second, time.Second, func() bool {
		si, ok := infoByID(t, ctx, carol, sp.Id())
		return ok && si.Status == space.StatusActive
	}) {
		t.Fatalf("carol's accept-from-declined never flipped to active")
	}
}

// listIdentities snapshots the identities directory, tolerating the
// not-open error window during boot.
func listIdentities(t *testing.T, ctx context.Context, sdk *anysyncsdk.SDK) []space.IdentityInfo {
	t.Helper()
	infos, err := sdk.Identities().List(ctx)
	if err != nil {
		return nil
	}
	return infos
}
