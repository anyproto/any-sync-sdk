package e2e

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_Identities_DirectoryResolveAndPrune is the end-to-end proof of
// the identities directory:
//
//   - two accounts share a space; each resolves the OTHER's profile from
//     identityRepo, decrypted with the metadata symkey distributed through
//     the ACL (owner-root for the owner, join-record for the joiner);
//   - the shared space id appears in each side's SpaceIds sighting set;
//   - Identities().Subscribe streams the resolution;
//   - deleting the space prunes the spaceId from the directory.
func TestE2E_Identities_DirectoryResolveAndPrune(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("staging config not available at %s: %v", confPath, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	mkSDK := func(name string) *anysyncsdk.SDK {
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

	owner := mkSDK("owner")
	joiner := mkSDK("joiner")
	require.NotEqual(t, owner.Account().Id(), joiner.Account().Id())
	ownerId := owner.Account().Id()
	joinerId := joiner.Account().Id()

	// Both publish profiles up-front so the peer can resolve them.
	require.NoError(t, owner.Account().UpdateMetadata(ctx, space.AccountMetadata{Name: "Owner Name", IconCID: "owner-cid"}))
	require.NoError(t, joiner.Account().UpdateMetadata(ctx, space.AccountMetadata{Name: "Joiner Name"}))

	// Owner creates + shares a space; joiner requests; owner accepts.
	sp, err := owner.Spaces().Create(ctx, space.CreateRequest{Name: "Identities E2E"})
	require.NoError(t, err)
	spaceId := sp.Id()

	inv, err := sp.ACL().CreateInvite(ctx)
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable: %v", err)
		}
		t.Fatalf("CreateInvite: %v", err)
	}
	token, err := space.EncodeInvite(inv)
	require.NoError(t, err)

	// Start the owner members watcher (also drives the identities write-through).
	cancelSub := sp.Members().Subscribe(func(space.MemberEvent) {})
	t.Cleanup(cancelSub)

	// Subscribe to the owner's identities directory to prove the stream fires.
	var (
		idMu     sync.Mutex
		idEvents []space.IdentityListEvent
	)
	cancelIDSub := owner.Identities().Subscribe(func(ev space.IdentityListEvent) {
		idMu.Lock()
		idEvents = append(idEvents, ev)
		idMu.Unlock()
	})
	t.Cleanup(cancelIDSub)

	_, err = joiner.Spaces().Join(ctx, space.JoinRequest{Invite: token})
	require.ErrorIs(t, err, spaceimpl.ErrJoinPending)

	var joinReq space.JoinRequestInfo
	require.Eventually(t, func() bool {
		_ = sp.SyncHeads(ctx)
		reqs, err := sp.Members().JoinRequests(ctx)
		if err != nil || len(reqs) == 0 {
			return false
		}
		joinReq = reqs[0]
		return true
	}, 120*time.Second, 2*time.Second, "owner never saw the join request")

	require.NoError(t, sp.ACL().AcceptRequest(ctx, joinReq.RecordId, space.PermissionWriter))

	// Owner resolves the joiner's profile into the directory, with the
	// shared space recorded as a sighting.
	require.Eventually(t, func() bool {
		info, ok, _ := owner.Identities().Get(ctx, joinerId)
		return ok && info.Name == "Joiner Name" && containsStr(info.SpaceIds, spaceId)
	}, 60*time.Second, time.Second, "owner directory should resolve joiner name + spaceId")

	// Owner's directory List includes the joiner.
	all, err := owner.Identities().List(ctx)
	require.NoError(t, err)
	assert.True(t, containsIdentity(all, joinerId), "List should include the joiner")

	// The subscription streamed at least one event mentioning the joiner.
	require.Eventually(t, func() bool {
		idMu.Lock()
		defer idMu.Unlock()
		for _, ev := range idEvents {
			for _, info := range append(ev.Added, ev.Updated...) {
				if info.Identity == joinerId {
					return true
				}
			}
		}
		return false
	}, 10*time.Second, 200*time.Millisecond, "Identities().Subscribe should stream the joiner")

	// Joiner side: once its space materializes, its members watcher resolves
	// the OWNER's profile (symkey from the ACL root) + records the spaceId.
	require.Eventually(t, func() bool {
		jsp, err := joiner.Spaces().Get(ctx, spaceId)
		if err != nil {
			return false
		}
		jsp.Members().Subscribe(func(space.MemberEvent) {})() // start watcher, drop cancel
		info, ok, _ := joiner.Identities().Get(ctx, ownerId)
		return ok && info.Name == "Owner Name" && containsStr(info.SpaceIds, spaceId)
	}, 90*time.Second, 2*time.Second, "joiner directory should resolve owner name + spaceId")

	// Prune-on-delete: owner deletes the space → spaceId leaves the joiner's
	// sighting set in the owner directory.
	require.NoError(t, owner.Spaces().Delete(ctx, spaceId))
	require.Eventually(t, func() bool {
		info, ok, _ := owner.Identities().Get(ctx, joinerId)
		return !ok || !containsStr(info.SpaceIds, spaceId)
	}, 30*time.Second, time.Second, "deleting the space should prune the spaceId sighting")
}

func containsStr(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

func containsIdentity(infos []space.IdentityInfo, id string) bool {
	for _, i := range infos {
		if i.Identity == id {
			return true
		}
	}
	return false
}
