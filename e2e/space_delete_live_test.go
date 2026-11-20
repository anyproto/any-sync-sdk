package e2e

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_OwnerDeletePropagatesToMember confirms the full network
// deletion loop against a real coordinator:
//
//   - Alice (owner) deletes a shared space. Her reconciler sends the
//     signed coordinator.SpaceDelete; if the coordinator rejects the
//     payload this never propagates and the test fails at step 6.
//   - The coordinator moves the space into deletion.
//   - Bob (a joined member) never called Delete, but his reconciler
//     polls StatusCheckMany, sees the space is gone network-side, and
//     offloads his local copy — proving both that Alice's delete reached
//     the coordinator and that inbound detection works.
//
// Requires a running any-sync network; set ANYSYNC_LOCAL_NETWORK to its
// node-config YAML to run. Skipped otherwise (e.g. CI without infra).
func TestE2E_OwnerDeletePropagatesToMember(t *testing.T) {
	t.Parallel()
	netPath := os.Getenv("ANYSYNC_LOCAL_NETWORK")
	if netPath == "" {
		t.Skip("set ANYSYNC_LOCAL_NETWORK to the node-config YAML to run this live test")
	}
	yaml, err := os.ReadFile(netPath)
	require.NoError(t, err, "read network config %s", netPath)

	// Shorten the reconcile interval so Bob's inbound-detection tick
	// fires within the test window (set before opening any SDK — the
	// loop reads the interval once at start).
	defer spaceimpl.SetDeletionReconcileIntervalForTest(2 * time.Second)()

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	open := func(name string) (*anysyncsdk.SDK, string) {
		t.Helper()
		dir := t.TempDir()
		cfg := config.Config{
			Storage: config.Storage{DataDir: dir, Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: yaml},
			Types:   []handler.Type{newBlocksType()},
		}
		sdk, oerr := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
		require.NoError(t, oerr, "%s: Open", name)
		t.Cleanup(func() { _ = sdk.Close() })
		return sdk, dir
	}

	alice, aliceDir := open("alice")
	bob, bobDir := open("bob")
	require.NotEqual(t, alice.Account().Id(), bob.Account().Id())

	// Alice creates a space + object + record.
	sp, err := alice.Spaces().Create(ctx, space.CreateRequest{Name: "DeleteLive"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on create: %v", err)
		}
		t.Fatalf("alice create: %v", err)
	}
	objId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: "blocks-type"})
	require.NoError(t, err)
	_, err = sp.Modify(ctx, space.ModifyBatch{
		ObjectId: objId,
		Dataset:  blocksDataset,
		Records:  []space.RecordModify{{Id: "rec-1", Upsert: true, Ops: []space.Op{{Type: space.OpSet, Path: "text", Value: "hi"}}}},
	})
	require.NoError(t, err)

	// Invite + join + accept.
	var inv space.Invite
	require.True(t, waitFor(ctx, 30*time.Second, time.Second, func() bool {
		inv, err = sp.ACL().CreateInvite(ctx)
		return err == nil
	}), "alice CreateInvite: %v", err)
	token, err := space.EncodeInvite(inv)
	require.NoError(t, err)

	_, err = bob.Spaces().Join(ctx, space.JoinRequest{Invite: token, Metadata: space.AccountMetadata{Name: "Bob"}})
	require.True(t, errors.Is(err, spaceimpl.ErrJoinPending), "bob Join: %v", err)

	var jr space.JoinRequestInfo
	require.True(t, waitFor(ctx, 90*time.Second, time.Second, func() bool {
		_ = sp.SyncHeads(ctx)
		reqs, _ := sp.Members().JoinRequests(ctx)
		if len(reqs) > 0 {
			jr = reqs[0]
			return true
		}
		return false
	}), "alice never saw bob's join request")
	require.NoError(t, sp.ACL().AcceptRequest(ctx, jr.RecordId, space.PermissionWriter))

	// Bob gets the space and reaches active membership.
	var bobSpace space.Space
	require.True(t, waitFor(ctx, 30*time.Second, time.Second, func() bool {
		bobSpace, err = bob.Spaces().Get(ctx, sp.Id())
		return err == nil
	}), "bob Get: %v", err)
	require.True(t, waitFor(ctx, 90*time.Second, time.Second, func() bool {
		_ = bobSpace.SyncHeads(ctx)
		me, mErr := bobSpace.Members().Me(ctx)
		return mErr == nil && me.Status == space.MemberStatusActive
	}), "bob never reached active")

	// Bob's local storage now exists.
	bobDB := filepath.Join(bobDir, "anysync", sp.Id()+".db")
	require.FileExists(t, bobDB, "bob should have local storage before deletion")

	// Alice deletes — local offload is immediate, network delete is sent
	// by her reconciler.
	require.NoError(t, alice.Spaces().Delete(ctx, sp.Id()))
	aliceDB := filepath.Join(aliceDir, "anysync", sp.Id()+".db")
	_, statErr := os.Stat(aliceDB)
	require.True(t, os.IsNotExist(statErr), "alice DB should be offloaded immediately")

	// The actual network confirmation: Bob's reconciler observes the
	// coordinator-side deletion and offloads his local copy.
	require.True(t, waitFor(ctx, 90*time.Second, 2*time.Second, func() bool {
		_, e := os.Stat(bobDB)
		return os.IsNotExist(e)
	}), "bob's local storage was never offloaded — owner delete did not propagate via the coordinator")
}
