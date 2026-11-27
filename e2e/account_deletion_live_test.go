package e2e

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
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

// TestE2E_AccountDeletionRequestAndRevert runs an account deletion
// against a real coordinator and reverts it:
//
//   - Alice has two devices, a private space and a space shared with
//     Bob. She requests the deletion: the state is pending with a date,
//     a second request is refused, and the coordinator refuses ACL
//     writes on her space.
//   - Several reconcile passes later every device still holds its
//     spaces: Alice's two devices and Bob's copy of the shared one.
//   - A device that holds nothing of the account reads the pending
//     state.
//   - She reverts: the state is active, a second revert is refused, the
//     coordinator accepts ACL writes again, and a record written after
//     the revert reaches her other device.
//
// Requires a running any-sync network whose coordinator has a deletion
// period longer than the test; set ANYSYNC_LOCAL_NETWORK to its
// node-config YAML to run. Skipped otherwise.
func TestE2E_AccountDeletionRequestAndRevert(t *testing.T) {
	// Not parallel: it counts the reconcile passes of its own devices.
	netPath := os.Getenv("ANYSYNC_LOCAL_NETWORK")
	if netPath == "" {
		t.Skip("set ANYSYNC_LOCAL_NETWORK to the node-config YAML to run this live test")
	}
	yaml, err := os.ReadFile(netPath)
	require.NoError(t, err, "read network config %s", netPath)

	// Short reconcile interval, so the passes that must leave pending
	// spaces alone fit in the test (set before opening any SDK).
	const reconcileEvery = 2 * time.Second
	defer spaceimpl.SetDeletionReconcileIntervalForTest(reconcileEvery)()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	open := func(name string, keys *fixedSeedProvider) (*anysyncsdk.SDK, string) {
		t.Helper()
		dir := t.TempDir()
		cfg := config.Config{
			Storage: config.Storage{DataDir: dir, Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: yaml},
			Types:   []handler.Type{newBlocksType()},
		}
		sdk, oerr := anysyncsdk.Open(ctx, cfg, keys)
		require.NoError(t, oerr, "%s: Open", name)
		t.Cleanup(func() { _ = sdk.Close() })
		return sdk, dir
	}
	storage := func(dir, spaceId string) string { return filepath.Join(dir, "anysync", spaceId+".db") }
	active := func(sdk *anysyncsdk.SDK, spaceId string) bool {
		list, lerr := sdk.Spaces().List(ctx)
		if lerr != nil {
			return false
		}
		for _, info := range list {
			if info.Id == spaceId {
				return info.Status == space.StatusActive
			}
		}
		return false
	}
	write := func(sp space.Space, objId, recId string) {
		t.Helper()
		_, merr := sp.Modify(ctx, space.ModifyBatch{
			ObjectId: objId,
			Dataset:  blocksDataset,
			Records:  []space.RecordModify{{Id: recId, Upsert: true, Ops: []space.Op{{Type: space.OpSet, Path: "text", Value: recId}}}},
		})
		require.NoError(t, merr, "write %s", recId)
	}

	// One account key, a device key per device: devices that share a
	// peer id close each other's connections.
	aliceKeys := newFixedSeedProvider(t)
	device := func() *fixedSeedProvider {
		t.Helper()
		_, dev, gerr := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, gerr)
		return &fixedSeedProvider{account: aliceKeys.account, device: dev}
	}
	aliceA, dirA := open("alice A", aliceKeys)
	bob, bobDir := open("bob", newFixedSeedProvider(t))

	// Alice: a private space and a shared one, each with a record.
	private, err := aliceA.Spaces().Create(ctx, space.CreateRequest{Name: "DeletionPrivate"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on create: %v", err)
		}
		t.Fatalf("alice create: %v", err)
	}
	privateObj, err := private.Objects().Create(ctx, space.CreateObjectOpts{Type: "blocks-type"})
	require.NoError(t, err)
	write(private, privateObj, "rec-1")
	shared, err := aliceA.Spaces().Create(ctx, space.CreateRequest{Name: "DeletionShared"})
	require.NoError(t, err)
	sharedObj, err := shared.Objects().Create(ctx, space.CreateObjectOpts{Type: "blocks-type"})
	require.NoError(t, err)
	write(shared, sharedObj, "rec-1")

	// Bob joins the shared space.
	var inv space.Invite
	require.True(t, waitFor(ctx, 30*time.Second, time.Second, func() bool {
		inv, err = shared.ACL().CreateInvite(ctx)
		return err == nil
	}), "alice CreateInvite: %v", err)
	token, err := space.EncodeInvite(inv)
	require.NoError(t, err)
	_, err = bob.Spaces().Join(ctx, space.JoinRequest{Invite: token, Metadata: space.AccountMetadata{Name: "Bob"}})
	require.True(t, errors.Is(err, spaceimpl.ErrJoinPending), "bob Join: %v", err)
	var jr space.JoinRequestInfo
	require.True(t, waitFor(ctx, 90*time.Second, time.Second, func() bool {
		_ = shared.SyncHeads(ctx)
		reqs, _ := shared.Members().JoinRequests(ctx)
		if len(reqs) > 0 {
			jr = reqs[0]
			return true
		}
		return false
	}), "alice never saw bob's join request")
	require.NoError(t, shared.ACL().AcceptRequest(ctx, jr.RecordId, space.PermissionWriter))
	require.True(t, waitFor(ctx, 90*time.Second, time.Second, func() bool {
		bobSpace, gerr := bob.Spaces().Get(ctx, shared.Id())
		if gerr != nil {
			return false
		}
		_ = bobSpace.SyncHeads(ctx)
		me, merr := bobSpace.Members().Me(ctx)
		return merr == nil && me.Status == space.MemberStatusActive
	}), "bob never reached active")
	require.FileExists(t, storage(bobDir, shared.Id()))

	// Alice's second device pulls both spaces from the nodes.
	_ = aliceA.Spaces().SyncSpaceList(ctx)
	_ = private.SyncHeads(ctx)
	_ = shared.SyncHeads(ctx)
	aliceB, dirB := open("alice B", device())
	for _, id := range []string{private.Id(), shared.Id()} {
		require.True(t, waitFor(ctx, 2*time.Minute, time.Second, func() bool {
			_, gerr := aliceB.Spaces().Get(ctx, id)
			return gerr == nil
		}), "alice B never loaded space %s", id)
	}
	privateB, err := aliceB.Spaces().Get(ctx, private.Id())
	require.NoError(t, err)
	require.True(t, waitFor(ctx, 2*time.Minute, time.Second, func() bool {
		_ = privateB.SyncHeads(ctx)
		rec, qerr := privateB.Query(privateObj, blocksDataset).Filter(map[string]any{"id": "rec-1"}).One(ctx)
		return qerr == nil && rec != nil
	}), "alice B never saw rec-1")

	// Request. The coordinator knows the account once its tech space
	// has been pushed.
	var (
		st   space.AccountDeletion
		lost bool // an attempt failed without an answer
	)
	require.True(t, waitFor(ctx, time.Minute, time.Second, func() bool {
		st, err = aliceA.Account().RequestDeletion(ctx)
		if errors.Is(err, space.ErrNetworkUnavailable) {
			lost = true
		}
		return !errors.Is(err, space.ErrAccountNotOnNetwork) && !errors.Is(err, space.ErrNetworkUnavailable)
	}), "the coordinator never took alice's request: %v", err)
	if lost && errors.Is(err, space.ErrAccountDeletionPending) {
		// The coordinator took the attempt whose answer was lost.
		kept, kerr := aliceA.Account().Deletion(ctx)
		require.NoError(t, kerr)
		require.NotNil(t, kept)
		st, err = *kept, nil
	}
	require.NoError(t, err, "RequestDeletion")
	require.Equal(t, space.AccountDeletionPending, st.State)
	require.True(t, st.DeleteAt.After(time.Now()), "DeleteAt %s is not in the future", st.DeleteAt)
	kept, err := aliceA.Account().Deletion(ctx)
	require.NoError(t, err)
	require.NotNil(t, kept)
	require.Equal(t, space.AccountDeletionPending, kept.State)
	require.True(t, st.DeleteAt.Equal(kept.DeleteAt))

	// A second request, from either device, is refused as pending, and
	// the refusing read leaves the state on that device.
	_, err = aliceA.Account().RequestDeletion(ctx)
	require.ErrorIs(t, err, space.ErrAccountDeletionPending)
	_, err = aliceB.Account().RequestDeletion(ctx)
	require.ErrorIs(t, err, space.ErrAccountDeletionPending)
	kept, err = aliceB.Account().Deletion(ctx)
	require.NoError(t, err)
	require.NotNil(t, kept)
	require.Equal(t, space.AccountDeletionPending, kept.State)
	require.WithinDuration(t, st.DeleteAt, kept.DeleteAt, time.Second)

	// The spaces Alice owns are pending on the coordinator: it refuses
	// an ACL write on them.
	demoteBob := []space.PermissionChange{{Identity: bob.Account().Id(), Permission: space.PermissionReader}}
	err = shared.ACL().ChangePermissions(ctx, demoteBob)
	require.ErrorIs(t, err, space.ErrCoordinatorSpaceDeleted, "ACL write on a space in pending deletion")

	// Several reconcile passes with live statuses on the three open
	// devices: at least three each, were they evenly spread.
	passes := spaceimpl.DeletionReconcilePassesForTest()
	select {
	case <-time.After(6 * reconcileEvery):
	case <-ctx.Done():
		t.Fatal("test deadline during the reconcile wait")
	}
	require.GreaterOrEqual(t, spaceimpl.DeletionReconcilePassesForTest()-passes, int64(9),
		"too few reconcile passes received coordinator statuses")
	for _, dev := range []struct {
		name string
		sdk  *anysyncsdk.SDK
		dir  string
	}{{"alice A", aliceA, dirA}, {"alice B", aliceB, dirB}} {
		for _, id := range []string{private.Id(), shared.Id()} {
			require.FileExists(t, storage(dev.dir, id), "%s lost a space while the deletion is pending", dev.name)
			require.True(t, active(dev.sdk, id), "%s: space %s is not active while the deletion is pending", dev.name, id)
		}
	}
	require.FileExists(t, storage(bobDir, shared.Id()), "bob lost the shared space while its owner's deletion is pending")
	require.True(t, active(bob, shared.Id()), "bob: the shared space is not active")

	// A device that holds nothing of the account reads the state.
	aliceC, _ := open("alice C", device())
	require.True(t, waitFor(ctx, time.Minute, time.Second, func() bool {
		d, derr := aliceC.Account().Deletion(ctx)
		return derr == nil && d != nil && d.State == space.AccountDeletionPending
	}), "a fresh device never read the pending state")

	// Revert.
	st, err = aliceA.Account().RevertDeletion(ctx)
	require.NoError(t, err, "RevertDeletion")
	require.Equal(t, space.AccountActive, st.State)
	require.True(t, st.DeleteAt.IsZero())
	_, err = aliceA.Account().RevertDeletion(ctx)
	require.ErrorIs(t, err, space.ErrAccountDeletionNotPending)
	kept, err = aliceA.Account().Deletion(ctx)
	require.NoError(t, err)
	require.NotNil(t, kept)
	require.Equal(t, space.AccountActive, kept.State)

	// The coordinator accepts ACL writes again, and a record written
	// after the revert reaches the other device.
	require.True(t, waitFor(ctx, time.Minute, 2*time.Second, func() bool {
		err = shared.ACL().ChangePermissions(ctx, demoteBob)
		return err == nil
	}), "ACL write after the revert: %v", err)
	write(private, privateObj, "rec-2")
	require.True(t, waitFor(ctx, 4*time.Minute, 2*time.Second, func() bool {
		_ = private.SyncHeads(ctx)
		_ = privateB.SyncHeads(ctx)
		rec, qerr := privateB.Query(privateObj, blocksDataset).Filter(map[string]any{"id": "rec-2"}).One(ctx)
		return qerr == nil && rec != nil
	}), "a record written after the revert never reached alice B")
}
