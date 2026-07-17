package e2e

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"
	"github.com/anyproto/any-sync-sdk/space"
)

// blocksDataset / blocksDataVersion mirror the shape `any/internal/markdown`
// uses on the `md_blocks` dataset, but isolated from any caller code so
// the test exercises only the SDK's per-object-tree write/sync path. A
// no-op handler accepts every op — we want to test propagation, not
// validation.
const (
	blocksDataset     = "blocks"
	blocksDataVersion = "blocks-v1"
)

func newBlocksType() handler.Type {
	return handler.Type{
		Id:   "blocks-type",
		Name: "Blocks",
		Datasets: []handler.Dataset{{
			Name:        blocksDataset,
			DataVersion: blocksDataVersion,
			Handler:     handler.DefaultHandler{},
		}},
	}
}

// TestE2E_JoinerDeletePropagatesToOwner is the SDK-level reproducer for
// the joiner-side push-Delete bug: a writer joiner's sp.Delete must
// reach the owner's projection within the headsync window, exactly the
// way a sp.Modify on the same dataset already does (which the
// `markdown` middleware verifies indirectly via TestE2E_MultipeerJoiner*
// over HTTP).
//
// The test pins the asymmetry: same peer pair, same custom dataset,
// same call site — only the op type changes. If Modify converges and
// Delete doesn't, the regression is in the per-tree push pipeline for
// delete-only changes (which is what the SetDeferredUpdater fix in
// spaceobjects/store.go addressed: any-sync's default
// AddRawChangesWithUpdater order fired the listener before
// storage.AddAll, so replayLocked's IterateAfterAddSeq scan never saw
// the new delete change in storage and silently no-op'd while heads
// advanced).
//
// Path:
//
//  1. Open Alice + Bob with distinct keys; both register the same
//     blocks-type so the SDK accepts writes on the `blocks` dataset.
//  2. Alice creates a space, writes a record on the `blocks` dataset of
//     a freshly-created object.
//  3. Alice mints a RequestToJoin invite. Bob joins, Alice accepts as
//     writer. Bob waits for Members.Me ⇒ active and for the seeded
//     record to appear in his Query.
//  4. Bob deletes the record via space.DeleteBatch.
//  5. Bob's own Query immediately stops returning the record (sanity:
//     local apply happened — covered by the SetDeferredUpdater fix
//     for inbound Update path; for local writes any-sync has always
//     persisted before broadcasting, so the local skip wouldn't even
//     reproduce).
//  6. Alice waits up to 2 minutes for the record to disappear from
//     her Query — the actual regression check.
func TestE2E_JoinerDeletePropagatesToOwner(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("invite-delete-propagation e2e is slow (~60-90s); rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	openSDK := func(name string) *anysyncsdk.SDK {
		t.Helper()
		cfg := config.Config{
			Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: yaml},
			Types:   []handler.Type{newBlocksType()},
		}
		sdk, err := anysyncsdk.Open(ctx, cfg, newFixedSeedProvider(t))
		require.NoError(t, err, "%s: Open", name)
		t.Cleanup(func() { _ = sdk.Close() })
		return sdk
	}

	alice := openSDK("alice")
	bob := openSDK("bob")
	require.NotEqual(t, alice.Account().Id(), bob.Account().Id(),
		"alice and bob must have distinct account ids")

	// Alice creates the space + object + a single block record.
	sp, err := alice.Spaces().Create(ctx, space.CreateRequest{Name: "DeletePropTest"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("alice: Spaces().Create: %v", err)
	}

	objId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{"blocks-type"}})
	require.NoError(t, err, "alice: Objects().Create")

	const recId = "rec-X"
	_, err = sp.Modify(ctx, space.ModifyBatch{
		ObjectId: objId,
		Dataset:  blocksDataset,
		Records: []space.RecordModify{{
			Id:     recId,
			Upsert: true,
			Ops: []space.Op{{
				Type:  space.OpSet,
				Path:  "text",
				Value: "alpha",
			}},
		}},
	})
	require.NoError(t, err, "alice: seed Modify")

	// Mint invite with the standard create-shareable retry — the local
	// network's coordinator sometimes lags behind on a freshly-pushed
	// space.
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

	_, err = bob.Spaces().Join(ctx, space.JoinRequest{
		Invite:   token,
		Metadata: space.AccountMetadata{Name: "Bob"},
	})
	require.True(t, errors.Is(err, spaceimpl.ErrJoinPending),
		"bob: Join must return ErrJoinPending, got %v", err)

	// Alice accepts.
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
	require.NoError(t, sp.ACL().AcceptRequest(ctx, joinReq.RecordId, space.PermissionWriter))

	// Bob's handle.
	var bobSpace space.Space
	require.True(t, waitFor(ctx, 30*time.Second, 1*time.Second, func() bool {
		bobSpace, err = bob.Spaces().Get(ctx, sp.Id())
		return err == nil
	}), "bob: Spaces().Get(%s) never succeeded: %v", sp.Id(), err)

	if !waitFor(ctx, 90*time.Second, 1*time.Second, func() bool {
		_ = bobSpace.SyncHeads(ctx)
		me, mErr := bobSpace.Members().Me(ctx)
		if mErr != nil {
			return false
		}
		return me.Status == space.MemberStatusActive
	}) {
		t.Fatalf("bob never reached MemberStatusActive")
	}

	// The accept record that activated bob also kicks his ACL mirror:
	// his tech-space row must surface the granted role as OwnRole.
	if !waitFor(ctx, 30*time.Second, 1*time.Second, func() bool {
		infos, lErr := bob.Spaces().List(ctx)
		if lErr != nil {
			return false
		}
		for _, info := range infos {
			if info.Id == sp.Id() {
				return info.OwnRole == space.PermissionWriter
			}
		}
		return false
	}) {
		t.Fatalf("bob's space row never mirrored OwnRole=writer")
	}

	// Bob waits for Alice's seed record to converge.
	if !waitFor(ctx, 2*time.Minute, 1*time.Second, func() bool {
		_ = bobSpace.SyncHeads(ctx)
		rec, qErr := bobSpace.Query(objId, blocksDataset).
			Filter(map[string]any{"id": recId}).One(ctx)
		return qErr == nil && rec != nil
	}) {
		t.Fatalf("bob never saw alice's seed record %q", recId)
	}

	// Step 4: Bob deletes.
	_, err = bobSpace.Delete(ctx, space.DeleteBatch{
		ObjectId:  objId,
		Dataset:   blocksDataset,
		RecordIds: []string{recId},
	})
	require.NoError(t, err, "bob: Delete")

	// Step 5: Bob's own Query no longer returns the record (local
	// projection — should be ~immediate; the 5s budget is paranoid
	// generosity, not waiting on network).
	if !waitFor(ctx, 5*time.Second, 200*time.Millisecond, func() bool {
		_, qErr := bobSpace.Query(objId, blocksDataset).
			Filter(map[string]any{"id": recId}).One(ctx)
		return qErr != nil && errors.Is(qErr, space.ErrNotFound)
	}) {
		t.Fatalf("bob: local Delete did not project — record %q still queryable", recId)
	}

	// Step 6: Alice converges. The actual regression check. The
	// failure mode pre-fix: heads on Alice's tree advance to Bob's
	// delete-change id (so a hypothetical heads probe would say
	// "synced"), but Alice's controller never tombstones — Query
	// keeps returning the record forever.
	deadline := 2 * time.Minute
	if !waitFor(ctx, deadline, 1*time.Second, func() bool {
		_ = sp.SyncHeads(ctx)
		_, qErr := sp.Query(objId, blocksDataset).
			Filter(map[string]any{"id": recId}).One(ctx)
		return qErr != nil && errors.Is(qErr, space.ErrNotFound)
	}) {
		// Best-effort surfacing of the still-live state in the failure
		// message — if the record stayed live the user can confirm
		// from the assertion alone whether sp.Delete propagated heads
		// without the projection, or didn't propagate at all.
		rec, qErr := sp.Query(objId, blocksDataset).
			Filter(map[string]any{"id": recId}).One(ctx)
		var dump string
		switch {
		case qErr == nil && rec != nil:
			dump = strings.TrimSpace(rec.String())
		case qErr != nil:
			dump = "Query err: " + qErr.Error()
		default:
			dump = "(nil record, nil err)"
		}
		t.Fatalf("alice never observed bob's delete after %s; record state on alice: %s",
			deadline, dump)
	}
}
