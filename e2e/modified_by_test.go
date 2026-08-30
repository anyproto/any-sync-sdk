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
	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_ModifiedBy_NamesTheWritersAccount: the objects row's
// modifiedBy is the account that signed the object's latest synced
// change, on every peer. Bob joins Alice's space as a writer and edits
// a dataset of her object: both rows converge to modifiedBy == Bob
// with modifiedAt == that write's time, while author stays Alice; a
// later write by Alice flips them back on both sides.
func TestE2E_ModifiedBy_NamesTheWritersAccount(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("two-account e2e is slow; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	openSDK := func(name string) *anysyncsdk.SDK {
		t.Helper()
		sdk, err := anysyncsdk.Open(ctx, config.Config{
			Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: yaml},
			Types:   []handler.Type{newNotesType()},
		}, newFixedSeedProvider(t))
		require.NoError(t, err, "%s: Open", name)
		t.Cleanup(func() { _ = sdk.Close() })
		return sdk
	}
	alice := openSDK("alice")
	bob := openSDK("bob")
	aliceId, bobId := alice.Account().Id(), bob.Account().Id()
	require.NotEqual(t, aliceId, bobId)
	require.NoError(t, bob.Account().UpdateMetadata(ctx, space.AccountMetadata{Name: "Bob"}))

	spA, err := alice.Spaces().Create(ctx, space.CreateRequest{Name: "ModifiedBy"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("alice: Spaces().Create: %v", err)
	}
	objId, err := spA.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{"notes-type"}})
	require.NoError(t, err)

	// Bob joins as a writer.
	var inv space.Invite
	if !waitFor(ctx, 30*time.Second, time.Second, func() bool {
		inv, err = spA.ACL().CreateInvite(ctx)
		return err == nil
	}) {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on CreateInvite: %v", err)
		}
		t.Fatalf("alice: CreateInvite: %v", err)
	}
	token, err := space.EncodeInvite(inv)
	require.NoError(t, err)
	_, err = bob.Spaces().Join(ctx, space.JoinRequest{Invite: token, Metadata: space.AccountMetadata{Name: "Bob"}})
	require.True(t, errors.Is(err, spaceimpl.ErrJoinPending), "bob: Join must return ErrJoinPending, got %v", err)
	var joinReq space.JoinRequestInfo
	if !waitFor(ctx, 90*time.Second, time.Second, func() bool {
		_ = spA.SyncHeads(ctx)
		reqs, _ := spA.Members().JoinRequests(ctx)
		if len(reqs) > 0 {
			joinReq = reqs[0]
			return true
		}
		return false
	}) {
		t.Fatalf("alice never saw bob's join request")
	}
	require.NoError(t, spA.ACL().AcceptRequest(ctx, joinReq.RecordId, space.PermissionWriter))
	if !waitFor(ctx, 90*time.Second, time.Second, func() bool {
		_ = bob.Spaces().SyncSpaceList(ctx)
		infos, lerr := bob.Spaces().List(ctx)
		if lerr != nil {
			return false
		}
		for _, si := range infos {
			if si.Id == spA.Id() && si.Status == space.StatusActive {
				return true
			}
		}
		return false
	}) {
		t.Fatalf("bob's space never flipped to active")
	}
	spB, err := bob.Spaces().Get(ctx, spA.Id())
	require.NoError(t, err)
	if !waitFor(ctx, 90*time.Second, time.Second, func() bool {
		_ = spB.SyncHeads(ctx)
		me, merr := spB.Members().Me(ctx)
		return merr == nil && me.Status == space.MemberStatusActive
	}) {
		t.Fatalf("bob never reached MemberStatusActive")
	}

	type stamps struct {
		at     int64
		by     string
		author string
	}
	stampsOf := func(sp space.Space) (stamps, bool) {
		row, qerr := sp.QueryObjects().Filter(map[string]any{"id": objId}).One(ctx)
		if qerr != nil || row == nil || row.Get("modifiedAt") == nil {
			return stamps{}, false
		}
		ms, merr := row.Get("modifiedAt").DateTimeMillis()
		if merr != nil {
			return stamps{}, false
		}
		return stamps{at: ms, by: row.GetString("modifiedBy"), author: row.GetString("author")}, true
	}
	writeNote := func(sp space.Space, text string) {
		t.Helper()
		_, werr := sp.Modify(ctx, space.ModifyBatch{
			ObjectId: objId,
			Dataset:  notesDataset,
			Records: []space.RecordModify{{
				Id:     "rec-1",
				Upsert: true,
				Ops:    []space.Op{{Type: space.OpSet, Path: "text", Value: text}},
			}},
		})
		require.NoError(t, werr)
	}
	// converge waits until both rows carry the same stamps and returns
	// them.
	converge := func(what string) stamps {
		t.Helper()
		var got stamps
		if !waitFor(ctx, 2*time.Minute, 2*time.Second, func() bool {
			_ = spA.SyncHeads(ctx)
			_ = spB.SyncHeads(ctx)
			a, okA := stampsOf(spA)
			b, okB := stampsOf(spB)
			if !okA || !okB || a != b {
				return false
			}
			got = a
			return true
		}) {
			a, _ := stampsOf(spA)
			b, _ := stampsOf(spB)
			t.Fatalf("%s: rows never converged: alice=%+v bob=%+v", what, a, b)
		}
		return got
	}

	// Bob sees Alice's object with the creating change's stamps.
	initial := converge("initial")
	assert.Equal(t, aliceId, initial.by, "a fresh object is last modified by its creator")
	assert.Equal(t, aliceId, initial.author)

	// Change timestamps have second resolution: space the writes out
	// so each stamp is distinguishable from the previous one.
	time.Sleep(1100 * time.Millisecond)
	writeNote(spB, "from bob")
	bobWrite, ok := stampsOf(spB)
	require.True(t, ok)
	afterBob := converge("after bob's write")
	assert.Equal(t, bobId, afterBob.by, "bob's dataset write names bob")
	assert.Equal(t, bobWrite.at, afterBob.at, "modifiedAt is bob's write time")
	assert.Greater(t, afterBob.at, initial.at)
	assert.Equal(t, aliceId, afterBob.author, "author stays the creator")

	time.Sleep(1100 * time.Millisecond)
	writeNote(spA, "from alice")
	afterAlice := converge("after alice's write")
	assert.Equal(t, aliceId, afterAlice.by, "alice's write flips it back")
	assert.Greater(t, afterAlice.at, afterBob.at)
}
