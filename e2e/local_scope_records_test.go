package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/space"
)

// A "notes" dataset with a synced text field and a declared LOCAL
// unread flag — the read-tracking materialization shape (a device-
// local annotation riding on synced records).
const (
	notesDataset     = "notes"
	notesDataVersion = "notes-v1"
)

func newNotesType() handler.Type {
	return handler.Type{
		Id:   "notes-type",
		Name: "Notes",
		Datasets: []handler.Dataset{{
			Name:        notesDataset,
			DataVersion: notesDataVersion,
			Handler:     handler.DefaultHandler{},
			Schema: handler.Schema{
				Fields: []handler.Field{
					{Id: "text", Name: "Text", Schema: handler.Leaf(handler.PropertyKindString), Scope: handler.ScopeSynced},
					{Id: "unread", Name: "Unread", Schema: handler.Leaf(handler.PropertyKindBoolean), Scope: handler.ScopeLocal},
				},
				Dynamic: true,
			},
		}},
	}
}

// TestE2E_LocalScopeDatasetRecords: one account, two devices, one
// object with a custom dataset carrying a SYNCED `text` field and a
// declared LOCAL `unread` flag on the same records — the dataset-
// record sibling of TestE2E_LocalScopeIsolation (which proves the
// same contract for property values).
//
// The write surface under test is Modify with ModifyBatch.Scope =
// ScopeLocal (routed through Object.LocalSet). Same causal-clock
// construction as the property test: A writes the local value FIRST,
// then a synced value; once B converges the synced value it has
// provably caught up past the local write, so if the local value were
// ever going to sync it would be present. It must be absent.
func TestE2E_LocalScopeDatasetRecords(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("local-scope records e2e is slow (~1-2min); rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	providerA := newFixedSeedProvider(t)
	providerB := sameAccountFreshDevice(t, providerA)

	// ---- Device A: create everything, seed a record, flag it ----
	sdkA, err := anysyncsdk.Open(ctx, config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
		Types:   []handler.Type{newNotesType()},
	}, providerA)
	require.NoError(t, err, "device A: Open")
	t.Cleanup(func() { _ = sdkA.Close() })

	spA, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "LocalScopeRecords"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("device A: Spaces().Create: %v", err)
	}
	spaceId := spA.Id()

	objId, err := spA.Objects().Create(ctx, space.CreateObjectOpts{Type: "notes-type"})
	require.NoError(t, err, "device A: Objects().Create")

	const recId = "rec-1"
	_, err = spA.Modify(ctx, space.ModifyBatch{
		ObjectId: objId,
		Dataset:  notesDataset,
		Records: []space.RecordModify{{
			Id:     recId,
			Upsert: true,
			Ops:    []space.Op{{Type: space.OpSet, Path: "text", Value: "alpha"}},
		}},
	})
	require.NoError(t, err, "device A: seed synced record")

	// The write under test: local-scope flag on the synced record.
	localSet := func(sp space.Space, val bool) (space.ModifyResult, error) {
		return sp.Modify(ctx, space.ModifyBatch{
			ObjectId: objId,
			Dataset:  notesDataset,
			Scope:    space.ScopeLocal,
			Records: []space.RecordModify{{
				Id:  recId,
				Ops: []space.Op{{Type: space.OpSet, Path: "unread", Value: val}},
			}},
		})
	}
	res, err := localSet(spA, true)
	require.NoError(t, err, "device A: local-scope Modify")
	require.Empty(t, res.Rejections, "device A: local write on a declared local field must not be rejected")
	require.NotEmpty(t, res.VersionId, "local writes mint a local VersionId")
	require.Empty(t, res.ChangeId, "local writes never get a DAG ChangeId")

	readRec := func(sp space.Space) *recView {
		rec, qerr := sp.Query(objId, notesDataset).Filter(map[string]any{"id": recId}).One(ctx)
		if qerr != nil || rec == nil {
			return nil
		}
		unread := rec.Get("unread")
		v := &recView{text: rec.GetString("text"), unreadPresent: unread != nil}
		if v.unreadPresent {
			v.unread = rec.GetBool("unread")
		}
		return v
	}

	// Read-your-writes on A: both fields visible in the same row.
	rowA := readRec(spA)
	require.NotNil(t, rowA)
	require.Equal(t, "alpha", rowA.text)
	require.True(t, rowA.unreadPresent && rowA.unread, "device A sees its own local flag")

	// ---- Route enforcement on A ----
	// Synced write to the local field: ValidateChange fails the whole
	// change fast, keeping it out of the DAG.
	_, err = spA.Modify(ctx, space.ModifyBatch{
		ObjectId: objId,
		Dataset:  notesDataset,
		Records: []space.RecordModify{{
			Id:  recId,
			Ops: []space.Op{{Type: space.OpSet, Path: "unread", Value: false}},
		}},
	})
	require.Error(t, err, "synced write to a local-scope field must be refused")

	// Local write to the synced field: the materialization route drops
	// the op as a rejection (nothing to keep out of a DAG).
	resBad, err := spA.Modify(ctx, space.ModifyBatch{
		ObjectId: objId,
		Dataset:  notesDataset,
		Scope:    space.ScopeLocal,
		Records: []space.RecordModify{{
			Id:  recId,
			Ops: []space.Op{{Type: space.OpSet, Path: "text", Value: "smuggled"}},
		}},
	})
	require.NoError(t, err)
	require.NotEmpty(t, resBad.Rejections, "local write to a synced field must be rejected op-level")
	rowA = readRec(spA)
	require.Equal(t, "alpha", rowA.text, "rejected local op must not touch the synced value")

	// Local write on an absent record: strict mode, surfaces as a
	// rejection (no local-only record creation).
	resAbsent, err := spA.Modify(ctx, space.ModifyBatch{
		ObjectId: objId,
		Dataset:  notesDataset,
		Scope:    space.ScopeLocal,
		Records: []space.RecordModify{{
			Id:  "no-such-record",
			Ops: []space.Op{{Type: space.OpSet, Path: "unread", Value: true}},
		}},
	})
	require.NoError(t, err)
	require.NotEmpty(t, resAbsent.Rejections, "local write on an absent record must be rejected, not create it")

	// Clock: a synced write AFTER the local one. B converging this
	// value proves it has replayed past the local write's moment.
	_, err = spA.Modify(ctx, space.ModifyBatch{
		ObjectId: objId,
		Dataset:  notesDataset,
		Records: []space.RecordModify{{
			Id:  recId,
			Ops: []space.Op{{Type: space.OpSet, Path: "text", Value: "clocked"}},
		}},
	})
	require.NoError(t, err, "device A: synced clock write")

	// ---- Device B: fresh data dir, same account ----
	sdkB, err := anysyncsdk.Open(ctx, config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
		Types:   []handler.Type{newNotesType()},
	}, providerB)
	require.NoError(t, err, "device B: Open")
	t.Cleanup(func() { _ = sdkB.Close() })

	var spB space.Space
	require.Eventually(t, func() bool {
		list, lerr := sdkB.Spaces().List(ctx)
		if lerr != nil {
			return false
		}
		for _, info := range list {
			if info.Id == spaceId {
				spB, lerr = sdkB.Spaces().Get(ctx, spaceId)
				return lerr == nil
			}
		}
		return false
	}, 3*time.Minute, 3*time.Second, "device B: space never appeared")

	require.Eventually(t, func() bool {
		_ = spB.SyncHeads(ctx)
		row := readRec(spB)
		return row != nil && row.text == "clocked"
	}, 2*time.Minute, 3*time.Second, "device B: synced text never converged")

	// THE NEGATIVE: B converged past A's local write and must not
	// carry the flag.
	rowB := readRec(spB)
	require.NotNil(t, rowB)
	assert.False(t, rowB.unreadPresent, "device B must NEVER see device A's local-scoped record field")

	// ---- Device-local independence, both directions ----
	// B writes its OWN flag at the same path (opposite value), plus a
	// synced clock value back toward A.
	resB, err := localSet(spB, false)
	require.NoError(t, err, "device B: own local-scope Modify")
	require.Empty(t, resB.Rejections)
	rowB = readRec(spB)
	require.True(t, rowB.unreadPresent, "device B sees its own local flag")
	require.False(t, rowB.unread)

	_, err = spB.Modify(ctx, space.ModifyBatch{
		ObjectId: objId,
		Dataset:  notesDataset,
		Records: []space.RecordModify{{
			Id:  recId,
			Ops: []space.Op{{Type: space.OpSet, Path: "text", Value: "clocked-back"}},
		}},
	})
	require.NoError(t, err, "device B: synced clock write back toward A")

	require.Eventually(t, func() bool {
		_ = spA.SyncHeads(ctx)
		row := readRec(spA)
		return row != nil && row.text == "clocked-back"
	}, 2*time.Minute, 3*time.Second, "device A: did not converge B's synced write")

	rowA = readRec(spA)
	require.True(t, rowA.unreadPresent && rowA.unread,
		"device A's local flag must be untouched by device B's local write at the same path")
}

type recView struct {
	text          string
	unread        bool
	unreadPresent bool
}
