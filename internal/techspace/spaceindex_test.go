package techspace_test

import (
	"context"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/techspace"
)

const (
	testObjectId = "spaceIndex-1"
	testDataVer  = "spaceIndexHandler-v1" // matches techspace.HandlerVersion
)

func newSpaceIndexController(t *testing.T) *crdt.Controller {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := anystore.Open(context.Background(), dbPath, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctrl, err := crdt.NewController(context.Background(), testObjectId, db,
		crdt.HandlerReg{Name: techspace.SpaceIndexDataset, Handler: techspace.SpaceIndexHandler{}},
	)
	require.NoError(t, err)
	return ctrl
}

func makeChange(versionId crdt.VersionId, recId string, upsert bool, ops ...crdt.Op) crdt.Change {
	return crdt.Change{
		ObjectId:    testObjectId,
		Dataset:     techspace.SpaceIndexDataset,
		ChangeId:    "ch-" + string(versionId),
		VersionId:   versionId,
		DataVersion: testDataVer,
		Records: []crdt.RecordChange{
			{Id: recId, Upsert: upsert, Ops: ops},
		},
	}
}

func setMulti(arena *anyenc.Arena, fields map[string]string) crdt.Op {
	obj := arena.NewObject()
	for k, v := range fields {
		obj.Set(k, arena.NewString(v))
	}
	return crdt.Op{Type: crdt.OpSet, Payload: obj}
}

// ----------------------------------------------------------------------------
// BeforeCreate — `type` is required
// ----------------------------------------------------------------------------

func TestSpaceIndexHandler_CreateLands(t *testing.T) {
	ctrl := newSpaceIndexController(t)
	arena := &anyenc.Arena{}

	const spaceId = "space-A"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", spaceId, true,
		setMulti(arena, map[string]string{
			techspace.FieldType: "private",
			techspace.FieldName: "My Space",
		}),
	)))

	rec := ctrl.Get(context.Background(), techspace.SpaceIndexDataset, spaceId)
	require.NotNil(t, rec)
	assert.Equal(t, "private", rec.GetString(techspace.FieldType))
	assert.Equal(t, "My Space", rec.GetString(techspace.FieldName))
}

func TestSpaceIndexHandler_CreateRejectedWithoutType(t *testing.T) {
	ctrl := newSpaceIndexController(t)
	arena := &anyenc.Arena{}

	const spaceId = "space-bare"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", spaceId, true,
		setMulti(arena, map[string]string{techspace.FieldName: "no type"}),
	)))

	assert.Nil(t, ctrl.Get(context.Background(), techspace.SpaceIndexDataset, spaceId),
		"create without type should be dropped")
}

// ----------------------------------------------------------------------------
// BeforeModify — type is pinned, deleted is terminal
// ----------------------------------------------------------------------------

func TestSpaceIndexHandler_NameEditPasses(t *testing.T) {
	ctrl := newSpaceIndexController(t)
	arena := &anyenc.Arena{}

	const spaceId = "space-rename"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", spaceId, true,
		setMulti(arena, map[string]string{
			techspace.FieldType: "private",
			techspace.FieldName: "Old",
		}),
	)))
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", spaceId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldName}, Payload: arena.NewString("New")},
	)))

	rec := ctrl.Get(context.Background(), techspace.SpaceIndexDataset, spaceId)
	require.NotNil(t, rec)
	assert.Equal(t, "New", rec.GetString(techspace.FieldName))
	assert.Equal(t, "private", rec.GetString(techspace.FieldType))
}

func TestSpaceIndexHandler_TypeEditDropped(t *testing.T) {
	ctrl := newSpaceIndexController(t)
	arena := &anyenc.Arena{}

	const spaceId = "space-pinned"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", spaceId, true,
		setMulti(arena, map[string]string{
			techspace.FieldType: "private",
			techspace.FieldName: "P",
		}),
	)))
	// Bundle a name edit alongside the type-flip attempt — name should
	// land, type should not (per-op rejection).
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", spaceId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldType}, Payload: arena.NewString("shared")},
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldName}, Payload: arena.NewString("Renamed")},
	)))

	rec := ctrl.Get(context.Background(), techspace.SpaceIndexDataset, spaceId)
	require.NotNil(t, rec)
	assert.Equal(t, "private", rec.GetString(techspace.FieldType), "type pinned")
	assert.Equal(t, "Renamed", rec.GetString(techspace.FieldName), "non-pinned op landed")
}

func TestSpaceIndexHandler_StatusActiveToDeletedPasses(t *testing.T) {
	ctrl := newSpaceIndexController(t)
	arena := &anyenc.Arena{}

	const spaceId = "space-bye"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", spaceId, true,
		setMulti(arena, map[string]string{
			techspace.FieldType:         "private",
			techspace.FieldRemoteStatus: techspace.StatusActive,
		}),
	)))
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", spaceId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldRemoteStatus}, Payload: arena.NewString(techspace.StatusDeleted)},
	)))

	rec := ctrl.Get(context.Background(), techspace.SpaceIndexDataset, spaceId)
	require.NotNil(t, rec)
	assert.Equal(t, techspace.StatusDeleted, rec.GetString(techspace.FieldRemoteStatus))
}

func TestSpaceIndexHandler_StatusOutOfDeletedDropped(t *testing.T) {
	ctrl := newSpaceIndexController(t)
	arena := &anyenc.Arena{}

	const spaceId = "space-zombie"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", spaceId, true,
		setMulti(arena, map[string]string{
			techspace.FieldType:         "private",
			techspace.FieldRemoteStatus: techspace.StatusDeleted,
		}),
	)))
	// Try to revive — terminal, op dropped.
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", spaceId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldRemoteStatus}, Payload: arena.NewString(techspace.StatusActive)},
	)))

	rec := ctrl.Get(context.Background(), techspace.SpaceIndexDataset, spaceId)
	require.NotNil(t, rec)
	assert.Equal(t, techspace.StatusDeleted, rec.GetString(techspace.FieldRemoteStatus),
		"terminal status survives revival attempt")
}

func TestSpaceIndexHandler_StatusOutOfDeletedDroppedMultiField(t *testing.T) {
	ctrl := newSpaceIndexController(t)
	arena := &anyenc.Arena{}

	const spaceId = "space-multi-zombie"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", spaceId, true,
		setMulti(arena, map[string]string{
			techspace.FieldType:         "private",
			techspace.FieldRemoteStatus: techspace.StatusDeleted,
		}),
	)))
	// Multi-field $set whose payload includes localStatus → whole op
	// dropped because the terminal-status rule fires on it.
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", spaceId, false,
		setMulti(arena, map[string]string{
			techspace.FieldRemoteStatus: techspace.StatusActive,
			techspace.FieldName:         "Zombie",
		}),
	)))

	rec := ctrl.Get(context.Background(), techspace.SpaceIndexDataset, spaceId)
	require.NotNil(t, rec)
	assert.Equal(t, techspace.StatusDeleted, rec.GetString(techspace.FieldRemoteStatus))
	// Bundled name change is also dropped — see rejectMultiField rationale.
	assert.NotEqual(t, "Zombie", rec.GetString(techspace.FieldName))
}

// ----------------------------------------------------------------------------
// BeforeDelete — delete ops are rejected; deletion is via status.
// ----------------------------------------------------------------------------

func TestSpaceIndexHandler_DeleteOpRejected(t *testing.T) {
	ctrl := newSpaceIndexController(t)
	arena := &anyenc.Arena{}

	const spaceId = "space-survives"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", spaceId, true,
		setMulti(arena, map[string]string{
			techspace.FieldType: "private",
			techspace.FieldName: "Survivor",
		}),
	)))
	// Issue a delete op — handler rejects, record unchanged.
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", spaceId, false,
		crdt.Op{Type: crdt.OpDelete},
	)))

	rec := ctrl.Get(context.Background(), techspace.SpaceIndexDataset, spaceId)
	require.NotNil(t, rec, "record must survive a rejected delete op")
	assert.Equal(t, "Survivor", rec.GetString(techspace.FieldName))
}
