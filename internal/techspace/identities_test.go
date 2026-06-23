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

func newIdentitiesController(t *testing.T) *crdt.Controller {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := anystore.Open(context.Background(), dbPath, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctrl, err := crdt.NewController(context.Background(), "identities-1", db,
		crdt.HandlerReg{Name: techspace.IdentitiesDataset, Handler: techspace.IdentitiesHandler{}, Schema: techspace.IdentitiesSchema()},
	)
	require.NoError(t, err)
	return ctrl
}

func identitiesChange(versionId crdt.VersionId, recId string, upsert bool, ops ...crdt.Op) crdt.Change {
	return crdt.Change{
		ObjectId:    "identities-1",
		Dataset:     techspace.IdentitiesDataset,
		ChangeId:    "ch-" + string(versionId),
		VersionId:   versionId,
		DataVersion: techspace.IdentitiesHandlerVersion,
		Records:     []crdt.RecordChange{{Id: recId, Upsert: upsert, Ops: ops}},
	}
}

// The symkey is the only SYNCED field, so it's the one writable via the
// controller's DAG route; the profile/spaceIds fields are ScopeLocal
// (Object.LocalSet only) and are exercised end-to-end by the SDK tests.
func TestIdentities_SymKeySyncedRoundTrip(t *testing.T) {
	ctrl := newIdentitiesController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	const id = "AAccountAddressOfAlice"
	require.NoError(t, ctrl.ApplyChange(ctx, identitiesChange("v1", id, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldIdentitySymKey}, Payload: arena.NewString("symkey-bytes")},
	)))

	rec := techspace.DecodeIdentityRecord(ctrl.Get(ctx, techspace.IdentitiesDataset, id))
	assert.Equal(t, id, rec.Identity)
	assert.Equal(t, "symkey-bytes", rec.SymKey)
}

// A local field rejected on the synced route — proves the symkey/profile
// scope split is enforced (profile must go through LocalSet).
func TestIdentities_LocalFieldRejectedOnSyncedRoute(t *testing.T) {
	ctrl := newIdentitiesController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	res, err := ctrl.ApplyChangeWithResult(ctx, identitiesChange("v1", "alice", true,
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldIdentityName}, Payload: arena.NewString("Alice")},
	))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)
}

func TestIdentities_EmptyIdRejected(t *testing.T) {
	ctrl := newIdentitiesController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	res, err := ctrl.ApplyChangeWithResult(ctx, identitiesChange("v1", "", true,
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldIdentitySymKey}, Payload: arena.NewString("x")},
	))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)
	assert.ErrorIs(t, res.Rejections[0].Err, crdt.ErrValidation)
}

// DecodeIdentityRecord reads the device-local profile + spaceIds array
// off a stored value (built directly, mirroring the LocalSet shape).
func TestIdentities_DecodeRecordWithSpaceIds(t *testing.T) {
	arena := &anyenc.Arena{}
	v := arena.NewObject()
	v.Set("id", arena.NewString("alice"))
	v.Set(techspace.FieldIdentitySymKey, arena.NewString("k"))
	v.Set(techspace.FieldIdentityName, arena.NewString("Alice"))
	v.Set(techspace.FieldIdentityIcon, arena.NewString("cid"))
	spaceIds := arena.NewArray()
	spaceIds.SetArrayItem(0, arena.NewString("sp1"))
	spaceIds.SetArrayItem(1, arena.NewString("sp2"))
	v.Set(techspace.FieldIdentitySpaceIds, spaceIds)

	rec := techspace.DecodeIdentityRecord(v)
	assert.Equal(t, "alice", rec.Identity)
	assert.Equal(t, "k", rec.SymKey)
	assert.Equal(t, "Alice", rec.Name)
	assert.Equal(t, "cid", rec.IconCID)
	assert.ElementsMatch(t, []string{"sp1", "sp2"}, rec.SpaceIds)
}
