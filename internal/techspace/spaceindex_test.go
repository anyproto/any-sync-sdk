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
	"github.com/anyproto/any-sync-sdk/space"
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
		crdt.HandlerReg{Name: techspace.SpaceIndexDataset, Handler: techspace.SpaceIndexHandler{}, Schema: techspace.SpaceIndexSchema()},
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

// Regression: localStatus was reclassified ScopeLocal after spaces had
// already been created. Old creates packed localStatus into ONE multi-field
// $set with type/name/remoteStatus. On replay the synced route can't write a
// Local-class field; before the per-key salvage fix the whole op dropped and
// the space vanished from the index. Now only localStatus is shed and the
// record still materializes from its synced fields.
func TestSpaceIndexHandler_LegacyCreateWithLocalStatusStillLands(t *testing.T) {
	ctrl := newSpaceIndexController(t)
	arena := &anyenc.Arena{}

	const spaceId = "space-legacy"
	res, err := ctrl.ApplyChangeWithResult(context.Background(), makeChange(
		"v1", spaceId, true,
		setMulti(arena, map[string]string{
			techspace.FieldType:         "private",
			techspace.FieldName:         "Legacy Space",
			techspace.FieldRemoteStatus: techspace.StatusActive,
			techspace.FieldLocalStatus:  techspace.StatusActive,
		}),
	))
	require.NoError(t, err, "a legacy local key must not abort the create on replay")
	require.Len(t, res.Rejections, 1, "only the localStatus key is rejected")
	assert.ErrorIs(t, res.Rejections[0].Err, crdt.ErrValidation)

	rec := ctrl.Get(context.Background(), techspace.SpaceIndexDataset, spaceId)
	require.NotNil(t, rec, "the space materialized from its synced fields")
	assert.Equal(t, "private", rec.GetString(techspace.FieldType))
	assert.Equal(t, "Legacy Space", rec.GetString(techspace.FieldName))
	assert.Equal(t, techspace.StatusActive, rec.GetString(techspace.FieldRemoteStatus))
	assert.Empty(t, rec.GetString(techspace.FieldLocalStatus), "the Local-class field never landed via the synced route")
}

// Type-less creates land: join/track register rows before the space's
// header is readable, so `type` is unknown until the first load
// backfills it.
func TestSpaceIndexHandler_CreateWithoutTypeLands(t *testing.T) {
	ctrl := newSpaceIndexController(t)
	arena := &anyenc.Arena{}

	const spaceId = "space-bare"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", spaceId, true,
		setMulti(arena, map[string]string{techspace.FieldName: "no type"}),
	)))

	rec := ctrl.Get(context.Background(), techspace.SpaceIndexDataset, spaceId)
	require.NotNil(t, rec, "create without type lands as an unknown-type row")
	assert.Empty(t, rec.GetString(techspace.FieldType))
	assert.Equal(t, "no type", rec.GetString(techspace.FieldName))
}

// `type` is set-once: an unknown-type row accepts exactly one fill (the
// header backfill) and pins from then on.
func TestSpaceIndexHandler_TypeSetOnce(t *testing.T) {
	ctrl := newSpaceIndexController(t)
	arena := &anyenc.Arena{}

	const spaceId = "space-backfill"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", spaceId, true,
		setMulti(arena, map[string]string{techspace.FieldName: "joined"}),
	)))
	// Backfill fills the empty type.
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", spaceId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldType}, Payload: arena.NewString("any.space")},
	)))
	rec := ctrl.Get(context.Background(), techspace.SpaceIndexDataset, spaceId)
	require.NotNil(t, rec)
	assert.Equal(t, "any.space", rec.GetString(techspace.FieldType))

	// A second write is an overwrite of a non-empty value — dropped.
	res, err := ctrl.ApplyChangeWithResult(context.Background(), makeChange(
		"v3", spaceId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldType}, Payload: arena.NewString("anytype.space")},
	))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)
	assert.ErrorIs(t, res.Rejections[0].Err, crdt.ErrValidation)
	rec = ctrl.Get(context.Background(), techspace.SpaceIndexDataset, spaceId)
	require.NotNil(t, rec)
	assert.Equal(t, "any.space", rec.GetString(techspace.FieldType), "type pinned after the fill")
}

// ----------------------------------------------------------------------------
// BeforeCreate — `createdAt` is stamped from the change timestamp
// ----------------------------------------------------------------------------

func TestSpaceIndexHandler_CreateStampsCreatedAt(t *testing.T) {
	ctrl := newSpaceIndexController(t)
	arena := &anyenc.Arena{}

	const spaceId = "space-stamped"
	ch := makeChange("v1", spaceId, true,
		setMulti(arena, map[string]string{techspace.FieldType: "private"}),
	)
	ch.Timestamp = 1717000000
	require.NoError(t, ctrl.ApplyChange(context.Background(), ch))

	rec := ctrl.Get(context.Background(), techspace.SpaceIndexDataset, spaceId)
	require.NotNil(t, rec)
	assert.Equal(t, int64(1717000000), techspace.DecodeSpaceIndexRecord(rec).CreatedAt)
}

func TestSpaceIndexHandler_CreatedAtInputOpRejected(t *testing.T) {
	ctrl := newSpaceIndexController(t)
	arena := &anyenc.Arena{}

	const spaceId = "space-forged"
	ch := makeChange("v1", spaceId, true,
		setMulti(arena, map[string]string{techspace.FieldType: "private"}),
	)
	ch.Timestamp = 1717000000
	require.NoError(t, ctrl.ApplyChange(context.Background(), ch))

	// createdAt is ScopeDerived — a synced input op targeting it is
	// dropped by the controller's scope enforcement. On the apply/remote
	// path the offending op is ignored (recorded as a rejection, never
	// fatal), so a forged change can't take effect or wedge a peer.
	res, err := ctrl.ApplyChangeWithResult(context.Background(), makeChange(
		"v2", spaceId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldCreatedAt}, Payload: arena.NewNumberInt(1)},
	))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)
	assert.ErrorIs(t, res.Rejections[0].Err, crdt.ErrValidation)

	rec := ctrl.Get(context.Background(), techspace.SpaceIndexDataset, spaceId)
	require.NotNil(t, rec)
	assert.Equal(t, int64(1717000000), techspace.DecodeSpaceIndexRecord(rec).CreatedAt,
		"derived createdAt must survive a forged input op")
}

func TestSpaceIndexHandler_CreateWithoutTimestampSkipsCreatedAt(t *testing.T) {
	ctrl := newSpaceIndexController(t)
	arena := &anyenc.Arena{}

	// Change without a timestamp — no stamp, field absent, decode reads
	// zero (same shape as rows created before the field existed).
	const spaceId = "space-legacy"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", spaceId, true,
		setMulti(arena, map[string]string{techspace.FieldType: "private"}),
	)))

	rec := ctrl.Get(context.Background(), techspace.SpaceIndexDataset, spaceId)
	require.NotNil(t, rec)
	assert.Nil(t, rec.Get(techspace.FieldCreatedAt))
	assert.Zero(t, techspace.DecodeSpaceIndexRecord(rec).CreatedAt)
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

// A multi-field op that bundles a pinned field (`type`) with an
// unconstrained one (`name`) through the MODIFY path sheds only the pinned
// key — the bundled name still lands. Before per-key salvage the handler
// dropped the whole op, which is how legacy creates that re-asserted
// type+name in one op vanished on replay through the modify branch.
func TestSpaceIndexHandler_MultiFieldTypeBundleSalvagesSiblings(t *testing.T) {
	ctrl := newSpaceIndexController(t)
	arena := &anyenc.Arena{}

	const spaceId = "space-bundle"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", spaceId, true,
		setMulti(arena, map[string]string{
			techspace.FieldType: "private",
			techspace.FieldName: "Old",
		}),
	)))
	// Record now exists → this multi-field op runs through BeforeModify.
	res, err := ctrl.ApplyChangeWithResult(context.Background(), makeChange(
		"v2", spaceId, false,
		setMulti(arena, map[string]string{
			techspace.FieldType: "shared", // pinned — rejected
			techspace.FieldName: "Renamed",
		}),
	))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1, "only the pinned key is rejected")
	assert.ErrorIs(t, res.Rejections[0].Err, crdt.ErrValidation)

	rec := ctrl.Get(context.Background(), techspace.SpaceIndexDataset, spaceId)
	require.NotNil(t, rec)
	assert.Equal(t, "private", rec.GetString(techspace.FieldType), "type stayed pinned")
	assert.Equal(t, "Renamed", rec.GetString(techspace.FieldName), "bundled sibling landed")
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
	// Multi-field $set bundling a terminal-status revival with a name edit:
	// per-key salvage sheds only the revival, the name still lands.
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", spaceId, false,
		setMulti(arena, map[string]string{
			techspace.FieldRemoteStatus: techspace.StatusActive,
			techspace.FieldName:         "Zombie",
		}),
	)))

	rec := ctrl.Get(context.Background(), techspace.SpaceIndexDataset, spaceId)
	require.NotNil(t, rec)
	assert.Equal(t, techspace.StatusDeleted, rec.GetString(techspace.FieldRemoteStatus),
		"terminal status held against the revival key")
	assert.Equal(t, "Zombie", rec.GetString(techspace.FieldName),
		"bundled non-status key still landed")
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

// ----------------------------------------------------------------------------
// Direct-add invite statuses (SYN-46) — non-terminal, decodable.
// ----------------------------------------------------------------------------

// The invitePending → active and inviteDeclined → active transitions must
// pass the handler: only StatusDeleted is terminal, and Accept flips
// either invite state back to active.
func TestSpaceIndexHandler_InviteStatusesNonTerminal(t *testing.T) {
	ctrl := newSpaceIndexController(t)
	arena := &anyenc.Arena{}

	const spaceId = "space-invited"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", spaceId, true,
		setMulti(arena, map[string]string{
			techspace.FieldType:         "private",
			techspace.FieldRemoteStatus: techspace.InvitePendingRemoteStatus,
		}),
	)))
	// Decline...
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", spaceId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldRemoteStatus}, Payload: arena.NewString(techspace.InviteDeclinedRemoteStatus)},
	)))
	rec := ctrl.Get(context.Background(), techspace.SpaceIndexDataset, spaceId)
	require.NotNil(t, rec)
	assert.Equal(t, techspace.InviteDeclinedRemoteStatus, rec.GetString(techspace.FieldRemoteStatus))

	// ...then accept-from-declined (non-terminal) lands.
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v3", spaceId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldRemoteStatus}, Payload: arena.NewString(techspace.StatusActive)},
	)))
	rec = ctrl.Get(context.Background(), techspace.SpaceIndexDataset, spaceId)
	require.NotNil(t, rec)
	assert.Equal(t, techspace.StatusActive, rec.GetString(techspace.FieldRemoteStatus))
}

// DecodeSpaceIndexRecord reads the device-local direct-add notify outbox
// (InviteNotifyPending array) off a stored value.
func TestSpaceIndexRecord_DecodeInviteNotifyPending(t *testing.T) {
	arena := &anyenc.Arena{}
	v := arena.NewObject()
	v.Set("id", arena.NewString("s1"))
	v.Set(techspace.FieldType, arena.NewString("private"))
	pending := arena.NewArray()
	pending.SetArrayItem(0, arena.NewString("bob"))
	pending.SetArrayItem(1, arena.NewString("carol"))
	v.Set(techspace.FieldInviteNotifyPending, pending)

	rec := techspace.DecodeSpaceIndexRecord(v)
	assert.Equal(t, "s1", rec.Id)
	assert.ElementsMatch(t, []string{"bob", "carol"}, rec.InviteNotifyPending)
}

func TestSpaceIndexRecord_DecodeOwnRole(t *testing.T) {
	arena := &anyenc.Arena{}
	v := arena.NewObject()
	v.Set("id", arena.NewString("s1"))
	v.Set(techspace.FieldType, arena.NewString("private"))
	v.Set(techspace.FieldOwnRole, arena.NewString("owner"))

	rec := techspace.DecodeSpaceIndexRecord(v)
	assert.Equal(t, space.PermissionOwner, rec.OwnRole)

	// Absent field — the never-mirrored row — decodes to None.
	bare := arena.NewObject()
	bare.Set("id", arena.NewString("s2"))
	assert.Equal(t, space.PermissionNone, techspace.DecodeSpaceIndexRecord(bare).OwnRole)

	// An unknown label (future vocabulary) degrades to None rather
	// than failing the decode.
	v.Set(techspace.FieldOwnRole, arena.NewString("superadmin"))
	assert.Equal(t, space.PermissionNone, techspace.DecodeSpaceIndexRecord(v).OwnRole)
}

// DecodeSpaceIndexRecord reads the issued-key custody object into the
// per-kind map; non-string subvalues are skipped, absence decodes nil.
func TestSpaceIndexRecord_DecodeIssuedInviteKeys(t *testing.T) {
	arena := &anyenc.Arena{}
	v := arena.NewObject()
	v.Set("id", arena.NewString("s1"))
	v.Set(techspace.FieldType, arena.NewString("private"))
	keys := arena.NewObject()
	keys.Set(techspace.IssuedKeyMember, arena.NewString("enc-member"))
	keys.Set(techspace.IssuedKeyGuest, arena.NewString("enc-guest"))
	v.Set(techspace.FieldIssuedInviteKeys, keys)

	rec := techspace.DecodeSpaceIndexRecord(v)
	assert.Equal(t, "enc-member", rec.IssuedInviteKey(techspace.IssuedKeyMember))
	assert.Equal(t, "enc-guest", rec.IssuedInviteKey(techspace.IssuedKeyGuest))

	// Absent field — nil map, lookups return "".
	bare := arena.NewObject()
	bare.Set("id", arena.NewString("s2"))
	assert.Empty(t, techspace.DecodeSpaceIndexRecord(bare).IssuedInviteKey(techspace.IssuedKeyMember))
}

// Per-kind custody subkeys write and clear independently through the
// controller: setting member never disturbs guest, unset removes just
// its kind.
func TestSpaceIndexHandler_IssuedInviteKeysPerKind(t *testing.T) {
	ctrl := newSpaceIndexController(t)
	arena := &anyenc.Arena{}

	const spaceId = "space-A"
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v1", spaceId, true,
		setMulti(arena, map[string]string{techspace.FieldType: "private"}),
	)))
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v2", spaceId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldIssuedInviteKeys, techspace.IssuedKeyMember}, Payload: arena.NewString("enc-member")},
	)))
	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v3", spaceId, false,
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldIssuedInviteKeys, techspace.IssuedKeyGuest}, Payload: arena.NewString("enc-guest")},
	)))

	rec := techspace.DecodeSpaceIndexRecord(ctrl.Get(context.Background(), techspace.SpaceIndexDataset, spaceId))
	assert.Equal(t, "enc-member", rec.IssuedInviteKey(techspace.IssuedKeyMember))
	assert.Equal(t, "enc-guest", rec.IssuedInviteKey(techspace.IssuedKeyGuest))

	require.NoError(t, ctrl.ApplyChange(context.Background(), makeChange(
		"v4", spaceId, false,
		crdt.Op{Type: crdt.OpUnset, Path: []string{techspace.FieldIssuedInviteKeys, techspace.IssuedKeyMember}},
	)))
	rec = techspace.DecodeSpaceIndexRecord(ctrl.Get(context.Background(), techspace.SpaceIndexDataset, spaceId))
	assert.Empty(t, rec.IssuedInviteKey(techspace.IssuedKeyMember))
	assert.Equal(t, "enc-guest", rec.IssuedInviteKey(techspace.IssuedKeyGuest))
}
