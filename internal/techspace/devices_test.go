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

func newDevicesController(t *testing.T) *crdt.Controller {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := anystore.Open(context.Background(), dbPath, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctrl, err := crdt.NewController(context.Background(), "devices-1", db,
		crdt.HandlerReg{Name: techspace.DevicesDataset, Handler: techspace.DevicesHandler{}, Schema: techspace.DevicesSchema()},
	)
	require.NoError(t, err)
	return ctrl
}

func devicesChange(versionId crdt.VersionId, recId string, upsert bool, ops ...crdt.Op) crdt.Change {
	return crdt.Change{
		ObjectId:    "devices-1",
		Dataset:     techspace.DevicesDataset,
		ChangeId:    "ch-" + string(versionId),
		VersionId:   versionId,
		DataVersion: techspace.DevicesHandlerVersion,
		Records:     []crdt.RecordChange{{Id: recId, Upsert: upsert, Ops: ops}},
	}
}

// The exact ops SetDevice emits, applied through the synced route,
// read back through DecodeDeviceRecord — the write/read contract for
// one full row.
func TestDevices_UpsertRoundTrip(t *testing.T) {
	ctrl := newDevicesController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	const peer = "12D3KooWDevA"
	ops, err := techspace.DeviceUpsertOps(arena, space.DeviceUpsert{
		Name:    "workstation",
		OS:      "linux",
		Version: "0.13.1",
		Apps:    map[string]map[string]any{"bao": {"version": "1.2.0"}},
	})
	require.NoError(t, err)
	require.NoError(t, ctrl.ApplyChange(ctx, devicesChange("v1", peer, true, ops...)))

	d := techspace.DecodeDeviceRecord(ctrl.Get(ctx, techspace.DevicesDataset, peer))
	assert.Equal(t, peer, d.PeerId)
	assert.Equal(t, "workstation", d.Name)
	assert.Equal(t, "linux", d.OS)
	assert.Equal(t, "0.13.1", d.Version)
	require.Contains(t, d.Apps, "bao")
	assert.Equal(t, "1.2.0", d.Apps["bao"]["version"])
	assert.Empty(t, d.ActiveClaims)
}

// Per-slug app ops: a second writer touching a DIFFERENT slug merges
// with the first instead of clobbering the whole apps object; a nil
// entry removes exactly its slug.
func TestDevices_AppsMergePerSlug(t *testing.T) {
	ctrl := newDevicesController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	const peer = "12D3KooWDevA"
	ops1, err := techspace.DeviceUpsertOps(arena, space.DeviceUpsert{Apps: map[string]map[string]any{"bao": {}}})
	require.NoError(t, err)
	require.NoError(t, ctrl.ApplyChange(ctx, devicesChange("v1", peer, true, ops1...)))

	ops2, err := techspace.DeviceUpsertOps(arena, space.DeviceUpsert{Apps: map[string]map[string]any{"other": {}}})
	require.NoError(t, err)
	require.NoError(t, ctrl.ApplyChange(ctx, devicesChange("v2", peer, true, ops2...)))

	d := techspace.DecodeDeviceRecord(ctrl.Get(ctx, techspace.DevicesDataset, peer))
	assert.Contains(t, d.Apps, "bao")
	assert.Contains(t, d.Apps, "other")

	ops3, err := techspace.DeviceUpsertOps(arena, space.DeviceUpsert{Apps: map[string]map[string]any{"bao": nil}})
	require.NoError(t, err)
	require.NoError(t, ctrl.ApplyChange(ctx, devicesChange("v3", peer, true, ops3...)))

	d = techspace.DecodeDeviceRecord(ctrl.Get(ctx, techspace.DevicesDataset, peer))
	assert.NotContains(t, d.Apps, "bao")
	assert.Contains(t, d.Apps, "other")
}

func TestDevices_UpsertOpsValidation(t *testing.T) {
	arena := &anyenc.Arena{}

	_, err := techspace.DeviceUpsertOps(arena, space.DeviceUpsert{})
	assert.ErrorIs(t, err, space.ErrDeviceEmptyUpsert)

	_, err = techspace.DeviceUpsertOps(arena, space.DeviceUpsert{Apps: map[string]map[string]any{"": {}}})
	assert.ErrorIs(t, err, space.ErrDeviceBadApp)

	_, err = techspace.DeviceUpsertOps(arena, space.DeviceUpsert{Apps: map[string]map[string]any{"a.b": {}}})
	assert.ErrorIs(t, err, space.ErrDeviceBadApp)

	_, err = techspace.DeviceUpsertOps(arena, space.DeviceUpsert{Apps: map[string]map[string]any{"bao": {"nested": map[string]any{}}}})
	assert.ErrorIs(t, err, space.ErrDeviceBadValue)
}

func TestDevices_EmptyIdRejected(t *testing.T) {
	ctrl := newDevicesController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	res, err := ctrl.ApplyChangeWithResult(ctx, devicesChange("v1", "", true,
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldDeviceName}, Payload: arena.NewString("x")},
	))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)
	assert.ErrorIs(t, res.Rejections[0].Err, crdt.ErrValidation)
}

// Deletes are allowed (unlike spaces/profile/identities) — pruning is
// the "device doesn't exist" signal — and the tombstone is sticky:
// the pruned peer id can never re-register, so a claim written after
// the delete is absorbed and the row stays dead.
func TestDevices_DeleteIsSticky(t *testing.T) {
	ctrl := newDevicesController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	const peer = "12D3KooWDevA"
	require.NoError(t, ctrl.ApplyChange(ctx, devicesChange("v1", peer, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldDeviceName}, Payload: arena.NewString("old laptop")},
	)))
	require.NoError(t, ctrl.ApplyChange(ctx, devicesChange("v2", peer, false,
		crdt.Op{Type: crdt.OpDelete},
	)))

	assert.Empty(t, ctrl.Records(ctx, techspace.DevicesDataset), "tombstones must not list")

	res, err := ctrl.ApplyChangeWithResult(ctx, devicesChange("v3", peer, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldDeviceName}, Payload: arena.NewString("back again")},
	))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)
	assert.ErrorIs(t, res.Rejections[0].Err, crdt.ErrRecordDeleted)
	assert.Empty(t, ctrl.Records(ctx, techspace.DevicesDataset))
}

// The claim payload ClaimActive writes, applied via the synced route
// and decoded back — the {seq, at} wire shape is the cross-repo
// contract the `any` server and the runtime read.
func TestDevices_ClaimRoundTrip(t *testing.T) {
	ctrl := newDevicesController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	const peer = "12D3KooWDevA"
	claim := arena.NewObject()
	claim.Set(techspace.DeviceClaimSeq, arena.NewNumberFloat64(3))
	claim.Set(techspace.DeviceClaimAt, arena.NewNumberFloat64(1770000000))
	require.NoError(t, ctrl.ApplyChange(ctx, devicesChange("v1", peer, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldDeviceApps, "bao"}, Payload: arena.NewObject()},
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldDeviceActiveClaims, "bao"}, Payload: claim},
	)))

	d := techspace.DecodeDeviceRecord(ctrl.Get(ctx, techspace.DevicesDataset, peer))
	require.Contains(t, d.Apps, "bao")
	assert.Equal(t, space.DeviceClaim{Seq: 3, At: 1770000000}, d.ActiveClaims["bao"])

	winner, ok := space.ActiveDevice([]space.Device{d}, "bao")
	require.True(t, ok)
	assert.Equal(t, peer, winner)
}
