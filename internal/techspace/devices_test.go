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

// A delete on a never-created id is rejected by BeforeDelete instead
// of minting a sticky tombstone — a mistyped peer id must stay free
// to register, and the gate holds for every writer, not just the
// exists-check in Service.DeleteDevice.
func TestDevices_DeleteAbsentRejected(t *testing.T) {
	ctrl := newDevicesController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	const peer = "12D3KooWDevA"
	res, err := ctrl.ApplyChangeWithResult(ctx, devicesChange("v1", peer, false,
		crdt.Op{Type: crdt.OpDelete},
	))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)
	assert.ErrorIs(t, res.Rejections[0].Err, crdt.ErrValidation)

	require.NoError(t, ctrl.ApplyChange(ctx, devicesChange("v2", peer, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldDeviceName}, Payload: arena.NewString("laptop")},
	)))
	d := techspace.DecodeDeviceRecord(ctrl.Get(ctx, techspace.DevicesDataset, peer))
	assert.Equal(t, "laptop", d.Name, "no tombstone minted — the id registers normally")
	assert.Len(t, ctrl.Records(ctx, techspace.DevicesDataset), 1)
}

// Claims feed the election, so the decode is strict: a claim whose
// seq is missing, non-numeric, zero, fractional, or outside the
// float64-exact integer range reads as absent. Out-of-range floats
// would otherwise convert implementation-dependently (amd64 MinInt64
// vs arm64 MaxInt64) and different architectures would elect
// different winners from the same synced data.
func TestDevices_DecodeSkipsMalformedClaims(t *testing.T) {
	ctrl := newDevicesController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	const peer = "12D3KooWDevA"
	claim := func(seq float64) *anyenc.Value {
		c := arena.NewObject()
		c.Set(techspace.DeviceClaimSeq, arena.NewNumberFloat64(seq))
		return c
	}
	good := claim(2)
	good.Set(techspace.DeviceClaimAt, arena.NewNumberFloat64(1770000000))
	require.NoError(t, ctrl.ApplyChange(ctx, devicesChange("v1", peer, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldDeviceApps, "good"}, Payload: arena.NewObject()},
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldDeviceActiveClaims, "good"}, Payload: good},
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldDeviceActiveClaims, "notObj"}, Payload: arena.NewString("junk")},
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldDeviceActiveClaims, "noSeq"}, Payload: arena.NewObject()},
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldDeviceActiveClaims, "huge"}, Payload: claim(1e300)},
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldDeviceActiveClaims, "zero"}, Payload: claim(0)},
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldDeviceActiveClaims, "frac"}, Payload: claim(1.5)},
	)))

	d := techspace.DecodeDeviceRecord(ctrl.Get(ctx, techspace.DevicesDataset, peer))
	assert.Equal(t, map[string]space.DeviceClaim{"good": {Seq: 2, At: 1770000000}}, d.ActiveClaims)
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

func claimOpsRow(t *testing.T, ctrl *crdt.Controller, peer string, apps ...string) space.Device {
	t.Helper()
	info := map[string]map[string]any{}
	for _, a := range apps {
		info[a] = map[string]any{}
	}
	ops, err := techspace.DeviceUpsertOps(&anyenc.Arena{}, space.DeviceUpsert{Name: peer, Apps: info})
	require.NoError(t, err)
	require.NoError(t, ctrl.ApplyChange(context.Background(), devicesChange(crdt.VersionId("v-"+peer), peer, true, ops...)))
	return techspace.DecodeDeviceRecord(ctrl.Get(context.Background(), techspace.DevicesDataset, peer))
}

// A claim for another device lands on that row only — just the claim,
// never apps — and wins the election over the claimer's older claim.
func TestDevices_ClaimOpsRemote(t *testing.T) {
	ctrl := newDevicesController(t)
	ctx := context.Background()
	const self, target = "12D3KooWSelf", "12D3KooWTarget"

	selfRow := claimOpsRow(t, ctrl, self, "bao")
	selfRow.ActiveClaims = map[string]space.DeviceClaim{"bao": {Seq: 4, At: 1770000000}}
	targetRow := claimOpsRow(t, ctrl, target, "bao")

	rec, err := techspace.ClaimActiveOps(&anyenc.Arena{}, []space.Device{selfRow, targetRow}, self, target, "bao", 1770000100)
	require.NoError(t, err)
	assert.Equal(t, target, rec.Id)
	assert.False(t, rec.Upsert, "a remote claim must never mint a row")
	require.Len(t, rec.Ops, 1)
	assert.Equal(t, []string{techspace.FieldDeviceActiveClaims, "bao"}, rec.Ops[0].Path)

	require.NoError(t, ctrl.ApplyChange(ctx, devicesChange("v-claim", rec.Id, rec.Upsert, rec.Ops...)))
	got := techspace.DecodeDeviceRecord(ctrl.Get(ctx, techspace.DevicesDataset, target))
	assert.Equal(t, space.DeviceClaim{Seq: 5, At: 1770000100}, got.ActiveClaims["bao"])

	winner, ok := space.ActiveDevice([]space.Device{selfRow, got}, "bao")
	require.True(t, ok)
	assert.Equal(t, target, winner)
}

func TestDevices_ClaimOpsRemoteRefusals(t *testing.T) {
	ctrl := newDevicesController(t)
	const self, target = "12D3KooWSelf", "12D3KooWTarget"
	devices := []space.Device{claimOpsRow(t, ctrl, self, "bao"), claimOpsRow(t, ctrl, target, "other")}

	_, err := techspace.ClaimActiveOps(&anyenc.Arena{}, devices, self, "12D3KooWNobody", "bao", 1)
	assert.ErrorIs(t, err, space.ErrDeviceUnknown)

	_, err = techspace.ClaimActiveOps(&anyenc.Arena{}, devices, self, target, "bao", 1)
	assert.ErrorIs(t, err, space.ErrDeviceAppNotInstalled)

	_, err = techspace.ClaimActiveOps(&anyenc.Arena{}, devices, self, target, "a.b", 1)
	assert.ErrorIs(t, err, space.ErrDeviceBadApp)
}

// A self claim — "" or the own peer id — upserts the own row and marks
// the app installed when it isn't yet.
func TestDevices_ClaimOpsSelf(t *testing.T) {
	const self = "12D3KooWSelf"
	for _, target := range []string{"", self} {
		rec, err := techspace.ClaimActiveOps(&anyenc.Arena{}, nil, self, target, "bao", 1770000000)
		require.NoError(t, err)
		assert.Equal(t, self, rec.Id)
		assert.True(t, rec.Upsert)
		require.Len(t, rec.Ops, 2)
		assert.Equal(t, []string{techspace.FieldDeviceApps, "bao"}, rec.Ops[0].Path)
		assert.Equal(t, []string{techspace.FieldDeviceActiveClaims, "bao"}, rec.Ops[1].Path)
	}

	withApp := []space.Device{{PeerId: self, Apps: map[string]map[string]any{"bao": {}}}}
	rec, err := techspace.ClaimActiveOps(&anyenc.Arena{}, withApp, self, "", "bao", 1770000000)
	require.NoError(t, err)
	require.Len(t, rec.Ops, 1)
}

// A remote claim on a row pruned after the snapshot is absorbed by the
// tombstone: it is not an upsert, so the row never resurrects.
func TestDevices_ClaimOpsRemoteOnPrunedRow(t *testing.T) {
	ctrl := newDevicesController(t)
	ctx := context.Background()
	const self, target = "12D3KooWSelf", "12D3KooWTarget"
	devices := []space.Device{claimOpsRow(t, ctrl, self, "bao"), claimOpsRow(t, ctrl, target, "bao")}

	rec, err := techspace.ClaimActiveOps(&anyenc.Arena{}, devices, self, target, "bao", 1)
	require.NoError(t, err)
	require.NoError(t, ctrl.ApplyChange(ctx, devicesChange("v-del", target, false, crdt.Op{Type: crdt.OpDelete})))

	_ = ctrl.ApplyChange(ctx, devicesChange("v-claim", rec.Id, rec.Upsert, rec.Ops...))
	for _, v := range ctrl.Records(ctx, techspace.DevicesDataset) {
		assert.NotEqual(t, target, techspace.DecodeDeviceRecord(v).PeerId)
	}
}
