package techspace_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

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

// A self claim's payload, applied via the synced route and decoded
// back — the {seq, at} shape without a target (TestDevices_ClaimOpsRemote
// covers the target) is the cross-repo contract the `any` server and
// the runtime read.
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

// applyClaim applies a planned claim through the synced route and
// returns the registry as every reader sees it.
func applyClaim(t *testing.T, ctrl *crdt.Controller, v crdt.VersionId, rec crdt.RecordChange) []space.Device {
	t.Helper()
	ctx := context.Background()
	res, err := ctrl.ApplyChangeWithResult(ctx, devicesChange(v, rec.Id, rec.Upsert, rec.Ops...))
	require.NoError(t, err)
	require.Empty(t, res.Rejections)
	return techspace.DecodeDevices(ctrl.Records(ctx, techspace.DevicesDataset))
}

func deviceById(devices []space.Device, peer string) space.Device {
	for _, d := range devices {
		if d.PeerId == peer {
			return d
		}
	}
	return space.Device{}
}

// A claim for another device lands on the claimer's own row, naming
// the target — the target row is never written. It replaces the
// claimer's own self claim (one claim per app per row) and hands the
// election to the target.
func TestDevices_ClaimOpsRemote(t *testing.T) {
	ctrl := newDevicesController(t)
	const self, target = "12D3KooWSelf", "12D3KooWTarget"
	claimOpsRow(t, ctrl, self, "bao")
	claimOpsRow(t, ctrl, target, "bao")

	selfClaim, err := techspace.ClaimActiveOps(&anyenc.Arena{}, nil, self, "", "bao", 1770000000)
	require.NoError(t, err)
	devices := applyClaim(t, ctrl, "v1-self", selfClaim)
	before := deviceById(devices, target)

	rec, err := techspace.ClaimActiveOps(&anyenc.Arena{}, devices, self, target, "bao", 1770000100)
	require.NoError(t, err)
	assert.Equal(t, self, rec.Id)
	require.Len(t, rec.Ops, 1)
	assert.Equal(t, []string{techspace.FieldDeviceActiveClaims, "bao"}, rec.Ops[0].Path)

	devices = applyClaim(t, ctrl, "v2-remote", rec)
	assert.Equal(t, space.DeviceClaim{Seq: 2, At: 1770000100, Target: target}, deviceById(devices, self).ActiveClaims["bao"])
	assert.Equal(t, before, deviceById(devices, target), "the target row is untouched")

	winner, ok := space.ActiveDevice(devices, "bao")
	require.True(t, ok)
	assert.Equal(t, target, winner)
}

// The claimer needs no app of its own: a device without bao (a phone
// running only the UI) can hand bao to one that has it.
func TestDevices_ClaimOpsRemoteFromDeviceWithoutApp(t *testing.T) {
	ctrl := newDevicesController(t)
	const self, target = "12D3KooWPhone", "12D3KooWBox"
	devices := []space.Device{claimOpsRow(t, ctrl, self, "ui"), claimOpsRow(t, ctrl, target, "bao")}

	rec, err := techspace.ClaimActiveOps(&anyenc.Arena{}, devices, self, target, "bao", 1)
	require.NoError(t, err)
	require.Len(t, rec.Ops, 1, "a claim that names a device never writes apps")
	devices = applyClaim(t, ctrl, "v2-remote", rec)
	assert.NotContains(t, deviceById(devices, self).Apps, "bao")

	winner, ok := space.ActiveDevice(devices, "bao")
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

// The runtime's self claim ("") writes no target, marks the app
// installed when the row doesn't carry it yet (including a row that
// doesn't exist), and wins once applied.
func TestDevices_ClaimOpsSelf(t *testing.T) {
	const self = "12D3KooWSelf"
	ctrl := newDevicesController(t)
	rec, err := techspace.ClaimActiveOps(&anyenc.Arena{}, nil, self, "", "bao", 1770000000)
	require.NoError(t, err)
	assert.Equal(t, self, rec.Id)
	assert.True(t, rec.Upsert)

	devices := applyClaim(t, ctrl, "v1-self", rec)
	got := deviceById(devices, self)
	assert.Contains(t, got.Apps, "bao")
	assert.Equal(t, space.DeviceClaim{Seq: 1, At: 1770000000}, got.ActiveClaims["bao"])
	winner, ok := space.ActiveDevice(devices, "bao")
	require.True(t, ok)
	assert.Equal(t, self, winner)

	again, err := techspace.ClaimActiveOps(&anyenc.Arena{}, devices, self, "", "bao", 1770000001)
	require.NoError(t, err)
	require.Len(t, again.Ops, 1, "the app is installed already")
	devices = applyClaim(t, ctrl, "v2-again", again)
	assert.Equal(t, int64(2), deviceById(devices, self).ActiveClaims["bao"].Seq)
}

// Pruning the target after a remote claim moves the election back to
// the best remaining claim; pruning the claimer drops its claim.
func TestDevices_ClaimOpsPruneMovesWinner(t *testing.T) {
	ctx := context.Background()
	const a, b, c = "12D3KooWA", "12D3KooWB", "12D3KooWC"
	setup := func(t *testing.T) (*crdt.Controller, []space.Device) {
		ctrl := newDevicesController(t)
		claimOpsRow(t, ctrl, a, "bao")
		claimOpsRow(t, ctrl, b, "bao")
		claimOpsRow(t, ctrl, c, "bao")
		selfA, err := techspace.ClaimActiveOps(&anyenc.Arena{}, nil, a, "", "bao", 1)
		require.NoError(t, err)
		devices := applyClaim(t, ctrl, "v-a", selfA)
		cForB, err := techspace.ClaimActiveOps(&anyenc.Arena{}, devices, c, b, "bao", 2)
		require.NoError(t, err)
		devices = applyClaim(t, ctrl, "v-c", cForB)
		winner, ok := space.ActiveDevice(devices, "bao")
		require.True(t, ok)
		require.Equal(t, b, winner)
		return ctrl, devices
	}
	prune := func(t *testing.T, ctrl *crdt.Controller, peer string) []space.Device {
		require.NoError(t, ctrl.ApplyChange(ctx, devicesChange(crdt.VersionId("v3-del-"+peer), peer, false, crdt.Op{Type: crdt.OpDelete})))
		return techspace.DecodeDevices(ctrl.Records(ctx, techspace.DevicesDataset))
	}

	t.Run("target pruned", func(t *testing.T) {
		ctrl, _ := setup(t)
		winner, ok := space.ActiveDevice(prune(t, ctrl, b), "bao")
		require.True(t, ok)
		assert.Equal(t, a, winner)
	})
	t.Run("claimer pruned", func(t *testing.T) {
		ctrl, _ := setup(t)
		winner, ok := space.ActiveDevice(prune(t, ctrl, c), "bao")
		require.True(t, ok)
		assert.Equal(t, a, winner)
	})
}

// The target round-trips through the synced route; a target that is
// present but not a non-empty string drops the claim, like a
// malformed seq — reading it as a self claim would hand the app back
// to the device that gave it away.
func TestDevices_ClaimTargetDecode(t *testing.T) {
	ctrl := newDevicesController(t)
	ctx := context.Background()
	arena := &anyenc.Arena{}

	const peer = "12D3KooWDevA"
	claim := func(target *anyenc.Value) *anyenc.Value {
		c := arena.NewObject()
		c.Set(techspace.DeviceClaimSeq, arena.NewNumberFloat64(1))
		c.Set(techspace.DeviceClaimTarget, target)
		return c
	}
	require.NoError(t, ctrl.ApplyChange(ctx, devicesChange("v1", peer, true,
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldDeviceActiveClaims, "str"}, Payload: claim(arena.NewString("12D3KooWDevB"))},
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldDeviceActiveClaims, "num"}, Payload: claim(arena.NewNumberFloat64(7))},
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldDeviceActiveClaims, "obj"}, Payload: claim(arena.NewObject())},
		crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldDeviceActiveClaims, "empty"}, Payload: claim(arena.NewString(""))},
	)))

	d := techspace.DecodeDeviceRecord(ctrl.Get(ctx, techspace.DevicesDataset, peer))
	assert.Equal(t, map[string]space.DeviceClaim{"str": {Seq: 1, Target: "12D3KooWDevB"}}, d.ActiveClaims)
}

// Naming this device by its peer id is not the runtime's self claim:
// it gets the same checks as any named target and never marks the app
// installed, so a device without the app can't elect itself this way.
func TestDevices_ClaimOpsExplicitSelf(t *testing.T) {
	const self = "12D3KooWSelf"

	_, err := techspace.ClaimActiveOps(&anyenc.Arena{}, nil, self, self, "bao", 1)
	assert.ErrorIs(t, err, space.ErrDeviceUnknown, "no own row yet")

	devices := []space.Device{{PeerId: self, Apps: map[string]map[string]any{"ui": {}}}}
	_, err = techspace.ClaimActiveOps(&anyenc.Arena{}, devices, self, self, "bao", 1)
	assert.ErrorIs(t, err, space.ErrDeviceAppNotInstalled)

	ctrl := newDevicesController(t)
	devices = []space.Device{claimOpsRow(t, ctrl, self, "bao")}
	rec, err := techspace.ClaimActiveOps(&anyenc.Arena{}, devices, self, self, "bao", 1770000000)
	require.NoError(t, err)
	require.Len(t, rec.Ops, 1)
	devices = applyClaim(t, ctrl, "v1-self", rec)
	assert.Equal(t, space.DeviceClaim{Seq: 1, At: 1770000000}, deviceById(devices, self).ActiveClaims["bao"])
}

// A pruned device is refused before any planning, whatever it names —
// a failed target check must not hide that it was pruned.
func TestDevices_PlanClaimActivePrunedFirst(t *testing.T) {
	ctx := context.Background()
	ctrl := newDevicesController(t)
	const self, other = "12D3KooWSelf", "12D3KooWOther"
	claimOpsRow(t, ctrl, self, "bao")
	claimOpsRow(t, ctrl, other, "bao")
	require.NoError(t, ctrl.ApplyChange(ctx, devicesChange("v9-del", self, false, crdt.Op{Type: crdt.OpDelete})))

	for _, target := range []string{"", self, other, "12D3KooWNobody"} {
		_, err := techspace.PlanClaimActive(ctx, ctrl, self, target, "bao", 1)
		assert.ErrorIs(t, err, space.ErrDevicePruned, "target %q", target)
	}
	_, err := techspace.PlanClaimActive(ctx, ctrl, other, self, "bao", 1)
	assert.ErrorIs(t, err, space.ErrDeviceUnknown, "a live device naming the pruned one")
}

// A claim at the seq ceiling can't be outranked: seq+1 would round back
// to the same float64 on the wire and tie instead of winning, so the
// claim is refused rather than written as a silent no-op.
func TestDevices_ClaimOpsSeqCeiling(t *testing.T) {
	const self, other = "12D3KooWSelf", "12D3KooWOther"
	row := func(seq int64) []space.Device {
		return []space.Device{{
			PeerId:       other,
			Apps:         map[string]map[string]any{"bao": {}},
			ActiveClaims: map[string]space.DeviceClaim{"bao": {Seq: seq}},
		}}
	}
	_, err := techspace.ClaimActiveOps(&anyenc.Arena{}, row(1<<53), self, "", "bao", 1)
	assert.Error(t, err)

	ctrl := newDevicesController(t)
	claimOpsRow(t, ctrl, self, "bao")
	rec, err := techspace.ClaimActiveOps(&anyenc.Arena{}, row(1<<53-1), self, "", "bao", 1)
	require.NoError(t, err)
	devices := applyClaim(t, ctrl, "v1-self", rec)
	assert.Equal(t, int64(1<<53), deviceById(devices, self).ActiveClaims["bao"].Seq)
}

// A claim waiting for the slot leaves as soon as its ctx ends, and a
// ctx already done never takes a free slot.
func TestDevices_LockClaimsHonorsCtx(t *testing.T) {
	s := techspace.New(nil, nil)
	unlock, err := s.LockClaimsForTest(context.Background())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = s.LockClaimsForTest(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), 5*time.Second)
	unlock()

	done, cancelDone := context.WithCancel(context.Background())
	cancelDone()
	_, err = s.LockClaimsForTest(done)
	assert.ErrorIs(t, err, context.Canceled)

	unlock, err = s.LockClaimsForTest(context.Background())
	require.NoError(t, err, "a refused waiter must not keep the slot")
	unlock()
}
