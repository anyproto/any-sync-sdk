// The devices registry — one row per device of the account, living on
// the tech-space index object next to spaces/profile/identities
// (SYN-165). Each row carries the device's self-reported metadata
// (name / os / version), the set of installed apps, and the per-app
// active claims that back the reader-side active-device election (see
// space.ActiveDevice — the single implementation of the rule).

package techspace

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/space"
)

const (
	// DevicesDataset name on the space-index tree. Piggybacks on the
	// index object like profile/identities — one extra handler reg, no
	// second derived tree.
	DevicesDataset = "devices"

	// DevicesHandlerVersion stamped on every change.
	DevicesHandlerVersion = "devicesHandler-v1"

	// Row id is the device's libp2p peer id. Every field below is
	// SYNCED — the registry's whole point is cross-device visibility;
	// per-device runtime state (online status etc.) is deliberately
	// kept out (KV / event bus territory, never persisted here).

	// FieldDeviceName is the device's display name (hostname or
	// user-set).
	FieldDeviceName = "name"
	// FieldDeviceOS is the device's operating system (runtime.GOOS
	// vocabulary by convention).
	FieldDeviceOS = "os"
	// FieldDeviceVersion is the device's engine build version.
	FieldDeviceVersion = "version"
	// FieldDeviceApps is the set of installed apps: a free-form object
	// keyed by app slug (an open set — nothing app-specific is
	// hardcoded), each value a free-form scalar bag ({version: ...}).
	// Presence of the slug = installed; written / removed per-slug
	// (apps.<slug>) so devices touching different slugs merge.
	FieldDeviceApps = "apps"
	// FieldDeviceActiveClaims holds the per-app active claims, keyed by
	// app slug: {seq, at} written by Service.ClaimActive. The claim is
	// writer-supplied data — NOT a CRDT version id, which is
	// peer-locally allocated and therefore not comparable across
	// devices. The winner is computed by every reader with the same
	// deterministic rule (space.ActiveDevice); concurrent claims may
	// collide on seq and are survivable via the tiebreak.
	FieldDeviceActiveClaims = "activeClaims"
)

// FieldDeviceActiveClaims subkeys — the claim shape.
const (
	// DeviceClaimSeq is the writer-supplied claim sequence:
	// max(existing seqs for the slug across all rows) + 1 at claim
	// time. Highest wins.
	DeviceClaimSeq = "seq"
	// DeviceClaimAt is the claim wall-clock time in unix seconds —
	// first tiebreak on equal seq (advisory, writer-supplied).
	DeviceClaimAt = "at"
)

// DevicesSchema declares the devices dataset. All fields synced — the
// per-device fields elsewhere in the tech space (localStatus, ownRole,
// …) are ScopeLocal and never cross devices; that scope would make
// this registry invisible exactly where it's needed.
func DevicesSchema() schema.Dataset {
	str := func() *schema.Schema { return schema.Leaf(schema.KindString) }
	return schema.Dataset{Fields: []schema.Field{
		{Id: FieldDeviceName, Name: "Name", Schema: str(), Scope: schema.ScopeSynced,
			Description: "Device display name; hostname until the user sets one.", XFormat: map[string]any{"type": "text"}},
		{Id: FieldDeviceOS, Name: "OS", Schema: str(), Scope: schema.ScopeSynced,
			Description: "Operating system, GOOS vocabulary."},
		{Id: FieldDeviceVersion, Name: "Version", Schema: str(), Scope: schema.ScopeSynced,
			Description: "Engine build version."},
		// KindObject with nil Properties = free-form shape (same
		// declaration trick as `settings` on the spaces dataset):
		// subkeys are app slugs, validated by the setter, not the
		// schema.
		{Id: FieldDeviceApps, Name: "Apps", Schema: schema.Leaf(schema.KindObject), Scope: schema.ScopeSynced,
			Description: "Installed apps by slug; presence means installed."},
		{Id: FieldDeviceActiveClaims, Name: "Active claims", Schema: schema.Leaf(schema.KindObject), Scope: schema.ScopeSynced,
			Description: "Per-slug {seq, at} claims the active-app election reads."},
	}}
}

// DevicesHandler validates ops on the devices dataset: any non-empty
// id (the peer id). Deletes are ALLOWED — pruning a decommissioned
// device row is the "device doesn't exist" signal the election rule
// keys off. Self-row-only writing is NOT enforceable here (a CRDT
// handler can't know which peer authored a change's row id); it is a
// write-path convention — Service.SetDevice / ClaimActive only ever
// target the local peer id — and the restricted HTTP surface enforces
// it at the API boundary.
type DevicesHandler struct{}

func (DevicesHandler) Init(_ context.Context) error { return nil }

func (DevicesHandler) BeforeCreate(_ *crdt.ChangeCtx, rec *crdt.RecordChange, _ *crdt.Sink) error {
	if rec.Id == "" {
		return crdt.ErrValidation
	}
	return nil
}

func (DevicesHandler) BeforeModify(_ *crdt.ChangeCtx, rec *crdt.RecordChange, _ *crdt.Op, _ *crdt.Sink) error {
	if rec.Id == "" {
		return crdt.ErrValidation
	}
	return nil
}

// BeforeDelete rejects tombstoning a row that was never created:
// deletes are otherwise allowed (pruning is the "device doesn't
// exist" signal), but the tombstone is sticky, and a delete on an
// absent id would permanently ban a possibly-mistyped peer id. A
// never-created id carries no creation marker (_ver) in ctx.Before —
// the same test the apply path's own create detection uses. The gate
// is deterministic (every replica evaluates it on the same causal
// prefix) and so covers every writer, not just the exists-check in
// Service.DeleteDevice.
func (DevicesHandler) BeforeDelete(ctx *crdt.ChangeCtx, _ *crdt.RecordChange, _ *crdt.Sink) error {
	if ctx.Before == nil || ctx.Before.Get(crdt.VersionsKey) == nil {
		return fmt.Errorf("%w: delete of a never-created device row", crdt.ErrValidation)
	}
	return nil
}

// DecodeDeviceRecord lifts a devices row (as returned by
// Controller.Get / Records) into the public space.Device. The row id
// (the peer id) is read from the CRDT-stamped "id" field. Returns the
// zero value for nil input.
func DecodeDeviceRecord(v *anyenc.Value) space.Device {
	if v == nil {
		return space.Device{}
	}
	d := space.Device{
		PeerId:  v.GetString(crdt.IdField),
		Name:    v.GetString(FieldDeviceName),
		OS:      v.GetString(FieldDeviceOS),
		Version: v.GetString(FieldDeviceVersion),
	}
	if apps := v.Get(FieldDeviceApps); apps != nil && apps.Type() == anyenc.TypeObject {
		if obj, err := apps.Object(); err == nil {
			d.Apps = make(map[string]map[string]any, obj.Len())
			obj.Visit(func(k []byte, val *anyenc.Value) {
				// Presence = installed; tolerate a non-object info bag
				// (unknown future writer) as "installed, no metadata".
				info, _ := val.GoType().(map[string]any)
				if info == nil {
					info = map[string]any{}
				}
				d.Apps[string(k)] = info
			})
		}
	}
	if claims := v.Get(FieldDeviceActiveClaims); claims != nil && claims.Type() == anyenc.TypeObject {
		if obj, err := claims.Object(); err == nil {
			d.ActiveClaims = make(map[string]space.DeviceClaim, obj.Len())
			obj.Visit(func(k []byte, val *anyenc.Value) {
				// Strict shape gate — unlike the tolerant Apps decode
				// above, a claim feeds the election: a malformed bag
				// (unknown future writer, corrupt data) decoding to
				// zeros would beat genuinely-unclaimed rows, and an
				// out-of-range seq would convert differently per
				// architecture (see claimNum). Seq must be a valid
				// number >= 1 (ClaimActive mints from 1); a bad claim
				// reads as absent.
				seq, okSeq := claimNum(val, DeviceClaimSeq)
				if !okSeq || seq < 1 {
					return
				}
				at, _ := claimNum(val, DeviceClaimAt) // advisory tiebreak; bad reads as 0
				d.ActiveClaims[string(k)] = space.DeviceClaim{Seq: seq, At: at}
			})
		}
	}
	return d
}

// maxClaimNum bounds claim numbers at 2^53 — the float64
// exactly-representable integer limit. Beyond it seq+1 could stop
// advancing (float64 rounding), long before int64 overflow matters.
const maxClaimNum = 1 << 53

// claimNum reads one numeric claim subfield. anyenc numbers are
// float64 on the wire, and Go's float-to-int conversion for
// out-of-range values is implementation-defined (amd64 saturates to
// MinInt64, arm64 to MaxInt64) — different architectures would elect
// different winners from the same synced claim. So anything missing,
// non-numeric, non-finite, fractional, negative, or above maxClaimNum
// is rejected here instead of converted.
func claimNum(v *anyenc.Value, key string) (int64, bool) {
	nv := v.Get(key)
	if nv == nil {
		return 0, false
	}
	f, err := nv.Float64()
	if err != nil {
		return 0, false
	}
	if math.IsNaN(f) || f < 0 || f > maxClaimNum || f != math.Trunc(f) {
		return 0, false
	}
	return int64(f), true
}

// liveDeviceRow reports whether v is a live devices row — present and
// not a sticky tombstone. The single definition of row liveness for
// this dataset.
func liveDeviceRow(v *anyenc.Value) bool {
	return v != nil && v.Get(crdt.DeletedAtField) == nil
}

// surfaceDeviceRejections lifts per-op handler rejections on an
// own-row devices write into a caller-visible error. The writes here
// are single-record and valid by construction, so any rejection means
// nothing landed; the critical case is delete-wins absorption
// (crdt.ErrRecordDeleted) — this device was pruned and the sticky
// tombstone absorbs every later write, which without this check would
// read as success forever.
func surfaceDeviceRejections(res object.WriteResult, peerId string) error {
	if len(res.Rejections) == 0 {
		return nil
	}
	rej := res.Rejections[0]
	if errors.Is(rej.Err, crdt.ErrRecordDeleted) {
		return fmt.Errorf("techspace: %w: %q", space.ErrDevicePruned, peerId)
	}
	return fmt.Errorf("techspace: devices write rejected: %w", rej.Err)
}

// PeerId returns this device's libp2p peer id — the devices-dataset
// row id every self-targeted write uses. Empty before Open.
func (s *Service) PeerId() string {
	keys := s.app.AccountKeys()
	if keys == nil {
		return ""
	}
	return keys.PeerKey.GetPublic().PeerId()
}

// validDeviceApp gates app slugs the same way settings keys are gated
// (a dotted slug would silently become a deeper path under apps. /
// activeClaims.): the shared single-level-key grammar, wrapping the
// devices sentinel.
func validDeviceApp(slug string) error {
	return validSingleLevelKey(slug, space.ErrDeviceBadApp)
}

// DeviceUpsertOps encodes a space.DeviceUpsert into per-path CRDT ops
// on the caller's own row: one OpSet per non-empty scalar field, one
// OpSet at [apps, slug] per app entry (or OpUnset for a nil entry —
// the uninstall signal). Per-path so writes touching different fields
// / slugs merge instead of clobbering. Exported for the handler-level
// tests. App info values are scalars only, same vocabulary as
// settings (string / bool / number, stored as float64).
func DeviceUpsertOps(a *anyenc.Arena, up space.DeviceUpsert) ([]crdt.Op, error) {
	var ops []crdt.Op
	setStr := func(field, val string) {
		if val != "" {
			ops = append(ops, crdt.Op{Type: crdt.OpSet, Path: []string{field}, Payload: a.NewString(val)})
		}
	}
	setStr(FieldDeviceName, up.Name)
	setStr(FieldDeviceOS, up.OS)
	setStr(FieldDeviceVersion, up.Version)
	for _, slug := range slices.Sorted(maps.Keys(up.Apps)) {
		if err := validDeviceApp(slug); err != nil {
			return nil, err
		}
		info := up.Apps[slug]
		if info == nil {
			ops = append(ops, crdt.Op{Type: crdt.OpUnset, Path: []string{FieldDeviceApps, slug}})
			continue
		}
		bag := a.NewObject()
		for _, k := range slices.Sorted(maps.Keys(info)) {
			v, err := encodeSettingsValue(a, info[k])
			if err != nil {
				return nil, fmt.Errorf("%w: app %q key %q: %w", space.ErrDeviceBadValue, slug, k, err)
			}
			bag.Set(k, v)
		}
		ops = append(ops, crdt.Op{Type: crdt.OpSet, Path: []string{FieldDeviceApps, slug}, Payload: bag})
	}
	if len(ops) == 0 {
		return nil, space.ErrDeviceEmptyUpsert
	}
	return ops, nil
}

// SetDevice upserts THIS device's row in the devices registry — the
// row id is always the local peer id, never caller-supplied (the
// self-row-only convention). SYNCED (LocalWrite → DAG → the account's
// other devices). Per-path ops per DeviceUpsertOps; at least one field
// must be non-empty.
func (s *Service) SetDevice(ctx context.Context, up space.DeviceUpsert) (object.WriteResult, error) {
	if !s.open.Load() {
		return object.WriteResult{}, errors.New("techspace: service not open")
	}
	peerId := s.PeerId()
	if peerId == "" {
		return object.WriteResult{}, errors.New("techspace: SetDevice: no peer key")
	}
	arena := &anyenc.Arena{}
	ops, err := DeviceUpsertOps(arena, up)
	if err != nil {
		return object.WriteResult{}, err
	}
	obj, err := s.indexObj(ctx)
	if err != nil {
		return object.WriteResult{}, err
	}
	change := crdt.Change{
		Dataset:     DevicesDataset,
		DataVersion: DevicesHandlerVersion,
		Records: []crdt.RecordChange{
			{Id: peerId, Upsert: true, Ops: ops},
		},
	}
	res, err := obj.LocalWrite(ctx, change)
	if err != nil {
		return res, err
	}
	return res, surfaceDeviceRejections(res, peerId)
}

// ClaimActive marks THIS device as the active instance of app: it
// writes activeClaims.<app> = {seq, at} on the own row, with seq =
// max(existing seqs for the slug across all live rows) + 1 and at =
// now (unix seconds). The read-then-write is not atomic — two devices
// claiming concurrently can mint the same seq — but the reader-side
// rule (space.ActiveDevice: highest seq, then at, then peer id) makes
// that survivable by design; see SYN-165.
//
// Known limit (v1): seq is minted from THIS replica's view — on a
// device that hasn't synced the latest claims yet, a fresh claim can
// mint a lower seq than an unseen earlier one and lose the election
// once heads converge, inverting the user's newest intent (the same
// caveat space.ActiveDevice documents). Claims are cheap: re-claim
// after sync.
//
// The same change also marks the app installed (apps.<app> = {}) when
// the own row doesn't carry it yet, so a claim can never dangle on a
// row the election filter would skip.
func (s *Service) ClaimActive(ctx context.Context, app string) (object.WriteResult, error) {
	if !s.open.Load() {
		return object.WriteResult{}, errors.New("techspace: service not open")
	}
	if err := validDeviceApp(app); err != nil {
		return object.WriteResult{}, err
	}
	peerId := s.PeerId()
	if peerId == "" {
		return object.WriteResult{}, errors.New("techspace: ClaimActive: no peer key")
	}
	obj, err := s.indexObj(ctx)
	if err != nil {
		return object.WriteResult{}, err
	}

	var maxSeq int64
	var selfHasApp bool
	for _, v := range obj.Controller().Records(ctx, DevicesDataset) {
		d := DecodeDeviceRecord(v)
		if c, ok := d.ActiveClaims[app]; ok && c.Seq > maxSeq {
			maxSeq = c.Seq
		}
		if d.PeerId == peerId {
			_, selfHasApp = d.Apps[app]
		}
	}

	arena := &anyenc.Arena{}
	claim := arena.NewObject()
	// Float64 constructors — anyenc numbers are float64 on the wire.
	claim.Set(DeviceClaimSeq, arena.NewNumberFloat64(float64(maxSeq+1)))
	claim.Set(DeviceClaimAt, arena.NewNumberFloat64(float64(time.Now().Unix())))
	ops := []crdt.Op{{Type: crdt.OpSet, Path: []string{FieldDeviceActiveClaims, app}, Payload: claim}}
	if !selfHasApp {
		ops = append([]crdt.Op{{Type: crdt.OpSet, Path: []string{FieldDeviceApps, app}, Payload: arena.NewObject()}}, ops...)
	}
	change := crdt.Change{
		Dataset:     DevicesDataset,
		DataVersion: DevicesHandlerVersion,
		Records: []crdt.RecordChange{
			{Id: peerId, Upsert: true, Ops: ops},
		},
	}
	res, err := obj.LocalWrite(ctx, change)
	if err != nil {
		return res, err
	}
	return res, surfaceDeviceRejections(res, peerId)
}

// DeleteDevice prunes peerId's row — the "device doesn't exist"
// signal that moves the active election away from it. SYNCED, and a
// sticky CRDT tombstone: the id can never re-register (a pruned
// device that comes back online stays unlisted until it re-derives
// its peer keys). The row must exist — a delete on an absent id would
// still mint a tombstone, permanently banning a possibly-mistyped id,
// so the unknown case errors instead (double-guarded by the handler's
// BeforeDelete, which covers non-service writers too). The local
// device's OWN row is refused (ErrDeviceSelfDelete): the sticky
// tombstone would permanently lock this installation out of the
// registry — prune it from another device.
func (s *Service) DeleteDevice(ctx context.Context, peerId string) (object.WriteResult, error) {
	if !s.open.Load() {
		return object.WriteResult{}, errors.New("techspace: service not open")
	}
	if peerId == "" {
		return object.WriteResult{}, fmt.Errorf("%w: empty peer id", space.ErrDeviceUnknown)
	}
	if peerId == s.PeerId() {
		return object.WriteResult{}, fmt.Errorf("techspace: %w: %q", space.ErrDeviceSelfDelete, peerId)
	}
	obj, err := s.indexObj(ctx)
	if err != nil {
		return object.WriteResult{}, err
	}
	if !liveDeviceRow(obj.Controller().Get(ctx, DevicesDataset, peerId)) {
		return object.WriteResult{}, fmt.Errorf("%w: %q", space.ErrDeviceUnknown, peerId)
	}
	change := crdt.Change{
		Dataset:     DevicesDataset,
		DataVersion: DevicesHandlerVersion,
		Records: []crdt.RecordChange{
			{Id: peerId, Ops: []crdt.Op{{Type: crdt.OpDelete}}},
		},
	}
	res, err := obj.LocalWrite(ctx, change)
	if err != nil {
		return res, err
	}
	return res, surfaceDeviceRejections(res, peerId)
}

// ListDevices returns every live devices row (tombstones excluded by
// Controller.Records). Inbound head-sync changes are projected live by
// the resident index object's listener, so a plain read is current.
// Unavailability (service not open, index object not loadable) is an
// error, never an empty slice — an empty registry and a closed
// service must stay distinguishable, or an election consumer reading
// during boot would wrongly self-claim against an "empty" registry.
func (s *Service) ListDevices(ctx context.Context) ([]space.Device, error) {
	if !s.open.Load() {
		return nil, errors.New("techspace: service not open")
	}
	obj, err := s.indexObj(ctx)
	if err != nil {
		return nil, err
	}
	rows := obj.Controller().Records(ctx, DevicesDataset)
	out := make([]space.Device, 0, len(rows))
	for _, v := range rows {
		out = append(out, DecodeDeviceRecord(v))
	}
	return out, nil
}
