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
	"slices"
	"strings"
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
		{Id: FieldDeviceName, Name: "Name", Schema: str(), Scope: schema.ScopeSynced},
		{Id: FieldDeviceOS, Name: "OS", Schema: str(), Scope: schema.ScopeSynced},
		{Id: FieldDeviceVersion, Name: "Version", Schema: str(), Scope: schema.ScopeSynced},
		// KindObject with nil Properties = free-form shape (same
		// declaration trick as `settings` on the spaces dataset):
		// subkeys are app slugs, validated by the setter, not the
		// schema.
		{Id: FieldDeviceApps, Name: "Apps", Schema: schema.Leaf(schema.KindObject), Scope: schema.ScopeSynced},
		{Id: FieldDeviceActiveClaims, Name: "Active claims", Schema: schema.Leaf(schema.KindObject), Scope: schema.ScopeSynced},
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

func (DevicesHandler) BeforeDelete(_ *crdt.ChangeCtx, _ *crdt.RecordChange, _ *crdt.Sink) error {
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
		PeerId:  v.GetString("id"),
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
				// Float64 reads — anyenc numbers are float64 on the
				// wire; GetInt would truncate on 32-bit platforms.
				d.ActiveClaims[string(k)] = space.DeviceClaim{
					Seq: int64(val.GetFloat64(DeviceClaimSeq)),
					At:  int64(val.GetFloat64(DeviceClaimAt)),
				}
			})
		}
	}
	return d
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

// validDeviceApp gates app slugs the same way settings keys are gated:
// non-empty, dot-free (a dotted slug would silently become a deeper
// path under apps./activeClaims.).
func validDeviceApp(slug string) error {
	if slug == "" {
		return fmt.Errorf("%w: empty", space.ErrDeviceBadApp)
	}
	if strings.ContainsRune(slug, '.') {
		return fmt.Errorf("%w: %q contains '.' (single-level slugs only)", space.ErrDeviceBadApp, slug)
	}
	return nil
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
	return obj.LocalWrite(ctx, change)
}

// ClaimActive marks THIS device as the active instance of app: it
// writes activeClaims.<app> = {seq, at} on the own row, with seq =
// max(existing seqs for the slug across all live rows) + 1 and at =
// now (unix seconds). The read-then-write is not atomic — two devices
// claiming concurrently can mint the same seq — but the reader-side
// rule (space.ActiveDevice: highest seq, then at, then peer id) makes
// that survivable by design; see SYN-165.
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
	return obj.LocalWrite(ctx, change)
}

// DeleteDevice prunes peerId's row — the "device doesn't exist"
// signal that moves the active election away from it. SYNCED, and a
// sticky CRDT tombstone: the id can never re-register (a pruned
// device that comes back online stays unlisted until it re-derives
// its peer keys). The row must exist — a delete on an absent id would
// still mint a tombstone, permanently banning a possibly-mistyped id,
// so the unknown case errors instead.
func (s *Service) DeleteDevice(ctx context.Context, peerId string) (object.WriteResult, error) {
	if !s.open.Load() {
		return object.WriteResult{}, errors.New("techspace: service not open")
	}
	if peerId == "" {
		return object.WriteResult{}, fmt.Errorf("%w: empty peer id", space.ErrDeviceUnknown)
	}
	obj, err := s.indexObj(ctx)
	if err != nil {
		return object.WriteResult{}, err
	}
	if v := obj.Controller().Get(ctx, DevicesDataset, peerId); v == nil || v.Get(crdt.DeletedAtField) != nil {
		return object.WriteResult{}, fmt.Errorf("%w: %q", space.ErrDeviceUnknown, peerId)
	}
	change := crdt.Change{
		Dataset:     DevicesDataset,
		DataVersion: DevicesHandlerVersion,
		Records: []crdt.RecordChange{
			{Id: peerId, Ops: []crdt.Op{{Type: crdt.OpDelete}}},
		},
	}
	return obj.LocalWrite(ctx, change)
}

// GetDevice returns one devices row. Tombstoned (pruned) rows read as
// absent.
func (s *Service) GetDevice(ctx context.Context, peerId string) (space.Device, bool) {
	if !s.open.Load() || peerId == "" {
		return space.Device{}, false
	}
	obj, err := s.indexObj(ctx)
	if err != nil {
		return space.Device{}, false
	}
	v := obj.Controller().Get(ctx, DevicesDataset, peerId)
	if v == nil || v.Get(crdt.DeletedAtField) != nil {
		return space.Device{}, false
	}
	return DecodeDeviceRecord(v), true
}

// ListDevices returns every live devices row (tombstones excluded by
// Controller.Records). Inbound head-sync changes are projected live by
// the resident index object's listener, so a plain read is current.
func (s *Service) ListDevices(ctx context.Context) []space.Device {
	if !s.open.Load() {
		return nil
	}
	obj, err := s.indexObj(ctx)
	if err != nil {
		return nil
	}
	rows := obj.Controller().Records(ctx, DevicesDataset)
	out := make([]space.Device, 0, len(rows))
	for _, v := range rows {
		out = append(out, DecodeDeviceRecord(v))
	}
	return out
}
