// The system properties handler — the single crdt.Handler every user
// object registers. See doc.go for the package role.

package properties

import (
	"context"
	"maps"
	"slices"
	"strings"

	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/types"
	anytype "github.com/anyproto/any-sync-sdk/internal/types/any"
)

// Dataset is the name every regular object uses for its base-scope
// property writes on its own CRDT. The handler is registered against
// a SHARED per-space collection (also called "objects") via the
// Controller's shared-collection override — every regular object's
// values land in one row in that collection, keyed by the change's
// ObjectId. Type objects don't register this handler; their own
// metadata (any.name etc.) lives in their per-type-object storage
// behind a different dataset.
//
// See docs/06-data-structure.md § "Storage" — the per-space
// `objects` collection model.
const Dataset = "objects"

// HandlerVersion is the DataVersion string stamped on every change
// this handler emits against an object's `properties` dataset. Bump
// the suffix when validation rules change in a way that must reject
// stale writers (see docs/types-properties-proposal.md § "Change-
// level DataVersion" — data-dataset version is a hardcoded handler
// identifier in Phase 1).
const HandlerVersion = "systemPropertyHandler-v1"

// SystemPropertiesHandler validates property writes on every user
// object. Corresponds to the `baseProperty` handler named in
// docs/06-data-structure.md § "Handlers" — renamed to emphasize its
// scope (the per-space `properties` system dataset) and to
// distinguish it from typetype.PropertyHandler (which governs
// property definitions on type objects).
//
// This handler serves the SYNCED route only — it runs on DAG-borne
// changes (local writes via LocalWrite included). Account-scoped
// values arrive via the tech-space mirror's injected applies and
// local-scoped values via Object.LocalSet, neither of which invokes
// dataset handlers; their validation is writer-side. The scope check
// in validateField is what keeps the three routes path-disjoint (see
// docs/scoped-properties-proposal.md).
//
// Two validation paths, split along the local/inbound line:
//
//   - PreValidate (local writes, before the DAG): STRICT. Path syntax,
//     any.types membership, type resolvability, unknown property, and
//     kind. The first violation rejects the WHOLE write with an
//     agent-readable *ValidationError.
//   - BeforeCreate / BeforeModify (inbound + replay, schema known):
//     DEFENSIVE per-op drop. Same checks minus the any.types membership
//     guard (which would break out-of-order tolerance); a failing op is
//     dropped, the rest of the record still applies.
//
// Path shape for a single-path op: `[typeId, propId]`. Multi-field
// $set with empty Path expects an object payload whose keys are dotted
// "{typeId}.{propId}" pairs; same rules per key; if any key fails the
// whole op drops (payloads aren't mutated to filter individual keys in
// v1). $unset / $delete touch no value and are always valid.
type SystemPropertiesHandler struct {
	// Registry resolves declared kinds. May be nil — when nil the
	// handler skips kind validation entirely (passes everything),
	// which is the bring-up mode before the type system is wired.
	Registry types.Registry
}

// New constructs a SystemPropertiesHandler bound to a Registry. Pass
// nil to disable kind validation (early bring-up).
func New(reg types.Registry) *SystemPropertiesHandler {
	return &SystemPropertiesHandler{Registry: reg}
}

// Compile-time checks: the modifier derives the author/createdAt
// creation stamps through CreateStamper and the modifiedAt touch stamp
// through TouchStamper.
var (
	_ crdt.CreateStamper = (*SystemPropertiesHandler)(nil)
	_ crdt.TouchStamper  = (*SystemPropertiesHandler)(nil)
)

func (*SystemPropertiesHandler) Init(_ context.Context) error { return nil }

// BeforeCreate validates every op in the creation payload, then
// auto-stamps `spaceId` via sink.Derive (stampAutoFields). The other
// auto fields are derived by the MODIFIER through optional interfaces:
// `author` / `createdAt` via CreateStamper at the `_ver.id` marker
// sites (DeriveCreateStamps) and `modifiedAt` via TouchStamper on
// every synced record-change (DeriveTouchStamps). All four are derived
// from the change envelope / apply context, never from caller input —
// they are ScopeDerived in the `any` type, read-only by contract
// (validateField rejects input ops on them via the scope check).
//
// Validation is per-op drop, same as BeforeModify: ops that fail the
// current schema are filtered out and the rest of the record still
// lands (spec §"Validation atomicity" — drop per op, skip the record
// only if every op drops). Local creates never hit this with bad ops
// (PreValidate rejected the whole write upstream); inbound creates
// that reach here have passed the DataVersion gate, so a dropped op
// means a removed-property replay or cross-peer bug, not a sync gap.
// No any.types membership check (out-of-order tolerance).
func (h *SystemPropertiesHandler) BeforeCreate(ctx *crdt.ChangeCtx, rec *crdt.RecordChange, sink *crdt.Sink) error {
	if h.Registry != nil && len(rec.Ops) > 0 {
		kept := rec.Ops[:0]
		for i := range rec.Ops {
			if verr := h.validateOp(&rec.Ops[i], nil); verr != nil {
				continue // per-op drop
			}
			kept = append(kept, rec.Ops[i])
		}
		rec.Ops = kept
	}
	stampAutoFields(ctx, sink)
	return nil
}

// stampAutoFields queues the create-path derived op for `spaceId`, the
// one row-root auto field still stamped from BeforeCreate. It is
// UNCONDITIONAL — emitted even when every op in the creating change was
// dropped by validation: the record row is minted either way, its value
// is the same constant from every change, and every peer mints the row
// through SOME create path, so presence converges. The other auto
// fields moved to modifier-invoked interfaces: `author` / `createdAt`
// to CreateStamper (marker sites, DeriveCreateStamps) and `modifiedAt`
// to TouchStamper (every synced record-change, DeriveTouchStamps).
func stampAutoFields(ctx *crdt.ChangeCtx, sink *crdt.Sink) {
	if ctx == nil || ctx.Change == nil || sink == nil {
		return
	}
	if spaceId := ctx.Change.SpaceId; spaceId != "" {
		a := &anyenc.Arena{}
		sink.Derive(crdt.Op{
			Type:    crdt.OpSet,
			Path:    []string{"spaceId"},
			Payload: a.NewString(spaceId),
		})
	}
}

// DeriveCreateStamps queues the `author` / `createdAt` creation stamps
// as $setCreate offers — the min-version-wins register that mirrors
// the `_ver.id` creation marker (see crdt.OpSetCreate).
//
// Implements crdt.CreateStamper: the MODIFIER invokes this at exactly
// the sites that touch the marker — record creation and every upsert
// modify on a live record — independent of per-op validation
// verdicts. Riding the accept path instead would diverge: the
// causally-earliest upsert can be all-rejected on the modify path of
// one peer while it stamped on the peer where it created the record,
// moving the marker without an offer. Tied to the marker, the stamps
// are exactly as convergent as `_ver.id` itself. The marker-site
// re-offers are also what make the fallback convergent at all (each
// peer's create-path stamp alone is a different one-shot per peer)
// and what backfill rows minted before these stamps existed.
//
// Sources: `author` and `createdAt` are taken from the tree's ROOT
// change (immutable header) when it carries them — constants per
// object, so every offer is identical and the first simply sticks.
// Derived trees carry a deterministic, timestamp-less, identity-less
// root (every device must derive byte-identical bytes), so
// ObjectCreatedAt is 0 and ObjectAuthor is "" there — fall back to the
// offering change's own envelope (Timestamp / Creator). Fallback
// offers differ per change, which is exactly what the min-rule
// resolves: every peer converges on the causally-earliest upsert's
// envelope, no matter which change first-touched the record locally.
//
// The fallback value is the author's unvalidated wall clock / signing
// identity, skew included — display/sort quality only, never a fencing
// token, same contract as modifiedAt and version-history timestamps.
func (h *SystemPropertiesHandler) DeriveCreateStamps(ctx *crdt.ChangeCtx, sink *crdt.Sink) {
	if ctx == nil || ctx.Change == nil || sink == nil {
		return
	}
	a := &anyenc.Arena{}
	author := ctx.Change.ObjectAuthor
	if author == "" {
		author = ctx.Change.Creator
	}
	if author != "" {
		sink.DeriveOnce(crdt.Op{
			Type:    crdt.OpSetCreate,
			Path:    []string{"author"},
			Payload: a.NewString(author),
		})
	}
	ts := ctx.Change.ObjectCreatedAt
	if ts <= 0 {
		ts = ctx.Change.Timestamp
	}
	if ts > 0 {
		sink.DeriveOnce(crdt.Op{
			Type: crdt.OpSetCreate,
			Path: []string{"createdAt"},
			// Float64, not NewNumberInt(int(ts)) — same wire encoding
			// (anyenc numbers are float64), but int() would truncate the
			// int64 timestamp on 32-bit platforms.
			Payload: a.NewNumberFloat64(float64(ts)),
		})
	}
}

// DeriveTouchStamps queues the derived row-root `modifiedAt` stamp —
// the Unix-seconds timestamp of the change being applied (the
// per-change envelope Timestamp). The max-flavored counterpart of the
// createdAt creation stamp: modifiedAt tracks the ordering-MAX change
// under standard LWW, createdAt the ordering-MIN under $setCreate.
//
// Implements crdt.TouchStamper: the MODIFIER invokes this for EVERY
// record-change of the synced route — create and modify, upsert or
// strict, whether or not any op survives validation or even reaches a
// handler hook. modifiedAt reads as "newest change that touched the
// row", valid or not, and that unconditionality is load-bearing twice
// over: the default `-modifiedAt` object sort rides a DENSE index (see
// the ensure-index comment in spaceobjects), so every minted row must
// carry the stamp; and the bump must be a function of the change
// alone, or peers that see the same change as a create on one side and
// an all-rejected (or fully filtered) modify on the other split on the
// stamp while agreeing on everything else.
//
// The value is the author's wall clock (display/sort quality only,
// never a fencing token), same contract as version-history timestamps.
func (h *SystemPropertiesHandler) DeriveTouchStamps(ctx *crdt.ChangeCtx, sink *crdt.Sink) {
	if ctx == nil || ctx.Change == nil || sink == nil {
		return
	}
	ts := ctx.Change.Timestamp
	if ts <= 0 {
		return
	}
	a := &anyenc.Arena{}
	sink.DeriveOnce(crdt.Op{
		Type: crdt.OpSet,
		Path: []string{"modifiedAt"},
		// Float64 for the same 32-bit-truncation reason as createdAt.
		Payload: a.NewNumberFloat64(float64(ts)),
	})
}

// BeforeModify validates one inbound op against the current schema.
// Drops the op silently (records a Rejection) on any validation
// failure; other ops in the same RecordChange still apply. This is
// the spec's defensive per-op layer (outcome 2): it only ever sees
// changes whose DataVersion is known (the gate parks not-synced ones),
// so a drop here means a removed-property replay, a cross-peer bug, or
// genuine junk — never data merely waiting on a schema sync.
//
// No any.types membership check here: an apply-time membership guard
// would drop values written before the attach-type change arrives,
// breaking out-of-order tolerance (docs/06-data-structure.md §397).
//
// No stamps are emitted here at all any more: `modifiedAt` is derived
// by the MODIFIER via TouchStamper on every record-change of the
// synced route (see DeriveTouchStamps — the bump must be a function of
// the change alone, not of per-op verdicts or of whether any op even
// reached this hook), and the creation stamps (`author` / `createdAt`)
// via CreateStamper at the `_ver.id` marker sites (DeriveCreateStamps).
func (h *SystemPropertiesHandler) BeforeModify(_ *crdt.ChangeCtx, _ *crdt.RecordChange, op *crdt.Op, _ *crdt.Sink) error {
	if h.Registry != nil {
		if verr := h.validateOp(op, nil); verr != nil {
			return verr
		}
	}
	return nil
}

// BeforeDelete is a no-op — deleting a property record (the per-
// object property store entry) is allowed; no Registry lookup
// applies to the record-as-a-whole.
func (*SystemPropertiesHandler) BeforeDelete(_ *crdt.ChangeCtx, _ *crdt.RecordChange, _ *crdt.Sink) error {
	return nil
}

// PreValidate is the local write-time pre-flight (LocalPreValidator).
// It runs strict, full validation BEFORE the change enters the DAG:
// path syntax, any.types membership, type resolvability, unknown
// property, and kind. The FIRST violation rejects the WHOLE write with
// an agent-readable *ValidationError — nothing is signed or shipped to
// peers. `before` is the record's current value (nil on first write).
//
// Stricter than BeforeModify by design: this catches programmer/agent
// mistakes locally, where rejecting is the right answer; inbound stays
// tolerant (gate parks not-synced; BeforeModify drops residual
// mismatches).
func (h *SystemPropertiesHandler) PreValidate(ch *crdt.Change, before *anyenc.Value) error {
	if h.Registry == nil || ch == nil {
		return nil
	}
	pf := h.buildPreflight(ch, before)
	for ri := range ch.Records {
		rc := &ch.Records[ri]
		for oi := range rc.Ops {
			if verr := h.validateOp(&rc.Ops[oi], pf); verr != nil {
				return verr
			}
		}
	}
	return nil
}

// preflight carries the local-write membership set (the typeIds the
// object effectively implements) used only by PreValidate. nil means
// apply-time mode — skip the membership check.
type preflight struct {
	members map[string]struct{}
	list    []string // sorted, for error messages
}

// buildPreflight computes the set of typeIds the object implements
// after this change: the universal `any` type, the types already in
// the record's any.types, plus any types this change attaches.
func (h *SystemPropertiesHandler) buildPreflight(ch *crdt.Change, before *anyenc.Value) *preflight {
	members := map[string]struct{}{anytype.TypeId: {}}
	if before != nil {
		for _, v := range before.GetArray("any", "types") {
			members[string(v.GetStringBytes())] = struct{}{}
		}
	}
	for ri := range ch.Records {
		for oi := range ch.Records[ri].Ops {
			collectTypeAdditions(&ch.Records[ri].Ops[oi], members)
		}
	}
	list := slices.Sorted(maps.Keys(members))
	return &preflight{members: members, list: list}
}

// collectTypeAdditions records typeIds an op adds to any.types, so a
// batch that attaches a type and writes its values in one change
// validates the new namespace. Handles the multi-field "any.types"
// key, the single-path ["any","types"] $set (array payload), and the
// ["any","types"] $addToSet (element payload).
func collectTypeAdditions(op *crdt.Op, members map[string]struct{}) {
	switch {
	case len(op.Path) == 0 && op.Type == crdt.OpSet:
		if op.Payload == nil || op.Payload.Type() != anyenc.TypeObject {
			return
		}
		obj, _ := op.Payload.Object()
		obj.Visit(func(k []byte, v *anyenc.Value) {
			if string(k) == anytype.TypeId+".types" {
				addArrayStrings(v, members)
			}
		})
	case len(op.Path) == 2 && op.Path[0] == anytype.TypeId && op.Path[1] == "types":
		switch op.Type {
		case crdt.OpSet:
			addArrayStrings(op.Payload, members)
		case crdt.OpAddToSet:
			if op.Payload != nil && op.Payload.Type() == anyenc.TypeString {
				members[string(op.Payload.GetStringBytes())] = struct{}{}
			}
		}
	}
}

// addArrayStrings unions the string elements of an array value into
// the set. No-op for non-array values.
func addArrayStrings(v *anyenc.Value, members map[string]struct{}) {
	if v == nil || v.Type() != anyenc.TypeArray {
		return
	}
	arr, _ := v.Array()
	for _, e := range arr {
		if e.Type() == anyenc.TypeString {
			members[string(e.GetStringBytes())] = struct{}{}
		}
	}
}

// validateOp routes by op shape. pf != nil enables the local pre-flight
// membership check; nil is apply-time mode. $unset / $delete touch no
// value and are always valid (no `required` keyword in v1).
func (h *SystemPropertiesHandler) validateOp(op *crdt.Op, pf *preflight) *ValidationError {
	if op.Type == crdt.OpUnset || op.Type == crdt.OpDelete {
		return nil
	}
	if len(op.Path) == 0 {
		return h.validateMultiField(op, pf)
	}
	return h.validateSinglePath(op.Path, op.Payload, op.Type, pf)
}

func (h *SystemPropertiesHandler) validateSinglePath(path []string, payload *anyenc.Value, opType crdt.OpType, pf *preflight) *ValidationError {
	if len(path) < 2 {
		return &ValidationError{Reason: ReasonInvalidPath, Path: path}
	}
	return h.validateField(path[0], path[1], payload, opType, pf)
}

// validateMultiField walks a multi-field $set payload. Every top-level
// key must be a dotted "typeId.propId" pair (no deeper nesting in v1);
// the first failing key rejects the whole op.
func (h *SystemPropertiesHandler) validateMultiField(op *crdt.Op, pf *preflight) *ValidationError {
	if op.Payload == nil || op.Payload.Type() != anyenc.TypeObject {
		return nil
	}
	obj, _ := op.Payload.Object()
	var firstErr *ValidationError
	obj.Visit(func(k []byte, v *anyenc.Value) {
		if firstErr != nil {
			return
		}
		key := string(k)
		dot := strings.IndexByte(key, '.')
		if dot <= 0 || dot == len(key)-1 || strings.IndexByte(key[dot+1:], '.') >= 0 {
			firstErr = &ValidationError{Reason: ReasonInvalidPath, Path: []string{key}}
			return
		}
		firstErr = h.validateField(key[:dot], key[dot+1:], v, op.Type, pf)
	})
	return firstErr
}

// validateField is the shared per-(typeId, propId) check. Order:
// membership (pre-flight only) → type resolvable → property declared →
// scope → kind. Returns the first violation as a *ValidationError, or
// nil.
//
// The scope check is the convergent guard that keeps version domains
// path-disjoint: this handler only ever runs on the synced (DAG)
// route, so any op targeting a property whose declared scope is not
// ScopeSynced is dropped — account values arrive via the tech-space
// mirror's injected applies and local values via LocalSet, neither of
// which runs this handler. A malicious or buggy peer addressing an
// account/local propId through the DAG is rejected identically on
// every peer (the DataVersion gate parks changes whose schema hasn't
// synced, so the registry lookup is never racing the definition).
func (h *SystemPropertiesHandler) validateField(typeId, propId string, payload *anyenc.Value, opType crdt.OpType, pf *preflight) *ValidationError {
	if pf != nil {
		if _, ok := pf.members[typeId]; !ok {
			return &ValidationError{Reason: ReasonTypeNotImplemented, TypeId: typeId, Types: pf.list}
		}
	}
	props, ok := h.Registry.PropsOf(typeId)
	if !ok {
		return &ValidationError{Reason: ReasonTypeUnknown, TypeId: typeId, PropId: propId}
	}
	var info types.PropInfo
	found := false
	for _, p := range props {
		if p.Id == propId {
			info, found = p, true
			break
		}
	}
	if !found {
		return &ValidationError{Reason: ReasonUnknownProperty, TypeId: typeId, PropId: propId, Known: props}
	}
	if sc := info.EffectiveScope(); sc != schema.ScopeSynced {
		return &ValidationError{
			Reason: ReasonScopeMismatch, TypeId: typeId, PropId: propId, PropName: info.Name,
			DeclaredScope: sc, WriteRoute: schema.ScopeSynced,
		}
	}
	expected, got, ok := kindCheck(opType, info.Kind, payload)
	if !ok {
		return &ValidationError{
			Reason: ReasonKindMismatch, TypeId: typeId, PropId: propId, PropName: info.Name,
			Expected: expected, Got: got,
		}
	}
	return nil
}

// kindCheck applies the op-type-aware kind rule and returns the kinds
// to report on mismatch.
//
//   - $set: the value's kind must equal the declared kind.
//   - $addToSet / $pull: the property must be an array (the element's
//     kind is unchecked in v1 — items schemas aren't in the registry).
//   - $inc / $incGated: the property must be a number.
func kindCheck(opType crdt.OpType, declared schema.Kind, payload *anyenc.Value) (expected, got schema.Kind, ok bool) {
	switch opType {
	case crdt.OpAddToSet, crdt.OpPull:
		return schema.KindArray, declared, declared == schema.KindArray
	case crdt.OpInc, crdt.OpIncGated:
		return schema.KindNumber, declared, declared == schema.KindNumber
	default: // OpSet
		got = schema.KindOf(payload)
		return declared, got, got == declared
	}
}
