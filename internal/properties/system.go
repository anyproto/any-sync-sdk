// The system properties handler — the single crdt.Handler every user
// object registers. See doc.go for the package role.

package properties

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/types"
	anytype "github.com/anyproto/any-sync-sdk/internal/types/any"
	collectiontype "github.com/anyproto/any-sync-sdk/internal/types/collection"
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
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
// The row also carries the object-level `modifiedAt` / `modifiedBy`
// pair: the handler is a crdt.ObjectStamper, so a synced change on
// ANY dataset of the object (editor blocks, chat messages, runtime
// datasets) stamps them, not only writes to the row itself.
//
// See docs/data-structure.md § "Storage" — the per-space
// `objects` collection model.
const Dataset = "objects"

// HandlerVersion is the DataVersion string stamped on every change
// this handler emits when a write carries no schema pairs (see
// docs/types-properties-proposal.md § "Change-level DataVersion").
const HandlerVersion = "systemPropertyHandler-v1"

// LocalVersion is the handler's LOCAL logic version (HandlerReg.Version)
// — bumped when already-materialized rows would come out different, so
// the SDK rebuilds them from the DAG (docs/versioning.md). v2: the
// derived createdAt / modifiedAt stamps are TypeDateTime instants, not
// epoch numbers. v3: modifiedAt is also stamped by changes on the
// object's other datasets (StampObject), so rows stamped by property
// writes alone are stale. v4: modifiedBy is stamped next to
// modifiedAt.
const LocalVersion = 4

// SystemPropertiesHandler validates property writes on every user
// object. Corresponds to the `baseProperty` handler named in
// docs/data-structure.md § "Handlers" — renamed to emphasize its
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
//     membership, type resolvability, unknown property, and
//     kind. The first violation rejects the WHOLE write with an
//     agent-readable *ValidationError.
//   - BeforeCreate / BeforeModify (inbound + replay, schema known):
//     DEFENSIVE per-op drop. Same checks minus the membership
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

	// Grants extends the local-write membership set: given what the
	// object is (its type, its collections, the markers), it returns
	// the extra namespaces the row may hold — the modules its type
	// declares datasets of. Nil grants nothing beyond the members.
	Grants func(members map[string]struct{}) []string

	// Classify reports what a definition id names (a type, a
	// collection, or nothing resolvable here) so the local write
	// pre-flight refuses a known id in the wrong slot: a collection
	// set as `any.type`, a type added to `any.collections`. Unknown
	// ids pass — the definition may not have synced yet; a read
	// failure fails the write closed. Nil classifies nothing.
	Classify func(ctx context.Context, id string) (OwnerKind, error)

	// ReservedCarrier reports whether a user type declares a reserved
	// module (handler.Module.Reserved). Such a type is carried only by
	// the object that is the type itself — the consumer's own install
	// root — so a local write attaching it to any other row is
	// refused. Registered types are never reserved carriers: a static
	// part is the consumer's compiled-in declaration, attachable by
	// design. Nil reserves nothing.
	ReservedCarrier func(typeId string) bool
}

// New constructs a SystemPropertiesHandler bound to a Registry. Pass
// nil to disable kind validation (early bring-up).
func New(reg types.Registry) *SystemPropertiesHandler {
	return &SystemPropertiesHandler{Registry: reg}
}

// NewWithGrants is New plus the namespace-grant resolver (module
// namespaces on the objects row).
func NewWithGrants(reg types.Registry, grants func(members map[string]struct{}) []string) *SystemPropertiesHandler {
	return &SystemPropertiesHandler{Registry: reg, Grants: grants}
}

func (*SystemPropertiesHandler) Init(_ context.Context) error { return nil }

// BeforeCreate validates every op in the creation payload, then
// auto-stamps the `any`-scope auto fields (author, createdAt,
// spaceId, modifiedAt, modifiedBy) via sink.Derive so every newly
// minted row in the per-space `objects` collection carries them. The
// stamps are derived from the change envelope (the signing identities,
// Timestamp from the change wire, SpaceId from the apply context),
// not from caller input — these fields are ScopeDerived in the `any`
// type, read-only by contract (validateField rejects input ops on
// them via the scope check).
//
// Validation is per-op drop, same as BeforeModify: ops that fail the
// current schema are filtered out and the rest of the record still
// lands (spec §"Validation atomicity" — drop per op, skip the record
// only if every op drops). Local creates never hit this with bad ops
// (PreValidate rejected the whole write upstream); inbound creates
// that reach here have passed the DataVersion gate, so a dropped op
// means a removed-property replay or cross-peer bug, not a sync gap.
// No membership check (out-of-order tolerance).
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

// stampAutoFields queues derived ops for the row-root auto fields
// — `author`, `createdAt`, `spaceId` — that live alongside `id` at
// the top of the record (NOT under `any.*`). Stamped once at record
// creation; BeforeCreate fires only on first-touch so they don't
// re-apply. Also seeds the initial `modifiedAt` / `modifiedBy` (per-
// change, keep moving on every modify — see stampModified).
//
// `author` and `createdAt` are taken from the tree's ROOT change
// (immutable header), not from the per-change envelope — those are
// constants per object, regardless of which change happens to land
// first locally. `spaceId` comes from the apply context. Derived
// ops route to row root (see recordModifier.Modify) so these land
// at `record.author` / `record.createdAt` / `record.spaceId`,
// alongside `id`.
//
// All three stamps inherit the change's VersionId, so concurrent
// peer-side BeforeCreate stamps converge under standard LWW.
func stampAutoFields(ctx *crdt.ChangeCtx, sink *crdt.Sink) {
	if ctx == nil || ctx.Change == nil || sink == nil {
		return
	}
	a := &anyenc.Arena{}
	if author := ctx.Change.ObjectAuthor; author != "" {
		sink.Derive(crdt.Op{
			Type:    crdt.OpSet,
			Path:    []string{"author"},
			Payload: a.NewString(author),
		})
	}
	if ts := ctx.Change.ObjectCreatedAt; ts > 0 {
		sink.Derive(crdt.Op{
			Type: crdt.OpSet,
			Path: []string{"createdAt"},
			// A TypeDateTime instant, not an epoch number: the shape
			// any-store orders, indexes and computes dates on. The
			// envelope carries unix SECONDS; TypeDateTime is millis.
			Payload: a.NewDateTimeMillis(ts * 1000),
		})
	}
	if spaceId := ctx.Change.SpaceId; spaceId != "" {
		sink.Derive(crdt.Op{
			Type:    crdt.OpSet,
			Path:    []string{"spaceId"},
			Payload: a.NewString(spaceId),
		})
	}
	stampModified(ctx, sink)
}

// stampModified queues the derived row-root `modifiedAt` /
// `modifiedBy` pair: the Unix-seconds timestamp of the change being
// applied (the per-change envelope Timestamp, NOT the root-header
// ObjectCreatedAt the createdAt stamp uses) and the account identity
// that signed it (the per-change Creator, NOT the root signer the
// author stamp uses). Fired from BeforeCreate (so every row carries
// them from birth, equal to the creating change's time and signer),
// BeforeModify (any synced write to the row) and StampObject (any
// synced write to another dataset of the object), so the pair reads
// as "the object changed, when and by whom", whatever dataset the
// change landed on.
//
// Convergence: both stamps inherit the change's VersionId, so under
// standard LWW every peer resolves them to the ordering-max change
// that touched the object — deterministic once all changes are
// delivered. The pair always moves together: a change whose signer is
// unknown (no Creator — a hand-built change without a tree) unsets
// modifiedBy at the same version rather than leaving an older signer
// next to a newer time, so an absent modifiedBy reads "signer unknown
// for the latest change", never someone else. No Timestamp (same
// origin) stamps nothing. The time is the author's wall clock
// (display/sort quality only, never a fencing token), same contract as
// version-history timestamps.
//
// DeriveOnce, not Derive: BeforeModify runs per op, and a multi-op
// RecordChange must stamp once, not once per op.
func stampModified(ctx *crdt.ChangeCtx, sink *crdt.Sink) {
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
		// TypeDateTime millis, as createdAt (envelope is seconds).
		Payload: a.NewDateTimeMillis(ts * 1000),
	})
	by := crdt.Op{Type: crdt.OpUnset, Path: []string{"modifiedBy"}}
	if creator := ctx.Change.Creator; creator != "" {
		by = crdt.Op{Type: crdt.OpSet, Path: []string{"modifiedBy"}, Payload: a.NewString(creator)}
	}
	sink.DeriveOnce(by)
}

// BeforeModify validates one inbound op against the current schema.
// Drops the op silently (records a Rejection) on any validation
// failure; other ops in the same RecordChange still apply. This is
// the spec's defensive per-op layer (outcome 2): it only ever sees
// changes whose DataVersion is known (the gate parks not-synced ones),
// so a drop here means a removed-property replay, a cross-peer bug, or
// genuine junk — never data merely waiting on a schema sync.
//
// No membership check here: an apply-time membership guard
// would drop values written before the attach-type change arrives,
// breaking out-of-order tolerance (docs/data-structure.md § Membership and the local write pre-flight).
//
// Every op that passes validation stamps the derived `modifiedAt` /
// `modifiedBy` pair (deduped via DeriveOnce — one stamp per
// RecordChange). Stamping after the validation gate means a fully-
// rejected change never bumps them; an op that passes here but later
// loses its per-field LWW race still bumps them, which is
// deterministic (every peer runs the same gates in the same order)
// and reads as "latest valid write attempt".
func (h *SystemPropertiesHandler) BeforeModify(ctx *crdt.ChangeCtx, _ *crdt.RecordChange, op *crdt.Op, sink *crdt.Sink) error {
	if h.Registry != nil {
		if verr := h.validateOp(op, nil); verr != nil {
			return verr
		}
	}
	stampModified(ctx, sink)
	return nil
}

// StampObject bumps the row's `modifiedAt` / `modifiedBy` for a synced
// change on any other dataset of the object (crdt.ObjectStamper). The
// controller applies the stamps to the existing row only — a row that
// does not exist yet gets them from BeforeCreate when it is created.
func (*SystemPropertiesHandler) StampObject(ctx *crdt.ChangeCtx, sink *crdt.Sink) {
	stampModified(ctx, sink)
}

// BeforeDelete is a no-op — deleting a property record (the per-
// object property store entry) is allowed; no Registry lookup
// applies to the record-as-a-whole.
func (*SystemPropertiesHandler) BeforeDelete(_ *crdt.ChangeCtx, _ *crdt.RecordChange, _ *crdt.Sink) error {
	return nil
}

// PreValidate is the local write-time pre-flight (LocalPreValidator).
// It runs strict, full validation BEFORE the change enters the DAG:
// path syntax, membership, type resolvability, unknown
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
	adds := collectMembership(ch)
	if adds.typeSet && adds.typeId == "" {
		// Every object has exactly one type: a $unset, or a $set to
		// the empty string, is refused.
		return &ValidationError{Reason: ReasonTypeRequired, ObjectId: ch.ObjectId}
	}
	if err := h.checkSlots(adds); err != nil {
		return err
	}
	if verr := h.checkReservedCarriers(ch, before, adds); verr != nil {
		return verr
	}
	pf := h.buildPreflight(ch, before, adds)
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

// membershipAdds is what a change does to the object's membership
// fields: the type it sets (typeSet reports a set even to the empty
// string — an unset), and the collections it adds ($set array or
// $addToSet).
type membershipAdds struct {
	typeSet     bool
	typeId      string
	collections map[string]struct{}
}

// collectMembership scans a change for writes to `any.type` and
// `any.collections`: the multi-field $set (dotted keys), the
// single-path $set on either field, $addToSet on the collections
// list, and $unset of the type.
func collectMembership(ch *crdt.Change) membershipAdds {
	var m membershipAdds
	for ri := range ch.Records {
		for oi := range ch.Records[ri].Ops {
			op := &ch.Records[ri].Ops[oi]
			switch {
			case len(op.Path) == 0 && op.Type == crdt.OpSet:
				if op.Payload == nil || op.Payload.Type() != anyenc.TypeObject {
					continue
				}
				obj, _ := op.Payload.Object()
				obj.Visit(func(k []byte, v *anyenc.Value) {
					switch string(k) {
					case anytype.TypeId + "." + anytype.FieldType:
						if v.Type() == anyenc.TypeString {
							m.typeSet, m.typeId = true, string(v.GetStringBytes())
						}
					case anytype.TypeId + "." + anytype.FieldCollections:
						m.addCollections(v)
					}
				})
			case len(op.Path) == 0 && op.Type == crdt.OpUnset:
				if op.Payload == nil || op.Payload.Type() != anyenc.TypeObject {
					continue
				}
				obj, _ := op.Payload.Object()
				obj.Visit(func(k []byte, _ *anyenc.Value) {
					// The whole `any` container going takes the type with it.
					if k := string(k); k == anytype.TypeId+"."+anytype.FieldType || k == anytype.TypeId {
						m.typeSet, m.typeId = true, ""
					}
				})
			case len(op.Path) == 1 && op.Path[0] == anytype.TypeId && op.Type == crdt.OpUnset:
				m.typeSet, m.typeId = true, ""
			case len(op.Path) == 2 && op.Path[0] == anytype.TypeId && op.Path[1] == anytype.FieldType:
				switch op.Type {
				case crdt.OpSet:
					if op.Payload != nil && op.Payload.Type() == anyenc.TypeString {
						m.typeSet, m.typeId = true, string(op.Payload.GetStringBytes())
					}
				case crdt.OpUnset:
					m.typeSet, m.typeId = true, ""
				}
			case len(op.Path) == 2 && op.Path[0] == anytype.TypeId && op.Path[1] == anytype.FieldCollections:
				switch op.Type {
				case crdt.OpSet:
					m.addCollections(op.Payload)
				case crdt.OpAddToSet:
					if op.Payload != nil && op.Payload.Type() == anyenc.TypeString {
						m.add(string(op.Payload.GetStringBytes()))
					}
				}
			}
		}
	}
	return m
}

func (m *membershipAdds) add(id string) {
	if m.collections == nil {
		m.collections = map[string]struct{}{}
	}
	m.collections[id] = struct{}{}
}

// addCollections unions the string elements of an array value. No-op
// for non-array values.
func (m *membershipAdds) addCollections(v *anyenc.Value) {
	if v == nil || v.Type() != anyenc.TypeArray {
		return
	}
	arr, _ := v.Array()
	for _, e := range arr {
		if e.Type() == anyenc.TypeString {
			m.add(string(e.GetStringBytes()))
		}
	}
}

// checkSlots refuses a known id written to the wrong membership field:
// a collection as `any.type`, a type in `any.collections`. The markers
// and the universal type are not classified (they are the slot's own
// vocabulary); an unknown id passes.
func (h *SystemPropertiesHandler) checkSlots(adds membershipAdds) error {
	if h.Classify == nil {
		return nil
	}
	// PreValidate carries no ctx (it runs under the object's tree
	// lock); the classifier's read is one row lookup on a warm store —
	// the first membership write on a cold store pays the objects
	// collection's open, index ensure included.
	ctx := context.Background()
	if adds.typeSet && adds.typeId != "" && !isMarker(adds.typeId) {
		k, err := h.Classify(ctx, adds.typeId)
		if err != nil {
			return fmt.Errorf("properties: classify %s: %w", adds.typeId, err)
		}
		if k == OwnerCollection {
			return &ValidationError{Reason: ReasonWrongSlot, TypeId: adds.typeId, Slot: anytype.FieldType, Kind: k}
		}
	}
	for id := range adds.collections {
		if isMarker(id) {
			return &ValidationError{Reason: ReasonWrongSlot, TypeId: id, Slot: anytype.FieldCollections, Kind: OwnerType}
		}
		k, err := h.Classify(ctx, id)
		if err != nil {
			return fmt.Errorf("properties: classify %s: %w", id, err)
		}
		if k == OwnerType {
			return &ValidationError{Reason: ReasonWrongSlot, TypeId: id, Slot: anytype.FieldCollections, Kind: k}
		}
	}
	return nil
}

// isMarker reports whether id is one of the definition markers or the
// universal type — vocabulary of the slot, never a definition to
// classify.
func isMarker(id string) bool {
	return id == typetype.MetaTypeMarker || id == collectiontype.MetaMarker || id == anytype.TypeId
}

// checkReservedCarriers refuses a local change setting a reserved
// carrier type (ReservedCarrier) as any row's type but the type's own
// — which is never the case: a definition object carries its marker,
// and hosts its own datasets under the implicit self grant. Only the
// type this change SETS is checked — a row already carrying one (an
// inbound copy) keeps writing. The row is the change's ObjectId — on
// the shared objects collection the controller stamps it before the
// pre-flight, and a caller's record id is never the row — so an
// unstamped change fails closed.
func (h *SystemPropertiesHandler) checkReservedCarriers(ch *crdt.Change, before *anyenc.Value, adds membershipAdds) *ValidationError {
	if h.ReservedCarrier == nil || !adds.typeSet || adds.typeId == "" {
		return nil
	}
	if before != nil && before.GetString(anytype.TypeId, anytype.FieldType) == adds.typeId {
		return nil
	}
	if adds.typeId == ch.ObjectId {
		return nil
	}
	if h.ReservedCarrier(adds.typeId) {
		return &ValidationError{Reason: ReasonReservedCarrier, TypeId: adds.typeId, ObjectId: ch.ObjectId}
	}
	return nil
}

// preflight carries the local-write membership set (the namespaces the
// object may hold) used only by PreValidate. nil means apply-time mode
// — skip the membership check.
type preflight struct {
	members map[string]struct{}
	list    []string // sorted, for error messages
}

// buildPreflight computes the namespaces the object holds after this
// change: the universal `any`, its type (the row's, or the one this
// change sets), its collections (the row's plus what this change
// adds), the meta namespace its marker grants, its own id when it is
// a definition (a type or collection object implicitly implements
// itself — docs/data-structure.md § Type and collections), and the module namespaces
// Grants derives from those.
//
// The markers are the one place value and namespace differ — a type
// object carries `__type__` in `any.type` but stores its type-only
// values under `type` (an underscore-prefixed top-level field is
// protocol-owned). The namespace is granted off the marker alone, so
// a row that merely names the meta id as its type cannot reach it.
func (h *SystemPropertiesHandler) buildPreflight(ch *crdt.Change, before *anyenc.Value, adds membershipAdds) *preflight {
	members := map[string]struct{}{anytype.TypeId: {}}
	typeId := ""
	if before != nil {
		typeId = before.GetString(anytype.TypeId, anytype.FieldType)
		for _, v := range before.GetArray(anytype.TypeId, anytype.FieldCollections) {
			members[string(v.GetStringBytes())] = struct{}{}
		}
	}
	if adds.typeSet {
		typeId = adds.typeId
	}
	for id := range adds.collections {
		members[id] = struct{}{}
	}
	if typeId != "" {
		members[typeId] = struct{}{}
	}
	delete(members, typetype.TypeId)
	delete(members, collectiontype.TypeId)
	switch typeId {
	case typetype.MetaTypeMarker:
		members[typetype.TypeId] = struct{}{}
		members[ch.ObjectId] = struct{}{}
	case collectiontype.MetaMarker:
		members[collectiontype.TypeId] = struct{}{}
		members[ch.ObjectId] = struct{}{}
	}
	delete(members, "")
	// Module namespaces: granted off the type the object has (or is),
	// never off its collections, and never listed in a membership
	// field themselves.
	if h.Grants != nil {
		granting := map[string]struct{}{}
		if typeId != "" {
			granting[typeId] = struct{}{}
		}
		if _, self := members[ch.ObjectId]; self && ch.ObjectId != "" {
			granting[ch.ObjectId] = struct{}{}
		}
		for _, ns := range h.Grants(granting) {
			members[ns] = struct{}{}
		}
	}
	list := slices.Sorted(maps.Keys(members))
	return &preflight{members: members, list: list}
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
	return h.validateFieldPath(path[0], path[1], len(path) > 2, payload, opType, pf)
}

// validateMultiField walks a multi-field $set payload. Every top-level
// key is a dotted "typeId.propId" pair, or "typeId.propId.sub…" for a
// leaf under an object-kind property; the first failing key rejects
// the whole op.
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
		if dot <= 0 || dot == len(key)-1 {
			firstErr = &ValidationError{Reason: ReasonInvalidPath, Path: []string{key}}
			return
		}
		typeId, rest := key[:dot], key[dot+1:]
		propId, nested := rest, false
		if sub := strings.IndexByte(rest, '.'); sub >= 0 {
			if sub == 0 || sub == len(rest)-1 {
				firstErr = &ValidationError{Reason: ReasonInvalidPath, Path: []string{key}}
				return
			}
			propId, nested = rest[:sub], true
		}
		firstErr = h.validateFieldPath(typeId, propId, nested, v, op.Type, pf)
	})
	return firstErr
}

// validateFieldPath is validateField for a path that may descend below
// the property: a nested write is admitted only under an object-kind
// property (the value shape below it is the writer's — a free-form
// object such as the meta-type's `meta` bag), and every op type on it
// must be a $set (the container ops address the property itself).
func (h *SystemPropertiesHandler) validateFieldPath(typeId, propId string, nested bool, payload *anyenc.Value, opType crdt.OpType, pf *preflight) *ValidationError {
	if !nested {
		return h.validateField(typeId, propId, payload, opType, pf)
	}
	if opType != crdt.OpSet {
		return &ValidationError{Reason: ReasonInvalidPath, Path: []string{typeId, propId}}
	}
	// Resolve and scope-check the property as a whole, then require an
	// object kind instead of checking the leaf's kind: a kindCheck
	// against a synthetic object payload would pass exactly when the
	// declared kind is object.
	if verr := h.validateField(typeId, propId, nestedProbe, crdt.OpSet, pf); verr != nil {
		return verr
	}
	return nil
}

// nestedProbe is the payload validateFieldPath checks a nested write's
// property with: an empty object, so the kind rule reads "declared
// kind must be object".
var nestedProbe = func() *anyenc.Value {
	a := &anyenc.Arena{}
	return a.NewObject()
}()

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
			return &ValidationError{Reason: ReasonTypeNotImplemented, TypeId: typeId, Members: pf.list}
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
