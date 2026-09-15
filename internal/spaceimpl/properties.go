package spaceimpl

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/accountvalues"
	"github.com/anyproto/any-sync-sdk/internal/anyencx"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/types"
	anytype "github.com/anyproto/any-sync-sdk/internal/types/any"
	"github.com/anyproto/any-sync-sdk/space"
)

// propertiesAPI implements space.PropertiesAPI. All three write routes
// are live: synced (object CRDT), account (tech-space carrier +
// mirror), local (LocalSet). See docs/scoped-properties-proposal.md.
type propertiesAPI struct {
	parent *spaceImpl
}

func newPropertiesAPI(parent *spaceImpl) *propertiesAPI { return &propertiesAPI{parent: parent} }

// Get returns the property record for objectId, verbatim. Values of
// every scope sit at their normal `{typeId}.{propId}` paths — there is
// nothing to collapse or strip.
//
// Returns nil with no error if the object has no property record
// yet (never written) or has been tombstoned. Pure any-store read —
// no any-sync tree build, no cold restore.
func (p *propertiesAPI) Get(ctx context.Context, objectId string) (*anyenc.Value, error) {
	coll, err := p.parent.store.SharedObjects(ctx)
	if err != nil {
		return nil, fmt.Errorf("propertiesAPI: shared objects: %w", err)
	}
	doc, err := coll.FindId(ctx, objectId)
	if err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("propertiesAPI: find %s: %w", objectId, err)
	}
	v := doc.Value()
	if v == nil || v.Get(crdt.DeletedAtField) != nil {
		return nil, nil
	}
	// Doc's buffer is reused; clone before returning so callers can
	// retain the value past this call (same contract as
	// Controller.Get).
	return anyencx.Clone(v), nil
}

// Set merges the patch into the object's property record on the route
// the keys' declared scope selects. Patch keys are propIds.
//
// Routing: every key's scope is resolved against the type registry; a
// patch whose RESOLVED keys span more than one scope is rejected up
// front (routes commit independently across different version domains
// — there is no cross-route rollback). Keys that don't resolve
// (unknown property, unknown type) don't pick a route; they fall
// through to the selected route's strict validation, which produces
// the precise agent-readable rejection (unknown_property /
// type_unknown / scope_mismatch).
func (p *propertiesAPI) Set(ctx context.Context, objectId, typeId string, patch map[string]any) (space.ModifyResult, error) {
	if objectId == "" || typeId == "" {
		return space.ModifyResult{}, errors.New("propertiesAPI: objectId and typeId required")
	}
	if len(patch) == 0 {
		return space.ModifyResult{}, errors.New("propertiesAPI: empty patch")
	}

	route, props, err := resolveRoute(p.parent.store.Registry(), typeId, patch)
	if err != nil {
		return space.ModifyResult{}, err
	}

	switch route {
	case schema.ScopeSynced:
		return p.setSynced(ctx, objectId, typeId, patch)
	case schema.ScopeLocal:
		return p.setLocal(ctx, objectId, typeId, patch, props)
	case schema.ScopeAccount:
		return p.setAccount(ctx, objectId, typeId, patch, props)
	}
	return space.ModifyResult{}, fmt.Errorf("propertiesAPI: unroutable scope %s", route)
}

// resolveRoute picks the single write route for a patch: the declared
// scope shared by every key that resolves in the registry. Returns
// ScopeSynced when nothing resolves (nil registry bring-up mode,
// unknown type, or all-unknown keys — the synced route's strict
// PreValidate then owns the precise rejection). A mix of resolved
// scopes is a caller error, reported with the per-scope key split so
// the caller can re-issue one call per scope.
//
// Also returns the type's declared properties (nil when the type is
// unresolvable) so the non-synced routes — which skip the dataset
// handler — can run the same strict validation writer-side.
func resolveRoute(reg types.Registry, typeId string, patch map[string]any) (schema.Scope, []types.PropInfo, error) {
	props, ok := reg.PropsOf(typeId)
	if !ok {
		return schema.ScopeSynced, nil, nil
	}
	byId := make(map[string]types.PropInfo, len(props))
	for _, pi := range props {
		byId[pi.Id] = pi
	}
	byScope := make(map[schema.Scope][]string)
	for key := range patch {
		pi, found := byId[key]
		if !found {
			continue
		}
		sc := pi.EffectiveScope()
		byScope[sc] = append(byScope[sc], key)
	}
	if len(byScope) <= 1 {
		for sc := range byScope {
			return sc, props, nil
		}
		return schema.ScopeSynced, props, nil
	}
	parts := make([]string, 0, len(byScope))
	for sc, keys := range byScope {
		slices.Sort(keys)
		parts = append(parts, fmt.Sprintf("%s: [%s]", sc, strings.Join(keys, ", ")))
	}
	slices.Sort(parts)
	return 0, nil, fmt.Errorf("propertiesAPI: patch spans multiple scopes — issue one Set per scope (%s)", strings.Join(parts, "; "))
}

// patchOps converts a patch into the multi-field $set / $unset op
// pair shared by every route: a nil patch value means "unset this
// property". Either op may be absent when its half is empty.
func patchOps(arena *anyenc.Arena, typeId string, patch map[string]any) ([]crdt.Op, error) {
	sets := arena.NewObject()
	unsets := arena.NewObject()
	nSet, nUnset := 0, 0
	for propId, val := range patch {
		if val == nil {
			unsets.Set(typeId+"."+propId, arena.NewTrue()) // value ignored by $unset
			nUnset++
			continue
		}
		v, err := goToAnyenc(arena, val)
		if err != nil {
			return nil, fmt.Errorf("propertiesAPI: convert %s.%s: %w", typeId, propId, err)
		}
		sets.Set(typeId+"."+propId, v)
		nSet++
	}
	var ops []crdt.Op
	if nSet > 0 {
		ops = append(ops, crdt.Op{Type: crdt.OpSet, Payload: sets})
	}
	if nUnset > 0 {
		ops = append(ops, crdt.Op{Type: crdt.OpUnset, Payload: unsets})
	}
	return ops, nil
}

// setSynced is the synced route: one change on the object's own CRDT.
// Patch keys are propIds; the resulting paths are `{typeId}.{propId}`;
// nil values unset.
func (p *propertiesAPI) setSynced(ctx context.Context, objectId, typeId string, patch map[string]any) (space.ModifyResult, error) {
	arena := &anyenc.Arena{}
	ops, err := patchOps(arena, typeId, patch)
	if err != nil {
		return space.ModifyResult{}, err
	}

	// DataVersion: the latest shortId for this typeId. Empty when the
	// type has had no important changes (e.g. type with no
	// properties yet) — the gate skips empty DataVersion writes.
	dataVersion, err := dataVersionForType(ctx, p.parent.store.Registry(), typeId)
	if err != nil {
		return space.ModifyResult{}, err
	}
	obj, err := p.parent.store.Get(ctx, objectId)
	if err != nil {
		return space.ModifyResult{}, err
	}
	res, err := obj.LocalWrite(ctx, crdt.Change{
		Dataset:     properties.Dataset,
		DataVersion: dataVersion,
		Records: []crdt.RecordChange{{
			Id:     objectId,
			Upsert: true,
			Ops:    ops,
		}},
	})
	if err != nil {
		return space.ModifyResult{}, wrapSlotErr(err)
	}
	return modifyResultFromWrite(res), nil
}

// setLocal is the local (device-only) route: a strict-mode LocalSet on
// the object's row — no DAG, no sync, versions minted by the local
// lexid allocator. The dataset handler doesn't run on local writes, so
// the strict validation the synced route gets from PreValidate happens
// here, writer-side (validateRoutedPatch). nil values unset.
//
// Strict (non-upsert): the row is born by the object's bootstrap
// create, so a missing row means the caller has the wrong object id —
// better a clear error than a local-domain creation marker.
func (p *propertiesAPI) setLocal(ctx context.Context, objectId, typeId string, patch map[string]any, props []types.PropInfo) (space.ModifyResult, error) {
	byId := make(map[string]types.PropInfo, len(props))
	for _, pi := range props {
		byId[pi.Id] = pi
	}
	if err := validateRoutedPatch(typeId, patch, props, schema.ScopeLocal); err != nil {
		return space.ModifyResult{}, err
	}
	arena := &anyenc.Arena{}
	ops, err := patchOps(arena, typeId, patch)
	if err != nil {
		return space.ModifyResult{}, err
	}

	obj, err := p.parent.store.Get(ctx, objectId)
	if err != nil {
		return space.ModifyResult{}, err
	}
	res, err := obj.LocalSet(ctx, crdt.Change{
		Dataset:     properties.Dataset,
		DataVersion: properties.HandlerVersion,
		Records: []crdt.RecordChange{{
			Id:  objectId,
			Ops: ops,
		}},
	})
	if err != nil {
		return space.ModifyResult{}, err
	}
	for _, rej := range res.Rejections {
		if errors.Is(rej.Err, crdt.ErrStrictSkipAbsent) {
			return space.ModifyResult{}, fmt.Errorf("propertiesAPI: object %s has no property record (not created in this space?)", objectId)
		}
	}
	return modifyResultFromWrite(res), nil
}

// setAccount is the account route: the patch is written to the
// tech-space carrier (synced across this account's devices only) and
// then mirrored into the local row inline so read-your-writes holds.
// The returned VersionId is the carrier tree's — the version the
// mirror stamps onto target rows everywhere.
//
// A LOCAL call addressing an object whose row doesn't exist on this
// device errors, symmetric with the local route — remote-originated
// carrier values for not-yet-synced objects wait in the carrier (the
// replay log) instead.
func (p *propertiesAPI) setAccount(ctx context.Context, objectId, typeId string, patch map[string]any, props []types.PropInfo) (space.ModifyResult, error) {
	if err := validateRoutedPatch(typeId, patch, props, schema.ScopeAccount); err != nil {
		return space.ModifyResult{}, err
	}
	// The row must exist locally — a caller writing account values for
	// an object this device doesn't hold is a caller bug, not a sync
	// state.
	row, err := p.Get(ctx, objectId)
	if err != nil {
		return space.ModifyResult{}, err
	}
	if row == nil {
		return space.ModifyResult{}, fmt.Errorf("propertiesAPI: object %s has no property record (not created in this space?)", objectId)
	}

	arena := &anyenc.Arena{}
	ops, err := patchOps(arena, typeId, patch)
	if err != nil {
		return space.ModifyResult{}, err
	}
	res, err := p.parent.tsp.WriteAccountValues(ctx, p.parent.id, crdt.RecordChange{
		Id:     accountvalues.Key(objectId, properties.Dataset, objectId),
		Upsert: true,
		Ops:    ops,
	})
	if err != nil {
		return space.ModifyResult{}, fmt.Errorf("propertiesAPI: account write: %w", err)
	}

	// Inline mirror: replay this object's carrier record into the
	// local row so an immediate Get reflects the write. The mirror
	// watcher's own (event-driven) apply of the same change gates to a
	// no-op.
	if m := p.parent.parent.accountMirror(p.parent.id); m != nil {
		m.reconcileObject(ctx, objectId)
	}

	return space.ModifyResult{
		VersionId: res.VersionId,
		ChangeId:  res.ChangeId,
		RecordIds: []string{objectId},
	}, nil
}

// validateRoutedPatch is the writer-side strict validation for routes
// that skip the dataset handler (local / account): every key must be a
// declared property of the route's scope, and non-nil values must
// match the declared kind (nil = unset, no kind to check).
func validateRoutedPatch(typeId string, patch map[string]any, props []types.PropInfo, route schema.Scope) error {
	byId := make(map[string]types.PropInfo, len(props))
	for _, pi := range props {
		byId[pi.Id] = pi
	}
	arena := &anyenc.Arena{}
	for propId, val := range patch {
		info, found := byId[propId]
		if !found {
			return &properties.ValidationError{
				Reason: properties.ReasonUnknownProperty, TypeId: typeId, PropId: propId, Known: props,
			}
		}
		if sc := info.EffectiveScope(); sc != route {
			return &properties.ValidationError{
				Reason: properties.ReasonScopeMismatch, TypeId: typeId, PropId: propId, PropName: info.Name,
				DeclaredScope: sc, WriteRoute: route,
			}
		}
		if val == nil {
			continue // unset — no kind to check
		}
		v, err := goToAnyenc(arena, val)
		if err != nil {
			return fmt.Errorf("propertiesAPI: convert %s.%s: %w", typeId, propId, err)
		}
		if got := schema.KindOf(v); got != info.Kind {
			return &properties.ValidationError{
				Reason: properties.ReasonKindMismatch, TypeId: typeId, PropId: propId, PropName: info.Name,
				Expected: info.Kind, Got: got,
			}
		}
	}
	return nil
}

// dataVersionForType encodes a single-type DataVersion using the
// registry's latest shortId. Empty registry / empty result yields
// the legacy hardcoded handler version, which the gate treats as
// "no schema constraint" and lets through.
//
// Used by the writer paths (Properties.Set synced route,
// Objects.Create bootstrap) for the per-space `objects` dataset.
func dataVersionForType(ctx context.Context, reg *types.LiveRegistry, typeId string) (string, error) {
	if reg == nil || typeId == "" {
		return properties.HandlerVersion, nil
	}
	shortId, err := reg.LatestShortId(ctx, typeId)
	if err != nil {
		return "", fmt.Errorf("dataVersionForType: %w", err)
	}
	if shortId == "" {
		// No important changes on this type yet; nothing to gate on.
		return properties.HandlerVersion, nil
	}
	return types.EncodeDataVersion([]types.DataVersionPair{{TypeId: typeId, ShortId: shortId}}), nil
}

// membershipWrite issues one membership op on the object's row —
// always the synced route: membership is structural and shared.
func (p *propertiesAPI) membershipWrite(ctx context.Context, objectId string, op crdt.Op) (space.ModifyResult, error) {
	obj, err := p.parent.store.Get(ctx, objectId)
	if err != nil {
		return space.ModifyResult{}, err
	}
	res, err := obj.LocalWrite(ctx, crdt.Change{
		Dataset:     properties.Dataset,
		DataVersion: properties.HandlerVersion,
		Records: []crdt.RecordChange{{
			Id:     objectId,
			Upsert: true,
			Ops:    []crdt.Op{op},
		}},
	})
	if err != nil {
		return space.ModifyResult{}, wrapSlotErr(err)
	}
	return modifyResultFromWrite(res), nil
}

// wrapSlotErr maps the handler's wrong_slot rejection onto the public
// sentinel so callers classify it without reaching into the handler
// package.
func wrapSlotErr(err error) error {
	if errors.Is(err, properties.ErrWrongSlot) {
		return fmt.Errorf("%w: %w", space.ErrWrongSlot, err)
	}
	return err
}

// SetType replaces the object's one type (`any.type`, a $set). The
// previous type's values and dataset records become orphan data,
// read-tolerant; its datasets refuse further writes. A known
// collection id is refused by the pre-flight (ErrWrongSlot). A pure
// membership write carries no values, so it stamps no schema
// constraint: a peer behind on the type's definitions still applies
// it.
func (p *propertiesAPI) SetType(ctx context.Context, objectId, typeId string) (space.ModifyResult, error) {
	if objectId == "" || typeId == "" {
		return space.ModifyResult{}, errors.New("propertiesAPI: objectId and typeId required")
	}
	arena := &anyenc.Arena{}
	return p.membershipWrite(ctx, objectId, crdt.Op{
		Type:    crdt.OpSet,
		Path:    []string{anytype.TypeId, anytype.FieldType},
		Payload: arena.NewString(typeId),
	})
}

// UnsetType clears the object's type ($unset any.type).
func (p *propertiesAPI) UnsetType(ctx context.Context, objectId string) (space.ModifyResult, error) {
	if objectId == "" {
		return space.ModifyResult{}, errors.New("propertiesAPI: objectId required")
	}
	return p.membershipWrite(ctx, objectId, crdt.Op{
		Type: crdt.OpUnset,
		Path: []string{anytype.TypeId, anytype.FieldType},
	})
}

// AttachCollection adds the object to a collection ($addToSet on
// any.collections — idempotent). A known type id is refused by the
// pre-flight (ErrWrongSlot).
func (p *propertiesAPI) AttachCollection(ctx context.Context, objectId, collectionId string) (space.ModifyResult, error) {
	if objectId == "" || collectionId == "" {
		return space.ModifyResult{}, errors.New("propertiesAPI: objectId and collectionId required")
	}
	arena := &anyenc.Arena{}
	return p.membershipWrite(ctx, objectId, crdt.Op{
		Type:    crdt.OpAddToSet,
		Path:    []string{anytype.TypeId, anytype.FieldCollections},
		Payload: arena.NewString(collectionId),
	})
}

// DetachCollection removes the object from a collection ($pull).
// Values in that namespace become orphan data, read-tolerant
// (docs/06-data-structure.md §"read tolerance").
func (p *propertiesAPI) DetachCollection(ctx context.Context, objectId, collectionId string) (space.ModifyResult, error) {
	if objectId == "" || collectionId == "" {
		return space.ModifyResult{}, errors.New("propertiesAPI: objectId and collectionId required")
	}
	arena := &anyenc.Arena{}
	return p.membershipWrite(ctx, objectId, crdt.Op{
		Type:    crdt.OpPull,
		Path:    []string{anytype.TypeId, anytype.FieldCollections},
		Payload: arena.NewString(collectionId),
	})
}
