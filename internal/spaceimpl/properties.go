package spaceimpl

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/anyenc/anyencutil"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/types"
	"github.com/anyproto/any-sync-sdk/space"
)

// propertiesAPI implements space.PropertiesAPI. The synced and local
// routes are live; the account route lands with its tech-space carrier
// (docs/scoped-properties-proposal.md slice 4).
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
	var cloned anyencutil.Value
	cloned.FillCopy(v)
	return cloned.Value, nil
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
		return space.ModifyResult{}, errors.New("propertiesAPI: account-scoped writes not implemented yet (scoped-properties slice 4)")
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
		sort.Strings(keys)
		parts = append(parts, fmt.Sprintf("%s: [%s]", sc, strings.Join(keys, ", ")))
	}
	sort.Strings(parts)
	return 0, nil, fmt.Errorf("propertiesAPI: patch spans multiple scopes — issue one Set per scope (%s)", strings.Join(parts, "; "))
}

// setSynced is the synced route: a multi-field $set in a single change
// on the object's own CRDT. Patch keys are propIds; the resulting
// paths are `{typeId}.{propId}`.
func (p *propertiesAPI) setSynced(ctx context.Context, objectId, typeId string, patch map[string]any) (space.ModifyResult, error) {
	arena := &anyenc.Arena{}
	multi := arena.NewObject()
	for propId, val := range patch {
		v, err := goToAnyenc(arena, val)
		if err != nil {
			return space.ModifyResult{}, fmt.Errorf("propertiesAPI: convert %s.%s: %w", typeId, propId, err)
		}
		multi.Set(typeId+"."+propId, v)
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
			Ops:    []crdt.Op{{Type: crdt.OpSet, Payload: multi}},
		}},
	})
	if err != nil {
		return space.ModifyResult{}, err
	}
	return modifyResultFromWrite(res), nil
}

// setLocal is the local (device-only) route: a strict-mode LocalSet on
// the object's row — no DAG, no sync, versions minted by the local
// lexid allocator. The dataset handler doesn't run on local writes, so
// the strict validation the synced route gets from PreValidate happens
// here, writer-side: every key must be a declared local-scoped
// property and the value's kind must match.
//
// Strict (non-upsert): the row is born by the object's bootstrap
// create, so a missing row means the caller has the wrong object id —
// better a clear error than a local-domain creation marker.
func (p *propertiesAPI) setLocal(ctx context.Context, objectId, typeId string, patch map[string]any, props []types.PropInfo) (space.ModifyResult, error) {
	byId := make(map[string]types.PropInfo, len(props))
	for _, pi := range props {
		byId[pi.Id] = pi
	}
	arena := &anyenc.Arena{}
	multi := arena.NewObject()
	for propId, val := range patch {
		info, found := byId[propId]
		if !found {
			return space.ModifyResult{}, &properties.ValidationError{
				Reason: properties.ReasonUnknownProperty, TypeId: typeId, PropId: propId, Known: props,
			}
		}
		v, err := goToAnyenc(arena, val)
		if err != nil {
			return space.ModifyResult{}, fmt.Errorf("propertiesAPI: convert %s.%s: %w", typeId, propId, err)
		}
		if got := schema.KindOf(v); got != info.Kind {
			return space.ModifyResult{}, &properties.ValidationError{
				Reason: properties.ReasonKindMismatch, TypeId: typeId, PropId: propId, PropName: info.Name,
				Expected: info.Kind, Got: got,
			}
		}
		multi.Set(typeId+"."+propId, v)
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
			Ops: []crdt.Op{{Type: crdt.OpSet, Payload: multi}},
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

// AttachType adds typeId to the object's any.types list, declaring that
// the object implements the type. Idempotent ($addToSet — re-attaching
// is a no-op). This is the sanctioned way to let an object host a type's
// properties or datasets: the write-time membership checks
// (SystemPropertiesHandler.PreValidate for properties, Modify for
// datasets) require the type to be present here first. Type membership
// is structural and shared, so this always rides the synced route.
func (p *propertiesAPI) AttachType(ctx context.Context, objectId, typeId string) (space.ModifyResult, error) {
	if objectId == "" || typeId == "" {
		return space.ModifyResult{}, errors.New("propertiesAPI: objectId and typeId required")
	}
	obj, err := p.parent.store.Get(ctx, objectId)
	if err != nil {
		return space.ModifyResult{}, err
	}
	arena := &anyenc.Arena{}
	res, err := obj.LocalWrite(ctx, crdt.Change{
		Dataset:     properties.Dataset,
		DataVersion: properties.HandlerVersion,
		Records: []crdt.RecordChange{{
			Id:     objectId,
			Upsert: true,
			Ops: []crdt.Op{{
				Type:    crdt.OpAddToSet,
				Path:    []string{"any", "types"},
				Payload: arena.NewString(typeId),
			}},
		}},
	})
	if err != nil {
		return space.ModifyResult{}, err
	}
	return modifyResultFromWrite(res), nil
}

// DetachType removes typeId from the object's any.types ($pull). Values
// in that namespace and records in the type's datasets become orphan
// data, read-tolerant (docs/06-data-structure.md §"read tolerance").
func (p *propertiesAPI) DetachType(ctx context.Context, objectId, typeId string) (space.ModifyResult, error) {
	if objectId == "" || typeId == "" {
		return space.ModifyResult{}, errors.New("propertiesAPI: objectId and typeId required")
	}
	obj, err := p.parent.store.Get(ctx, objectId)
	if err != nil {
		return space.ModifyResult{}, err
	}
	arena := &anyenc.Arena{}
	res, err := obj.LocalWrite(ctx, crdt.Change{
		Dataset:     properties.Dataset,
		DataVersion: properties.HandlerVersion,
		Records: []crdt.RecordChange{{
			Id:     objectId,
			Upsert: true,
			Ops: []crdt.Op{{
				Type:    crdt.OpPull,
				Path:    []string{"any", "types"},
				Payload: arena.NewString(typeId),
			}},
		}},
	})
	if err != nil {
		return space.ModifyResult{}, err
	}
	return modifyResultFromWrite(res), nil
}
