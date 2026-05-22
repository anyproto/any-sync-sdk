package spaceimpl

import (
	"context"
	"errors"
	"fmt"

	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/internal/types"
	"github.com/anyproto/any-sync-sdk/space"
)

// uniqueTypes returns the deduplicated typeIds touched by a Create
// bootstrap — opts.Types ∪ keys(opts.InitialProperties). Order is
// stable (Types first, then InitialProperties keys in iteration
// order).
func uniqueTypes(opts space.CreateObjectOpts) []string {
	seen := make(map[string]struct{}, len(opts.Types)+len(opts.InitialProperties))
	out := make([]string, 0, len(opts.Types)+len(opts.InitialProperties))
	for _, t := range opts.Types {
		if _, dup := seen[t]; dup {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	for t := range opts.InitialProperties {
		if _, dup := seen[t]; dup {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	return out
}

// dataVersionForTypes encodes a multi-type DataVersion using the
// registry's latest shortId per type. Skips types with no important
// changes — their pair is omitted (an absent type contributes
// nothing to the gate).
//
// Empty result (no types known to the registry) falls back to the
// hardcoded handler version, which the gate treats as "no schema
// constraint".
func dataVersionForTypes(ctx context.Context, reg *types.LiveRegistry, typeIds []string) (string, error) {
	if reg == nil || len(typeIds) == 0 {
		return properties.HandlerVersion, nil
	}
	pairs := make([]types.DataVersionPair, 0, len(typeIds))
	for _, t := range typeIds {
		shortId, err := reg.LatestShortId(ctx, t)
		if err != nil {
			return "", fmt.Errorf("dataVersionForTypes[%s]: %w", t, err)
		}
		if shortId == "" {
			continue
		}
		pairs = append(pairs, types.DataVersionPair{TypeId: t, ShortId: shortId})
	}
	encoded := types.EncodeDataVersion(pairs)
	if encoded == "" {
		return properties.HandlerVersion, nil
	}
	return encoded, nil
}

// objectService implements space.ObjectService backed by the
// per-space spaceobjects.Store.
type objectService struct {
	parent *spaceImpl
}

func newObjectService(parent *spaceImpl) *objectService { return &objectService{parent: parent} }

// Create makes a fresh object on the space, runs the optional
// bootstrap modify (any.types + InitialProperties) in one extra
// change, and returns the new objectId.
func (o *objectService) Create(ctx context.Context, opts space.CreateObjectOpts) (string, error) {
	obj, err := o.parent.store.Create(ctx, spaceobjects.CreateOpts{
		ChangeType: "object",
	})
	if err != nil {
		return "", err
	}
	objectId := obj.Id()

	if needsBootstrap(opts) {
		if _, err := o.bootstrap(ctx, objectId, opts); err != nil {
			return "", err
		}
	}
	return objectId, nil
}

// Derive creates a deterministic object from a seed. Idempotent — a
// second call with the same seed returns the same id.
func (o *objectService) Derive(ctx context.Context, opts space.DeriveObjectOpts) (string, error) {
	obj, err := o.parent.store.Derive(ctx, spaceobjects.DeriveOpts{
		ChangeType:    "object",
		ChangePayload: opts.Seed,
	})
	if err != nil {
		return "", err
	}
	objectId := obj.Id()

	// Type-binding is best-effort on Derive: if the index record
	// doesn't exist yet we attempt to seed it. Subsequent calls (for
	// the same id) skip — handler-level upsert semantics handle the
	// idempotent case.
	if len(opts.Types) > 0 {
		if _, err := o.bootstrap(ctx, objectId, space.CreateObjectOpts{Types: opts.Types}); err != nil {
			return "", err
		}
	}
	return objectId, nil
}

// Delete tombstones the object, then marks it deleted via the space's
// settings tree and drops our cached state.
//
// The CRDT `delete` op below runs first, while the tree is still
// live: the apply pipeline tombstones the object's `objects` record
// (Query's tombstone filter then hides it) and fires the Deleted
// subscription event via afterApply. The any-sync settings-tree write
// then fans the deletion out to peers; the local any-store tombstone
// row stays until a future cleanup pass — Query skips it, and ids are
// content-addressable so reuse can't happen.
func (o *objectService) Delete(ctx context.Context, objectId string) error {
	if objectId == "" {
		return errors.New("spaceimpl: Objects.Delete requires objectId")
	}
	obj, err := o.parent.store.Get(ctx, objectId)
	if err != nil {
		return fmt.Errorf("spaceimpl: get object %s: %w", objectId, err)
	}
	if _, err := obj.LocalWrite(ctx, crdt.Change{
		Dataset:     properties.Dataset,
		DataVersion: properties.HandlerVersion,
		Records: []crdt.RecordChange{
			{Id: objectId, Ops: []crdt.Op{{Type: crdt.OpDelete}}},
		},
	}); err != nil {
		return fmt.Errorf("spaceimpl: tombstone %s: %w", objectId, err)
	}

	handle, err := o.parent.app.GetSpace(ctx, o.parent.id)
	if err != nil {
		return fmt.Errorf("spaceimpl: get space: %w", err)
	}
	if err := handle.Inner().DeleteTree(ctx, objectId); err != nil {
		return fmt.Errorf("spaceimpl: DeleteTree %s: %w", objectId, err)
	}
	o.parent.store.Drop(objectId)
	return nil
}

// needsBootstrap returns true when CreateObjectOpts carries any
// caller-supplied initial state.
func needsBootstrap(opts space.CreateObjectOpts) bool {
	if len(opts.Types) > 0 {
		return true
	}
	for _, props := range opts.InitialProperties {
		if len(props) > 0 {
			return true
		}
	}
	return false
}

// bootstrap writes the post-create initial state (any.types +
// per-type property values) into the object's `properties` dataset.
// One Modify call regardless of how many types / properties are
// being seeded — keeps the DAG clean.
func (o *objectService) bootstrap(ctx context.Context, objectId string, opts space.CreateObjectOpts) (space.ModifyResult, error) {
	arena := &anyenc.Arena{}
	multi := arena.NewObject()

	if len(opts.Types) > 0 {
		arr := arena.NewArray()
		for i, t := range opts.Types {
			arr.SetArrayItem(i, arena.NewString(t))
		}
		multi.Set("any.types", arr)
	}
	for typeId, kv := range opts.InitialProperties {
		for propId, val := range kv {
			v, err := goToAnyenc(arena, val)
			if err != nil {
				return space.ModifyResult{}, fmt.Errorf("InitialProperties[%s.%s]: %w", typeId, propId, err)
			}
			multi.Set(typeId+"."+propId, v)
		}
	}

	// Compute DataVersion across every type touched: opts.Types
	// (binding side-effect) and the keys of opts.InitialProperties
	// (value writes). Each contributes one (typeId, latestShortId)
	// pair if available.
	touched := uniqueTypes(opts)
	dataVersion, err := dataVersionForTypes(ctx, o.parent.store.Registry(), touched)
	if err != nil {
		return space.ModifyResult{}, err
	}
	obj, err := o.parent.store.Get(ctx, objectId)
	if err != nil {
		return space.ModifyResult{}, err
	}
	res, err := obj.LocalWrite(ctx, crdt.Change{
		Dataset:     properties.Dataset,
		DataVersion: dataVersion,
		Records: []crdt.RecordChange{
			{
				Id:     objectId,
				Upsert: true,
				Ops:    []crdt.Op{{Type: crdt.OpSet, Payload: multi}},
			},
		},
	})
	if err != nil {
		return space.ModifyResult{}, err
	}
	return modifyResultFromWrite(res), nil
}
