package spaceimpl

import (
	"context"
	"errors"
	"fmt"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/anyencx"
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

// objectChangeType is the tree change-type every user-space object is
// created and derived with. It is part of a derived object's id, so
// every deriver of the same object must pass the same value.
const objectChangeType = "object"

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
		ChangeType: objectChangeType,
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

// Get reads the object's row from the shared objects collection. An
// absent or tombstoned row alone is ambiguous (deletion hard-removes
// the row), so the tree's deleted status decides between ErrNotFound
// and ErrObjectDeleted.
func (o *objectService) Get(ctx context.Context, objectId string) (*anyenc.Value, error) {
	if objectId == "" {
		return nil, errors.New("spaceimpl: Objects().Get: empty object id")
	}
	coll, err := o.parent.store.SharedObjects(ctx)
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: shared objects: %w", err)
	}
	var row *anyenc.Value
	doc, err := coll.FindId(ctx, objectId)
	switch {
	case err == nil:
		row = doc.Value()
	case errors.Is(err, anystore.ErrDocNotFound):
	default:
		return nil, fmt.Errorf("spaceimpl: find object %s: %w", objectId, err)
	}
	if live := liveObjectRow(row); live {
		// Doc's buffer is reused; clone before returning.
		return anyencx.Clone(row), nil
	}
	deleted, err := o.parent.store.TreeDeleted(ctx, objectId)
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: object %s: deleted status: %w", objectId, err)
	}
	if deleted {
		return nil, fmt.Errorf("spaceimpl: object %s: %w", objectId, space.ErrObjectDeleted)
	}
	return nil, fmt.Errorf("spaceimpl: object %s: %w", objectId, space.ErrNotFound)
}

// liveObjectRow reports whether v is a present, non-tombstoned row.
func liveObjectRow(v *anyenc.Value) bool {
	return v != nil && v.Get(crdt.DeletedAtField) == nil
}

// Derive creates a deterministic object from a seed. Idempotent — a
// second call with the same seed returns the same id and, when the
// requested Types are already attached, writes no change at all.
func (o *objectService) Derive(ctx context.Context, opts space.DeriveObjectOpts) (string, error) {
	obj, err := o.parent.store.Derive(ctx, spaceobjects.DeriveOpts{
		ChangeType:    objectChangeType,
		ChangePayload: opts.Seed,
		ParentId:      opts.ParentId,
	})
	if err != nil {
		return "", err
	}
	objectId := obj.Id()

	// Type-binding is idempotent in the DAG, not just in state: the
	// requested types are compared against the object's current
	// any.types and only the missing ones are attached, one $addToSet
	// op each — never a whole-array $set, so a type attached
	// concurrently (or by a later handler, e.g. a multitype object)
	// is not clobbered. Derive runs on hot resolve paths ("the
	// well-known chat/brain object of this space"), so a no-op call
	// must not append a change.
	if len(opts.Types) > 0 {
		missing, err := o.missingTypes(ctx, objectId, opts.Types)
		if err != nil {
			return "", err
		}
		if len(missing) > 0 {
			if err := o.attachTypes(ctx, objectId, missing); err != nil {
				return "", err
			}
		}
	}
	return objectId, nil
}

// missingTypes returns the entries of want absent from the object's
// current any.types (deduplicated, input order preserved). A missing
// row reads as "implements nothing", so every requested type is
// reported missing on first derive.
func (o *objectService) missingTypes(ctx context.Context, objectId string, want []string) ([]string, error) {
	current, err := o.parent.store.ObjectTypes(ctx, objectId)
	if err != nil {
		return nil, err
	}
	have := make(map[string]struct{}, len(current)+len(want))
	for _, t := range current {
		have[t] = struct{}{}
	}
	out := make([]string, 0, len(want))
	for _, t := range want {
		if _, dup := have[t]; dup {
			continue
		}
		have[t] = struct{}{}
		out = append(out, t)
	}
	return out, nil
}

// attachTypes appends typeIds to the object's any.types in one change,
// one $addToSet op per type — the same op AttachType uses, so
// concurrent attaches merge instead of last-write-wins.
func (o *objectService) attachTypes(ctx context.Context, objectId string, typeIds []string) error {
	dataVersion, err := dataVersionForTypes(ctx, o.parent.store.Registry(), typeIds)
	if err != nil {
		return err
	}
	obj, err := o.parent.store.Get(ctx, objectId)
	if err != nil {
		return err
	}
	arena := &anyenc.Arena{}
	ops := make([]crdt.Op, 0, len(typeIds))
	for _, t := range typeIds {
		ops = append(ops, crdt.Op{
			Type:    crdt.OpAddToSet,
			Path:    []string{"any", "types"},
			Payload: arena.NewString(t),
		})
	}
	_, err = obj.LocalWrite(ctx, crdt.Change{
		Dataset:     properties.Dataset,
		DataVersion: dataVersion,
		Records: []crdt.RecordChange{{
			Id:     objectId,
			Upsert: true,
			Ops:    ops,
		}},
	})
	return err
}

// Delete records the deletion in the any-sync settings tree — the
// authoritative, synced deletion record — and reclaims the object's local
// state.
//
// The settings-tree write propagates the deletion to every device and
// cascades to any bound children; each device's DeleteTree/MarkTreeDeleted
// callback then purges its own local projection. We also reclaim locally
// and synchronously so the object leaves local queries immediately: the
// store.DeleteTree call tombstones the object's own tree FIRST (so any-sync
// rejects any further apply and no concurrent inbound change can
// re-materialize the row) and then purges. It is best-effort — the async
// settings-tree cascade is authoritative and re-runs it (a no-op once the
// tree is deleted). No CRDT tombstone is written; any-sync's head storage
// is the durable, cross-device record that the tree is deleted.
func (o *objectService) Delete(ctx context.Context, objectId string) error {
	if objectId == "" {
		return errors.New("spaceimpl: Objects.Delete requires objectId")
	}
	if err := o.parent.writeGate(ctx); err != nil {
		return err
	}
	handle, err := o.parent.app.GetSpace(ctx, o.parent.id)
	if err != nil {
		return fmt.Errorf("spaceimpl: get space: %w", err)
	}
	if err := handle.Inner().DeleteTree(ctx, objectId); err != nil {
		return fmt.Errorf("spaceimpl: DeleteTree %s: %w", objectId, err)
	}
	// Best-effort local reclaim; whichever of this call and the async
	// cascade runs first tombstones the tree, the other no-ops on it.
	_ = o.parent.store.DeleteTree(ctx, objectId)
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
