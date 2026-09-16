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
	anytype "github.com/anyproto/any-sync-sdk/internal/types/any"
	"github.com/anyproto/any-sync-sdk/space"
)

// uniqueOwners returns the deduplicated owner ids a Create bootstrap
// touches — the type, the collections and the InitialProperties keys.
// Order is stable (type, collections, then the keys in iteration
// order).
func uniqueOwners(opts space.CreateObjectOpts) []string {
	seen := make(map[string]struct{}, 1+len(opts.Collections)+len(opts.InitialProperties))
	out := make([]string, 0, 1+len(opts.Collections)+len(opts.InitialProperties))
	if opts.Type != "" {
		seen[opts.Type] = struct{}{}
		out = append(out, opts.Type)
	}
	for _, t := range opts.Collections {
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

// dataVersionForOwners encodes a multi-owner DataVersion using the
// registry's latest shortId per owner (a type or a collection). Skips
// owners with no important changes — their pair is omitted (an absent
// owner contributes nothing to the gate).
//
// Empty result (no owner known to the registry) falls back to the
// hardcoded handler version, which the gate treats as "no schema
// constraint".
func dataVersionForOwners(ctx context.Context, reg *types.LiveRegistry, ownerIds []string) (string, error) {
	if reg == nil || len(ownerIds) == 0 {
		return properties.HandlerVersion, nil
	}
	pairs := make([]types.DataVersionPair, 0, len(ownerIds))
	for _, t := range ownerIds {
		shortId, err := reg.LatestShortId(ctx, t)
		if err != nil {
			return "", fmt.Errorf("dataVersionForOwners[%s]: %w", t, err)
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
// bootstrap modify (membership + InitialProperties) in one extra
// change, and returns the new objectId.
func (o *objectService) Create(ctx context.Context, opts space.CreateObjectOpts) (string, error) {
	if opts.Type == "" {
		return "", fmt.Errorf("%w: CreateObjectOpts.Type", space.ErrTypeRequired)
	}
	obj, err := o.parent.store.Create(ctx, spaceobjects.CreateOpts{
		ChangeType: objectChangeType,
	})
	if err != nil {
		return "", err
	}
	objectId := obj.Id()

	if needsBootstrap(opts) {
		if _, err := o.bootstrap(ctx, objectId, opts); err != nil {
			// A refused bootstrap (a wrong slot, an owner the object
			// does not have) would leave a bare tree nothing references;
			// reclaim it best-effort, the error is the caller's answer.
			_ = o.Delete(ctx, objectId)
			return "", err
		}
	}
	return objectId, nil
}

// Get reads the object's row from the shared objects collection. An
// absent or tombstoned row alone is ambiguous (deletion hard-removes
// the row, and a live object that never wrote a property has none),
// so the tree decides: deleted → ErrObjectDeleted, present without a
// row → a synthetic {id} row, unknown → ErrNotFound.
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
	present, err := o.parent.store.HasTree(ctx, objectId)
	if err != nil {
		return nil, fmt.Errorf("spaceimpl: object %s: tree presence: %w", objectId, err)
	}
	if present {
		a := &anyenc.Arena{}
		row := a.NewObject()
		row.Set("id", a.NewString(objectId))
		return row, nil
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

	// Membership is idempotent in the DAG, not just in state: the
	// requested type is set only when the row has none (a type it
	// already has is never replaced), and only the collections the row
	// lacks are added, one $addToSet op each — never a whole-array
	// $set, so a collection attached concurrently is not clobbered.
	// Derive runs on hot resolve paths ("the well-known chat/brain
	// object of this space"), so a no-op call must not append a change.
	setType, missing, err := o.missingMembers(ctx, objectId, opts.Type, opts.Collections)
	if err != nil {
		return "", err
	}
	if setType != "" || len(missing) > 0 {
		if err := o.attachMembers(ctx, objectId, setType, missing); err != nil {
			return "", err
		}
	}
	return objectId, nil
}

// missingMembers compares the wanted membership against the object's
// row: the type to set (empty when the row already has one; a row
// with none needs one — ErrTypeRequired) and the collections absent
// from the row (deduplicated, input order preserved). A missing row
// reads as nothing, so everything requested is reported on first
// derive.
func (o *objectService) missingMembers(ctx context.Context, objectId, wantType string, wantCollections []string) (string, []string, error) {
	current, err := o.parent.store.ObjectMembers(ctx, objectId)
	if err != nil {
		return "", nil, err
	}
	setType := ""
	if current.Type == "" {
		if wantType == "" {
			return "", nil, fmt.Errorf("%w: DeriveObjectOpts.Type on an object with no type yet", space.ErrTypeRequired)
		}
		setType = wantType
	}
	have := make(map[string]struct{}, len(current.Collections)+len(wantCollections))
	for _, t := range current.Collections {
		have[t] = struct{}{}
	}
	out := make([]string, 0, len(wantCollections))
	for _, t := range wantCollections {
		if _, dup := have[t]; dup {
			continue
		}
		have[t] = struct{}{}
		out = append(out, t)
	}
	return setType, out, nil
}

// attachMembers writes the membership the object lacks in one change:
// a $set of the type when setType is non-empty, one $addToSet per
// collection — the same ops SetType / AttachCollection use, so
// concurrent attaches merge instead of last-write-wins.
func (o *objectService) attachMembers(ctx context.Context, objectId, setType string, collections []string) error {
	owners := make([]string, 0, 1+len(collections))
	if setType != "" {
		owners = append(owners, setType)
	}
	owners = append(owners, collections...)
	dataVersion, err := dataVersionForOwners(ctx, o.parent.store.Registry(), owners)
	if err != nil {
		return err
	}
	obj, err := o.parent.store.Get(ctx, objectId)
	if err != nil {
		return err
	}
	arena := &anyenc.Arena{}
	ops := make([]crdt.Op, 0, 1+len(collections))
	if setType != "" {
		ops = append(ops, crdt.Op{
			Type:    crdt.OpSet,
			Path:    []string{anytype.TypeId, anytype.FieldType},
			Payload: arena.NewString(setType),
		})
	}
	for _, t := range collections {
		ops = append(ops, crdt.Op{
			Type:    crdt.OpAddToSet,
			Path:    []string{anytype.TypeId, anytype.FieldCollections},
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
	return wrapSlotErr(err)
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
	if opts.Type != "" || len(opts.Collections) > 0 {
		return true
	}
	for _, props := range opts.InitialProperties {
		if len(props) > 0 {
			return true
		}
	}
	return false
}

// bootstrap writes the post-create initial state (membership +
// per-owner property values) into the object's `properties` dataset.
// One Modify call regardless of how many owners / properties are
// being seeded — keeps the DAG clean.
func (o *objectService) bootstrap(ctx context.Context, objectId string, opts space.CreateObjectOpts) (space.ModifyResult, error) {
	arena := &anyenc.Arena{}
	multi := arena.NewObject()

	if opts.Type != "" {
		multi.Set(anytype.TypeId+"."+anytype.FieldType, arena.NewString(opts.Type))
	}
	if len(opts.Collections) > 0 {
		arr := arena.NewArray()
		for i, t := range opts.Collections {
			arr.SetArrayItem(i, arena.NewString(t))
		}
		multi.Set(anytype.TypeId+"."+anytype.FieldCollections, arr)
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

	// Compute DataVersion across every owner touched: the type, the
	// collections and the keys of opts.InitialProperties. Each
	// contributes one (ownerId, latestShortId) pair if available.
	touched := uniqueOwners(opts)
	dataVersion, err := dataVersionForOwners(ctx, o.parent.store.Registry(), touched)
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
		return space.ModifyResult{}, wrapSlotErr(err)
	}
	return modifyResultFromWrite(res), nil
}
