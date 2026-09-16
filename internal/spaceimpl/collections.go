package spaceimpl

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/internal/types"
	anytype "github.com/anyproto/any-sync-sdk/internal/types/any"
	collectiontype "github.com/anyproto/any-sync-sdk/internal/types/collection"
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
	"github.com/anyproto/any-sync-sdk/space"
)

// collectionsAPI implements space.CollectionsAPI. A collection object
// is a type object without parts: the same objects row, the same
// `properties` / `shortIds` datasets, a different marker in `any.type`
// and its metadata under `collection.*`. The property-definition
// methods are the types API's, owner-agnostic.
type collectionsAPI struct {
	parent *spaceImpl
	defs   *typesAPI
}

func newCollectionsAPI(parent *spaceImpl, defs *typesAPI) *collectionsAPI {
	return &collectionsAPI{parent: parent, defs: defs}
}

func (c *collectionsAPI) Properties(ctx context.Context, ownerId string) ([]space.PropertyDef, error) {
	return c.defs.Properties(ctx, ownerId)
}
func (c *collectionsAPI) AddProperty(ctx context.Context, ownerId string, draft space.PropertyDraft) (string, error) {
	return c.defs.AddProperty(ctx, ownerId, draft)
}
func (c *collectionsAPI) RemoveProperty(ctx context.Context, ownerId, propId string) error {
	return c.defs.RemoveProperty(ctx, ownerId, propId)
}
func (c *collectionsAPI) PatchProperty(ctx context.Context, ownerId, propId string, patch space.PropertyPatch) error {
	return c.defs.PatchProperty(ctx, ownerId, propId, patch)
}

// Create mints a new collection object: a fresh any-sync tree whose
// objects row carries the display metadata under `any`, the handle
// and flags under `collection`, and `any.type = "__collection__"` —
// set in the same change so the local pre-flight grants the namespace.
func (c *collectionsAPI) Create(ctx context.Context, params space.CollectionCreateParams) (string, error) {
	obj, err := c.parent.store.Create(ctx, spaceobjects.CreateOpts{ChangeType: "collection"})
	if err != nil {
		return "", err
	}
	id := obj.Id()

	arena := &anyenc.Arena{}
	multi := arena.NewObject()
	if params.Name != "" {
		multi.Set("any.name", arena.NewString(params.Name))
	}
	if params.Description != "" {
		multi.Set("any.description", arena.NewString(params.Description))
	}
	if params.IconCID != "" {
		multi.Set("any.icon", arena.NewString(params.IconCID))
	}
	if params.XKey != "" {
		multi.Set(collectiontype.TypeId+"."+typetype.FieldXKeyProp, arena.NewString(params.XKey))
	}
	if params.Hidden {
		multi.Set(collectiontype.TypeId+"."+typetype.FieldHiddenProp, arena.NewTrue())
	}
	for _, k := range slices.Sorted(maps.Keys(params.Meta)) {
		v, err := typeMetaValue(arena, k, params.Meta[k])
		if err != nil {
			return "", fmt.Errorf("collectionsAPI: CollectionCreateParams.Meta: %w", err)
		}
		if v == nil {
			continue
		}
		multi.Set(collectiontype.TypeId+"."+typetype.FieldMetaProp+"."+k, v)
	}
	multi.Set(anytype.TypeId+"."+anytype.FieldType, arena.NewString(collectiontype.MetaMarker))

	dataVersion, err := c.parent.store.DataVersion(properties.Dataset)
	if err != nil {
		return "", err
	}
	if _, err := obj.LocalWrite(ctx, crdt.Change{
		Dataset:     properties.Dataset,
		DataVersion: dataVersion,
		Records: []crdt.RecordChange{{
			Id:     id,
			Upsert: true,
			Ops:    []crdt.Op{{Type: crdt.OpSet, Payload: multi}},
		}},
	}); err != nil {
		if derr := c.parent.store.DeleteTree(ctx, id); derr != nil {
			objectLog.Warn("orphaned tree after a refused collection stamp", zap.String("collectionId", id), zap.Error(derr))
		}
		return "", fmt.Errorf("collectionsAPI: seed collection metadata: %w", err)
	}
	return id, nil
}

// List returns the meta `collection` built-in, the registered
// collections and every live `__collection__` row of the space.
func (c *collectionsAPI) List(ctx context.Context) ([]space.CollectionInfo, error) {
	coll, err := c.parent.store.SharedObjects(ctx)
	if err != nil {
		return nil, fmt.Errorf("collectionsAPI: shared objects: %w", err)
	}
	iter, err := coll.Find(spaceobjects.LiveCollectionRowsFilter).Iter(ctx)
	if err != nil {
		return nil, fmt.Errorf("collectionsAPI: iter: %w", err)
	}
	defer iter.Close()

	registered := c.parent.store.ExternalCollections()
	out := make([]space.CollectionInfo, 0, 1+len(registered))
	out = append(out, builtInMetaCollectionInfo())
	for _, rc := range registered {
		out = append(out, registeredCollectionInfo(rc))
	}
	for iter.Next() {
		doc, err := iter.Doc()
		if err != nil {
			return nil, fmt.Errorf("collectionsAPI: doc: %w", err)
		}
		if v := doc.Value(); v != nil {
			out = append(out, collectionInfoFromRow(v))
		}
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("collectionsAPI: iter err: %w", err)
	}
	return out, nil
}

// Get returns one collection, or space.ErrNotFound — a type id
// included: the two surfaces never alias.
func (c *collectionsAPI) Get(ctx context.Context, id string) (space.CollectionInfo, error) {
	if id == collectiontype.TypeId {
		return builtInMetaCollectionInfo(), nil
	}
	if rc, ok := c.defs.findRegisteredCollection(id); ok {
		return registeredCollectionInfo(rc), nil
	}
	coll, err := c.parent.store.SharedObjects(ctx)
	if err != nil {
		return space.CollectionInfo{}, fmt.Errorf("collectionsAPI: shared objects: %w", err)
	}
	doc, err := coll.FindId(ctx, id)
	if err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return space.CollectionInfo{}, space.ErrNotFound
		}
		return space.CollectionInfo{}, fmt.Errorf("collectionsAPI: find %s: %w", id, err)
	}
	v := doc.Value()
	if v == nil || v.Get(crdt.DeletedAtField) != nil {
		return space.CollectionInfo{}, space.ErrNotFound
	}
	switch markerOf(v) {
	case collectiontype.MetaMarker:
		return collectionInfoFromRow(v), nil
	case typetype.MetaTypeMarker:
		return space.CollectionInfo{}, fmt.Errorf("%w: %q", space.ErrNotACollection, id)
	}
	return space.CollectionInfo{}, space.ErrNotFound
}

func (c *collectionsAPI) Delete(_ context.Context, _ string) error {
	return errors.New("collectionsAPI: Delete not implemented")
}

// Patch rewrites a user collection's display and listing metadata in
// one change on its objects row. Absent fields keep their value; an
// empty string clears a text field; a nil Meta value unsets the key.
func (c *collectionsAPI) Patch(ctx context.Context, id string, patch space.CollectionPatch) error {
	if c.defs.staticType(id) {
		return fmt.Errorf("%w: %q", space.ErrTypeRegistered, id)
	}
	if _, err := c.Get(ctx, id); err != nil {
		return err
	}
	arena := &anyenc.Arena{}
	set := arena.NewObject()
	unset := arena.NewObject()
	text := func(path string, v *string) {
		if v == nil {
			return
		}
		if *v == "" {
			unset.Set(path, arena.NewNull())
			return
		}
		set.Set(path, arena.NewString(*v))
	}
	text("any.name", patch.Name)
	text("any.description", patch.Description)
	text("any.icon", patch.IconCID)
	if patch.Hidden != nil {
		set.Set(collectiontype.TypeId+"."+typetype.FieldHiddenProp, arena.NewBool(*patch.Hidden))
	}
	for _, k := range slices.Sorted(maps.Keys(patch.Meta)) {
		v, err := typeMetaValue(arena, k, patch.Meta[k])
		if err != nil {
			return fmt.Errorf("collectionsAPI: CollectionPatch.Meta: %w", err)
		}
		path := collectiontype.TypeId + "." + typetype.FieldMetaProp + "." + k
		if v == nil {
			unset.Set(path, arena.NewNull())
		} else {
			set.Set(path, v)
		}
	}
	var ops []crdt.Op
	if set.GetObject() != nil && set.GetObject().Len() > 0 {
		ops = append(ops, crdt.Op{Type: crdt.OpSet, Payload: set})
	}
	if unset.GetObject() != nil && unset.GetObject().Len() > 0 {
		ops = append(ops, crdt.Op{Type: crdt.OpUnset, Payload: unset})
	}
	if len(ops) == 0 {
		return nil
	}
	dataVersion, err := c.parent.store.DataVersion(properties.Dataset)
	if err != nil {
		return err
	}
	obj, err := c.parent.store.Get(ctx, id)
	if err != nil {
		return err
	}
	if _, err := c.parent.localWriteRetry(ctx, obj, id, crdt.Change{
		Dataset:     properties.Dataset,
		DataVersion: dataVersion,
		Records:     []crdt.RecordChange{{Id: id, Ops: ops}},
	}); err != nil {
		return fmt.Errorf("collectionsAPI: patch collection: %w", err)
	}
	return nil
}

// builtInMetaCollectionInfo describes the synthetic `collection`
// meta-type as a collection entry, so Collections().List surfaces
// every collection namespace a row can hold.
func builtInMetaCollectionInfo() space.CollectionInfo {
	return space.CollectionInfo{
		Id:          collectiontype.TypeId,
		Name:        collectiontype.Name,
		Description: collectiontype.Description,
		BuiltIn:     true,
	}
}

// registeredCollectionInfo maps a caller-registered handler.Collection
// into the public shape; the Name falls back to the Id.
func registeredCollectionInfo(c handler.Collection) space.CollectionInfo {
	name := c.Name
	if name == "" {
		name = c.Id
	}
	return space.CollectionInfo{
		Id:          c.Id,
		Name:        name,
		Description: c.Description,
		IconCID:     c.IconCID,
		Hidden:      c.Hidden,
		BuiltIn:     true,
	}
}

// collectionInfoFromRow decodes a `__collection__` row.
func collectionInfoFromRow(rec *anyenc.Value) space.CollectionInfo {
	return space.CollectionInfo{
		Id:          rec.GetString(crdt.IdField),
		Name:        rec.GetString("any", "name"),
		Description: rec.GetString("any", "description"),
		IconCID:     rec.GetString("any", "icon"),
		XKey:        rec.GetString(collectiontype.TypeId, typetype.FieldXKeyProp),
		Hidden:      rec.GetBool(collectiontype.TypeId, typetype.FieldHiddenProp),
		Meta:        types.DecodeXFormat(rec.Get(collectiontype.TypeId, typetype.FieldMetaProp)),
	}
}
