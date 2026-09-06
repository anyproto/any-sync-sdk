package spaceimpl

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/internal/types"
	anytype "github.com/anyproto/any-sync-sdk/internal/types/any"
	"github.com/anyproto/any-sync-sdk/internal/types/spaceindex"
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
	"github.com/anyproto/any-sync-sdk/space"
)

// listTypesFilter selects rows from the per-space `objects` collection
// whose `any.types` array carries the meta-type marker and that aren't
// tombstoned. One definition, shared with the store's catalog scan.
var listTypesFilter = spaceobjects.LiveTypeRowsFilter

// typesAPI implements space.TypesAPI. MVP scope: Create + AddProperty.
// The other methods return "not implemented" so callers see a
// consistent failure mode while we wait for the Registry / List /
// Get implementations.
type typesAPI struct {
	parent *spaceImpl
}

func newTypesAPI(parent *spaceImpl) *typesAPI { return &typesAPI{parent: parent} }

// Create mints a new type object: a fresh any-sync tree whose
// `properties` dataset record carries the type's display metadata
// (any.name / any.description / any.icon), its programmatic handle
// (type.xkey) and `any.types = ["__type__"]` to mark it as a meta-type
// instance.
//
// The universal fields stay under `any`; `xkey` is meaningful only on
// a type object, so it lives in the meta-type's own namespace — which
// also makes it unwritable on a row that doesn't carry the marker
// (properties.SystemPropertiesHandler membership check).
//
// The returned typeId is the new tree's id (= root change id) — used
// as the namespace prefix in property paths on instance objects.
func (t *typesAPI) Create(ctx context.Context, params space.TypeCreateParams) (string, error) {
	obj, err := t.parent.store.Create(ctx, spaceobjects.CreateOpts{
		ChangeType: "type",
	})
	if err != nil {
		return "", err
	}
	typeId := obj.Id()

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
		multi.Set(typetype.TypeId+"."+typetype.FieldXKeyProp, arena.NewString(params.XKey))
	}
	if params.Weight != 0 {
		multi.Set(typetype.TypeId+"."+typetype.FieldWeightProp, arena.NewNumberInt(params.Weight))
	}
	if len(params.Layout) > 0 {
		layout, err := encodeXFormat(arena, params.Layout)
		if err != nil {
			return "", fmt.Errorf("typesAPI: TypeCreateParams.Layout: %w", err)
		}
		multi.Set(typetype.TypeId+"."+typetype.FieldLayoutProp, layout)
	}
	if params.Hidden {
		multi.Set(typetype.TypeId+"."+typetype.FieldHiddenProp, arena.NewTrue())
	}
	for _, k := range slices.Sorted(maps.Keys(params.Meta)) {
		v, err := typeMetaValue(arena, k, params.Meta[k])
		if err != nil {
			return "", fmt.Errorf("typesAPI: TypeCreateParams.Meta: %w", err)
		}
		if v == nil {
			continue
		}
		multi.Set(typetype.TypeId+"."+typetype.FieldMetaProp+"."+k, v)
	}
	// Mark the object as a meta-type instance — the convention we use
	// in MVP to distinguish types from regular objects without a
	// dedicated catalog. Set in the same change as the xkey write, so
	// the local pre-flight sees the namespace as implemented.
	types := arena.NewArray()
	types.SetArrayItem(0, arena.NewString(typetype.MetaTypeMarker))
	multi.Set("any.types", types)

	dataVersion, err := t.parent.store.DataVersion(properties.Dataset)
	if err != nil {
		return "", err
	}
	if _, err := obj.LocalWrite(ctx, crdt.Change{
		Dataset:     properties.Dataset,
		DataVersion: dataVersion,
		Records: []crdt.RecordChange{{
			Id:     typeId,
			Upsert: true,
			Ops:    []crdt.Op{{Type: crdt.OpSet, Payload: multi}},
		}},
	}); err != nil {
		return "", fmt.Errorf("typesAPI: seed type metadata: %w", err)
	}
	return typeId, nil
}

// AddProperty writes a property-definition record to the type
// object's `defs` dataset. The record's id (= propId) is auto-derived
// from the change's ChangeId per the empty-id-resolution rule;
// returned so the caller can reference it from instance writes.
//
// Registered types from config.Config.Types are statically declared
// and don't accept runtime property additions — AddProperty returns
// an error. Use a custom dataset/handler for that type's mutable
// state.
func (t *typesAPI) AddProperty(ctx context.Context, typeId string, draft space.PropertyDraft) (string, error) {
	if t.staticType(typeId) {
		return "", fmt.Errorf("%w: %q", space.ErrTypeRegistered, typeId)
	}
	if err := validatePropertyDraft(&draft); err != nil {
		return "", err
	}
	payload, err := propertyDefPayload(&anyenc.Arena{}, &draft)
	if err != nil {
		return "", err
	}
	res, err := t.writePropertyDefs(ctx, typeId, crdt.RecordChange{
		Upsert: true, // empty Id → propId derived from ChangeId
		Ops:    []crdt.Op{{Type: crdt.OpSet, Payload: payload}},
	})
	if err != nil {
		return "", err
	}
	if len(res.RecordIds) == 0 {
		return "", errors.New("typesAPI: write returned no record id")
	}
	return res.RecordIds[0], nil
}

// validatePropertyDraft is the caller-side gate every property
// definition passes — AddProperty's and a bundle's alike.
func validatePropertyDraft(draft *space.PropertyDraft) error {
	if draft.Kind == 0 {
		return errors.New("typesAPI: PropertyDraft.Kind required")
	}
	if propertyKindLabel(draft.Kind) == "" {
		return fmt.Errorf("typesAPI: PropertyDraft.Kind %d is not a property kind", draft.Kind)
	}
	switch draft.Scope {
	case 0, space.ScopeSynced, space.ScopeAccount, space.ScopeLocal:
	default:
		return fmt.Errorf("typesAPI: PropertyDraft.Scope must be synced/account/local (derived is reserved for built-ins); got %s", draft.Scope)
	}
	return nil
}

// propertyDefPayload builds the definition record a draft describes —
// the single creation shape the property handler validates.
func propertyDefPayload(arena *anyenc.Arena, draft *space.PropertyDraft) (*anyenc.Value, error) {
	payload := arena.NewObject()
	payload.Set(typetype.FieldKind, arena.NewString(propertyKindLabel(draft.Kind)))
	if len(draft.XFormat) > 0 {
		xf, err := encodeXFormat(arena, draft.XFormat)
		if err != nil {
			return nil, fmt.Errorf("typesAPI: PropertyDraft.XFormat: %w", err)
		}
		payload.Set(typetype.FieldXFormat, xf)
	}
	if draft.Scope != 0 && draft.Scope != space.ScopeSynced {
		// Synced is the implicit default — only non-default scopes are
		// written, so pre-scope and default definitions stay byte-
		// identical on the wire. Pinned post-create (schemaBearingFields).
		payload.Set(typetype.FieldScope, arena.NewString(draft.Scope.String()))
	}
	if draft.Name != "" {
		payload.Set(typetype.FieldName, arena.NewString(draft.Name))
	}
	if draft.Description != "" {
		payload.Set(typetype.FieldDescription, arena.NewString(draft.Description))
	}
	if draft.XKey != "" {
		payload.Set(typetype.FieldXKey, arena.NewString(draft.XKey))
	}
	if len(draft.Meta) > 0 {
		metaObj := arena.NewObject()
		for k, v := range draft.Meta {
			metaObj.Set(k, arena.NewString(v))
		}
		payload.Set(typetype.FieldMeta, metaObj)
	}
	return payload, nil
}

// writePropertyDefs is the shared LocalWrite helper for the property
// definitions dataset.
func (t *typesAPI) writePropertyDefs(ctx context.Context, typeId string, recs ...crdt.RecordChange) (object.WriteResult, error) {
	dataVersion, err := t.parent.store.DataVersion(typetype.DatasetPropertyDefs)
	if err != nil {
		return object.WriteResult{}, err
	}
	obj, err := t.parent.store.Get(ctx, typeId)
	if err != nil {
		return object.WriteResult{}, err
	}
	return t.parent.localWriteRetry(ctx, obj, typeId, crdt.Change{
		Dataset:     typetype.DatasetPropertyDefs,
		DataVersion: dataVersion,
		Records:     recs,
	})
}

// propertyDefExists reports whether the type object carries a property
// definition record under propId — live or tombstoned. A removed
// definition keeps its id, which is how a bundle's adopt path tells
// "never declared" from "removed".
func (t *typesAPI) propertyDefExists(ctx context.Context, typeId, propId string) (bool, error) {
	coll, err := t.parent.store.OpenObjectCollection(ctx, typeId, typetype.DatasetPropertyDefs)
	if err != nil {
		if errors.Is(err, anystore.ErrCollectionNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("typesAPI: open defs %s: %w", typeId, err)
	}
	if _, err := coll.FindId(ctx, propId); err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("typesAPI: find def %s: %w", propId, err)
	}
	return true, nil
}

// builtInAnyTypeInfo describes the synthetic `any` type — present in
// every space without materialisation. Returned by List / Get so
// callers can iterate the type catalog uniformly.
func builtInAnyTypeInfo() space.TypeInfo {
	return space.TypeInfo{
		Id:          anytype.TypeId,
		Name:        anytype.Name,
		Description: anytype.Description,
		BuiltIn:     true,
	}
}

// builtInSpaceIndexTypeInfo describes the synthetic `spaceIndex`
// type — one derived object per space carrying name / description /
// icon / spaceType. Surfaced alongside `any` so callers see the
// full built-in catalog.
func builtInSpaceIndexTypeInfo() space.TypeInfo {
	return space.TypeInfo{
		Id:          spaceindex.TypeId,
		Name:        spaceindex.Name,
		Description: spaceindex.Description,
		BuiltIn:     true,
	}
}

// builtInMetaTypeInfo describes the synthetic `type` meta-type —
// the shape of type objects themselves. Every type object implements
// it (that's what the marker in `any.types` says), so it belongs in
// the catalog next to `any` and `spaceIndex`.
func builtInMetaTypeInfo() space.TypeInfo {
	return space.TypeInfo{
		Id:          typetype.TypeId,
		Name:        typetype.Name,
		Description: typetype.Description,
		BuiltIn:     true,
	}
}

// registeredTypeInfo maps a caller-registered handler.Type into the
// public TypeInfo shape. Registered types are statically declared at
// SDK init (via config.Config.Types), so they're surfaced with
// BuiltIn=true to indicate "not user-created in this space" — same
// semantics as the synthetic `any` type. The caller's Name falls
// back to the type Id when empty so list rendering never shows a
// blank label.
func registeredTypeInfo(t handler.Type) space.TypeInfo {
	name := t.Name
	if name == "" {
		name = t.Id
	}
	return space.TypeInfo{
		Id:          t.Id,
		Name:        name,
		Description: t.Description,
		IconCID:     t.IconCID,
		Hidden:      t.Hidden,
		BuiltIn:     true,
	}
}

// registeredTypeParts is the compiled view of a registered type's
// static parts — the declared ones, or one implicit part per dataset.
func (t *typesAPI) registeredTypeParts(rt handler.Type) (*types.CompiledType, error) {
	ct, err := spaceobjects.StaticTypeParts(rt, t.parent.store.Modules())
	if err != nil {
		return nil, fmt.Errorf("typesAPI: static parts of %q: %w", rt.Id, err)
	}
	return ct, nil
}

// staticType reports whether typeId is a type whose declarations are
// hardcoded — a caller-registered type from config.Config.Types, or
// one of the synthetic built-ins (`any`, `spaceIndex`, `type`). Every
// runtime mutator rejects them with ErrTypeRegistered; without the
// built-in half a mutator called with an enumerable id like `type`
// (Types().List surfaces it) falls through to a tree build on that
// literal id and surfaces an opaque CID error instead.
func (t *typesAPI) staticType(typeId string) bool {
	if _, reserved := spaceobjects.ReservedTypeIds[typeId]; reserved {
		return true
	}
	_, ok := t.findRegisteredType(typeId)
	return ok
}

// findRegisteredType returns the catalog entry for typeId, or
// (zero, false) if none.
func (t *typesAPI) findRegisteredType(typeId string) (handler.Type, bool) {
	for _, rt := range t.parent.store.ExternalTypes() {
		if rt.Id == typeId {
			return rt, true
		}
	}
	return handler.Type{}, false
}

// List returns every object whose persisted `properties` row is a
// meta-type instance (`any.types` contains "__type__"). One any-store
// query against the per-space `objects` collection — no tree builds,
// no any-sync calls. Eventual-consistency: rows that the controller
// has applied are visible; whatever any-sync hasn't replayed yet
// isn't, which is what the user wants from a read-only listing.
func (t *typesAPI) List(ctx context.Context) ([]space.TypeInfo, error) {
	coll, err := t.parent.store.SharedObjects(ctx)
	if err != nil {
		return nil, fmt.Errorf("typesAPI: shared objects: %w", err)
	}
	iter, err := coll.Find(listTypesFilter).Iter(ctx)
	if err != nil {
		return nil, fmt.Errorf("typesAPI: iter: %w", err)
	}
	defer iter.Close()

	registered := t.parent.store.ExternalTypes()
	out := make([]space.TypeInfo, 0, 3+len(registered))
	out = append(out, builtInAnyTypeInfo())
	out = append(out, builtInSpaceIndexTypeInfo())
	out = append(out, builtInMetaTypeInfo())
	for _, rt := range registered {
		out = append(out, registeredTypeInfo(rt))
	}
	for iter.Next() {
		doc, err := iter.Doc()
		if err != nil {
			return nil, fmt.Errorf("typesAPI: doc: %w", err)
		}
		v := doc.Value()
		if v == nil {
			continue
		}
		out = append(out, typeInfoFromRow(v))
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("typesAPI: iter err: %w", err)
	}
	return out, nil
}

// Get returns one type's display metadata or space.ErrNotFound when
// the id isn't a meta-type instance on this space. Special-cases the
// synthetic `any` built-in and any caller-registered type from
// config.Config.Types — both are statically declared at SDK init and
// don't live in the space's tree storage.
//
// Reads from the per-space `objects` collection in any-store. No
// any-sync calls.
func (t *typesAPI) Get(ctx context.Context, typeId string) (space.TypeInfo, error) {
	if typeId == anytype.TypeId {
		return builtInAnyTypeInfo(), nil
	}
	if typeId == spaceindex.TypeId {
		return builtInSpaceIndexTypeInfo(), nil
	}
	if typeId == typetype.TypeId {
		return builtInMetaTypeInfo(), nil
	}
	if rt, ok := t.findRegisteredType(typeId); ok {
		return registeredTypeInfo(rt), nil
	}
	coll, err := t.parent.store.SharedObjects(ctx)
	if err != nil {
		return space.TypeInfo{}, fmt.Errorf("typesAPI: shared objects: %w", err)
	}
	doc, err := coll.FindId(ctx, typeId)
	if err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return space.TypeInfo{}, space.ErrNotFound
		}
		return space.TypeInfo{}, fmt.Errorf("typesAPI: find %s: %w", typeId, err)
	}
	v := doc.Value()
	if v == nil || v.Get(crdt.DeletedAtField) != nil || !hasTypeMarker(v) {
		return space.TypeInfo{}, space.ErrNotFound
	}
	return typeInfoFromRow(v), nil
}

// typeInfoFromRow decodes a row from the per-space `objects`
// collection into the public TypeInfo shape. Caller is responsible
// for having checked hasTypeMarker.
func typeInfoFromRow(rec *anyenc.Value) space.TypeInfo {
	return space.TypeInfo{
		Id:          rec.GetString(crdt.IdField),
		Name:        rec.GetString("any", "name"),
		Description: rec.GetString("any", "description"),
		IconCID:     rec.GetString("any", "icon"),
		XKey:        rec.GetString(typetype.TypeId, typetype.FieldXKeyProp),
		Weight:      int(rec.GetFloat64(typetype.TypeId, typetype.FieldWeightProp)),
		Layout:      types.DecodeXFormat(rec.Get(typetype.TypeId, typetype.FieldLayoutProp)),
		Hidden:      rec.GetBool(typetype.TypeId, typetype.FieldHiddenProp),
		Meta:        types.DecodeXFormat(rec.Get(typetype.TypeId, typetype.FieldMetaProp)),
		BuiltIn:     false,
	}
}

// typeMetaValue validates one meta entry — a single-level key and a
// scalar value — and encodes it. A nil value encodes to nil: the
// caller's unset.
func typeMetaValue(a *anyenc.Arena, key string, v any) (*anyenc.Value, error) {
	if key == "" || strings.ContainsAny(key, ".$") || len(key) > 64 {
		return nil, fmt.Errorf("%w: meta key %q must be a single-level key (no '.', no '$', at most 64 bytes)", space.ErrInvalidFieldValue, key)
	}
	switch t := v.(type) {
	case nil:
		return nil, nil
	case string:
		return a.NewString(t), nil
	case bool:
		return a.NewBool(t), nil
	case int:
		return a.NewNumberFloat64(float64(t)), nil
	case int32:
		return a.NewNumberFloat64(float64(t)), nil
	case int64:
		return a.NewNumberFloat64(float64(t)), nil
	case float32:
		return a.NewNumberFloat64(float64(t)), nil
	case float64:
		return a.NewNumberFloat64(t), nil
	}
	return nil, fmt.Errorf("%w: meta key %q: value must be a string, bool or number (got %T)", space.ErrInvalidFieldValue, key, v)
}

// Patch rewrites a user type's display and rendering metadata in one
// change on its objects row: the universal fields under `any`, weight
// and layout under the meta-type's namespace. Absent fields keep their
// value; an empty string clears a text field; ClearLayout unsets the
// layout.
func (t *typesAPI) Patch(ctx context.Context, typeId string, patch space.TypePatch) error {
	if t.staticType(typeId) {
		return fmt.Errorf("%w: %q", space.ErrTypeRegistered, typeId)
	}
	if _, err := t.Get(ctx, typeId); err != nil {
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
	if patch.Weight != nil {
		set.Set(typetype.TypeId+"."+typetype.FieldWeightProp, arena.NewNumberInt(*patch.Weight))
	}
	if patch.Hidden != nil {
		set.Set(typetype.TypeId+"."+typetype.FieldHiddenProp, arena.NewBool(*patch.Hidden))
	}
	for _, k := range slices.Sorted(maps.Keys(patch.Meta)) {
		v, err := typeMetaValue(arena, k, patch.Meta[k])
		if err != nil {
			return fmt.Errorf("typesAPI: TypePatch.Meta: %w", err)
		}
		path := typetype.TypeId + "." + typetype.FieldMetaProp + "." + k
		if v == nil {
			unset.Set(path, arena.NewNull())
		} else {
			set.Set(path, v)
		}
	}
	switch {
	case patch.ClearLayout:
		unset.Set(typetype.TypeId+"."+typetype.FieldLayoutProp, arena.NewNull())
	case patch.Layout != nil:
		layout, err := encodeXFormat(arena, patch.Layout)
		if err != nil {
			return fmt.Errorf("typesAPI: TypePatch.Layout: %w", err)
		}
		set.Set(typetype.TypeId+"."+typetype.FieldLayoutProp, layout)
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
	dataVersion, err := t.parent.store.DataVersion(properties.Dataset)
	if err != nil {
		return err
	}
	obj, err := t.parent.store.Get(ctx, typeId)
	if err != nil {
		return err
	}
	if _, err := t.parent.localWriteRetry(ctx, obj, typeId, crdt.Change{
		Dataset:     properties.Dataset,
		DataVersion: dataVersion,
		Records:     []crdt.RecordChange{{Id: typeId, Ops: ops}},
	}); err != nil {
		return fmt.Errorf("typesAPI: patch type: %w", err)
	}
	return nil
}

// hasTypeMarker reports whether `record.any.types` contains the
// reserved meta-type label "__type__". Used to distinguish type
// objects from regular objects without a separate catalog.
func hasTypeMarker(rec *anyenc.Value) bool {
	arr := rec.GetArray("any", "types")
	for _, v := range arr {
		if string(v.GetStringBytes()) == typetype.MetaTypeMarker {
			return true
		}
	}
	return false
}

func (t *typesAPI) Delete(_ context.Context, _ string) error {
	return errors.New("typesAPI: Delete not implemented")
}

// Properties returns the property definitions of a type. For the
// built-in `any`, `spaceIndex` and `type` types, the list is
// hardcoded (one entry per package-level Properties table, e.g.
// anytype.Properties). Caller-registered types own datasets, not
// property definitions, so an empty slice is returned (those types
// expose state via custom datasets reached through Space.Modify /
// Query, not through the property API). For user-created types, the
// list is read off the type object's `defs` dataset.
//
// Pure any-store read — no any-sync tree build. Empty slice if the
// type has no `defs` writes yet (collection not initialised) or if
// the typeId isn't a known type.
func (t *typesAPI) Properties(ctx context.Context, typeId string) ([]space.PropertyDef, error) {
	if typeId == anytype.TypeId {
		return builtInAnyProperties(), nil
	}
	if typeId == spaceindex.TypeId {
		return builtInSpaceIndexProperties(), nil
	}
	if typeId == typetype.TypeId {
		return builtInMetaTypeProperties(), nil
	}
	if rt, ok := t.findRegisteredType(typeId); ok {
		return registeredTypeProperties(rt), nil
	}
	coll, err := t.parent.store.OpenObjectCollection(ctx, typeId, typetype.DatasetPropertyDefs)
	if err != nil {
		if errors.Is(err, anystore.ErrCollectionNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("typesAPI: open defs %s: %w", typeId, err)
	}
	iter, err := coll.Find(nil).Iter(ctx)
	if err != nil {
		return nil, fmt.Errorf("typesAPI: iter defs: %w", err)
	}
	defer iter.Close()
	var out []space.PropertyDef
	for iter.Next() {
		doc, err := iter.Doc()
		if err != nil {
			return nil, fmt.Errorf("typesAPI: doc: %w", err)
		}
		v := doc.Value()
		if v == nil || v.Get(crdt.DeletedAtField) != nil {
			continue
		}
		out = append(out, decodePropertyDef(v))
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("typesAPI: iter err: %w", err)
	}
	return out, nil
}

// builtInAnyProperties translates anytype.Properties into the public
// PropertyDef shape. Built-ins use human-readable ids — they pass
// through to the SDK's permissive validator unchanged.
func builtInAnyProperties() []space.PropertyDef {
	out := make([]space.PropertyDef, 0, len(anytype.Properties))
	for _, p := range anytype.Properties {
		out = append(out, space.PropertyDef{
			Id:    p.Id,
			Name:  p.Name,
			Kind:  schemaKindToPropertyKind(p.Kind),
			Scope: p.Scope,
		})
	}
	return out
}

// builtInSpaceIndexProperties translates spaceindex.Properties into
// the public PropertyDef shape. Same treatment as `any` — human-
// readable ids, permissive validator.
func builtInSpaceIndexProperties() []space.PropertyDef {
	out := make([]space.PropertyDef, 0, len(spaceindex.Properties))
	for _, p := range spaceindex.Properties {
		out = append(out, space.PropertyDef{
			Id:    p.Id,
			Name:  p.Name,
			Kind:  schemaKindToPropertyKind(p.Kind),
			Scope: p.Scope,
		})
	}
	return out
}

// builtInMetaTypeProperties translates typetype.Properties into the
// public PropertyDef shape — the values a type object carries in its
// own namespace (`xkey`), as opposed to the universal ones it carries
// under `any`.
func builtInMetaTypeProperties() []space.PropertyDef {
	out := make([]space.PropertyDef, 0, len(typetype.Properties))
	for _, p := range typetype.Properties {
		out = append(out, space.PropertyDef{
			Id:    p.Id,
			Name:  p.Name,
			Kind:  schemaKindToPropertyKind(p.Kind),
			Scope: p.Scope,
		})
	}
	return out
}

// registeredTypeProperties maps a caller-registered type's declared
// PropertyDecls into the public PropertyDef shape, so
// space.Types().Properties() surfaces the same schema the SDK
// validates writes against. Returns nil for a type that declares no
// properties (owns only separate datasets).
func registeredTypeProperties(t handler.Type) []space.PropertyDef {
	if len(t.Properties) == 0 {
		return nil
	}
	out := make([]space.PropertyDef, 0, len(t.Properties))
	for _, p := range t.Properties {
		def := space.PropertyDef{
			Id:    p.Id,
			Name:  p.Name,
			Kind:  handlerKindToPropertyKind(p.Kind),
			Scope: p.Scope,
		}
		if def.Scope == 0 {
			def.Scope = space.ScopeSynced
		}
		// Deep-copied: the registration is process-shared, a view is
		// the caller's to mutate.
		def.XFormat = types.CloneXFormat(p.XFormat)
		out = append(out, def)
	}
	return out
}

// handlerKindToPropertyKind maps the public handler.PropertyKind enum
// to space.PropertyKind. The two track 1:1 but live in different
// packages.
func handlerKindToPropertyKind(k handler.PropertyKind) space.PropertyKind {
	switch k {
	case handler.PropertyKindString:
		return space.PropertyKindString
	case handler.PropertyKindNumber:
		return space.PropertyKindNumber
	case handler.PropertyKindBoolean:
		return space.PropertyKindBoolean
	case handler.PropertyKindNull:
		return space.PropertyKindNull
	case handler.PropertyKindArray:
		return space.PropertyKindArray
	case handler.PropertyKindObject:
		return space.PropertyKindObject
	case handler.PropertyKindDatetime:
		return space.PropertyKindDatetime
	}
	return 0
}

// decodePropertyDef parses one row from a type object's `defs`
// dataset into the public PropertyDef shape. Unknown or missing
// fields default to zero values.
func decodePropertyDef(v *anyenc.Value) space.PropertyDef {
	if v == nil {
		return space.PropertyDef{}
	}
	def := space.PropertyDef{
		Id:          v.GetString("id"),
		Name:        v.GetString(typetype.FieldName),
		Description: v.GetString(typetype.FieldDescription),
		XKey:        v.GetString(typetype.FieldXKey),
		// Absent scope (pre-scope and default-synced definitions) reads
		// as synced — the historical behavior.
		Scope: space.ScopeSynced,
	}
	if k, ok := schema.ParseKind(v.GetString(typetype.FieldKind)); ok {
		def.Kind = schemaKindToPropertyKind(k)
	}
	if sc, ok := schema.ParseScope(v.GetString(typetype.FieldScope)); ok {
		def.Scope = sc
	}
	def.XFormat = types.DecodeXFormat(v.Get(typetype.FieldXFormat))
	if meta := decodeStringMap(v.GetObject(typetype.FieldMeta)); meta != nil {
		def.Meta = meta
	}
	return def
}

// decodeStringMap reads an anyenc object into a string→string map,
// skipping non-string leaves. Returns nil for an absent or empty object.
func decodeStringMap(obj *anyenc.Object) map[string]string {
	if obj == nil {
		return nil
	}
	out := map[string]string{}
	obj.Visit(func(key []byte, val *anyenc.Value) {
		if val.Type() == anyenc.TypeString {
			out[string(key)] = string(val.GetStringBytes())
		}
	})
	if len(out) == 0 {
		return nil
	}
	return out
}

// schemaKindToPropertyKind maps the internal schema.Kind enum to the
// public space.PropertyKind. Values track 1:1 but differ by package.
func schemaKindToPropertyKind(k schema.Kind) space.PropertyKind {
	switch k {
	case schema.KindString:
		return space.PropertyKindString
	case schema.KindNumber:
		return space.PropertyKindNumber
	case schema.KindBoolean:
		return space.PropertyKindBoolean
	case schema.KindNull:
		return space.PropertyKindNull
	case schema.KindArray:
		return space.PropertyKindArray
	case schema.KindObject:
		return space.PropertyKindObject
	case schema.KindDatetime:
		return space.PropertyKindDatetime
	}
	return 0
}

// RemoveProperty drops a property definition by tombstoning its record
// in the type object's defs dataset (the handler's BeforeDelete sweeps
// the sibling shortId row). Existing instance value records are NOT
// cleaned up — subsequent writes touching that propId are dropped
// op-by-op via the unknown-property rule. Returns space.ErrNotFound for
// an unknown or already-removed propId (a pre-flight, so a stray delete
// can't mint a tombstone for a record that never existed).
func (t *typesAPI) RemoveProperty(ctx context.Context, typeId, propId string) error {
	if t.staticType(typeId) {
		return fmt.Errorf("%w: %q", space.ErrTypeRegistered, typeId)
	}
	if _, err := t.findPropertyDef(ctx, typeId, propId); err != nil {
		return err
	}
	dataVersion, err := t.parent.store.DataVersion(typetype.DatasetPropertyDefs)
	if err != nil {
		return err
	}
	obj, err := t.parent.store.Get(ctx, typeId)
	if err != nil {
		return err
	}
	if _, err := obj.LocalWrite(ctx, crdt.Change{
		Dataset:     typetype.DatasetPropertyDefs,
		DataVersion: dataVersion,
		Records: []crdt.RecordChange{{
			Id:  propId,
			Ops: []crdt.Op{{Type: crdt.OpDelete}},
		}},
	}); err != nil {
		return fmt.Errorf("typesAPI: remove property: %w", err)
	}
	return nil
}

// PatchProperty applies a generic per-path patch to a property
// definition via a single multi-field $set/$unset change on the type
// object's defs dataset. Set assigns dotted-path values; Unset removes
// dotted paths (subtree removals allowed, e.g. an entire option key
// under x-format).
//
// Pinned paths (key, kind, scope, items, properties) are rejected
// up-front — the WHOLE patch fails rather than the handler silently
// dropping the offending op and applying the rest. Every other path,
// x-format included, takes any JSON value: the descriptor's vocabulary
// and its leaf-only patch rule are the consumer's.
func (t *typesAPI) PatchProperty(ctx context.Context, typeId, propId string, patch space.PropertyPatch) error {
	if t.staticType(typeId) {
		return fmt.Errorf("%w: %q", space.ErrTypeRegistered, typeId)
	}
	if len(patch.Set) == 0 && len(patch.Unset) == 0 {
		return nil
	}

	arena := &anyenc.Arena{}
	setObj := arena.NewObject()
	unsetObj := arena.NewObject()

	checkPath := func(path string) error {
		if path == "" {
			return errors.New("typesAPI: patch path is empty")
		}
		segs := strings.Split(path, ".")
		for _, s := range segs {
			if s == "" {
				return fmt.Errorf("typesAPI: patch path %q has an empty segment", path)
			}
		}
		if typetype.IsPinnedPath(segs) {
			return fmt.Errorf("%w: path %q is pinned after first write", space.ErrPinnedField, path)
		}
		return nil
	}

	for path, val := range patch.Set {
		if err := checkPath(path); err != nil {
			return err
		}
		v, err := goToAnyenc(arena, val)
		if err != nil {
			return fmt.Errorf("typesAPI: patch property: convert %q: %w", path, err)
		}
		setObj.Set(path, v)
	}
	for _, path := range patch.Unset {
		if err := checkPath(path); err != nil {
			return err
		}
		unsetObj.Set(path, arena.NewNull())
	}

	// Existence pre-flight: with Upsert=false a modify against an
	// unknown propId would silently no-op.
	if _, err := t.findPropertyDef(ctx, typeId, propId); err != nil {
		return err
	}

	var ops []crdt.Op
	if len(patch.Set) > 0 {
		ops = append(ops, crdt.Op{Type: crdt.OpSet, Payload: setObj})
	}
	if len(patch.Unset) > 0 {
		ops = append(ops, crdt.Op{Type: crdt.OpUnset, Payload: unsetObj})
	}
	dataVersion, err := t.parent.store.DataVersion(typetype.DatasetPropertyDefs)
	if err != nil {
		return err
	}
	obj, err := t.parent.store.Get(ctx, typeId)
	if err != nil {
		return err
	}
	if _, err := obj.LocalWrite(ctx, crdt.Change{
		Dataset:     typetype.DatasetPropertyDefs,
		DataVersion: dataVersion,
		Records: []crdt.RecordChange{{
			Id:  propId,
			Ops: ops,
		}},
	}); err != nil {
		return fmt.Errorf("typesAPI: patch property: %w", err)
	}
	return nil
}

// findPropertyDef reads one property definition off the type's defs
// dataset. Returns space.ErrNotFound for unknown ids or tombstones.
func (t *typesAPI) findPropertyDef(ctx context.Context, typeId, propId string) (space.PropertyDef, error) {
	coll, err := t.parent.store.OpenObjectCollection(ctx, typeId, typetype.DatasetPropertyDefs)
	if err != nil {
		if errors.Is(err, anystore.ErrCollectionNotFound) {
			return space.PropertyDef{}, space.ErrNotFound
		}
		return space.PropertyDef{}, fmt.Errorf("typesAPI: open defs %s: %w", typeId, err)
	}
	doc, err := coll.FindId(ctx, propId)
	if err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return space.PropertyDef{}, space.ErrNotFound
		}
		return space.PropertyDef{}, fmt.Errorf("typesAPI: find def %s: %w", propId, err)
	}
	v := doc.Value()
	if v == nil || v.Get(crdt.DeletedAtField) != nil {
		return space.PropertyDef{}, space.ErrNotFound
	}
	return decodePropertyDef(v), nil
}

// encodeXFormat converts a descriptor bag for the record payload: one
// whole object, the handler's single creation shape. The bag is
// opaque — converted, never inspected — with one guard: a map whose
// only key is an extended-JSON wrapper (`$date`, `$oid`, …) converts
// to a scalar, which the handler would reject with a raw validation
// error, so it is refused here with a clean one.
func encodeXFormat(arena *anyenc.Arena, m map[string]any) (*anyenc.Value, error) {
	xf, err := goToAnyenc(arena, m)
	if err != nil {
		return nil, err
	}
	if xf.Type() != anyenc.TypeObject {
		return nil, errors.New("XFormat must be a JSON object (not an extended-JSON wrapper)")
	}
	return xf, nil
}

// propertyKindLabel maps the public PropertyKind enum to the on-wire
// label typetype.PropertyHandler expects.
func propertyKindLabel(k space.PropertyKind) string {
	switch k {
	case space.PropertyKindString:
		return "string"
	case space.PropertyKindNumber:
		return "number"
	case space.PropertyKindBoolean:
		return "boolean"
	case space.PropertyKindNull:
		return "null"
	case space.PropertyKindArray:
		return "array"
	case space.PropertyKindObject:
		return "object"
	case space.PropertyKindDatetime:
		return "datetime"
	default:
		return ""
	}
}
