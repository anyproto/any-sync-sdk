package spaceimpl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"

	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	anytype "github.com/anyproto/any-sync-sdk/internal/types/any"
	"github.com/anyproto/any-sync-sdk/internal/types/spaceindex"
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
	"github.com/anyproto/any-sync-sdk/space"
)

// listTypesFilter selects rows from the per-space `objects` collection
// whose `any.types` array carries the meta-type marker and that aren't
// tombstoned. Compiled once — ParseCondition allocates and walks the
// query tree on every call, which we don't want on a hot read path.
// JSON literal (not map[string]any) because ParseCondition's map path
// goes through json.Marshal under the hood, which we'd rather skip.
var listTypesFilter = query.MustParseCondition(
	`{"any.types":{"$in":["__type__"]},"_deletedAt":{"$exists":false}}`,
)

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
// (any.name / any.description / any.icon / any.xkey) and `any.types =
// ["__type__"]` to mark it as a meta-type instance.
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
		multi.Set("any.xkey", arena.NewString(params.XKey))
	}
	// Mark the object as a meta-type instance — the convention we use
	// in MVP to distinguish types from regular objects without a
	// dedicated catalog.
	types := arena.NewArray()
	types.SetArrayItem(0, arena.NewString("__type__"))
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
	if _, ok := t.findRegisteredType(typeId); ok {
		return "", fmt.Errorf("typesAPI: %q is a registered type — properties are statically declared", typeId)
	}
	filterJSON, err := validateFormatDraft(&draft)
	if err != nil {
		return "", err
	}
	if draft.Kind == 0 {
		return "", errors.New("typesAPI: PropertyDraft.Kind required")
	}
	switch draft.Scope {
	case 0, space.ScopeSynced, space.ScopeAccount, space.ScopeLocal:
	default:
		return "", fmt.Errorf("typesAPI: PropertyDraft.Scope must be synced/account/local (derived is reserved for built-ins); got %s", draft.Scope)
	}
	arena := &anyenc.Arena{}
	payload := arena.NewObject()
	payload.Set(typetype.FieldKind, arena.NewString(propertyKindLabel(draft.Kind)))
	if draft.Format != nil {
		formatObj := arena.NewObject()
		formatObj.Set(typetype.FormatKeyType, arena.NewString(draft.Format.Type.String()))
		if draft.Format.UI != "" {
			formatObj.Set(typetype.FormatKeyUi, arena.NewString(draft.Format.UI))
		}
		if filterJSON != "" {
			formatObj.Set(typetype.FormatKeyFilter, arena.NewString(filterJSON))
		}
		payload.Set(typetype.FieldFormat, formatObj)
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
	if draft.XKind != "" {
		payload.Set(typetype.FieldXKind, arena.NewString(draft.XKind))
	}
	if len(draft.Meta) > 0 {
		metaObj := arena.NewObject()
		for k, v := range draft.Meta {
			metaObj.Set(k, arena.NewString(v))
		}
		payload.Set(typetype.FieldMeta, metaObj)
	}

	dataVersion, err := t.parent.store.DataVersion(typetype.DatasetPropertyDefs)
	if err != nil {
		return "", err
	}
	obj, err := t.parent.store.Get(ctx, typeId)
	if err != nil {
		return "", err
	}
	res, err := obj.LocalWrite(ctx, crdt.Change{
		Dataset:     typetype.DatasetPropertyDefs,
		DataVersion: dataVersion,
		Records: []crdt.RecordChange{{
			Upsert: true, // empty Id → propId derived from ChangeId
			Ops:    []crdt.Op{{Type: crdt.OpSet, Payload: payload}},
		}},
	})
	if err != nil {
		return "", err
	}
	if len(res.RecordIds) == 0 {
		return "", errors.New("typesAPI: write returned no record id")
	}
	return res.RecordIds[0], nil
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
		BuiltIn:     true,
	}
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
	out := make([]space.TypeInfo, 0, 2+len(registered))
	out = append(out, builtInAnyTypeInfo())
	out = append(out, builtInSpaceIndexTypeInfo())
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
		XKey:        rec.GetString("any", "xkey"),
		BuiltIn:     false,
	}
}

// hasTypeMarker reports whether `record.any.types` contains the
// reserved meta-type label "__type__". Used to distinguish type
// objects from regular objects without a separate catalog.
func hasTypeMarker(rec *anyenc.Value) bool {
	arr := rec.GetArray("any", "types")
	for _, v := range arr {
		if string(v.GetStringBytes()) == "__type__" {
			return true
		}
	}
	return false
}

func (t *typesAPI) Delete(_ context.Context, _ string) error {
	return errors.New("typesAPI: Delete not implemented")
}

// Properties returns the property definitions of a type. For the
// built-in `any` type, the list is hardcoded (one entry per
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
		if p.Format != nil {
			def.Format = &space.PropertyFormat{
				// handler.FormatType tracks space.FormatType 1:1.
				Type:   space.FormatType(p.Format.Type),
				UI:     p.Format.UI,
				Filter: p.Format.Filter,
			}
		}
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
		XKind:       v.GetString(typetype.FieldXKind),
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
	// A format whose type label this SDK doesn't know (written by a
	// newer SDK) reads back as nil Format — read tolerance.
	if ft, ok := space.ParseFormatType(v.GetString(typetype.FieldFormat, typetype.FormatKeyType)); ok {
		def.Format = &space.PropertyFormat{
			Type:   ft,
			UI:     v.GetString(typetype.FieldFormat, typetype.FormatKeyUi),
			Filter: v.GetString(typetype.FieldFormat, typetype.FormatKeyFilter),
		}
	}
	if metaObj := v.GetObject(typetype.FieldMeta); metaObj != nil {
		meta := map[string]string{}
		metaObj.Visit(func(key []byte, val *anyenc.Value) {
			if val.Type() == anyenc.TypeString {
				meta[string(key)] = string(val.GetStringBytes())
			}
		})
		if len(meta) > 0 {
			def.Meta = meta
		}
	}
	return def
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
	}
	return 0
}

func (t *typesAPI) RemoveProperty(_ context.Context, _, _ string) error {
	return errors.New("typesAPI: RemoveProperty not implemented")
}

// UpdatePropertyMeta mutates the CRDT-mutable definition fields via a
// single multi-field $set/$unset change on the type object's defs
// dataset. A nil pointer leaves the field unchanged; a pointer to ""
// unsets it. Format leaves are written as `format.ui` / `format.filter`
// dotted paths — never a broad `format` replace, which the handler pins
// (it could smuggle a `format.type` change). Setting a format leaf on a
// property that never declared a format is rejected: `format.type` is
// pinned-absent, and a leaf write would materialize a type-less format
// object.
//
// Like all mutable definition metadata the strings are stored opaquely
// — no ui-vocabulary or filter-syntax validation (a consumer concern).
func (t *typesAPI) UpdatePropertyMeta(ctx context.Context, typeId, propId string, update space.PropertyMetaUpdate) error {
	if _, ok := t.findRegisteredType(typeId); ok {
		return fmt.Errorf("typesAPI: %q is a registered type — properties are statically declared", typeId)
	}
	arena := &anyenc.Arena{}
	setObj := arena.NewObject()
	unsetObj := arena.NewObject()
	var sets, unsets int
	stage := func(field string, v *string) {
		if v == nil {
			return
		}
		if *v == "" {
			unsetObj.Set(field, arena.NewNull())
			unsets++
			return
		}
		setObj.Set(field, arena.NewString(*v))
		sets++
	}
	stage(typetype.FieldName, update.Name)
	stage(typetype.FieldDescription, update.Description)
	stage(typetype.FieldXKey, update.XKey)
	stage(typetype.FieldXKind, update.XKind)
	stage(typetype.FieldFormat+"."+typetype.FormatKeyUi, update.FormatUI)
	stage(typetype.FieldFormat+"."+typetype.FormatKeyFilter, update.FormatFilter)
	if sets+unsets == 0 {
		return nil
	}

	// Existence pre-flight: with Upsert=false a modify against an
	// unknown propId would silently no-op; and format-leaf writes are
	// only legal on records that pinned a format.type at creation.
	def, err := t.findPropertyDef(ctx, typeId, propId)
	if err != nil {
		return err
	}
	if (update.FormatUI != nil || update.FormatFilter != nil) && def.Format == nil {
		return fmt.Errorf("typesAPI: property %s has no format — format.ui/format.filter require one declared at AddProperty", propId)
	}

	var ops []crdt.Op
	if sets > 0 {
		ops = append(ops, crdt.Op{Type: crdt.OpSet, Payload: setObj})
	}
	if unsets > 0 {
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
		return fmt.Errorf("typesAPI: update property meta: %w", err)
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

// validateFormatDraft pre-flights draft.Format before the definition
// write: the format type must be a known enum value (FormatTags is
// rejected until the space-level tag table lands), the declared Kind —
// defaulted from the format type when zero — must satisfy the
// format→kind coupling (links ⇒ array of string; date/datetime ⇒
// string), and Filter is serialized to its JSON text. Returns the
// filter JSON to store ("" = none).
//
// Structure only: UI and the filter contents are stored opaquely — the
// SDK does not validate ui vocabulary or filter syntax (a consumer
// concern, e.g. the `any` server).
func validateFormatDraft(draft *space.PropertyDraft) (filterJSON string, err error) {
	f := draft.Format
	if f == nil {
		return "", nil
	}
	var requiredKind space.PropertyKind
	switch f.Type {
	case space.FormatLinks:
		requiredKind = space.PropertyKindArray
	case space.FormatDate, space.FormatDatetime:
		requiredKind = space.PropertyKindString
	case space.FormatTags:
		return "", errors.New("typesAPI: format `tags` is not supported yet (space-level tag table pending)")
	default:
		return "", fmt.Errorf("typesAPI: unknown PropertyFormatDraft.Type %d", f.Type)
	}
	if draft.Kind == 0 {
		draft.Kind = requiredKind
	} else if draft.Kind != requiredKind {
		return "", fmt.Errorf("typesAPI: format %q requires Kind %s; got %s",
			f.Type, propertyKindLabel(requiredKind), propertyKindLabel(draft.Kind))
	}
	if requiredKind == space.PropertyKindArray && draft.Items != nil && draft.Items.Kind != space.PropertyKindString {
		return "", fmt.Errorf("typesAPI: format %q requires string array items; got %s",
			f.Type, propertyKindLabel(draft.Items.Kind))
	}
	switch filter := f.Filter.(type) {
	case nil:
		return "", nil
	case string:
		return filter, nil
	default:
		raw, err := json.Marshal(filter)
		if err != nil {
			return "", fmt.Errorf("typesAPI: marshal PropertyFormatDraft.Filter: %w", err)
		}
		return string(raw), nil
	}
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
	default:
		return ""
	}
}
