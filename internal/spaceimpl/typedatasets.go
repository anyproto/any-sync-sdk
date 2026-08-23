package spaceimpl

// Runtime dataset definitions — the TypesAPI dataset half (SYN-147).
// Definitions live as CRDT records on the type object's `datasets`
// dataset (typetype.DatasetDefs); writes here mirror the property-def
// writers (AddProperty / RemoveProperty / PatchProperty) one file over.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/types"
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
	"github.com/anyproto/any-sync-sdk/space"
)

// datasetPropertyKindToSchema maps the public PropertyKind onto the
// schema Kind (zero maps to KindUnknown — "unset").
func datasetPropertyKindToSchema(k space.PropertyKind) schema.Kind {
	switch k {
	case space.PropertyKindString:
		return schema.KindString
	case space.PropertyKindNumber:
		return schema.KindNumber
	case space.PropertyKindBoolean:
		return schema.KindBoolean
	case space.PropertyKindNull:
		return schema.KindNull
	case space.PropertyKindArray:
		return schema.KindArray
	case space.PropertyKindObject:
		return schema.KindObject
	case space.PropertyKindDatetime:
		return schema.KindDatetime
	}
	return schema.KindUnknown
}

// draftFieldDecl resolves one field draft into its schema.Field form,
// defaulting stamped kinds (creator ⇒ string, times ⇒ datetime).
func draftFieldDecl(draft *space.DatasetFieldDraft) (schema.Field, error) {
	f := schema.Field{
		Id:        draft.Key,
		Name:      draft.Name,
		Scope:     draft.Scope,
		Required:  draft.Required,
		MutableBy: draft.MutableBy,
		Stamp:     draft.Stamp,
	}
	kind := datasetPropertyKindToSchema(draft.Kind)
	if draft.Shape != nil {
		if kind != schema.KindUnknown && kind != draft.Shape.Kind {
			return f, fmt.Errorf("typesAPI: field %q: Kind and Shape.Kind disagree", draft.Key)
		}
		kind = draft.Shape.Kind
	}
	if kind == schema.KindUnknown {
		switch draft.Stamp {
		case space.StampCreator:
			kind = schema.KindString
		case space.StampCreateTime, space.StampModifyTime:
			kind = schema.KindDatetime
		default:
			return f, fmt.Errorf("typesAPI: field %q: Kind required", draft.Key)
		}
	}
	// A time stamp's value is handler-produced, so a declared kind is
	// only a way to get it wrong. Rejected rather than normalized: the
	// declaration is signed into the type object and read back by
	// discovery, so accepting one the apply path then overrides would
	// publish a lie. (Declarations already stored under the old default
	// are normalized on the read side — schema.Dataset.Normalized.)
	switch draft.Stamp {
	case space.StampCreateTime, space.StampModifyTime:
		if kind != schema.KindDatetime {
			return f, fmt.Errorf("typesAPI: field %q: stamp %s requires kind datetime; got %s",
				draft.Key, draft.Stamp, kind)
		}
	}
	if draft.Shape != nil {
		f.Schema = draft.Shape
		return f, nil
	}
	f.Schema = schema.Leaf(kind)
	return f, nil
}

// draftToDecl assembles and validates the full declaration a draft
// describes — the same shape the catalog compiler will produce once the
// records sync, so a draft rejected here can never half-register.
func draftToDecl(draft *space.DatasetDraft) (schema.Dataset, error) {
	ds := schema.Dataset{
		Dynamic:   draft.Dynamic,
		DeleteBy:  draft.DeleteBy,
		IdRule:    draft.IdRule,
		IdPattern: draft.IdPattern,
		IdMaxLen:  draft.IdMaxLen,
		Search:    draft.Search,
	}
	for i := range draft.Fields {
		f, err := draftFieldDecl(&draft.Fields[i])
		if err != nil {
			return ds, err
		}
		ds.Fields = append(ds.Fields, f)
	}
	if err := typetype.ValidateDatasetName(draft.Name); err != nil {
		return ds, err
	}
	if err := schema.ValidateDatasetDecl(ds); err != nil {
		return ds, err
	}
	return ds, nil
}

// encodeShapeInto writes a value shape's kind/items/properties onto a
// record payload — the encoding schema.CompileShape parses back.
func encodeShapeInto(arena *anyenc.Arena, obj *anyenc.Value, shape *schema.Schema) {
	obj.Set(typetype.FieldKind, arena.NewString(shape.Kind.String()))
	if shape.Kind == schema.KindArray && shape.Items != nil {
		items := arena.NewObject()
		encodeShapeInto(arena, items, shape.Items)
		obj.Set(typetype.FieldItems, items)
	}
	if shape.Kind == schema.KindObject && shape.Properties != nil {
		props := arena.NewObject()
		for k, sub := range shape.Properties {
			node := arena.NewObject()
			encodeShapeInto(arena, node, sub)
			props.Set(k, node)
		}
		obj.Set(typetype.FieldProperties, props)
	}
}

// encodeDatasetHead builds the head record payload from a draft.
func encodeDatasetHead(arena *anyenc.Arena, draft *space.DatasetDraft) *anyenc.Value {
	payload := arena.NewObject()
	payload.Set(typetype.DefFieldDef, arena.NewString(typetype.DefKindDataset))
	payload.Set(typetype.DefFieldName, arena.NewString(draft.Name))
	if draft.Dynamic {
		payload.Set(typetype.DefFieldDynamic, arena.NewTrue())
	}
	if draft.IdRule == space.IdUser {
		payload.Set(typetype.DefFieldIdRule, arena.NewString(draft.IdRule.String()))
		if draft.IdPattern != "" {
			payload.Set(typetype.DefFieldIdPattern, arena.NewString(draft.IdPattern))
		}
		if draft.IdMaxLen > 0 {
			payload.Set(typetype.DefFieldIdMaxLen, arena.NewNumberInt(draft.IdMaxLen))
		}
	}
	if draft.DeleteBy == space.DeleteByAuthor {
		payload.Set(typetype.DefFieldDeleteBy, arena.NewString(draft.DeleteBy.String()))
	}
	if draft.SkipHistory {
		payload.Set(typetype.DefFieldSkipHistory, arena.NewTrue())
	}
	if draft.Search != nil && (draft.Search.Title != "" || len(draft.Search.Text) > 0) {
		search := arena.NewObject()
		if draft.Search.Title != "" {
			search.Set(typetype.SearchKeyTitle, arena.NewString(draft.Search.Title))
		}
		// Canonical wire form: a single key rides as the bare string,
		// so single-field records are byte-identical to those written
		// before the array form existed.
		if text := schema.SearchTextToAnyenc(arena, draft.Search.Text); text != nil {
			search.Set(typetype.SearchKeyText, text)
		}
		if draft.Search.Scope != "" {
			search.Set(typetype.SearchKeyScope, arena.NewString(draft.Search.Scope))
		}
		payload.Set(typetype.DefFieldSearch, search)
	}
	if draft.DisplayName != "" {
		payload.Set(typetype.DefFieldDisplayName, arena.NewString(draft.DisplayName))
	}
	if draft.Description != "" {
		payload.Set(typetype.FieldDescription, arena.NewString(draft.Description))
	}
	return payload
}

// encodeDatasetField builds a field record payload from its resolved
// declaration.
func encodeDatasetField(arena *anyenc.Arena, headId string, draft *space.DatasetFieldDraft, decl *schema.Field) *anyenc.Value {
	payload := arena.NewObject()
	payload.Set(typetype.DefFieldDef, arena.NewString(typetype.DefKindField))
	payload.Set(typetype.DefFieldDataset, arena.NewString(headId))
	payload.Set(typetype.FieldKey, arena.NewString(decl.Id))
	encodeShapeInto(arena, payload, decl.Schema)
	if decl.Stamp != space.StampNone {
		payload.Set(typetype.DefFieldStamp, arena.NewString(decl.Stamp.String()))
	} else if decl.Scope != 0 && decl.Scope != space.ScopeSynced {
		payload.Set(typetype.FieldScope, arena.NewString(decl.Scope.String()))
	}
	if decl.Required {
		payload.Set(typetype.DefFieldRequired, arena.NewTrue())
	}
	if decl.MutableBy != space.MutableNever {
		payload.Set(typetype.DefFieldMutableBy, arena.NewString(decl.MutableBy.String()))
	}
	if decl.Name != "" {
		payload.Set(typetype.FieldName, arena.NewString(decl.Name))
	}
	if draft.Description != "" {
		payload.Set(typetype.FieldDescription, arena.NewString(draft.Description))
	}
	return payload
}

// writeDatasetDefs is the shared LocalWrite helper for the defs dataset.
func (t *typesAPI) writeDatasetDefs(ctx context.Context, typeId string, recs ...crdt.RecordChange) (object.WriteResult, error) {
	dataVersion, err := t.parent.store.DataVersion(typetype.DatasetDefs)
	if err != nil {
		return object.WriteResult{}, err
	}
	obj, err := t.parent.store.Get(ctx, typeId)
	if err != nil {
		return object.WriteResult{}, err
	}
	return obj.LocalWrite(ctx, crdt.Change{
		Dataset:     typetype.DatasetDefs,
		DataVersion: dataVersion,
		Records:     recs,
	})
}

// newDatasetDefId mints the head record's id client-side. The head id
// must be known before the write so the field records — which
// reference it — can ride the SAME change: one atomic change means a
// crash can never strand an orphan head with half its fields.
// Uniqueness comes from randomness (concurrent same-name definitions
// are distinct heads, resolved by the catalog's DefId tiebreak).
func newDatasetDefId() (string, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("typesAPI: mint dataset def id: %w", err)
	}
	return "dsd" + hex.EncodeToString(b[:]), nil
}

func (t *typesAPI) AddDataset(ctx context.Context, typeId string, draft space.DatasetDraft) (string, error) {
	if t.staticType(typeId) {
		return "", fmt.Errorf("%w: %q", space.ErrTypeRegistered, typeId)
	}
	return t.declareDataset(ctx, typeId, draft)
}

// declareDataset validates the draft, preflights its name against the
// static catalog and the runtime catalog, and writes head + field
// records in one change on typeId's `datasets` dataset. Returns the
// head (definition) id. Shared by AddDataset and bundle installs.
func (t *typesAPI) declareDataset(ctx context.Context, typeId string, draft space.DatasetDraft) (string, error) {
	decl, err := draftToDecl(&draft)
	if err != nil {
		return "", err
	}
	// Name collision preflight: static catalog (built-ins +
	// config-registered) and already-active runtime datasets. The
	// compile layer resolves races deterministically anyway — this
	// just fails the obvious case fast with a readable error.
	if _, err := t.parent.store.DataVersion(draft.Name); err == nil {
		return "", fmt.Errorf("typesAPI: dataset name %q is already registered", draft.Name)
	}
	if existing, ok := t.parent.store.RuntimeDataset(draft.Name); ok {
		return "", fmt.Errorf("typesAPI: dataset name %q is already defined on type %q", draft.Name, existing.TypeId)
	}

	headId, err := newDatasetDefId()
	if err != nil {
		return "", err
	}
	arena := &anyenc.Arena{}
	recs := make([]crdt.RecordChange, 0, len(draft.Fields)+1)
	recs = append(recs, crdt.RecordChange{
		Id:     headId,
		Upsert: true,
		Ops:    []crdt.Op{{Type: crdt.OpSet, Payload: encodeDatasetHead(arena, &draft)}},
	})
	for i := range draft.Fields {
		recs = append(recs, crdt.RecordChange{
			Upsert: true, // empty Id → fieldDefId derived from ChangeId
			Ops:    []crdt.Op{{Type: crdt.OpSet, Payload: encodeDatasetField(arena, headId, &draft.Fields[i], &decl.Fields[i])}},
		})
	}
	if _, err := t.writeDatasetDefs(ctx, typeId, recs...); err != nil {
		return "", fmt.Errorf("typesAPI: dataset %q: %w", draft.Name, err)
	}
	return headId, nil
}

func (t *typesAPI) AddDatasetField(ctx context.Context, typeId, datasetDefId string, draft space.DatasetFieldDraft) (string, error) {
	if t.staticType(typeId) {
		return "", fmt.Errorf("%w: %q", space.ErrTypeRegistered, typeId)
	}
	if draft.Required {
		// A required field added later would reject the dataset's own
		// history: fresh devices replay old creates against the CURRENT
		// schema. Required fields exist only from AddDataset.
		return "", errors.New("typesAPI: additive fields cannot be required — declare required fields at AddDataset")
	}
	decl, err := draftFieldDecl(&draft)
	if err != nil {
		return "", err
	}
	def, err := t.findDatasetDef(ctx, typeId, datasetDefId)
	if err != nil {
		return "", err
	}
	for _, f := range def.Fields {
		if f.Key == decl.Id {
			return "", fmt.Errorf("typesAPI: dataset %q already declares field %q", def.Name, decl.Id)
		}
	}
	// Validate the COMBINED declaration, not the field in isolation: a
	// field that invalidates the fold (author rule without a creator
	// stamp, duplicate stamp kind) would otherwise sync everywhere and
	// drop the whole dataset from every peer's catalog. Adding a field
	// that REPAIRS an invalid definition passes by the same rule.
	combined := defToDecl(&def)
	combined.Fields = append(combined.Fields, decl)
	if err := schema.ValidateDatasetDecl(combined); err != nil {
		return "", fmt.Errorf("typesAPI: field %q would invalidate dataset %q: %w", decl.Id, def.Name, err)
	}
	arena := &anyenc.Arena{}
	res, err := t.writeDatasetDefs(ctx, typeId, crdt.RecordChange{
		Upsert: true,
		Ops:    []crdt.Op{{Type: crdt.OpSet, Payload: encodeDatasetField(arena, datasetDefId, &draft, &decl)}},
	})
	if err != nil {
		return "", err
	}
	if len(res.RecordIds) == 0 {
		return "", errors.New("typesAPI: dataset field write returned no record id")
	}
	return res.RecordIds[0], nil
}

func (t *typesAPI) RemoveDataset(ctx context.Context, typeId, datasetDefId string) error {
	return t.removeDatasetDefRecord(ctx, typeId, datasetDefId)
}

func (t *typesAPI) RemoveDatasetField(ctx context.Context, typeId, fieldDefId string) error {
	if t.staticType(typeId) {
		return fmt.Errorf("%w: %q", space.ErrTypeRegistered, typeId)
	}
	// Validate the declaration MINUS the field before writing the
	// delete: removing e.g. the creator stamp of an author-gated
	// dataset would invalidate the fold and drop the dataset from
	// every peer's catalog (the AddDatasetField guard's mirror). A
	// dataset that is ALREADY invalid stays removable — cleanup must
	// never be blocked.
	defs, err := t.Datasets(ctx, typeId)
	if err != nil {
		return err
	}
	for i := range defs {
		def := &defs[i]
		for j := range def.Fields {
			if def.Fields[j].Id != fieldDefId {
				continue
			}
			if !def.Invalid {
				remaining := defToDecl(def)
				remaining.Fields = append(remaining.Fields[:j], remaining.Fields[j+1:]...)
				if verr := schema.ValidateDatasetDecl(remaining); verr != nil {
					return fmt.Errorf("typesAPI: removing field %q would invalidate dataset %q: %w", def.Fields[j].Key, def.Name, verr)
				}
			}
			return t.removeDatasetDefRecord(ctx, typeId, fieldDefId)
		}
	}
	return fmt.Errorf("typesAPI: field definition %q not found on type %q", fieldDefId, typeId)
}

func (t *typesAPI) removeDatasetDefRecord(ctx context.Context, typeId, defId string) error {
	if t.staticType(typeId) {
		return fmt.Errorf("%w: %q", space.ErrTypeRegistered, typeId)
	}
	if defId == "" {
		return errors.New("typesAPI: definition id required")
	}
	if _, err := t.writeDatasetDefs(ctx, typeId, crdt.RecordChange{
		Id:  defId,
		Ops: []crdt.Op{{Type: crdt.OpDelete}},
	}); err != nil {
		return fmt.Errorf("typesAPI: remove dataset definition: %w", err)
	}
	return nil
}

func (t *typesAPI) PatchDataset(ctx context.Context, typeId, defId string, patch space.DatasetDefPatch) error {
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
		if typetype.IsDatasetDefPinnedPath(segs) {
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
			return fmt.Errorf("typesAPI: patch dataset: convert %q: %w", path, err)
		}
		if strings.HasPrefix(path, typetype.DefFieldSearch+".") {
			segs := strings.Split(path, ".")
			if lerr := typetype.CheckSearchLeafValue(segs, v); lerr != nil {
				return fmt.Errorf("%w: search path %q: %w", space.ErrInvalidFieldValue, path, lerr)
			}
			if segs[len(segs)-1] == typetype.SearchKeyText {
				// Re-encode canonically (a single-element array becomes
				// the bare string, matching encodeDatasetHead and the
				// x-search marshal) and refuse the empty spellings —
				// clearing the mapping is Unset's job.
				text := schema.SearchTextToAnyenc(arena, schema.SearchTextFromAnyenc(v))
				if text == nil {
					return fmt.Errorf("%w: search path %q: empty text mapping — use Unset to clear", space.ErrInvalidFieldValue, path)
				}
				v = text
			}
		}
		setObj.Set(path, v)
	}
	for _, path := range patch.Unset {
		if err := checkPath(path); err != nil {
			return err
		}
		unsetObj.Set(path, arena.NewNull())
	}

	var ops []crdt.Op
	if len(patch.Set) > 0 {
		ops = append(ops, crdt.Op{Type: crdt.OpSet, Payload: setObj})
	}
	if len(patch.Unset) > 0 {
		ops = append(ops, crdt.Op{Type: crdt.OpUnset, Payload: unsetObj})
	}
	if _, err := t.writeDatasetDefs(ctx, typeId, crdt.RecordChange{Id: defId, Ops: ops}); err != nil {
		return fmt.Errorf("typesAPI: patch dataset definition: %w", err)
	}
	return nil
}

func (t *typesAPI) Datasets(ctx context.Context, typeId string) ([]space.DatasetDef, error) {
	compiled, err := t.parent.store.DatasetDefs(ctx, typeId)
	if err != nil {
		return nil, err
	}
	out := make([]space.DatasetDef, 0, len(compiled))
	for i := range compiled {
		out = append(out, compiledToDatasetDef(&compiled[i]))
	}
	return out, nil
}

// findDatasetDef resolves one compiled definition by head id.
func (t *typesAPI) findDatasetDef(ctx context.Context, typeId, defId string) (space.DatasetDef, error) {
	compiled, err := t.parent.store.DatasetDefs(ctx, typeId)
	if err != nil {
		return space.DatasetDef{}, err
	}
	for i := range compiled {
		if compiled[i].DefId == defId {
			return compiledToDatasetDef(&compiled[i]), nil
		}
	}
	return space.DatasetDef{}, fmt.Errorf("typesAPI: dataset definition %q not found on type %q", defId, typeId)
}

// defToDecl rebuilds the schema declaration a compiled DatasetDef
// describes — for combined re-validation on additive evolution. Value
// shapes reduce to leaf kinds (sufficient for the decl rules, which
// never inspect nested shapes).
func defToDecl(def *space.DatasetDef) schema.Dataset {
	ds := schema.Dataset{
		Dynamic:   def.Dynamic,
		IdRule:    def.IdRule,
		IdPattern: def.IdPattern,
		IdMaxLen:  def.IdMaxLen,
		DeleteBy:  def.DeleteBy,
		Search:    def.Search,
	}
	for _, f := range def.Fields {
		ds.Fields = append(ds.Fields, schema.Field{
			Id:        f.Key,
			Name:      f.Name,
			Schema:    schema.Leaf(datasetPropertyKindToSchema(f.Kind)),
			Scope:     f.Scope,
			Required:  f.Required,
			MutableBy: f.MutableBy,
			Stamp:     f.Stamp,
		})
	}
	return ds
}

func compiledToDatasetDef(c *types.CompiledDataset) space.DatasetDef {
	def := space.DatasetDef{
		Id:            c.DefId,
		Name:          c.Name,
		DisplayName:   c.DisplayName,
		Description:   c.Description,
		Dynamic:       c.Schema.Dynamic,
		IdRule:        c.Schema.IdRule,
		IdPattern:     c.Schema.IdPattern,
		IdMaxLen:      c.Schema.IdMaxLen,
		DeleteBy:      c.Schema.DeleteBy,
		SkipHistory:   c.SkipHistory,
		Search:        c.Search,
		Invalid:       c.Invalid,
		InvalidReason: c.InvalidReason,
	}
	for i, f := range c.Schema.Fields {
		fd := space.DatasetFieldDef{
			Key:       f.Id,
			Name:      f.Name,
			Scope:     f.Scope,
			Required:  f.Required,
			MutableBy: f.MutableBy,
			Stamp:     f.Stamp,
		}
		if i < len(c.FieldDefIds) {
			fd.Id = c.FieldDefIds[i]
		}
		if f.Schema != nil {
			fd.Kind = schemaKindToPropertyKind(f.Schema.Kind)
		}
		def.Fields = append(def.Fields, fd)
	}
	return def
}
