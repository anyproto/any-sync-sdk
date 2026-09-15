package spaceimpl

// Parts and dataset definitions — the TypesAPI dataset half.
// Definitions live as CRDT records on the type object's `datasets`
// dataset (typetype.DatasetDefs): one record per part, one head per
// dataset, one per field. Writes here mirror the property-def writers
// (AddProperty / RemoveProperty / PatchProperty) one file over.

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
		Id:          draft.Key,
		Name:        draft.Name,
		Description: draft.Description,
		Scope:       draft.Scope,
		Required:    draft.Required,
		MutableBy:   draft.MutableBy,
		Stamp:       draft.Stamp,
		XFormat:     draft.XFormat,
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

// normalizeDatasetDraft fills the draft's defaults — records when no
// module is named, the canonical collection as the key of a shared
// dataset — and applies the collection rule, returning the collection
// the declaration will address. The same rule the compiler applies, so
// a draft accepted here compiles valid on every peer that carries the
// module.
func normalizeDatasetDraft(modules types.Modules, typeId string, draft *space.DatasetDraft) (string, error) {
	if draft.Module == "" {
		draft.Module = space.RecordsModule
	}
	if draft.Shared && draft.Key == "" {
		if mi, ok := modules[draft.Module]; ok {
			draft.Key = mi.Canonical
		}
	}
	return modules.Collection(typeId, draft.Key, draft.Module, draft.Shared)
}

// checkReservedModule refuses a runtime draft naming a reserved module
// (handler.Module.Reserved): the consumer's own installs declare it,
// nothing else. Draft-time only — an applied declaration stays valid.
func checkReservedModule(modules types.Modules, draft *space.DatasetDraft) error {
	if modules.Reserved(draft.Module) {
		return fmt.Errorf("typesAPI: dataset %q: %w: %q", draft.Key, space.ErrModuleReserved, draft.Module)
	}
	return nil
}

// draftToDecl assembles and validates the full declaration a draft
// describes — the same shape the catalog compiler will produce once the
// records sync, so a draft rejected here can never half-register. A
// module-served dataset carries no declaration of its own.
func draftToDecl(draft *space.DatasetDraft) (schema.Dataset, error) {
	ds := schema.Dataset{
		Dynamic:   draft.Dynamic,
		DeleteBy:  draft.DeleteBy,
		IdRule:    draft.IdRule,
		IdPattern: draft.IdPattern,
		IdMaxLen:  draft.IdMaxLen,
		Search:    draft.Search,
	}
	if err := typetype.ValidateKey("dataset", draft.Key); err != nil {
		return ds, err
	}
	if draft.Module != "" && draft.Module != space.RecordsModule {
		if len(draft.Fields) > 0 {
			return ds, fmt.Errorf("%w: dataset %q: a %q dataset declares no fields", space.ErrModuleOwned, draft.Key, draft.Module)
		}
		return ds, nil
	}
	for i := range draft.Fields {
		f, err := draftFieldDecl(&draft.Fields[i])
		if err != nil {
			return ds, err
		}
		ds.Fields = append(ds.Fields, f)
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

// encodePart builds the part record payload from a draft.
func encodePart(arena *anyenc.Arena, draft *space.PartDraft) (*anyenc.Value, error) {
	payload := arena.NewObject()
	payload.Set(typetype.DefFieldDef, arena.NewString(typetype.DefKindPart))
	payload.Set(typetype.FieldKey, arena.NewString(draft.Key))
	if draft.Name != "" {
		payload.Set(typetype.FieldName, arena.NewString(draft.Name))
	}
	if draft.Icon != "" {
		payload.Set(typetype.PartFieldIcon, arena.NewString(draft.Icon))
	}
	if draft.Pos != "" {
		payload.Set(typetype.PartFieldPos, arena.NewString(draft.Pos))
	}
	if draft.Hidden {
		payload.Set(typetype.PartFieldHidden, arena.NewTrue())
	}
	if len(draft.UI) > 0 {
		ui, err := encodeXFormat(arena, draft.UI)
		if err != nil {
			return nil, fmt.Errorf("typesAPI: part %q: UI: %w", draft.Key, err)
		}
		payload.Set(typetype.PartFieldUI, ui)
	}
	if len(draft.Uses) > 0 {
		uses := arena.NewArray()
		for i, u := range draft.Uses {
			uses.SetArrayItem(i, arena.NewString(u))
		}
		payload.Set(typetype.PartFieldUses, uses)
	}
	return payload, nil
}

// encodeDatasetHead builds the head record payload from a draft.
func encodeDatasetHead(arena *anyenc.Arena, partId string, draft *space.DatasetDraft) *anyenc.Value {
	payload := arena.NewObject()
	payload.Set(typetype.DefFieldDef, arena.NewString(typetype.DefKindDataset))
	payload.Set(typetype.FieldKey, arena.NewString(draft.Key))
	payload.Set(typetype.DefFieldModule, arena.NewString(draft.Module))
	payload.Set(typetype.DefFieldPart, arena.NewString(partId))
	if draft.Shared {
		payload.Set(typetype.DefFieldShared, arena.NewTrue())
	}
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
// declaration. The descriptor bag rides as one whole object — the
// handler's single creation shape — converted, never inspected.
func encodeDatasetField(arena *anyenc.Arena, headId string, decl *schema.Field) (*anyenc.Value, error) {
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
	if decl.Description != "" {
		payload.Set(typetype.FieldDescription, arena.NewString(decl.Description))
	}
	if len(decl.XFormat) > 0 {
		xf, err := encodeXFormat(arena, decl.XFormat)
		if err != nil {
			return nil, fmt.Errorf("typesAPI: field %q: XFormat: %w", decl.Id, err)
		}
		payload.Set(typetype.FieldXFormat, xf)
	}
	return payload, nil
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
	return t.parent.localWriteRetry(ctx, obj, typeId, crdt.Change{
		Dataset:     typetype.DatasetDefs,
		DataVersion: dataVersion,
		Records:     recs,
	})
}

// newDefId mints a part / head record id client-side. The id must be
// known before the write so the records referencing it (heads on a
// part, fields on a head) can ride the SAME change: one atomic change
// means a crash can never strand an orphan. Uniqueness comes from
// randomness (concurrent same-key definitions are distinct records,
// folded by the compiler).
func newDefId(prefix string) (string, error) {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("typesAPI: mint definition id: %w", err)
	}
	return prefix + hex.EncodeToString(b[:]), nil
}

// compiled returns the type's compiled parts and datasets (empty, not
// nil, when nothing is declared).
func (t *typesAPI) compiled(ctx context.Context, typeId string) (*types.CompiledType, error) {
	ct, err := t.parent.store.TypeParts(ctx, typeId)
	if err != nil {
		return nil, err
	}
	if ct == nil {
		ct = &types.CompiledType{}
	}
	return ct, nil
}

// preflightDataset normalizes and validates one dataset draft against
// the module catalog and the type's current declarations: the key is
// free (or the caller is re-declaring under a new part — refused), and
// at most one shared dataset per module per type.
func (t *typesAPI) preflightDataset(typeId string, draft *space.DatasetDraft, existing *types.CompiledType, sibling map[string]struct{}) error {
	if _, err := normalizeDatasetDraft(t.parent.store.Modules(), typeId, draft); err != nil {
		return fmt.Errorf("typesAPI: dataset %q: %w", draft.Key, err)
	}
	if err := checkReservedModule(t.parent.store.Modules(), draft); err != nil {
		return err
	}
	if _, err := draftToDecl(draft); err != nil {
		return err
	}
	if _, dup := sibling[draft.Key]; dup {
		return fmt.Errorf("typesAPI: dataset %q declared twice", draft.Key)
	}
	sibling[draft.Key] = struct{}{}
	if existing == nil {
		return nil
	}
	for _, ds := range existing.Datasets {
		if ds.Key == draft.Key {
			return fmt.Errorf("typesAPI: dataset %q is already declared on type %q", draft.Key, typeId)
		}
		if draft.Shared && ds.Shared && ds.Module == draft.Module && !ds.Invalid {
			return fmt.Errorf("typesAPI: type %q already declares a shared %q dataset (%q)", typeId, draft.Module, ds.Key)
		}
	}
	return nil
}

// preflightPart validates a part draft and its datasets against the
// type's current declarations.
func (t *typesAPI) preflightPart(typeId string, draft *space.PartDraft, existing *types.CompiledType) error {
	if err := typetype.ValidateKey("part", draft.Key); err != nil {
		return err
	}
	if existing != nil {
		for _, p := range existing.Parts {
			if p.Key == draft.Key {
				return fmt.Errorf("typesAPI: part %q is already declared on type %q", draft.Key, typeId)
			}
		}
	}
	sibling := map[string]struct{}{}
	shared := map[string]struct{}{}
	for i := range draft.Datasets {
		d := &draft.Datasets[i]
		if err := t.preflightDataset(typeId, d, existing, sibling); err != nil {
			return err
		}
		if d.Shared {
			if _, dup := shared[d.Module]; dup {
				return fmt.Errorf("typesAPI: part %q declares two shared %q datasets", draft.Key, d.Module)
			}
			shared[d.Module] = struct{}{}
		}
	}
	return nil
}

// partRecords builds the records of one part: the part record plus
// every dataset's head and fields, all referencing ids minted here so
// they ride one change. Returns the part id with the records.
func partRecords(arena *anyenc.Arena, draft *space.PartDraft) (string, []crdt.RecordChange, error) {
	partId, err := newDefId("prt")
	if err != nil {
		return "", nil, err
	}
	payload, err := encodePart(arena, draft)
	if err != nil {
		return "", nil, err
	}
	recs := []crdt.RecordChange{{
		Id:     partId,
		Upsert: true,
		Ops:    []crdt.Op{{Type: crdt.OpSet, Payload: payload}},
	}}
	for i := range draft.Datasets {
		_, r, err := datasetDefRecords(arena, partId, &draft.Datasets[i])
		if err != nil {
			return "", nil, err
		}
		recs = append(recs, r...)
	}
	return partId, recs, nil
}

// datasetDefRecords validates the draft and builds its head + field
// records (one head under partId, fields referencing it). Returns the
// minted head id with the records.
func datasetDefRecords(arena *anyenc.Arena, partId string, draft *space.DatasetDraft) (string, []crdt.RecordChange, error) {
	decl, err := draftToDecl(draft)
	if err != nil {
		return "", nil, err
	}
	headId, err := newDefId("dsd")
	if err != nil {
		return "", nil, err
	}
	recs := make([]crdt.RecordChange, 0, len(draft.Fields)+1)
	recs = append(recs, crdt.RecordChange{
		Id:     headId,
		Upsert: true,
		Ops:    []crdt.Op{{Type: crdt.OpSet, Payload: encodeDatasetHead(arena, partId, draft)}},
	})
	for i := range decl.Fields {
		payload, err := encodeDatasetField(arena, headId, &decl.Fields[i])
		if err != nil {
			return "", nil, err
		}
		recs = append(recs, crdt.RecordChange{
			Upsert: true, // empty Id → fieldDefId derived from ChangeId
			Ops:    []crdt.Op{{Type: crdt.OpSet, Payload: payload}},
		})
	}
	return headId, recs, nil
}

func (t *typesAPI) Parts(ctx context.Context, typeId string) ([]space.PartDef, error) {
	var ct *types.CompiledType
	var err error
	if rt, ok := t.findRegisteredType(typeId); ok {
		ct, err = t.registeredTypeParts(rt)
	} else {
		if err = t.requireType(ctx, typeId); err != nil {
			return nil, err
		}
		ct, err = t.compiled(ctx, typeId)
	}
	if err != nil {
		return nil, err
	}
	out := make([]space.PartDef, 0, len(ct.Parts))
	for i := range ct.Parts {
		out = append(out, compiledToPartDef(&ct.Parts[i]))
	}
	return out, nil
}

func (t *typesAPI) AddPart(ctx context.Context, typeId string, draft space.PartDraft) (string, error) {
	if t.staticType(typeId) {
		return "", fmt.Errorf("%w: %q", space.ErrTypeRegistered, typeId)
	}
	if err := t.requireType(ctx, typeId); err != nil {
		return "", err
	}
	existing, err := t.compiled(ctx, typeId)
	if err != nil {
		return "", err
	}
	if err := t.preflightPart(typeId, &draft, existing); err != nil {
		return "", err
	}
	partId, recs, err := partRecords(&anyenc.Arena{}, &draft)
	if err != nil {
		return "", err
	}
	if _, err := t.writeDatasetDefs(ctx, typeId, recs...); err != nil {
		return "", fmt.Errorf("typesAPI: part %q: %w", draft.Key, err)
	}
	return partId, nil
}

func (t *typesAPI) PatchPart(ctx context.Context, typeId, partId string, patch space.DatasetDefPatch) error {
	if t.staticType(typeId) {
		return fmt.Errorf("%w: %q", space.ErrTypeRegistered, typeId)
	}
	if err := t.requireType(ctx, typeId); err != nil {
		return err
	}
	if partId == "" {
		return errors.New("typesAPI: part id required")
	}
	if len(patch.Set) == 0 && len(patch.Unset) == 0 {
		return nil
	}
	arena := &anyenc.Arena{}
	setObj := arena.NewObject()
	unsetObj := arena.NewObject()
	checkPath := func(path string) ([]string, error) {
		if path == "" {
			return nil, errors.New("typesAPI: patch path is empty")
		}
		segs := strings.Split(path, ".")
		for _, s := range segs {
			if s == "" {
				return nil, fmt.Errorf("typesAPI: patch path %q has an empty segment", path)
			}
		}
		if typetype.IsPartPinnedPath(segs) {
			return nil, fmt.Errorf("%w: path %q is pinned after first write", space.ErrPinnedField, path)
		}
		return segs, nil
	}
	for path, val := range patch.Set {
		segs, err := checkPath(path)
		if err != nil {
			return err
		}
		v, err := goToAnyenc(arena, val)
		if err != nil {
			return fmt.Errorf("typesAPI: patch part: convert %q: %w", path, err)
		}
		switch segs[0] {
		case typetype.PartFieldUI:
			if v.Type() != anyenc.TypeObject {
				return fmt.Errorf("%w: ui must be an object", space.ErrInvalidFieldValue)
			}
		case typetype.PartFieldUses:
			if v.Type() != anyenc.TypeArray {
				return fmt.Errorf("%w: uses must be an array of dataset keys", space.ErrInvalidFieldValue)
			}
			arr, _ := v.Array()
			for _, e := range arr {
				if e == nil || e.Type() != anyenc.TypeString {
					return fmt.Errorf("%w: uses entries must be strings", space.ErrInvalidFieldValue)
				}
			}
		case typetype.PartFieldHidden:
			if v.Type() != anyenc.TypeTrue && v.Type() != anyenc.TypeFalse {
				return fmt.Errorf("%w: hidden must be a boolean", space.ErrInvalidFieldValue)
			}
		default:
			if v.Type() != anyenc.TypeString {
				return fmt.Errorf("%w: %s must be a string", space.ErrInvalidFieldValue, path)
			}
		}
		setObj.Set(path, v)
	}
	for _, path := range patch.Unset {
		if _, err := checkPath(path); err != nil {
			return err
		}
		unsetObj.Set(path, arena.NewNull())
	}
	ct, err := t.compiled(ctx, typeId)
	if err != nil {
		return err
	}
	found := false
	for _, p := range ct.Parts {
		if p.Id == partId {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("%w: part %q on type %q", space.ErrNotFound, partId, typeId)
	}
	var ops []crdt.Op
	if len(patch.Set) > 0 {
		ops = append(ops, crdt.Op{Type: crdt.OpSet, Payload: setObj})
	}
	if len(patch.Unset) > 0 {
		ops = append(ops, crdt.Op{Type: crdt.OpUnset, Payload: unsetObj})
	}
	if _, err := t.writeDatasetDefs(ctx, typeId, crdt.RecordChange{Id: partId, Ops: ops}); err != nil {
		return fmt.Errorf("typesAPI: patch part: %w", err)
	}
	return nil
}

// RemovePart tombstones the part, every concurrent duplicate of it
// (same key), and every head declaring one of its datasets — the
// winner and the hidden duplicates alike, so nothing resurfaces once
// the winner is gone. Field records go orphan and fold out.
func (t *typesAPI) RemovePart(ctx context.Context, typeId, partId string) error {
	if t.staticType(typeId) {
		return fmt.Errorf("%w: %q", space.ErrTypeRegistered, typeId)
	}
	if err := t.requireType(ctx, typeId); err != nil {
		return err
	}
	if partId == "" {
		return errors.New("typesAPI: part id required")
	}
	ct, err := t.compiled(ctx, typeId)
	if err != nil {
		return err
	}
	var part *types.CompiledPart
	for i := range ct.Parts {
		if ct.Parts[i].Id == partId {
			part = &ct.Parts[i]
			break
		}
	}
	if part == nil {
		return fmt.Errorf("%w: part %q on type %q", space.ErrNotFound, partId, typeId)
	}
	ids, err := t.parent.store.PartIds(ctx, typeId, part.Key)
	if err != nil {
		return fmt.Errorf("typesAPI: remove part: %w", err)
	}
	if len(ids) == 0 {
		ids = []string{partId}
	}
	for _, ds := range part.Datasets {
		heads, err := t.parent.store.DatasetHeadIds(ctx, typeId, ds.Key)
		if err != nil {
			return fmt.Errorf("typesAPI: remove part: %w", err)
		}
		ids = append(ids, heads...)
	}
	recs := make([]crdt.RecordChange, 0, len(ids))
	for _, id := range ids {
		recs = append(recs, crdt.RecordChange{Id: id, Ops: []crdt.Op{{Type: crdt.OpDelete}}})
	}
	if _, err := t.writeDatasetDefs(ctx, typeId, recs...); err != nil {
		return fmt.Errorf("typesAPI: remove part: %w", err)
	}
	return nil
}

func (t *typesAPI) AddDataset(ctx context.Context, typeId, partId string, draft space.DatasetDraft) (string, error) {
	if t.staticType(typeId) {
		return "", fmt.Errorf("%w: %q", space.ErrTypeRegistered, typeId)
	}
	if err := t.requireType(ctx, typeId); err != nil {
		return "", err
	}
	if partId == "" {
		return "", errors.New("typesAPI: part id required")
	}
	existing, err := t.compiled(ctx, typeId)
	if err != nil {
		return "", err
	}
	found := false
	for _, p := range existing.Parts {
		if p.Id == partId {
			found = true
			break
		}
	}
	if !found {
		return "", fmt.Errorf("%w: part %q on type %q", space.ErrNotFound, partId, typeId)
	}
	if err := t.preflightDataset(typeId, &draft, existing, map[string]struct{}{}); err != nil {
		return "", err
	}
	headId, recs, err := datasetDefRecords(&anyenc.Arena{}, partId, &draft)
	if err != nil {
		return "", err
	}
	if _, err := t.writeDatasetDefs(ctx, typeId, recs...); err != nil {
		return "", fmt.Errorf("typesAPI: dataset %q: %w", draft.Key, err)
	}
	return headId, nil
}

func (t *typesAPI) AddDatasetField(ctx context.Context, typeId, datasetDefId string, draft space.DatasetFieldDraft) (string, error) {
	if t.staticType(typeId) {
		return "", fmt.Errorf("%w: %q", space.ErrTypeRegistered, typeId)
	}
	if err := t.requireType(ctx, typeId); err != nil {
		return "", err
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
	if def.Module != space.RecordsModule {
		return "", fmt.Errorf("%w: dataset %q is served by %q", space.ErrModuleOwned, def.Key, def.Module)
	}
	for _, f := range def.Fields {
		if f.Key == decl.Id {
			return "", fmt.Errorf("typesAPI: dataset %q already declares field %q", def.Key, decl.Id)
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
		return "", fmt.Errorf("typesAPI: field %q would invalidate dataset %q: %w", decl.Id, def.Key, err)
	}
	arena := &anyenc.Arena{}
	payload, err := encodeDatasetField(arena, datasetDefId, &decl)
	if err != nil {
		return "", err
	}
	res, err := t.writeDatasetDefs(ctx, typeId, crdt.RecordChange{
		Upsert: true,
		Ops:    []crdt.Op{{Type: crdt.OpSet, Payload: payload}},
	})
	if err != nil {
		return "", err
	}
	if len(res.RecordIds) == 0 {
		return "", errors.New("typesAPI: dataset field write returned no record id")
	}
	return res.RecordIds[0], nil
}

// RemoveDataset tombstones the definition and every live duplicate
// head declaring the same key (concurrent declarations converge to
// one visible definition; the hidden duplicates would otherwise
// resurface as the dataset the moment the winner is removed).
func (t *typesAPI) RemoveDataset(ctx context.Context, typeId, datasetDefId string) error {
	if t.staticType(typeId) {
		return fmt.Errorf("%w: %q", space.ErrTypeRegistered, typeId)
	}
	if err := t.requireType(ctx, typeId); err != nil {
		return err
	}
	if datasetDefId == "" {
		return errors.New("typesAPI: definition id required")
	}
	// Resolve the id's dataset key from the raw heads — the caller may
	// hold a LOSING duplicate's id (a DefId read before convergence),
	// and removing by it must remove the DATASET: the winner and every
	// duplicate, not just the hidden head.
	ids := []string{datasetDefId}
	key, err := t.parent.store.DatasetHeadKey(ctx, typeId, datasetDefId)
	if err != nil {
		return fmt.Errorf("typesAPI: remove dataset definition: %w", err)
	}
	if key != "" {
		heads, err := t.parent.store.DatasetHeadIds(ctx, typeId, key)
		if err != nil {
			return fmt.Errorf("typesAPI: remove dataset definition: %w", err)
		}
		for _, h := range heads {
			if h != datasetDefId {
				ids = append(ids, h)
			}
		}
	}
	recs := make([]crdt.RecordChange, 0, len(ids))
	for _, id := range ids {
		recs = append(recs, crdt.RecordChange{Id: id, Ops: []crdt.Op{{Type: crdt.OpDelete}}})
	}
	if _, err := t.writeDatasetDefs(ctx, typeId, recs...); err != nil {
		return fmt.Errorf("typesAPI: remove dataset definition: %w", err)
	}
	return nil
}

func (t *typesAPI) RemoveDatasetField(ctx context.Context, typeId, fieldDefId string) error {
	if t.staticType(typeId) {
		return fmt.Errorf("%w: %q", space.ErrTypeRegistered, typeId)
	}
	if err := t.requireType(ctx, typeId); err != nil {
		return err
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
					return fmt.Errorf("typesAPI: removing field %q would invalidate dataset %q: %w", def.Fields[j].Key, def.Key, verr)
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
	if err := t.requireType(ctx, typeId); err != nil {
		return err
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
	if err := t.requireType(ctx, typeId); err != nil {
		return err
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

// PatchDatasetField edits one field record's mutable leaves — name,
// description, x-format and every path under it — via one multi-field
// $set/$unset change. Pinned paths (the behavioral declaration) are
// rejected up-front so the whole patch fails rather than partially
// applying; the record must be a live field of typeId (with
// Upsert=false a modify against an unknown id would silently no-op).
func (t *typesAPI) PatchDatasetField(ctx context.Context, typeId, fieldDefId string, patch space.DatasetDefPatch) error {
	if t.staticType(typeId) {
		return fmt.Errorf("%w: %q", space.ErrTypeRegistered, typeId)
	}
	if err := t.requireType(ctx, typeId); err != nil {
		return err
	}
	if fieldDefId == "" {
		return errors.New("typesAPI: field definition id required")
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
		if typetype.IsDatasetFieldPinnedPath(segs) {
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
			return fmt.Errorf("typesAPI: patch dataset field: convert %q: %w", path, err)
		}
		setObj.Set(path, v)
	}
	for _, path := range patch.Unset {
		if err := checkPath(path); err != nil {
			return err
		}
		unsetObj.Set(path, arena.NewNull())
	}

	defs, err := t.Datasets(ctx, typeId)
	if err != nil {
		return err
	}
	found := false
	for i := range defs {
		for _, f := range defs[i].Fields {
			if f.Id == fieldDefId {
				found = true
			}
		}
	}
	if !found {
		return fmt.Errorf("%w: field definition %q on type %q", space.ErrNotFound, fieldDefId, typeId)
	}

	var ops []crdt.Op
	if len(patch.Set) > 0 {
		ops = append(ops, crdt.Op{Type: crdt.OpSet, Payload: setObj})
	}
	if len(patch.Unset) > 0 {
		ops = append(ops, crdt.Op{Type: crdt.OpUnset, Payload: unsetObj})
	}
	if _, err := t.writeDatasetDefs(ctx, typeId, crdt.RecordChange{Id: fieldDefId, Ops: ops}); err != nil {
		return fmt.Errorf("typesAPI: patch dataset field: %w", err)
	}
	return nil
}

func (t *typesAPI) Datasets(ctx context.Context, typeId string) ([]space.DatasetDef, error) {
	var compiled []types.CompiledDataset
	if rt, ok := t.findRegisteredType(typeId); ok {
		ct, err := t.registeredTypeParts(rt)
		if err != nil {
			return nil, err
		}
		compiled = ct.Datasets
	} else {
		if err := t.requireType(ctx, typeId); err != nil {
			return nil, err
		}
		var err error
		if compiled, err = t.parent.store.DatasetDefs(ctx, typeId); err != nil {
			return nil, err
		}
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
// describes — for combined re-validation on additive evolution.
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
		shape := f.Shape
		if shape == nil {
			shape = schema.Leaf(datasetPropertyKindToSchema(f.Kind))
		}
		ds.Fields = append(ds.Fields, schema.Field{
			Id:          f.Key,
			Name:        f.Name,
			Description: f.Description,
			Schema:      shape,
			Scope:       f.Scope,
			Required:    f.Required,
			MutableBy:   f.MutableBy,
			Stamp:       f.Stamp,
			XFormat:     f.XFormat,
		})
	}
	return ds
}

func compiledToPartDef(p *types.CompiledPart) space.PartDef {
	def := space.PartDef{
		Id:     p.Id,
		Key:    p.Key,
		Name:   p.Name,
		Icon:   p.Icon,
		Pos:    p.Pos,
		Hidden: p.Hidden,
		UI:     types.CloneXFormat(p.UI),
		Uses:   append([]string(nil), p.Uses...),
	}
	for i := range p.Datasets {
		def.Datasets = append(def.Datasets, compiledToDatasetDef(&p.Datasets[i]))
	}
	return def
}

func compiledToDatasetDef(c *types.CompiledDataset) space.DatasetDef {
	def := space.DatasetDef{
		Id:            c.DefId,
		Key:           c.Key,
		Collection:    c.Name,
		Module:        c.Module,
		Shared:        c.Shared,
		PartId:        c.PartId,
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
			Key:         f.Id,
			Name:        f.Name,
			Description: f.Description,
			Shape:       f.Schema,
			Scope:       f.Scope,
			Required:    f.Required,
			MutableBy:   f.MutableBy,
			Stamp:       f.Stamp,
			XFormat:     f.XFormat,
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
