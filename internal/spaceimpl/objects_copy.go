package spaceimpl

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/anyproto/any-store/v2/anyenc"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/types"
	anytype "github.com/anyproto/any-sync-sdk/internal/types/any"
	collectiontype "github.com/anyproto/any-sync-sdk/internal/types/collection"
	"github.com/anyproto/any-sync-sdk/internal/types/template"
	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
	"github.com/anyproto/any-sync-sdk/space"
)

// copySource is the object CreateObjectOpts.From names, resolved
// before the new tree is minted: its id, its type and the datasets
// whose records the copy carries over.
type copySource struct {
	id       string
	typeId   string
	datasets []types.CompiledDataset
}

// resolveCopy reads the source row once and folds it into opts: the
// source's type (opts.Type must be absent or equal), its collections
// minus the template marker ahead of the request's, its synced `any.*`
// values and the synced values of every owner the new object has,
// then the request's InitialProperties over them. A source filed under
// `template` also stamps any.template {id, version}. The returned
// source drives copyDatasets after the bootstrap.
func (o *objectService) resolveCopy(ctx context.Context, opts space.CreateObjectOpts) (*copySource, space.CreateObjectOpts, error) {
	row, err := o.Get(ctx, opts.From)
	if err != nil {
		if errors.Is(err, space.ErrNotFound) {
			return nil, opts, fmt.Errorf("%w: CreateObjectOpts.From %s: %w", space.ErrObjectNotFound, opts.From, err)
		}
		return nil, opts, err
	}
	srcType := string(row.GetStringBytes(anytype.TypeId, anytype.FieldType))
	if srcType == "" {
		return nil, opts, fmt.Errorf("%w: CreateObjectOpts.From %s has no type", space.ErrTypeRequired, opts.From)
	}
	if properties.IsMetaType(srcType) || srcType == typetype.MetaTypeMarker || srcType == collectiontype.MetaMarker {
		return nil, opts, fmt.Errorf("%w: CreateObjectOpts.From %s is a definition", space.ErrMetaType, opts.From)
	}
	if opts.Type != "" && opts.Type != srcType {
		return nil, opts, fmt.Errorf("%w: CreateObjectOpts.From %s is a %s, Type names %s",
			space.ErrFromTypeMismatch, opts.From, srcType, opts.Type)
	}
	opts.Type = srcType

	isTemplate := false
	seen := make(map[string]struct{})
	var collections []string
	add := func(id string) {
		if id == "" {
			return
		}
		if _, dup := seen[id]; dup {
			return
		}
		seen[id] = struct{}{}
		collections = append(collections, id)
	}
	for _, v := range row.GetArray(anytype.TypeId, anytype.FieldCollections) {
		id := string(v.GetStringBytes())
		if id == template.CollectionId {
			isTemplate = true
			continue
		}
		add(id)
	}
	for _, id := range opts.Collections {
		add(id)
	}
	opts.Collections = collections

	initial := make(map[string]map[string]any)
	put := func(owner, prop string, v any) {
		group, ok := initial[owner]
		if !ok {
			group = make(map[string]any)
			initial[owner] = group
		}
		group[prop] = v
	}
	// `any`: the synced display values; membership and the stamp are
	// the copy's own.
	for _, p := range anytype.Properties {
		if p.Scope != schema.ScopeSynced || p.Id == anytype.FieldType || p.Id == anytype.FieldCollections || p.Id == anytype.FieldTemplate {
			continue
		}
		if v := row.Get(anytype.TypeId, p.Id); v != nil {
			put(anytype.TypeId, p.Id, v)
		}
	}
	// Every owner the new object has: its declared synced values. An
	// owner the registry cannot resolve, or a value under a property
	// it no longer declares, is orphan data and stays behind.
	reg := o.parent.store.Registry()
	owners := append([]string{srcType}, opts.Collections...)
	for _, owner := range owners {
		group := row.GetObject(owner)
		if group == nil {
			continue
		}
		props, ok := reg.PropsOf(owner)
		if !ok {
			continue
		}
		for _, p := range props {
			if p.EffectiveScope() != schema.ScopeSynced {
				continue
			}
			if v := group.Get(p.Id); v != nil {
				put(owner, p.Id, v)
			}
		}
	}
	if isTemplate {
		a := &anyenc.Arena{}
		stamp := a.NewObject()
		stamp.Set(anytype.TemplateStampId, a.NewString(opts.From))
		stamp.Set(anytype.TemplateStampVersion, a.NewNumberFloat64(row.GetFloat64(template.CollectionId, template.PropVersion)))
		put(anytype.TypeId, anytype.FieldTemplate, stamp)
	}
	for owner, kv := range opts.InitialProperties {
		for prop, v := range kv {
			put(owner, prop, v)
		}
	}
	opts.InitialProperties = initial

	datasets, err := o.copyableDatasets(ctx, srcType)
	if err != nil {
		return nil, opts, err
	}
	return &copySource{id: opts.From, typeId: srcType, datasets: datasets}, opts, nil
}

// copyableDatasets lists the datasets the source's type declares
// through its parts, minus an invalid declaration and a reserved
// module's (the consumer's own install root carries that one).
func (o *objectService) copyableDatasets(ctx context.Context, typeId string) ([]types.CompiledDataset, error) {
	var compiled []types.CompiledDataset
	if rt, ok := o.parent.types.findRegisteredType(typeId); ok {
		ct, err := o.parent.types.registeredTypeParts(rt)
		if err != nil {
			return nil, err
		}
		compiled = ct.Datasets
	} else {
		var err error
		if compiled, err = o.parent.store.DatasetDefs(ctx, typeId); err != nil {
			return nil, err
		}
	}
	modules := o.parent.store.Modules()
	out := make([]types.CompiledDataset, 0, len(compiled))
	for _, ds := range compiled {
		if ds.Invalid || modules.Reserved(ds.Module) {
			continue
		}
		out = append(out, ds)
	}
	return out, nil
}

// copyDatasets writes the source's live records into the new object,
// one change per dataset, after the bootstrap set the type that admits
// them. Record ids are kept — a per-object dataset is its own
// collection and a shared one prefixes the new object's id — except
// where the generic schema handler derives them (IdAuto), which
// refuses an explicit id.
func (o *objectService) copyDatasets(ctx context.Context, src *copySource, objectId string) error {
	if len(src.datasets) == 0 {
		return nil
	}
	// A resident source controller built before a runtime dataset was
	// declared lacks its registration; the eviction must precede Get.
	for _, ds := range src.datasets {
		o.parent.store.EnsureDatasetRegistered(ctx, src.id, ds.Name)
	}
	srcObj, err := o.parent.store.Get(ctx, src.id)
	if err != nil {
		return fmt.Errorf("spaceimpl: copy from %s: %w", src.id, err)
	}
	for i := range src.datasets {
		ds := &src.datasets[i]
		rows := srcObj.Controller().Records(ctx, ds.Name)
		records := make([]space.RecordModify, 0, len(rows))
		for _, row := range rows {
			if rec, ok := copyRecord(src.id, ds, row); ok {
				records = append(records, rec)
			}
		}
		if len(records) == 0 {
			continue
		}
		if _, err := o.parent.Modify(ctx, space.ModifyBatch{ObjectId: objectId, Dataset: ds.Name, Records: records}); err != nil {
			return fmt.Errorf("spaceimpl: copy %s from %s: %w", ds.Name, src.id, err)
		}
	}
	return nil
}

// copyRecord turns one live source row into the upsert that recreates
// it: every field but the id, the `_`-prefixed system fields and the
// fields the dataset declares derived, stamped or local.
func copyRecord(srcId string, ds *types.CompiledDataset, row *anyenc.Value) (space.RecordModify, bool) {
	obj, err := row.Object()
	if err != nil {
		return space.RecordModify{}, false
	}
	skip := make(map[string]struct{}, len(ds.Schema.Fields))
	for _, f := range ds.Schema.Fields {
		if f.Stamp != 0 || (f.Scope != 0 && f.Scope != schema.ScopeSynced) {
			skip[f.Id] = struct{}{}
		}
	}
	a := &anyenc.Arena{}
	fields := a.NewObject()
	n := 0
	obj.Visit(func(k []byte, v *anyenc.Value) {
		key := string(k)
		if key == recordIdField || strings.HasPrefix(key, "_") {
			return
		}
		if _, s := skip[key]; s {
			return
		}
		fields.Set(key, v)
		n++
	})
	if n == 0 {
		return space.RecordModify{}, false
	}
	rec := space.RecordModify{Upsert: true, Ops: []space.Op{{Type: space.OpSet, Value: fields}}}
	if !(ds.Module == types.RecordsModule && ds.Schema.IdRule == schema.IdAuto) {
		rec.Id = crdt.KeyedRecordId(srcId, string(row.GetStringBytes(recordIdField)))
	}
	return rec, true
}

// recordIdField is the record's own id on an any-store row.
const recordIdField = "id"

// logCopyAbort notes a half copy that could not be reclaimed: the row
// is written and synced by the time a dataset copy fails.
func logCopyAbort(objectId string, err error) {
	objectLog.Warn("object left behind after a failed copy", zap.String("objectId", objectId), zap.Error(err))
}
