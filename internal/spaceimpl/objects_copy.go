package spaceimpl

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/anyproto/any-store/v2/anyenc"
	"go.uber.org/zap"

	"github.com/anyproto/any-sync-sdk/internal/anyencx"
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

// copyBatch bounds the records one change carries when a dataset is
// copied, so a large body becomes several DAG changes instead of one.
const copyBatch = 1000

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
// minus the registered markers ahead of the request's, its synced
// `any.*` values and the declared synced values of every owner the new
// object has, then the request's InitialProperties over them. A source
// filed under `template` also stamps any.template {id, version}. The
// returned source drives copyDatasets after the bootstrap.
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
		// A tree without its row: the bootstrap has not landed here
		// (parked behind its type definition, or still syncing). Not
		// addressable yet, like an unknown id; a retry after sync works.
		return nil, opts, fmt.Errorf("%w: CreateObjectOpts.From %s has no row here yet", space.ErrObjectNotFound, opts.From)
	}
	if properties.IsMetaType(srcType) || srcType == typetype.MetaTypeMarker || srcType == collectiontype.MetaMarker {
		return nil, opts, fmt.Errorf("%w: CreateObjectOpts.From %s is a definition", space.ErrMetaType, opts.From)
	}
	if opts.Type != "" && opts.Type != srcType {
		return nil, opts, fmt.Errorf("%w: CreateObjectOpts.From %s is a %s, Type names %s",
			space.ErrFromTypeMismatch, opts.From, srcType, opts.Type)
	}
	opts.Type = srcType

	// Registered collections are markers of the source object — binned,
	// pinned, a template — never content: none travels. The request's
	// collections are added afterwards, so a copy can be filed under
	// one deliberately (save as template).
	markers := make(map[string]struct{})
	for _, c := range o.parent.store.ExternalCollections() {
		markers[c.Id] = struct{}{}
	}
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
		}
		if _, marker := markers[id]; marker {
			continue
		}
		add(id)
	}
	// The owners whose values travel: the source's own, before the
	// request's collections join — a copy refiled under a marker takes
	// the marker's values from the request, never from the source.
	owners := append([]string{srcType}, collections...)
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
	// `any`: every synced value but membership. The stamp travels too
	// — a duplicate keeps the lineage of what it was copied from —
	// and is replaced below when the source is itself a template.
	for _, p := range anytype.Properties {
		if p.Scope != schema.ScopeSynced || p.Id == anytype.FieldType || p.Id == anytype.FieldCollections {
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
// through its parts, minus an invalid declaration and the modules a
// copy skips (Modules.SkipsCopy: reserved and NoCopy).
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
		if ds.Invalid || modules.SkipsCopy(ds.Module) {
			continue
		}
		out = append(out, ds)
	}
	return out, nil
}

// copyDatasets writes the source's live records into the new object,
// dataset by dataset in changes of at most copyBatch records, after the
// bootstrap set the type that admits them. Record ids are kept — a
// per-object dataset is its own collection and a shared one prefixes
// the new object's id — except where the generic schema handler
// derives them, which refuses an explicit id. A record the dataset's
// create rules refuse today, or a handler rejects at apply, fails the
// copy (ErrFromRecordRefused): a silent partial copy is worse than
// none.
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
		if deleted, derr := o.parent.store.TreeDeleted(ctx, src.id); derr == nil && deleted {
			return fmt.Errorf("spaceimpl: copy from %s: %w", src.id, space.ErrObjectDeleted)
		}
		return fmt.Errorf("spaceimpl: copy from %s: %w", src.id, err)
	}
	ctrl := srcObj.Controller()
	for i := range src.datasets {
		ds := &src.datasets[i]
		rows, err := copyRows(ctx, ctrl, ds.Name)
		if err != nil {
			return fmt.Errorf("spaceimpl: copy %s from %s: %w", ds.Name, src.id, err)
		}
		if len(rows) == 0 {
			continue
		}
		decl, ok := ctrl.DatasetSchema(ds.Name)
		if !ok {
			decl = ds.Schema.Normalized()
		}
		deriveIds := ctrl.DerivesRecordIds(ds.Name)
		records := make([]space.RecordModify, 0, len(rows))
		for _, row := range rows {
			records = append(records, copyRecord(src.id, &decl, deriveIds, row))
		}
		for start := 0; start < len(records); start += copyBatch {
			end := min(start+copyBatch, len(records))
			res, err := o.parent.Modify(ctx, space.ModifyBatch{ObjectId: objectId, Dataset: ds.Name, Records: records[start:end]})
			if err != nil {
				if errors.Is(err, crdt.ErrValidation) {
					return fmt.Errorf("%w: %s from %s: %w", space.ErrFromRecordRefused, ds.Name, src.id, err)
				}
				return fmt.Errorf("spaceimpl: copy %s from %s: %w", ds.Name, src.id, err)
			}
			if len(res.Rejections) > 0 {
				rj := res.Rejections[0]
				if rj.ReasonErr != nil {
					return fmt.Errorf("%w: %s from %s: record %q: %w", space.ErrFromRecordRefused, ds.Name, src.id, rj.RecordId, rj.ReasonErr)
				}
				return fmt.Errorf("%w: %s from %s: record %q: %s", space.ErrFromRecordRefused, ds.Name, src.id, rj.RecordId, rj.Reason)
			}
		}
	}
	// A source deleted while its datasets were being read leaves the
	// later ones unreadable — an absent collection reads as empty — so
	// the copy is whole only while the source still is.
	if deleted, err := o.parent.store.TreeDeleted(ctx, src.id); err == nil && deleted {
		return fmt.Errorf("spaceimpl: copy from %s: %w", src.id, space.ErrObjectDeleted)
	}
	return nil
}

// copyRows reads the dataset's live records of the controller's object,
// each cloned off the iterator's buffer. A dataset with no collection
// here has no records; a read that fails is an error, never an empty
// copy.
func copyRows(ctx context.Context, ctrl *crdt.Controller, dataset string) ([]*anyenc.Value, error) {
	coll, err := ctrl.OpenForRead(ctx, dataset)
	if err != nil {
		return nil, err
	}
	if coll == nil {
		return nil, nil
	}
	var filter any
	if ctrl.IsKeyed(dataset) {
		filter = crdt.KeyedRows(ctrl.ObjectId())
	}
	iter, err := coll.Find(filter).Iter(ctx)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []*anyenc.Value
	for iter.Next() {
		doc, err := iter.Doc()
		if err != nil {
			return nil, err
		}
		v := doc.Value()
		if v.Get(crdt.DeletedAtField) != nil {
			continue
		}
		if cloned := anyencx.Clone(v); cloned != nil {
			out = append(out, cloned)
		}
	}
	return out, iter.Err()
}

// copyRecord turns one live source row into the upsert that recreates
// it: the id and the `_`-prefixed system fields stay behind, so do the
// fields the dataset declares derived, stamped or local, and on a
// dataset that is not Dynamic every undeclared field (one a removed
// definition left on the record). A record with nothing else is still
// recreated — its id is what it says.
func copyRecord(srcId string, decl *schema.Dataset, deriveIds bool, row *anyenc.Value) space.RecordModify {
	keep := make(map[string]bool, len(decl.Fields))
	for _, f := range decl.Fields {
		keep[f.Id] = f.Stamp == 0 && (f.Scope == 0 || f.Scope == schema.ScopeSynced)
	}
	a := &anyenc.Arena{}
	fields := a.NewObject()
	if obj, err := row.Object(); err == nil {
		obj.Visit(func(k []byte, v *anyenc.Value) {
			key := string(k)
			if key == recordIdField || strings.HasPrefix(key, "_") {
				return
			}
			if declared, ok := keep[key]; ok {
				if !declared {
					return
				}
			} else if !decl.Dynamic {
				return
			}
			fields.Set(key, v)
		})
	}
	rec := space.RecordModify{Upsert: true, Ops: []space.Op{{Type: space.OpSet, Value: fields}}}
	if !deriveIds {
		rec.Id = crdt.KeyedRecordId(srcId, string(row.GetStringBytes(recordIdField)))
	}
	return rec
}

// recordIdField is the record's own id on an any-store row.
const recordIdField = "id"

// logCopyAbort notes a half copy that could not be reclaimed: the row
// is written and synced by the time a dataset copy fails.
func logCopyAbort(objectId string, err error) {
	objectLog.Warn("object left behind after a failed copy", zap.String("objectId", objectId), zap.Error(err))
}
