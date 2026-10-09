package spaceimpl

import (
	"context"
	"errors"
	"fmt"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"

	"github.com/anyproto/any-sync-sdk/internal/anyencx"
	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/internal/subscribe"
	"github.com/anyproto/any-sync-sdk/space"
)

// queryImpl implements space.Query backed by an any-store collection.
//
// Every terminal reads AND-of(<caller-filter>, _deletedAt missing), so
// callers writing free-form filters never see deleted rows by accident
// and a limit/offset window is cut over live rows; only the find path
// asked for tombstones (ProjectionOpts.IncludeDeleted) drops the
// clause. Variants/meta projection is a no-op in MVP — the raw record
// comes back as-is.
//
// The chained setters mutate the underlying receiver and return it;
// the public contract is immutable-looking, but a concrete *queryImpl
// is single-shot — re-using the builder across queries would conflate
// state. Callers go through Space.Query() each time.
type queryImpl struct {
	store    *spaceobjects.Store
	objectId string
	dataset  string
	// allObjects reads a shared dataset across every object that holds
	// it (Space.QueryDataset); objectId is unused.
	allObjects bool
	// rows bounds a per-object read to the object's key range. Set when
	// the collection is resolved, from the controller that supplies it:
	// the catalog may no longer call the dataset shared while a resident
	// controller still reads the per-space collection.
	rows query.Filter

	// filter / sort hold the parsed query.Filter / query.Sort. Parse
	// errors are stashed in parseErr and surfaced on the first
	// terminal call (Iter / All / One / Count) so the caller sees
	// them in the same path as any other read error.
	filter   query.Filter
	sort     query.Sort
	limit    uint
	offset   uint
	opts     space.ProjectionOpts
	parseErr error
}

func newQuery(store *spaceobjects.Store, objectId, dataset string) *queryImpl {
	return &queryImpl{store: store, objectId: objectId, dataset: dataset}
}

// scoped is the filter a read runs: the caller's, narrowed to the
// object's rows when the collection holds every object's (q.rows).
func (q *queryImpl) scoped() query.Filter {
	switch {
	case q.rows == nil:
		return q.filter
	case q.filter == nil:
		return q.rows
	}
	return query.And{q.rows, q.filter}
}

// newDatasetQuery builds a queryImpl over the per-space collection of a
// shared dataset — every object's records, no object named.
func newDatasetQuery(store *spaceobjects.Store, dataset string) *queryImpl {
	return &queryImpl{store: store, dataset: dataset, allObjects: true}
}

// resolveDatasetCollection resolves the per-space collection of a
// shared dataset. No object is loaded: the read sees the space's
// materialized state, as a QueryObjects read does.
func resolveDatasetCollection(ctx context.Context, store *spaceobjects.Store, dataset string) (anystore.Collection, error) {
	coll, ok, err := store.KeyedCollection(ctx, dataset)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("query: %w: %q", space.ErrDatasetNotShared, dataset)
	}
	return coll, nil
}

// sharedObjectsDataset is the sentinel dataset name that makes the
// query/aggregation builders resolve to the per-space `objects`
// collection instead of a per-object dataset.
const sharedObjectsDataset = "<shared:objects>"

// newSharedQuery builds a queryImpl that resolves to the per-space
// `objects` collection on Iter — independent of any objectId.
func newSharedQuery(store *spaceobjects.Store) *queryImpl {
	return &queryImpl{store: store, dataset: sharedObjectsDataset}
}

// Filter parses the caller-supplied condition eagerly via
// parseCondition (an already-built query.Filter, a JSON string, or a
// map literal converted like a record value). Multiple Filter calls
// AND together. Parse errors are stashed and surfaced on the first
// terminal call.
func (q *queryImpl) Filter(filter any) space.Query {
	if q.parseErr != nil {
		return q
	}
	parsed, err := parseCondition(filter)
	if err != nil {
		q.parseErr = fmt.Errorf("query: filter: %w", err)
		return q
	}
	if q.filter == nil {
		q.filter = parsed
	} else {
		q.filter = query.And{q.filter, parsed}
	}
	return q
}

// Sort parses the caller-supplied sort keys eagerly via
// query.ParseSort. Each call appends; multiple sorts compose in
// declaration order. Parse errors are stashed and surfaced on the
// first terminal call.
func (q *queryImpl) Sort(sorts ...any) space.Query {
	if q.parseErr != nil || len(sorts) == 0 {
		return q
	}
	parsed, err := query.ParseSort(sorts...)
	if err != nil {
		q.parseErr = fmt.Errorf("query: sort: %w", err)
		return q
	}
	if q.sort == nil {
		q.sort = parsed
	} else {
		q.sort = query.Sorts{q.sort, parsed}
	}
	return q
}

func (q *queryImpl) Limit(n int) space.Query {
	if n > 0 {
		q.limit = uint(n)
	}
	return q
}

func (q *queryImpl) Offset(n int) space.Query {
	if n > 0 {
		q.offset = uint(n)
	}
	return q
}

// Projection records the requested options. Variant collapse and
// meta-stripping are still no-ops (tracked tasks; every record returns
// raw for those). IncludeDeleted is honored by the find path: Iter /
// All / One / Count run without the tombstone-skip clause.
func (q *queryImpl) Projection(opts space.ProjectionOpts) space.Query {
	q.opts = opts
	return q
}

// All materializes every match into a slice. Each row is cloned out
// of the iterator's reusable buffer onto a fresh parser arena, so
// caller-held pointers stay valid past the iteration.
func (q *queryImpl) All(ctx context.Context) ([]*anyenc.Value, error) {
	it, err := q.Iter(ctx)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var out []*anyenc.Value
	for it.Next() {
		doc, err := it.Doc()
		if err != nil {
			return nil, err
		}
		out = append(out, anyencx.Clone(doc))
	}
	return out, it.Err()
}

// One returns the first match or space.ErrNotFound. Stops the iterator
// after the first row. Cloned for caller-retain safety.
func (q *queryImpl) One(ctx context.Context) (*anyenc.Value, error) {
	q.limit = 1
	it, err := q.Iter(ctx)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	if !it.Next() {
		if e := it.Err(); e != nil {
			return nil, e
		}
		return nil, space.ErrNotFound
	}
	doc, err := it.Doc()
	if err != nil {
		return nil, err
	}
	return anyencx.Clone(doc), nil
}

// Snapshot returns a point-in-time view plus, when opts.IncludeTotal is
// set, the count of matching records ignoring limit/offset and HasNext.
// The page and the count read from a single transaction. The
// tombstone-skip clause is pushed into the page filter so the page and
// the count match.
func (q *queryImpl) Snapshot(ctx context.Context, opts space.QueryOpts) (*space.QueryResult, error) {
	if q.parseErr != nil {
		return nil, q.parseErr
	}
	coll, err := q.collection(ctx)
	if err != nil {
		return nil, err
	}
	if coll == nil {
		// Dataset not materialised yet → empty page, zero total.
		total := -1
		if opts.IncludeTotal {
			total = 0
		}
		return &space.QueryResult{Initial: nil, Total: total}, nil
	}

	combined := withoutTombstones(q.scoped())

	tx, err := coll.ReadTx(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Commit() }() // release the read tx

	initial, err := q.page(tx.Context(), coll, combined)
	if err != nil {
		return nil, err
	}

	total := -1
	hasNext := false
	if opts.IncludeTotal {
		total, err = q.totalWithin(tx.Context(), coll, combined, len(initial))
		if err != nil {
			return nil, err
		}
		hasNext = int(q.offset)+len(initial) < total
	}

	return &space.QueryResult{Initial: initial, Total: total, HasNext: hasNext}, nil
}

// page materializes the limit/offset window of `combined` within the
// supplied (transaction-bound) context. Rows are cloned off the
// iterator's reused buffer so they survive past iteration.
func (q *queryImpl) page(ctx context.Context, coll anystore.Collection, combined query.Filter) ([]*anyenc.Value, error) {
	aq := coll.Find(combined)
	if q.sort != nil {
		aq = aq.Sort(q.sort)
	}
	if q.limit > 0 {
		aq = aq.Limit(q.limit)
	}
	if q.offset > 0 {
		aq = aq.Offset(q.offset)
	}
	it, err := aq.Iter(ctx)
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var out []*anyenc.Value
	for it.Next() {
		doc, err := it.Doc()
		if err != nil {
			return nil, err
		}
		out = append(out, anyencx.Clone(doc.Value()))
	}
	return out, it.Err()
}

// totalWithin returns the count of `combined` matches ignoring
// limit/offset, reading within the supplied context. When the page
// already reached the end of the result set the total is offset+pageLen
// and no count query runs: that holds for an unbounded query (limit 0)
// or a page shorter than the limit, with the pageLen>0 || offset==0
// guard ruling out an offset past the end of the result set.
func (q *queryImpl) totalWithin(ctx context.Context, coll anystore.Collection, combined query.Filter, pageLen int) (int, error) {
	if (q.limit == 0 || pageLen < int(q.limit)) && (pageLen > 0 || q.offset == 0) {
		return int(q.offset) + pageLen, nil
	}
	return coll.Find(combined).Count(ctx)
}

// Subscribe is the live-query terminal. Builds a windowed sub via the
// per-space engine; the snapshot read runs under engine.mu so apply
// events are fenced between the snapshot and the sub's registration.
//
// The object is resolved BEFORE the engine lock. Store.Get can load
// the object, and a load replays stored changes through Engine.OnApply,
// which takes the same lock: a load started (or joined) from inside the
// snapshot callback deadlocks the engine, and with it every apply in
// the space. Under the fence only the collection lookup, the snapshot
// read and the registration run — and the lookup peeks
// (Controller.PeekCollection): the first open of a dataset collection
// ensures its indexes in a write transaction, and that transaction
// waiting on any-store's writer under the fence would stall every
// apply in the space for as long as the writer is busy. The warm-up
// before the lock takes the open outside the fence; the peek covers a
// collection that appears in between.
//
// Initial in the returned QueryResult is the user-visible window
// (limit rows excluding the sentinel; or all rows when limit == 0),
// the caller's to drop once rendered: the subscription keeps no
// reference to it. Subsequent live updates flow through
// QueryResult.Sub.
func (q *queryImpl) Subscribe(ctx context.Context, opts space.QueryOpts) (*space.QueryResult, error) {
	if q.parseErr != nil {
		return nil, q.parseErr
	}
	if q.limit > 0 && q.sort == nil {
		return nil, subscribe.ErrLimitWithoutSort
	}

	// Resolve scope from the original builder shape.
	var scope subscribe.Scope
	switch {
	case q.dataset == sharedObjectsDataset:
		scope = subscribe.Scope{Shared: true}
	case q.allObjects:
		scope = subscribe.Scope{AllObjects: true, Dataset: q.dataset}
	default:
		scope = subscribe.Scope{Shared: false, ObjectId: q.objectId, Dataset: q.dataset}
	}

	var (
		sharedColl anystore.Collection
		obj        *object.Object
		err        error
	)
	if scope.Shared {
		if sharedColl, err = q.store.SharedObjects(ctx); err != nil {
			return nil, err
		}
	} else if scope.AllObjects {
		if sharedColl, err = resolveDatasetCollection(ctx, q.store, q.dataset); err != nil {
			return nil, err
		}
	} else {
		q.store.EnsureDatasetRegistered(ctx, q.objectId, q.dataset)
		// A missing object is an empty initial. The engine still
		// registers the sub; if the object lands later, its events flow
		// in — same as a not-yet-materialised dataset.
		obj, err = q.store.Get(ctx, q.objectId)
		if err != nil && !errors.Is(err, treestorage.ErrUnknownTreeId) {
			return nil, fmt.Errorf("query: %w", err)
		}
		if obj != nil {
			// Warm the controller's handle outside the fence: the first
			// open of a dataset collection ensures its indexes in a
			// write tx. Under the fence the lookup is then a map hit; a
			// collection that appears in between is peeked without
			// that write.
			obj.Controller().Collection(ctx, q.dataset)
			if obj.Controller().IsKeyed(q.dataset) {
				q.rows = crdt.KeyedRows(q.objectId)
			}
		}
	}
	// The tombstone-skip clause is part of the filter, so the engine's
	// live-path evaluation rejects tombstoned post-states the same way
	// the snapshot read does.
	combined := withoutTombstones(q.scoped())
	// collection resolves the dataset's collection from the already
	// resident owner; nil means nothing materialised yet. peek selects
	// Controller.PeekCollection for the read under engine.mu, where
	// every apply in the space waits: a collection that appeared since
	// the warm-up is opened without the write transaction that ensures
	// its indexes. No object load and no DAG apply there either.
	collection := func(ctx context.Context, peek bool) (anystore.Collection, error) {
		if scope.Shared || scope.AllObjects {
			return sharedColl, nil
		}
		if obj == nil {
			return nil, nil
		}
		if peek {
			return obj.Controller().PeekCollection(ctx, q.dataset)
		}
		return obj.Controller().Collection(ctx, q.dataset), nil
	}

	// One iteration feeds both the engine's initial population and the
	// user-visible Initial slice.
	var snapshotRows []*anyenc.Value
	totalCount := -1

	cfg := subscribe.SubConfig{
		Scope:       scope,
		Filter:      combined,
		Sort:        q.sort,
		Limit:       int(q.limit),
		MailboxCap:  opts.MailboxCapacity,
		DriftBudget: opts.DriftBudgetPercent,
	}

	sub, err := q.store.SubEngine().Subscribe(cfg, func(yield func(id string, doc *anyenc.Value)) error {
		coll, err := collection(ctx, true)
		if err != nil {
			return err
		}
		if coll == nil {
			return nil // dataset has no materialised collection yet → empty
		}
		aq := coll.Find(combined)
		if q.sort != nil {
			aq = aq.Sort(q.sort)
		}
		// Snapshot pulls limit+1 to seed the sentinel; the user sees only the
		// first `limit` rows in Initial.
		if q.limit > 0 {
			aq = aq.Limit(q.limit + 1)
		}
		if q.offset > 0 {
			aq = aq.Offset(q.offset)
		}
		it, err := aq.Iter(ctx)
		if err != nil {
			return err
		}
		defer it.Close()
		for it.Next() {
			doc, err := it.Doc()
			if err != nil {
				return err
			}
			v := doc.Value()
			if v == nil {
				continue
			}
			id := string(v.GetStringBytes("id"))
			if id == "" {
				continue
			}
			// Initial's copy of the visible rows, cloned off the
			// iterator's reused buffer. The sentinel is the engine's
			// alone, and an unbounded sub keeps no copy of its own.
			if q.limit == 0 || len(snapshotRows) < int(q.limit) {
				snapshotRows = append(snapshotRows, anyencx.Clone(v))
			}
			yield(id, v)
		}
		return it.Err()
	})
	if err != nil {
		return nil, err
	}

	if opts.IncludeTotal {
		// Run a separate Count(filter) outside the snapshot fence (engine.mu
		// already released). Slightly stale relative to events that fired
		// after we released, but documented as snapshot-only semantics.
		totalCount = 0
		coll, err := collection(ctx, false)
		if err == nil && coll != nil {
			totalCount, err = coll.Find(combined).Count(ctx)
		}
		if err != nil {
			_ = sub.Close()
			return nil, err
		}
	}

	hasNext := false
	if opts.IncludeTotal {
		hasNext = int(q.offset)+len(snapshotRows) < totalCount
	}
	return &space.QueryResult{Initial: snapshotRows, Total: totalCount, HasNext: hasNext, Sub: sub}, nil
}

// tombstoneSkip matches live rows only: a tombstone keeps _deletedAt
// and no user field. Pushed into every read's filter so the
// limit/offset window, the count and the live-path evaluation see one
// row set — a tombstone would otherwise sort first under a user-field
// sort and take a slot of the window.
var tombstoneSkip = query.Key{Path: []string{crdt.DeletedAtField}, Filter: query.Not{Filter: query.Exists{}}}

// withoutTombstones returns userFilter ANDed with tombstoneSkip.
func withoutTombstones(userFilter query.Filter) query.Filter {
	if userFilter == nil {
		return tombstoneSkip
	}
	return query.And{userFilter, tombstoneSkip}
}

// findFilter is the filter the find terminals (Iter / All / One /
// Count) run: the scoped filter, tombstones excluded unless
// ProjectionOpts.IncludeDeleted asks for them.
func (q *queryImpl) findFilter() query.Filter {
	if q.opts.IncludeDeleted {
		return q.scoped()
	}
	return withoutTombstones(q.scoped())
}

// Count returns the match count over the find filter.
func (q *queryImpl) Count(ctx context.Context) (int, error) {
	if q.parseErr != nil {
		return 0, q.parseErr
	}
	coll, err := q.collection(ctx)
	if err != nil {
		return 0, err
	}
	if coll == nil {
		return 0, nil
	}
	return coll.Find(q.findFilter()).Count(ctx)
}

// Iter opens a streaming iterator over the matching records.
func (q *queryImpl) Iter(ctx context.Context) (space.Iterator, error) {
	coll, err := q.collection(ctx)
	if err != nil {
		return nil, err
	}
	if coll == nil {
		return emptyIterator{}, nil
	}
	built, err := q.build(coll)
	if err != nil {
		return nil, err
	}
	asIter, err := built.Iter(ctx)
	if err != nil {
		return nil, err
	}
	return &queryIterator{inner: asIter}, nil
}

// collection resolves the dataset's any-store collection. Returns
// (nil, nil) when the dataset has no rows materialised yet — callers
// treat that as an empty result rather than an error, so the first
// query on a fresh object surfaces an empty list / zero count instead
// of a 500. The (nil, nil) signal also fires for unknown datasets;
// distinguishing the two cases would require a Controller-level
// accessor and isn't worth it — querying a typo dataset name already
// silently returns empty in any-store too.
func (q *queryImpl) collection(ctx context.Context) (anystore.Collection, error) {
	if q.allObjects {
		return resolveDatasetCollection(ctx, q.store, q.dataset)
	}
	coll, keyed, err := resolveCollection(ctx, q.store, q.objectId, q.dataset)
	if keyed {
		q.rows = crdt.KeyedRows(q.objectId)
	}
	return coll, err
}

// resolveCollection is the dataset → any-store collection lookup
// shared by the query and aggregation builders. dataset
// "<shared:objects>" selects the per-space objects collection;
// anything else goes through the object's controller. See
// queryImpl.collection for the (nil, nil) empty-dataset contract.
func resolveCollection(ctx context.Context, store *spaceobjects.Store, objectId, dataset string) (coll anystore.Collection, keyed bool, err error) {
	if dataset == sharedObjectsDataset {
		coll, err = store.SharedObjects(ctx)
		return coll, false, err
	}
	// A resident controller built before a runtime dataset was defined
	// doesn't know its collection; reload it so a just-defined dataset
	// reads back real rows instead of silently empty results.
	store.EnsureDatasetRegistered(ctx, objectId, dataset)
	obj, err := store.Get(ctx, objectId)
	if err != nil {
		return nil, false, fmt.Errorf("query: %w", err)
	}
	// keyed comes from the controller that supplies the collection.
	return obj.Controller().Collection(ctx, dataset), obj.Controller().IsKeyed(dataset), nil
}

// emptyIterator is the Iter() result when the dataset has no
// materialised collection yet. Next always returns false; Doc never
// gets called by well-behaved callers.
type emptyIterator struct{}

func (emptyIterator) Next() bool                  { return false }
func (emptyIterator) Doc() (*anyenc.Value, error) { return nil, nil }
func (emptyIterator) Err() error                  { return nil }
func (emptyIterator) Close() error                { return nil }

// build folds the find filter / sort / limit / offset into an
// any-store Query. The tombstone skip is part of the filter, so the
// limit/offset window is cut over live rows.
func (q *queryImpl) build(coll anystore.Collection) (anystore.Query, error) {
	if q.parseErr != nil {
		return nil, q.parseErr
	}
	out := coll.Find(q.findFilter())
	if q.sort != nil {
		out = out.Sort(q.sort)
	}
	if q.limit > 0 {
		out = out.Limit(q.limit)
	}
	if q.offset > 0 {
		out = out.Offset(q.offset)
	}
	return out, nil
}

// queryIterator adapts any-store's Iterator to space.Iterator. Doc
// returns the value on the iterator's arena, valid until the next Next
// call; callers clone what they retain (All and One do).
type queryIterator struct {
	inner anystore.Iterator
}

func (i *queryIterator) Next() bool { return i.inner.Next() }

func (i *queryIterator) Doc() (*anyenc.Value, error) {
	doc, err := i.inner.Doc()
	if err != nil {
		return nil, err
	}
	return doc.Value(), nil
}

func (i *queryIterator) Err() error { return i.inner.Err() }

func (i *queryIterator) Close() error { return i.inner.Close() }

// Compile-time interface check.
var _ space.Query = (*queryImpl)(nil)
