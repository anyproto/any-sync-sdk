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
// Tombstones are filtered with an AND-of(<caller-filter>, _deletedAt
// missing) so callers writing free-form filters never see deleted
// rows by accident. Variants/meta projection is a no-op in MVP — the
// raw record comes back as-is.
//
// The chained setters mutate the underlying receiver and return it;
// the public contract is immutable-looking, but a concrete *queryImpl
// is single-shot — re-using the builder across queries would conflate
// state. Callers go through Space.Query() each time.
type queryImpl struct {
	store    *spaceobjects.Store
	objectId string
	dataset  string

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
// raw for those). IncludeDeleted is honored by the find path —
// Iter / All / One gate the post-iteration tombstone skip on it, and
// Count gates the pushed-down skip filter consistently.
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

	combined, err := combineWithTombstoneSkip(q.filter)
	if err != nil {
		return nil, err
	}

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
// read and the registration run.
//
// Initial in the returned QueryResult is the user-visible window
// (limit rows excluding the sentinel; or all rows when limit == 0).
// Subsequent live updates flow through QueryResult.Sub.
func (q *queryImpl) Subscribe(ctx context.Context, opts space.QueryOpts) (*space.QueryResult, error) {
	if q.parseErr != nil {
		return nil, q.parseErr
	}
	if q.limit > 0 && q.sort == nil {
		return nil, subscribe.ErrLimitWithoutSort
	}

	// Resolve scope from the original builder shape.
	var scope subscribe.Scope
	if q.dataset == sharedObjectsDataset {
		scope = subscribe.Scope{Shared: true}
	} else {
		scope = subscribe.Scope{Shared: false, ObjectId: q.objectId, Dataset: q.dataset}
	}

	// Combine user filter with the tombstone-skip clause so the engine
	// matches what the snapshot path returns (queryIterator.Next drops
	// _deletedAt rows post-iteration; we push it into the filter for
	// the live path).
	combined, err := combineWithTombstoneSkip(q.filter)
	if err != nil {
		return nil, err
	}

	var (
		sharedColl anystore.Collection
		obj        *object.Object
	)
	if scope.Shared {
		if sharedColl, err = q.store.SharedObjects(ctx); err != nil {
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
	}
	// collection resolves the dataset's collection from the already
	// resident owner; nil means nothing materialised yet. Safe under
	// engine.mu: no object load, no DAG apply.
	collection := func(ctx context.Context) anystore.Collection {
		if scope.Shared {
			return sharedColl
		}
		if obj == nil {
			return nil
		}
		return obj.Controller().Collection(ctx, q.dataset)
	}

	// Hold the snapshot rows for both the engine's initial population
	// AND the user-visible Initial slice. Same iteration, two outputs.
	var snapshotRows []*anyenc.Value
	var snapshotIds []string
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
		coll := collection(ctx)
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
			// Capture for Initial — clone now so the slice survives past
			// the iterator (which reuses its buffer).
			snapshotRows = append(snapshotRows, anyencx.Clone(v))
			snapshotIds = append(snapshotIds, id)
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
		if coll := collection(ctx); coll != nil {
			n, cerr := coll.Find(combined).Count(ctx)
			if cerr != nil {
				_ = sub.Close()
				return nil, cerr
			}
			totalCount = n
		}
	}

	// Initial = first limit rows (excluding the sentinel). When limit==0
	// (unbounded), all rows are visible.
	var initial []*anyenc.Value
	if q.limit > 0 && len(snapshotRows) > int(q.limit) {
		initial = snapshotRows[:q.limit]
	} else {
		initial = snapshotRows
	}
	_ = snapshotIds // currently unused, but kept for symmetry / future debugging hooks

	hasNext := false
	if opts.IncludeTotal {
		hasNext = int(q.offset)+len(initial) < totalCount
	}
	return &space.QueryResult{Initial: initial, Total: totalCount, HasNext: hasNext, Sub: sub}, nil
}

// combineWithTombstoneSkip returns the user filter ANDed with a
// `_deletedAt missing` clause. The engine's live-path filter eval
// must reject tombstoned post-states the same way the snapshot-time
// filter does — pushing the skip into the filter keeps the two paths
// in lock-step.
func combineWithTombstoneSkip(userFilter query.Filter) (query.Filter, error) {
	skip, err := query.ParseCondition(map[string]any{
		crdt.DeletedAtField: map[string]any{"$exists": false},
	})
	if err != nil {
		return nil, fmt.Errorf("subscribe: build tombstone-skip filter: %w", err)
	}
	if userFilter == nil {
		return skip, nil
	}
	return query.And{userFilter, skip}, nil
}

// Count returns the match count. The find path gates tombstones on
// ProjectionOpts.IncludeDeleted the same way Iter does: by default the
// _deletedAt-missing clause is pushed into the filter so deleted rows
// don't count; with IncludeDeleted set, tombstones are counted too.
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
	filter := q.filter
	if !q.opts.IncludeDeleted {
		filter, err = combineWithTombstoneSkip(filter)
		if err != nil {
			return 0, err
		}
	}
	return coll.Find(filter).Count(ctx)
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
	return &queryIterator{inner: asIter, includeDeleted: q.opts.IncludeDeleted}, nil
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
	return resolveCollection(ctx, q.store, q.objectId, q.dataset)
}

// resolveCollection is the dataset → any-store collection lookup
// shared by the query and aggregation builders. dataset
// "<shared:objects>" selects the per-space objects collection;
// anything else goes through the object's controller. See
// queryImpl.collection for the (nil, nil) empty-dataset contract.
func resolveCollection(ctx context.Context, store *spaceobjects.Store, objectId, dataset string) (anystore.Collection, error) {
	if dataset == sharedObjectsDataset {
		return store.SharedObjects(ctx)
	}
	// A resident controller built before a runtime dataset was defined
	// doesn't know its collection; reload it so a just-defined dataset
	// reads back real rows instead of silently empty results.
	store.EnsureDatasetRegistered(ctx, objectId, dataset)
	obj, err := store.Get(ctx, objectId)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	return obj.Controller().Collection(ctx, dataset), nil
}

// emptyIterator is the Iter() result when the dataset has no
// materialised collection yet. Next always returns false; Doc never
// gets called by well-behaved callers.
type emptyIterator struct{}

func (emptyIterator) Next() bool                  { return false }
func (emptyIterator) Doc() (*anyenc.Value, error) { return nil, nil }
func (emptyIterator) Err() error                  { return nil }
func (emptyIterator) Close() error                { return nil }

// build folds the parsed filter / sort / limit / offset into an
// any-store Query. Filter and Sort are already typed query.Filter /
// query.Sort values (parsed by Filter() / Sort() at chain time);
// any-store accepts both directly. Tombstones are filtered post-
// iteration in queryIterator.Next.
func (q *queryImpl) build(coll anystore.Collection) (anystore.Query, error) {
	if q.parseErr != nil {
		return nil, q.parseErr
	}
	var out anystore.Query
	if q.filter != nil {
		out = coll.Find(q.filter)
	} else {
		out = coll.Find(nil)
	}
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

// queryIterator adapts any-store's Iterator to space.Iterator. The
// underlying any-store iter holds buffers we don't want exposed —
// Doc() copies the value out via FastJson roundtrip-free path:
// the value is already on the iterator's arena, valid until the
// next Next() call. Caller-side races with Next() between Doc() and
// use are the caller's problem (same contract as any-store).
type queryIterator struct {
	inner          anystore.Iterator
	includeDeleted bool
	cur            *anyenc.Value
	lastErr        error
}

// Next advances past tombstones automatically — checks each doc and
// skips ones with `_deletedAt` set. Any-store iterators don't expose
// peek, so we have to consume to filter; that's fine, the cost is
// proportional to deleted-row density. When includeDeleted is set
// (ProjectionOpts.IncludeDeleted) the skip is disabled and tombstone
// rows surface to the caller — used by consumer-side indexers to stream
// deletions.
func (i *queryIterator) Next() bool {
	if i.inner == nil {
		return false
	}
	for i.inner.Next() {
		doc, err := i.inner.Doc()
		if err != nil {
			i.lastErr = err
			return true
		}
		v := doc.Value()
		if !i.includeDeleted && v != nil && v.Get(crdt.DeletedAtField) != nil {
			continue // tombstone
		}
		i.cur = v
		return true
	}
	return false
}

func (i *queryIterator) Doc() (*anyenc.Value, error) {
	if i.lastErr != nil {
		return nil, i.lastErr
	}
	if i.cur != nil {
		return i.cur, nil
	}
	doc, err := i.inner.Doc()
	if err != nil {
		return nil, err
	}
	return doc.Value(), nil
}

func (i *queryIterator) Err() error {
	if i.inner == nil {
		return nil
	}
	return i.inner.Err()
}

func (i *queryIterator) Close() error {
	if i.inner == nil {
		return nil
	}
	return i.inner.Close()
}

// Compile-time interface check.
var _ space.Query = (*queryImpl)(nil)
