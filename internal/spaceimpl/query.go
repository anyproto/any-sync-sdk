package spaceimpl

import (
	"context"
	"fmt"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/anyenc/anyencutil"
	"github.com/anyproto/any-store/v2/query"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
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
	parent   *spaceImpl
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
	parseErr error
}

func newQuery(parent *spaceImpl, objectId, dataset string) *queryImpl {
	return &queryImpl{parent: parent, objectId: objectId, dataset: dataset}
}

// newSharedQuery builds a queryImpl that resolves to the per-space
// `objects` collection on Iter — independent of any objectId.
func newSharedQuery(parent *spaceImpl) *queryImpl {
	return &queryImpl{parent: parent, dataset: "<shared:objects>"}
}

// Filter parses the caller-supplied condition eagerly via
// query.ParseCondition (which itself accepts an already-built
// query.Filter, a JSON string, or a map literal). Multiple Filter
// calls AND together. Parse errors are stashed and surfaced on the
// first terminal call.
func (q *queryImpl) Filter(filter any) space.Query {
	if q.parseErr != nil {
		return q
	}
	parsed, err := query.ParseCondition(filter)
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

// Projection is a no-op in MVP. Variant collapse and meta-stripping
// are tracked tasks; until they land, every record returns raw.
func (q *queryImpl) Projection(_ space.ProjectionOpts) space.Query {
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
		cloned, err := cloneAnyenc(doc)
		if err != nil {
			return nil, err
		}
		out = append(out, cloned)
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
	return cloneAnyenc(doc)
}

// cloneAnyenc copies a value off the iterator's reused buffer onto a
// fresh parser arena via anyencutil.Value.FillCopy. Returns nil for
// a nil input.
func cloneAnyenc(v *anyenc.Value) (*anyenc.Value, error) {
	if v == nil {
		return nil, nil
	}
	var w anyencutil.Value
	w.FillCopy(v)
	return w.Value, nil
}

// Count returns the match count.
func (q *queryImpl) Count(ctx context.Context) (int, error) {
	coll, err := q.collection(ctx)
	if err != nil {
		return 0, err
	}
	built, err := q.build(coll)
	if err != nil {
		return 0, err
	}
	return built.Count(ctx)
}

// Iter opens a streaming iterator over the matching records.
func (q *queryImpl) Iter(ctx context.Context) (space.Iterator, error) {
	coll, err := q.collection(ctx)
	if err != nil {
		return nil, err
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

func (q *queryImpl) collection(ctx context.Context) (anystore.Collection, error) {
	if q.dataset == "<shared:objects>" {
		return q.parent.store.SharedObjects(ctx)
	}
	obj, err := q.parent.store.Get(ctx, q.objectId)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	coll := obj.Controller().Collection(q.dataset)
	if coll == nil {
		return nil, fmt.Errorf("query: dataset %q has no collection", q.dataset)
	}
	return coll, nil
}

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
	inner   anystore.Iterator
	cur     *anyenc.Value
	lastErr error
}

// Next advances past tombstones automatically — checks each doc and
// skips ones with `_deletedAt` set. Any-store iterators don't expose
// peek, so we have to consume to filter; that's fine, the cost is
// proportional to deleted-row density.
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
		if v != nil && v.Get(crdt.DeletedAtField) != nil {
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
