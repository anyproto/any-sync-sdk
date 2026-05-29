package spaceimpl

import (
	"context"
	"fmt"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/anyenc/anyencutil"
	"github.com/anyproto/any-store/v2/query"

	"github.com/anyproto/any-sync-sdk/space"
)

// membersQuery is a chainable query over the materialised members
// collection. Independent of queryImpl because the members collection
// is plain any-store (no CRDT controller, no `_deletedAt` tombstone
// filter, no projection knobs). Stays small.
type membersQuery struct {
	api *membersAPI

	filter   query.Filter
	sort     query.Sort
	limit    uint
	offset   uint
	parseErr error
}

func newMembersQuery(api *membersAPI) *membersQuery { return &membersQuery{api: api} }

func (q *membersQuery) Filter(filter any) space.Query {
	if q.parseErr != nil {
		return q
	}
	parsed, err := query.ParseCondition(filter)
	if err != nil {
		q.parseErr = fmt.Errorf("members.query: filter: %w", err)
		return q
	}
	if q.filter == nil {
		q.filter = parsed
	} else {
		q.filter = query.And{q.filter, parsed}
	}
	return q
}

func (q *membersQuery) Sort(sorts ...any) space.Query {
	if q.parseErr != nil || len(sorts) == 0 {
		return q
	}
	parsed, err := query.ParseSort(sorts...)
	if err != nil {
		q.parseErr = fmt.Errorf("members.query: sort: %w", err)
		return q
	}
	if q.sort == nil {
		q.sort = parsed
	} else {
		q.sort = query.Sorts{q.sort, parsed}
	}
	return q
}

func (q *membersQuery) Limit(n int) space.Query {
	if n > 0 {
		q.limit = uint(n)
	}
	return q
}

func (q *membersQuery) Offset(n int) space.Query {
	if n > 0 {
		q.offset = uint(n)
	}
	return q
}

// Projection is a no-op on the members collection — there are no
// reserved-namespace fields here (no _device/_account/_base etc.).
func (q *membersQuery) Projection(_ space.ProjectionOpts) space.Query { return q }

func (q *membersQuery) All(ctx context.Context) ([]*anyenc.Value, error) {
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

func (q *membersQuery) One(ctx context.Context) (*anyenc.Value, error) {
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

// Snapshot returns the snapshot of matching members with optional total.
func (q *membersQuery) Snapshot(ctx context.Context, opts space.QueryOpts) (*space.QueryResult, error) {
	initial, err := q.All(ctx)
	if err != nil {
		return nil, err
	}
	total := -1
	if opts.IncludeTotal {
		n, err := q.Count(ctx)
		if err != nil {
			return nil, err
		}
		total = n
	}
	return &space.QueryResult{Initial: initial, Total: total}, nil
}

// Subscribe is not supported for members in v1 — the members watcher
// writes any-store rows directly without feeding the subscribe engine.
// Adding a synthetic emission path is a follow-up.
func (q *membersQuery) Subscribe(_ context.Context, _ space.QueryOpts) (*space.QueryResult, error) {
	return nil, space.ErrSubscribeUnsupported
}

func (q *membersQuery) Count(ctx context.Context) (int, error) {
	coll, err := q.api.collection(ctx)
	if err != nil {
		return 0, err
	}
	built, err := q.build(coll)
	if err != nil {
		return 0, err
	}
	return built.Count(ctx)
}

func (q *membersQuery) Iter(ctx context.Context) (space.Iterator, error) {
	coll, err := q.api.collection(ctx)
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
	return &membersIterator{inner: asIter}, nil
}

func (q *membersQuery) build(coll anystore.Collection) (anystore.Query, error) {
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

// membersIterator adapts any-store's Iterator. Simpler than
// queryIterator (objects) — no tombstone filter; the watcher
// physically deletes rows that should disappear.
type membersIterator struct {
	inner anystore.Iterator
}

func (i *membersIterator) Next() bool { return i.inner != nil && i.inner.Next() }

func (i *membersIterator) Doc() (*anyenc.Value, error) {
	doc, err := i.inner.Doc()
	if err != nil {
		return nil, err
	}
	return doc.Value(), nil
}

func (i *membersIterator) Err() error {
	if i.inner == nil {
		return nil
	}
	return i.inner.Err()
}

func (i *membersIterator) Close() error {
	if i.inner == nil {
		return nil
	}
	return i.inner.Close()
}

// Compile-time interface check.
var _ space.Query = (*membersQuery)(nil)

// (anyencutil import retained — used here for cloneAnyenc that lives
// in query.go in the same package, but the types library reaches
// across files within spaceimpl.)
var _ = anyencutil.Value{}
