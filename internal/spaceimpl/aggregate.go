package spaceimpl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/space"
)

// aggImpl implements space.Agg backed by an any-store collection's
// Aggregate pipeline.
//
// The user pipeline is normalized to an anyenc array eagerly at build
// time; normalization errors are stashed in parseErr and surfaced on
// the first terminal call, same as queryImpl. A `_deletedAt missing`
// $match stage is prepended before execution so tombstoned rows never
// reach the pipeline — a leading $match chain folds into any-store's
// pushdown prefix, so the skip stays index-planned regardless of what
// the user pipeline starts with.
//
// Single-shot, like queryImpl — callers go through Space.Aggregate()
// per read.
type aggImpl struct {
	store    *spaceobjects.Store
	objectId string
	dataset  string

	// pipeline is the normalized user pipeline (an anyenc array; nil
	// for an absent/empty pipeline). arena owns the values aggImpl
	// itself builds (the tombstone stage, the combined array, and any
	// fastjson conversion); both must stay reachable until the
	// terminal call hands the combined array to coll.Aggregate, which
	// detaches it.
	pipeline *anyenc.Value
	arena    *anyenc.Arena
	parseErr error

	// Limit overrides. Pointer tri-state: nil = any-store default, set
	// = pass through verbatim (negative means unlimited).
	groupLimit *int
	accumLimit *int
	memLimit   *int
}

func newAgg(store *spaceobjects.Store, objectId, dataset string, pipeline any) *aggImpl {
	a := &aggImpl{store: store, objectId: objectId, dataset: dataset, arena: &anyenc.Arena{}}
	a.pipeline, a.parseErr = a.normalizePipeline(pipeline)
	return a
}

// newSharedAgg builds an aggImpl over the per-space `objects`
// collection — independent of any objectId.
func newSharedAgg(store *spaceobjects.Store, pipeline any) *aggImpl {
	return newAgg(store, "", sharedObjectsDataset, pipeline)
}

// normalizePipeline accepts the same input forms as Query.Filter — an
// *anyenc.Value, *fastjson.Value, JSON string, marshaled-anyenc
// []byte, or any JSON-marshalable Go value — and returns an anyenc
// array (nil for a nil/absent pipeline). Every failure wraps
// space.ErrBadPipeline.
func (a *aggImpl) normalizePipeline(pipeline any) (*anyenc.Value, error) {
	if pipeline == nil {
		return nil, nil
	}
	var (
		v   *anyenc.Value
		err error
	)
	switch p := pipeline.(type) {
	case *anyenc.Value:
		v = p
	case *fastjson.Value:
		v = a.arena.NewFromFastJson(p)
	case string:
		v, err = anyenc.ParseJson(p)
	case []byte:
		v, err = anyenc.Parse(p)
	default:
		var raw []byte
		if raw, err = json.Marshal(p); err == nil {
			v, err = anyenc.ParseJson(string(raw))
		}
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %w", space.ErrBadPipeline, err)
	}
	if v == nil {
		return nil, nil
	}
	if v.Type() != anyenc.TypeArray {
		return nil, fmt.Errorf("%w: pipeline must be an array of stages, got %s", space.ErrBadPipeline, v.Type())
	}
	return v, nil
}

// combined returns the pipeline that actually executes: the
// tombstone-skip $match prepended to the user stages.
func (a *aggImpl) combined() *anyenc.Value {
	exists := a.arena.NewObject()
	exists.Set("$exists", a.arena.NewFalse())
	match := a.arena.NewObject()
	match.Set(crdt.DeletedAtField, exists)
	skipStage := a.arena.NewObject()
	skipStage.Set("$match", match)

	out := a.arena.NewArray()
	out.SetArrayItem(0, skipStage)
	if a.pipeline != nil {
		for i, st := range a.pipeline.GetArray() {
			out.SetArrayItem(i+1, st)
		}
	}
	return out
}

func (a *aggImpl) GroupLimit(n int) space.Agg      { a.groupLimit = &n; return a }
func (a *aggImpl) AccumArrayLimit(n int) space.Agg { a.accumLimit = &n; return a }
func (a *aggImpl) MemoryLimit(bytes int) space.Agg { a.memLimit = &bytes; return a }

// buildAgg assembles the any-store AggQuery with limit overrides
// applied. coll.Aggregate parses (and detaches from) the combined
// pipeline; parse errors surface from the terminal call.
func (a *aggImpl) buildAgg(coll anystore.Collection) anystore.AggQuery {
	aq := coll.Aggregate(a.combined())
	if a.groupLimit != nil {
		aq = aq.GroupLimit(*a.groupLimit)
	}
	if a.accumLimit != nil {
		aq = aq.AccumArrayLimit(*a.accumLimit)
	}
	if a.memLimit != nil {
		aq = aq.MemoryLimit(*a.memLimit)
	}
	return aq
}

// classifyAggErr wraps terminal-call errors as space.ErrBadPipeline.
// Context errors and the limit sentinels pass through untouched.
// Heuristic: any-store's pipeline parse / stage validation / prefix
// violation errors are plain errors with no sentinel, so a rare
// genuine storage failure at Iter time gets mislabeled as a bad
// pipeline — acceptable; the alternative (parse errors reading as
// internal failures) is worse.
func classifyAggErr(err error) error {
	if err == nil ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, space.ErrBadPipeline) ||
		errors.Is(err, anystore.ErrGroupLimitExceeded) ||
		errors.Is(err, anystore.ErrAccumArrayLimitExceeded) ||
		errors.Is(err, anystore.ErrAggMemoryLimitExceeded) {
		return err
	}
	return fmt.Errorf("%w: %w", space.ErrBadPipeline, err)
}

// Iter executes the pipeline and streams result documents. Results
// are pipeline output (a $group doc carries the group key as `id`, a
// $count doc has no id at all), so no tombstone post-filtering or id
// handling applies — the skip stage in the pushdown prefix already
// excluded deleted rows.
func (a *aggImpl) Iter(ctx context.Context) (space.Iterator, error) {
	if a.parseErr != nil {
		return nil, a.parseErr
	}
	coll, err := resolveCollection(ctx, a.store, a.objectId, a.dataset)
	if err != nil {
		return nil, err
	}
	if coll == nil {
		return emptyIterator{}, nil
	}
	it, err := a.buildAgg(coll).Iter(ctx)
	if err != nil {
		return nil, classifyAggErr(err)
	}
	return &aggIterator{inner: it}, nil
}

// All materializes every result document, cloned off the iterator's
// reused buffers. Mid-iteration errors (limit sentinels, IO) pass
// through from Err untouched.
func (a *aggImpl) All(ctx context.Context) ([]*anyenc.Value, error) {
	it, err := a.Iter(ctx)
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

// Count executes the pipeline and returns the number of result
// documents.
func (a *aggImpl) Count(ctx context.Context) (int, error) {
	if a.parseErr != nil {
		return 0, a.parseErr
	}
	coll, err := resolveCollection(ctx, a.store, a.objectId, a.dataset)
	if err != nil {
		return 0, err
	}
	if coll == nil {
		return 0, nil
	}
	n, err := a.buildAgg(coll).Count(ctx)
	return n, classifyAggErr(err)
}

// Explain returns the pushed prefix's access plan plus the
// in-pipeline stage list. Diagnostic only.
func (a *aggImpl) Explain(ctx context.Context) (string, error) {
	if a.parseErr != nil {
		return "", a.parseErr
	}
	coll, err := resolveCollection(ctx, a.store, a.objectId, a.dataset)
	if err != nil {
		return "", err
	}
	if coll == nil {
		return "empty: dataset not materialised", nil
	}
	ex, err := a.buildAgg(coll).Explain(ctx)
	if err != nil {
		return "", classifyAggErr(err)
	}
	return ex.Plan, nil
}

// aggIterator adapts any-store's Iterator to space.Iterator. Unlike
// queryIterator there is no tombstone skip (handled in the pipeline
// prefix) — it's a plain passthrough.
type aggIterator struct {
	inner anystore.Iterator
}

func (i *aggIterator) Next() bool { return i.inner.Next() }

func (i *aggIterator) Doc() (*anyenc.Value, error) {
	doc, err := i.inner.Doc()
	if err != nil {
		return nil, err
	}
	return doc.Value(), nil
}

func (i *aggIterator) Err() error   { return i.inner.Err() }
func (i *aggIterator) Close() error { return i.inner.Close() }

// Compile-time interface check.
var _ space.Agg = (*aggImpl)(nil)
