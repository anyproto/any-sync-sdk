package space

import (
	"context"
	"errors"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
)

// ErrBadPipeline wraps every pipeline normalization/parse/validation
// failure surfaced by Agg terminals — a malformed JSON pipeline, a
// non-array pipeline, an unknown stage or accumulator, or a $text /
// vector clause outside the pushdown prefix. errors.Is-able.
var ErrBadPipeline = errors.New("space: aggregate: bad pipeline")

// Aggregation limit sentinels, aliased from any-store so errors.Is
// works against either spelling. All three surface mid-iteration
// (from Iterator.Err / All / Count), not from Iter itself.
var (
	// ErrAggGroupLimitExceeded — too many unique $group keys.
	ErrAggGroupLimitExceeded = anystore.ErrGroupLimitExceeded
	// ErrAggAccumArrayLimitExceeded — a $push/$addToSet array grew past
	// the limit.
	ErrAggAccumArrayLimitExceeded = anystore.ErrAccumArrayLimitExceeded
	// ErrAggMemoryLimitExceeded — the blocking stages ($group state,
	// in-pipeline $sort) exceeded their retained-bytes budget.
	ErrAggMemoryLimitExceeded = anystore.ErrAggMemoryLimitExceeded
)

// Agg is the caller-facing aggregation builder, returned by
// Space.Aggregate / Space.AggregateObjects. It wraps any-store's
// MongoDB-style pipeline ($match / $sort / $skip / $limit / $count /
// $project / $addFields / $unwind / $group): the longest pushable
// prefix runs through the access planner (indexes, $text, vector),
// the rest streams in-pipeline. See any-store docs/aggregation.md for
// stage semantics and the deliberate MongoDB divergences.
//
// Like Query, the builder is single-shot — call Space.Aggregate again
// per read. Snapshot-only: there is no live/subscribe variant.
//
// Tombstoned rows are always excluded (a `_deletedAt missing` $match
// is prepended to the pipeline, so it stays in the pushdown prefix);
// there is no IncludeDeleted escape hatch. Deleted objects are purged
// rather than tombstoned, so they never appear here regardless; observe
// object deletions via QueryObjects().Subscribe (see ChangeIndexAPI).
// IndexHint is not exposed either; both are additive later if needed.
type Agg interface {
	// GroupLimit overrides the maximum number of unique $group keys
	// (any-store default 50 000; negative = unlimited).
	GroupLimit(n int) Agg

	// AccumArrayLimit overrides the maximum $push / $addToSet array
	// length (any-store default 10 000; negative = unlimited).
	AccumArrayLimit(n int) Agg

	// MemoryLimit overrides the retained-bytes budget shared by the
	// pipeline's blocking stages (any-store default 256 MiB; negative =
	// unlimited).
	MemoryLimit(bytes int) Agg

	// Iter executes the pipeline and streams result documents. Results
	// are pipeline output, not dataset rows — a $group doc carries the
	// group key as `id`, a $count doc is `{<name>: N}` with no id at
	// all. Each value is valid only until the next Next() call.
	Iter(ctx context.Context) (Iterator, error)

	// All materializes every result into a slice, cloned off the
	// iterator's reused buffers so caller-held pointers stay valid.
	All(ctx context.Context) ([]*anyenc.Value, error)

	// Count executes the pipeline and returns the number of result
	// documents. (A terminal $count stage instead emits the count as a
	// document.)
	Count(ctx context.Context) (int, error)

	// Explain returns the access plan of the pushed prefix plus the
	// in-pipeline stage list. Diagnostic only, NOT a stable format —
	// don't parse it.
	Explain(ctx context.Context) (string, error)
}
