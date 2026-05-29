package space

import (
	"context"
	"errors"

	"github.com/anyproto/any-store/v2/anyenc"
)

// ErrNotFound is returned by Query.One when the query produced no
// match. Other lookups (Get, etc.) return their own dedicated errors.
var ErrNotFound = errors.New("space: not found")

// Query is the caller-facing query builder, returned by Space.Query.
// Chainable — each option returns a new Query (the underlying builder
// may be mutable but the public contract is immutable-looking).
type Query interface {
	// Filter applies a query condition. Anything query.ParseCondition
	// accepts works — an already-built query.Filter, a JSON-shaped
	// string ("{\"any.name\":\"Casablanca\"}"), or a map literal with
	// mongo-style operators ($eq, $gt, $in, $and, $or, …). The
	// argument is parsed eagerly; a malformed filter surfaces on the
	// first terminal call (Iter / All / One / Count), not silently.
	Filter(filter any) Query

	// Sort orders results by the given keys. Anything query.ParseSort
	// accepts works — string keys ("-_ver.id" for descending) or
	// already-built query.Sort values. Parsed eagerly; errors surface
	// on the terminal call.
	Sort(sorts ...any) Query

	// Limit caps returned records. Zero means unlimited.
	Limit(n int) Query

	// Offset skips the first n records. Use cautiously for large result
	// sets — cursor-based pagination is a future add.
	Offset(n int) Query

	// Projection controls which reserved fields appear in returned
	// records. See ProjectionOpts.
	Projection(opts ProjectionOpts) Query

	// Iter opens a streaming iterator. Caller must Close when done.
	Iter(ctx context.Context) (Iterator, error)

	// All materializes every match into a slice. Convenience wrapper
	// around Iter; avoid for large result sets.
	All(ctx context.Context) ([]*anyenc.Value, error)

	// One returns the first match or (nil, ErrNotFound).
	One(ctx context.Context) (*anyenc.Value, error)

	// Count returns the match count without returning documents.
	Count(ctx context.Context) (int, error)

	// Snapshot is the typed terminal that returns an initial result set
	// plus an optional one-shot total count, without subscribing for
	// live updates. Same result shape as Subscribe — callers that only
	// need a point-in-time view can use this instead of chaining
	// All + Count separately.
	Snapshot(ctx context.Context, opts QueryOpts) (*QueryResult, error)

	// Subscribe returns the initial snapshot AND a live QuerySubscription
	// whose mailbox carries incremental updates (added / updated /
	// removed records within the windowed view). The window tracks the
	// chained filter / sort / limit; offset applies to the initial
	// snapshot only.
	//
	// Sub closes with ErrSubscriptionOverflow when its mailbox fills,
	// or ErrSubscriptionDrifted when too many in-window records leave
	// without replacements. Both signal "resubscribe to recover".
	Subscribe(ctx context.Context, opts QueryOpts) (*QueryResult, error)
}

// QueryOpts is the option block for Snapshot and Subscribe. The same
// struct serves both terminals; subscription-only fields are documented
// and ignored by Snapshot.
type QueryOpts struct {
	// IncludeTotal asks for a one-shot count of filter-matching records
	// (independent of limit/offset), returned in QueryResult.Total.
	// Snapshot-only: no live total events are emitted by Subscribe.
	// Callers who need a refreshed count call Snapshot again. When
	// false, Total is -1.
	IncludeTotal bool

	// MailboxCapacity bounds the per-subscription event queue. Default
	// 256, minimum 16. On overflow the subscription closes; Wait
	// returns mb.ErrClosed and Err() returns ErrSubscriptionOverflow.
	// Subscribe-only.
	MailboxCapacity int

	// DriftBudgetPercent: when more than this fraction of Limit
	// records leave the held window without replacements, the
	// subscription closes with ErrSubscriptionDrifted. Default 30.
	// Ignored when Limit == 0. Subscribe-only.
	DriftBudgetPercent int
}

// QueryResult is what Snapshot and Subscribe return. Sub is nil for
// Snapshot, non-nil for Subscribe. Initial is always the materialised
// point-in-time view bounded by the chained limit/offset.
type QueryResult struct {
	Initial []*anyenc.Value
	Total   int               // -1 unless QueryOpts.IncludeTotal=true
	Sub     QuerySubscription // nil for Snapshot
}

// Iterator streams query results. Usage:
//
//	it, _ := q.Iter(ctx)
//	defer it.Close()
//	for it.Next() {
//	    doc, err := it.Doc()
//	    ...
//	}
type Iterator interface {
	Next() bool
	Doc() (*anyenc.Value, error)
	Err() error
	Close() error
}

// ProjectionOpts controls which reserved ("_"-prefixed) fields are
// visible in results.
type ProjectionOpts struct {
	// IncludeVariants returns _device/_account/_base namespaces on
	// property records. Default hides them; computed root values are
	// always present.
	IncludeVariants bool

	// IncludeMeta returns _ver, _traces, _deletedAt. Default hides
	// them unless the caller needs per-field version info (e.g. to
	// reconcile optimistic state).
	IncludeMeta bool
}
