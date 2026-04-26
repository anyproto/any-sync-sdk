package space

import (
	"context"

	"github.com/anyproto/any-store/anyenc"
)

// Query is the caller-facing query builder, returned by Space.Query.
// Chainable — each option returns a new Query (the underlying builder
// may be mutable but the public contract is immutable-looking).
type Query interface {
	// Filter applies a mongo-style filter. Value is typically a
	// map[string]any; see docs/06-data-structure.md for supported
	// operators ($eq, $gt, $in, $and, …).
	Filter(filter any) Query

	// Sort orders results by the given keys. Prefix "-" for descending
	// (e.g. "-_ver.id").
	Sort(keys ...string) Query

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
