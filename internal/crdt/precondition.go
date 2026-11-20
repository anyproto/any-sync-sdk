package crdt

import (
	"context"
	"errors"
	"fmt"

	"github.com/anyproto/any-store/v2/query"
)

// ErrPreconditionFailed is a conditional local write's refusal: the
// dataset changed after the apply sequence the caller wrote against.
// Nothing was written.
var ErrPreconditionFailed = errors.New("crdt: precondition failed: the dataset changed after the given apply sequence")

var applySeqPath = []string{ApplySeqField}

// ChangedSince reports whether any record of this object's dataset,
// tombstones included, carries an _applySeq above seq — whether the
// dataset changed after a read that saw seq as its highest stamp.
//
// The caller holds the object's write lock (tree.Lock), which every
// apply takes, so the answer holds until the caller's own apply. Every
// apply advances the per-object watermark past the stamps it writes and
// nothing rewinds it (a reindex rebuilds rows with fresh stamps above
// it), so a watermark at or below seq answers without a scan. A keyed
// dataset's collection holds every object's rows; the scan is bounded
// to this object's. A shared dataset (on main, the per-space objects
// collection) is refused: its rows carry no object prefix, and Modify
// refuses a precondition on objects before the object loads.
func (c *Controller) ChangedSince(ctx context.Context, dataset string, seq uint64) (bool, error) {
	if c.IsShared(dataset) {
		return false, fmt.Errorf("crdt: ChangedSince: %q is a shared dataset", dataset)
	}
	if c.maxApplySeq <= seq {
		return false, nil
	}
	// A read-only open: a failed open is an error, not "no rows", and
	// an absent collection is not created.
	coll, err := c.openForRead(ctx, dataset)
	if err != nil {
		return false, fmt.Errorf("crdt: ChangedSince: %w", err)
	}
	if coll == nil {
		return false, nil
	}
	var filter query.Filter = query.Key{Path: applySeqPath, Filter: query.NewComp(query.CompOpGt, seq)}
	if c.IsKeyed(dataset) {
		filter = query.And{KeyedRows(c.objectId), filter}
	}
	it, err := coll.Find(filter).Limit(1).Iter(ctx)
	if err != nil {
		return false, fmt.Errorf("crdt: ChangedSince: %w", err)
	}
	changed := it.Next()
	if err := errors.Join(it.Err(), it.Close()); err != nil {
		return false, fmt.Errorf("crdt: ChangedSince: %w", err)
	}
	return changed, nil
}
