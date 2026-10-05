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
// apply also advances the per-object watermark past the stamps it
// writes, so a watermark at or below seq answers without a scan; a
// reindex restarts the watermark below the rows it rebuilds, so the
// shortcut is off while one is pending. A shared dataset (on main, the
// per-space objects collection) holds every object's rows, which this
// per-object scan cannot scope, so it is refused; Modify refuses a
// precondition on objects before the object loads.
func (c *Controller) ChangedSince(ctx context.Context, dataset string, seq uint64) (bool, error) {
	if c.IsShared(dataset) {
		return false, fmt.Errorf("crdt: ChangedSince: %q is a shared dataset", dataset)
	}
	if !c.reindexPending && c.maxApplySeq <= seq {
		return false, nil
	}
	// collectionForRead answers nil on any open error; a precondition
	// must not read that as "unchanged". The write is about to create
	// the collection anyway.
	coll, err := c.collectionForWrite(ctx, dataset)
	if err != nil {
		return false, fmt.Errorf("crdt: ChangedSince: %w", err)
	}
	it, err := coll.Find(query.Key{Path: applySeqPath, Filter: query.NewComp(query.CompOpGt, seq)}).Limit(1).Iter(ctx)
	if err != nil {
		return false, fmt.Errorf("crdt: ChangedSince: %w", err)
	}
	changed := it.Next()
	if err := errors.Join(it.Err(), it.Close()); err != nil {
		return false, fmt.Errorf("crdt: ChangedSince: %w", err)
	}
	return changed, nil
}
