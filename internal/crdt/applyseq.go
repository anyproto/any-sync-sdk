package crdt

import (
	"context"
	"sync"
)

// ApplySeqField is the reserved record field carrying the per-space
// apply sequence — the consumer-feed watermark. Unlike AddSeqField
// (any-sync's DAG delivery counter, which only DAG-borne changes have),
// _applySeq is minted by the SDK on EVERY apply that mutates a record:
// DAG changes, the tech-space account mirror's injected applies, and
// device-local LocalSet writes. It answers "is a consumer (indexer)
// caught up with any-store", while AddSeq keeps answering "is any-store
// caught up with any-sync" — two reconciliation links, each keyed by
// its upstream's coordinate. See docs/scoped-properties-proposal.md
// § applySeq.
//
// Strictly local: never on the wire, never compared across peers. Gaps
// are normal (a rolled-back tx skips its allocated seqs); only
// monotonicity matters.
const ApplySeqField = "_applySeq"

// ApplySeqAllocator mints the per-space apply sequence. One allocator
// per space, shared by every object Controller in it (the same sharing
// pattern as the per-space VersionAllocator).
//
// Seeding is lazy: the first Next runs seedFn to learn the highest
// already-persisted applySeq (after the one-off backfill that seeds
// legacy rows from their AddSeq, this is ≥ every historical AddSeq, so
// pre-existing consumer cursors stay valid on one continuous axis).
//
// Crash safety needs no separate counter row: every allocated seq that
// matters is persisted via some object's _meta row in the same WriteTx
// as the record stamp, and re-seeding reads the max of those — the
// counter can never regress below a persisted stamp. Callers must
// allocate AFTER acquiring the WriteTx (any-store's single writer then
// serializes allocation order = commit order, so an ascending-cursor
// consumer can't skip a seq that commits late).
type ApplySeqAllocator struct {
	mu     sync.Mutex
	last   uint64
	seeded bool
	seedFn func(ctx context.Context) (uint64, error)
}

// NewApplySeqAllocator builds an allocator that lazily seeds from
// seedFn. A nil seedFn seeds at 0 (fresh space, unit tests).
func NewApplySeqAllocator(seedFn func(ctx context.Context) (uint64, error)) *ApplySeqAllocator {
	return &ApplySeqAllocator{seedFn: seedFn}
}

// Seed forces the one-off lazy seed now, so no later Next runs seedFn
// while a WriteTx is held (the purge path allocates inside its tx). Call
// once at store load, single-threaded. Idempotent.
func (a *ApplySeqAllocator) Seed(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.seeded {
		return nil
	}
	if a.seedFn != nil {
		seed, err := a.seedFn(ctx)
		if err != nil {
			return err
		}
		a.last = seed
	}
	a.seeded = true
	return nil
}

// Next returns the next apply sequence, seeding on first use.
func (a *ApplySeqAllocator) Next(ctx context.Context) (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.seeded {
		if a.seedFn != nil {
			seed, err := a.seedFn(ctx)
			if err != nil {
				return 0, err
			}
			a.last = seed
		}
		a.seeded = true
	}
	a.last++
	return a.last, nil
}
