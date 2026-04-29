package object

import (
	"sync"

	"github.com/anyproto/lexid"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

// lexidParams matches any-sync's tree-internal allocator
// (commonspace/object/tree/objecttree/tree.go).
//
// CharsAllNoEscape: visible ASCII minus JSON-unsafe characters.
// blockSize=4, stepSize=100: leaves ample headroom for collaborative
// inserts at any point in the sequence.
var lexidGen = lexid.Must(lexid.CharsAllNoEscape, 4, 100)

// VersionAllocator hands out monotonic lexids for the future
// device-scope write path — changes that don't propagate through
// any-sync at all (e.g. per-device UI state). Any-sync-routed writes
// (the only kind today) take their VersionId from any-sync's
// per-tree OrderId on the StorageChange returned by AddContent /
// emitted by GetAfterAddSeq; that lexid is owned and persisted by
// any-sync, which is why it survives process restart. Using a
// fresh-on-restart local allocator on those paths regressed LWW
// gating and silently dropped writes (any new write's versionId
// could be lower than fields stamped in a prior session).
//
// Wiring is kept here so the device-scope path can land later
// without re-plumbing.
type VersionAllocator struct {
	mu   sync.Mutex
	last string
}

// NewVersionAllocator initialises with `last` as the highest VersionId
// already in use. Empty string is the "no version yet" sentinel; the
// first Next() call returns the smallest lexid for the configured
// alphabet.
func NewVersionAllocator(last crdt.VersionId) *VersionAllocator {
	return &VersionAllocator{last: string(last)}
}

// Last returns the most recently allocated VersionId without
// allocating a new one. Useful for persistence handshakes.
func (a *VersionAllocator) Last() crdt.VersionId {
	a.mu.Lock()
	defer a.mu.Unlock()
	return crdt.VersionId(a.last)
}

// Next allocates the next VersionId. Safe for concurrent use, though
// the apply path is single-threaded today.
func (a *VersionAllocator) Next() crdt.VersionId {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.last = lexidGen.Next(a.last)
	return crdt.VersionId(a.last)
}

// Bump raises the watermark to at least v. Used during cold restore
// to advance the allocator past any VersionId already stamped on
// records before live traffic resumes.
func (a *VersionAllocator) Bump(v crdt.VersionId) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if string(v) > a.last {
		a.last = string(v)
	}
}
