package space

import (
	"errors"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/cheggaaa/mb/v3"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

// EventOp is one $set or $unset operation inside a SubRecord.Ops
// slice. Path is the dotted-segment field path; for $set, an empty
// Path activates the multi-field form (Payload is an object whose
// keys are dot-separated paths). $inc / $addToSet / $pull /
// $incGated / delete never reach the wire — those are projected
// down to $set / $unset against the post-apply value before
// delivery.
type EventOp struct {
	Type    crdt.OpType
	Path    []string
	Payload *anyenc.Value
}

// QuerySubscription is the live handle returned by Query.Subscribe.
// It mirrors the shape of Subscription but carries SubscriptionEvent
// (added/updated/removed within a windowed view) instead of the raw
// per-change Event.
//
// The mailbox closes for one of three reasons:
//   - Caller invoked Close: Err() returns nil.
//   - Mailbox overflowed: Err() returns ErrSubscriptionOverflow.
//   - Held window drifted past its budget: Err() returns
//     ErrSubscriptionDrifted.
//
// In both error cases the client should resubscribe to recover.
type QuerySubscription interface {
	// Events returns the underlying mb/v3 mailbox. Use Wait for
	// batched delivery or WaitOne for single events.
	Events() *mb.MB[SubscriptionEvent]

	// Err returns the close reason after the mailbox closes. nil
	// while the subscription is live or after a user-initiated Close.
	Err() error

	// Close releases the subscription. Idempotent.
	Close() error
}

// RemoveReason classifies why a record left the visible window, carried
// per id on SubscriptionEvent.Removed. RemoveDeleted means the object is
// gone from the database; the other two mean it left your result set but
// still exists, so a Snapshot/Query.One would still return it.
type RemoveReason uint8

const (
	// RemoveDeleted: the record was tombstoned; it no longer exists in
	// the database. A Query.One for this id now returns ErrNotFound.
	RemoveDeleted RemoveReason = iota
	// RemoveFilteredOut: an update changed a field so the query's filter
	// no longer matches the record. The record still exists.
	RemoveFilteredOut
	// RemoveDisplaced: a higher-priority arrival (or the record's own
	// sort-key change) pushed it past the Limit boundary. The record
	// still matches the filter; it just sits outside the visible window.
	RemoveDisplaced
)

// String renders the reason for logging and test output.
func (r RemoveReason) String() string {
	switch r {
	case RemoveDeleted:
		return "deleted"
	case RemoveFilteredOut:
		return "filtered-out"
	case RemoveDisplaced:
		return "displaced"
	default:
		return "unknown"
	}
}

// RemovedRecord is one id that left the visible window, tagged with the
// cause. Symmetric with SubRecord on Added/Updated, minus the doc/ops —
// a removed record carries no post-apply payload.
type RemovedRecord struct {
	Id     string
	Reason RemoveReason
}

// SubscriptionEvent is one batch of windowed transitions delivered to
// a QuerySubscription. It groups every record-level change observed
// during one CRDT apply: records that entered the visible window
// (Added), records already in the window whose state changed
// (Updated), and records that left the visible window (Removed).
//
// Removed carries every id that left the visible window between apply
// ticks, each tagged with a RemoveReason. Branch on RemoveDeleted to
// tell "the object is gone" (drop it for good) from RemoveFilteredOut /
// RemoveDisplaced ("it left your result set but still exists" — a
// Snapshot or Query.One would still return it). See RemoveReason.
//
// VersionId carries the per-change DAG order of the underlying CRDT
// apply this event was derived from. Useful for consumers that want
// fence-and-replay semantics (e.g. "I've already processed up to
// version X — discard anything ≤ X"). Note VersionIds are
// locally-scoped: each peer assigns its own; don't compare across
// peers.
//
// Total is intentionally absent — the live counter is not maintained.
// Callers who need a refreshed count call Snapshot.
type SubscriptionEvent struct {
	VersionId crdt.VersionId
	Added     []SubRecord
	Updated   []SubRecord
	Removed   []RemovedRecord
}

// SubRecord is one record's worth of state inside a SubscriptionEvent.
// Doc carries the full post-apply value (cloned, safe to retain past
// the event). Ops carries the per-field $set / $unset ops from the
// triggering change — same payload shape as EventOp on the raw event
// stream, so callers can apply atomic updates against a local mirror
// without re-materialising the whole record.
type SubRecord struct {
	Id  string
	Doc *anyenc.Value
	Ops []EventOp
}

// ErrSubscriptionOverflow is returned by QuerySubscription.Err when
// the per-sub mailbox filled before the consumer could drain it. The
// engine drops the rest of the batch and closes the mailbox. The
// client should resubscribe to recover.
var ErrSubscriptionOverflow = errors.New("space: subscription overflowed; resubscribe required")

// ErrSubscriptionDrifted is returned by QuerySubscription.Err when
// the engine has lost more than QueryOpts.DriftBudgetPercent of the
// held window without replacements. The engine never re-queries
// any-store on the hot path to backfill — it closes the subscription
// and the client is expected to resubscribe.
var ErrSubscriptionDrifted = errors.New("space: subscription drifted (too many records left the held window); resubscribe required")

// ErrSubscribeUnsupported is returned by Query.Subscribe for query
// shapes the engine doesn't support yet (members in v1).
var ErrSubscribeUnsupported = errors.New("space: live subscription not supported for this query target")
