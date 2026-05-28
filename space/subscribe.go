package space

import (
	"errors"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/cheggaaa/mb/v3"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/eventbus"
)

// EventOp is one $set or $unset operation inside a SubRecord.Ops
// slice. Path is the dotted-segment field path; for $set, an empty
// Path activates the multi-field form (Payload is an object whose
// keys are dot-separated paths). $inc / $addToSet / $pull /
// $incGated / delete never reach the wire — those are projected
// down to $set / $unset against the post-apply value before
// delivery.
type EventOp = eventbus.EventOp

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

// SubscriptionEvent is one batch of windowed transitions delivered to
// a QuerySubscription. It groups every record-level change observed
// during one CRDT apply: records that entered the visible window
// (Added), records already in the window whose state changed
// (Updated), and records that left the visible window (Removed).
//
// Removed semantics — read carefully:
//
// Removed carries every id that left the visible window between
// apply ticks. Three engine-internal causes funnel into the same
// signal:
//
//   - Deleted: the record was tombstoned in the database.
//   - Filter-rejected: an update changed a field so the query's
//     filter no longer matches the record; the record still exists.
//   - Displaced: a higher-priority arrival pushed this record past
//     the Limit boundary; the record still matches the filter, but
//     sits outside the visible window now.
//
// They are NOT distinguished on the wire. From the consumer's view
// the action is the same regardless of cause: drop the id from your
// local mirror. If you need to know the record's current state, call
// Snapshot or Query.One with the id — that disambiguates (deleted ⇒
// ErrNotFound; filter-rejected ⇒ doc that doesn't match the active
// filter; displaced ⇒ doc that does). Causes are debugging-grade
// information, not view-rendering information; we deliberately don't
// branch view code on them.
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
	Removed   []string
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
