// Package subscribe owns the per-space live-query engine and the
// projected Event types the apply path hands to it. One Engine per
// loaded space; afterApply builds an Event via BuildEvent and calls
// Engine.OnApply, which routes each event to matching querySubs
// whose windows are maintained incrementally.
//
// Two subscription scopes:
//
//   - Shared-objects: fires on every event whose dataset is the
//     per-space `objects` collection. Reached via Space.QueryObjects().
//   - Per-(objectId, dataset): fires only on exact matches.
//
// Per-sub window: entries map[string]*entry plus minRef/maxRef
// pointers for O(1) boundary access. When Limit > 0 the engine holds
// limit+1 entries, each with its sort tuple and last doc encoded, so
// the largest-tuple entry serves as a sentinel; single-event shifts
// (top-of-sort arrivals) absorb cleanly without an any-store query.
// Entries with equal tuples keep the order storage returned them in,
// and a live arrival sorts after its ties: the sentinel of a tied edge
// is the row the read returned last, the one Initial leaves out.
// When Limit == 0 there is no boundary and an entry is membership
// only.
//
// A window at offset 0 with matches past its edge refills from storage
// (SubConfig.Refill) when a row leaves it, a row's rank becomes
// unknown, or a change's row cannot be read; the refill reads through
// Event.Source and looks up dropped rows for their reason. A refill
// deferred by an object's rebuild counts nothing, and Engine.Resync
// re-reads the window afterwards; a failed refill counts toward the
// drift budget (see settle), and the sub closes with
// space.ErrSubscriptionDrifted at DriftBudgetPercent of limit. Mailbox
// overflow closes with space.ErrSubscriptionOverflow. Both signal
// "resubscribe".
//
// Events: the change's own event ships, under its VersionId, each
// touched row's transition from before the apply to after the
// records; the refill's event, with no VersionId and no ops, the
// transition from there to now.
//
// Locking: a single engine.mu serializes register / close / OnApply.
// No per-sub locks. Holding engine.mu during the initial snapshot
// fences the apply path so the new sub never misses an event.
package subscribe
