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
// When Limit == 0 there is no boundary and an entry is membership
// only. A window at offset 0 with matches past its edge refills from
// storage (SubConfig.Refill) when a held row leaves it, when a row's
// own sort move puts it in the sentinel slot unranked, or when a
// change's row could not be read: the window is re-read and the
// difference shipped, so the window stays equal to a fresh query. A
// window that holds every match reads nothing. A departure the engine
// cannot refill counts toward the drift budget, and the sub closes
// with space.ErrSubscriptionDrifted at DriftBudgetPercent of limit;
// an unreadable row with no refill closes it at once. Mailbox overflow
// closes with space.ErrSubscriptionOverflow. Both signal
// "resubscribe".
//
// Locking: a single engine.mu serializes register / close / OnApply.
// No per-sub locks. Holding engine.mu during the initial snapshot
// fences the apply path so the new sub never misses an event.
package subscribe
