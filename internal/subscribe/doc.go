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
// storage (SubConfig.Refill) when a held row leaves it or when a row's
// own sort move puts it in the sentinel slot with a larger key than it
// was ranked at: the window is re-read and the difference shipped, so
// it stays equal to a fresh query. A window that holds every match
// reads nothing when rows leave it, and a window that ends an apply
// at capacity with every row ranked needs no re-read. A change whose
// row could not be read is settled by the same re-read, for an
// unbounded sub too. The re-read runs through Event.Source, the object
// the apply ran on, so a replay that fires while the object loads
// refills as well; it looks up each held row it no longer returns, so
// the row's reason is storage's. While the re-read fails, a departure
// counts toward the drift budget once and the window's unranked state
// once, until a re-read ranks it, and the sub closes with
// space.ErrSubscriptionDrifted at DriftBudgetPercent of limit; an
// unreadable row with no refill closes it at once. A re-read deferred
// because storage is not final (an object rebuilding its rows from
// its tree) counts nothing: the store calls Engine.Resync when the
// rows are back, which re-reads every window on the object and every
// shared window that holds its row or was deferred. Mailbox overflow
// closes with space.ErrSubscriptionOverflow. Both signal
// "resubscribe".
//
// Events are built from the ids an apply touched, each with its view
// before the apply: the change's own event ships, under its
// VersionId, a row's one transition from that view to its view after
// the apply's records, however many records or implicit sentinel
// moves touched it in between; the re-read's event, with no VersionId
// and no ops, ships the transition from there to the row's view now.
//
// Locking: a single engine.mu serializes register / close / OnApply.
// No per-sub locks. Holding engine.mu during the initial snapshot
// fences the apply path so the new sub never misses an event.
package subscribe
