// Package subscribe owns the per-space live-query engine. One Engine
// per loaded space; afterApply calls Engine.OnApply alongside the
// existing eventbus dispatcher, and the engine routes each event to
// matching querySubs whose windows are maintained incrementally.
//
// Two subscription scopes:
//
//   - Shared-objects: fires on every event whose dataset is the
//     per-space `objects` collection. Reached via Space.QueryObjects().
//   - Per-(objectId, dataset): fires only on exact matches.
//
// Per-sub window: entries map[string]*entry (id -> sort tuple) plus
// minRef/maxRef pointers for O(1) boundary access. When Limit > 0 the
// engine holds limit+1 entries so the largest-tuple entry serves as a
// sentinel; single-event shifts (top-of-sort arrivals) absorb cleanly
// without an any-store query. When too many records leave the held
// window without replacement (>= DriftBudgetPercent of limit), the sub
// closes with space.ErrSubscriptionDrifted; mailbox overflow closes
// with space.ErrSubscriptionOverflow. Both signal "resubscribe".
//
// Locking: a single engine.mu serializes register / close / OnApply.
// No per-sub locks. Holding engine.mu during the initial snapshot
// fences the apply path so the new sub never misses an event.
package subscribe
