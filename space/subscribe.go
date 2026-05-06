package space

import "github.com/anyproto/any-sync-sdk/internal/eventbus"

// Event is one CRDT apply event delivered to a Subscription.
//
// Pipeline: receive change → apply to CRDT → commit any-store tx →
// fire this event. By the time it reaches a subscriber the change
// is durable; a follow-up Query in the same process reflects the
// same state.
//
// Carries the routing tuple, the per-change VersionId, and the
// post-apply effect projected to a flat $set/$unset op list per
// record (Records). A consumer applies Records to a JSON-like
// local copy without needing a CRDT engine — `$inc`, `$addToSet`
// etc. have already been merged on this side.
type Event = eventbus.Event

// EventRecord is one record's worth of projected change inside an
// Event. See eventbus.EventRecord for details.
type EventRecord = eventbus.EventRecord

// EventOp is one $set or $unset op inside an EventRecord. By
// construction, only $set and $unset ever ship — see
// eventbus.EventOp.
type EventOp = eventbus.EventOp

// Subscription is the consumer-side handle returned by
// Space.Subscribe and Space.SubscribeProperties.
//
// Mailbox() exposes the underlying mb/v3 mailbox so callers can use
// its full vocabulary:
//
//   - Wait(ctx) for batched delivery (free coalescing — one call
//     returns every event currently buffered).
//   - WaitOne(ctx) for single-event consumers.
//   - NewCond / WithMin / WithFilter for filtered or threshold
//     waits.
//
// Close releases the mailbox; subsequent Wait calls return
// mb.ErrClosed. The mailbox is bounded — slow consumers drop events
// rather than stall the apply pipeline. v1 has no overflow signal.
type Subscription = eventbus.Subscription
