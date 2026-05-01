package space

import "github.com/anyproto/any-sync-sdk/internal/eventbus"

// Event is one CRDT apply event delivered to a Subscription.
//
// v1 carries only the routing tuple plus AddSeq. There is no
// per-field delta or insert/update/delete classification —
// consumers re-query for current state via Space.Query /
// Space.QueryObjects when a richer view is needed.
type Event = eventbus.Event

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
