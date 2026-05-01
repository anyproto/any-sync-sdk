// Package eventbus is the per-space dispatcher for CRDT apply
// events. Each loaded space owns one Dispatcher; the per-Object
// AfterApply hook calls Dispatcher.Dispatch with the just-committed
// change, and the dispatcher routes it to matching subscribers.
//
// v1 surface — kept deliberately small:
//
//   - SubscribeProperties: firehose for the per-space `objects`
//     dataset (= property values across every object). Test-grade
//     coarse subscription; finer filters are out of scope.
//
//   - Subscribe(objectId, dataset): explicit (object, dataset) pair.
//     Fires only on exact matches.
//
//   - HasSubscribers: single atomic load. Apply hooks gate event
//     production on this so the cold-restore path stays free when
//     nobody is listening.
//
// Mailboxes are bounded buffered channels per subscriber; Dispatch
// uses non-blocking sends and drops on overflow. No backpressure
// signal in v1 — slow consumers lose events. See docs/06-data-
// structure.md §"Subscriptions / Event Flow" for the broader plan
// (per-field deltas, insert/update/delete classification) once a
// concrete UI need lands.
package eventbus
