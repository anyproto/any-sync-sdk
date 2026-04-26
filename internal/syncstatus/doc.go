// Package syncstatus tracks per-space, per-object, and per-peer sync
// state for the SyncStatus accessor exposed by space/. Subscribes to
// eventbus for apply events and to anysyncx for transport/peer signals,
// computes derived status, and emits updates middleware can observe.
//
// Designed as a separate subsystem so status UI concerns stay out of
// the CRDT/apply hot path. See docs/00-common-context.md §"SDK
// Sections" (9) — the concrete status model is groomed in its own pass.
package syncstatus
