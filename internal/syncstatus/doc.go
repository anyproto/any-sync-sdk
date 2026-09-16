// Package syncstatus tracks per-space, per-object sync state from
// any-sync's StatusUpdater hooks and exposes it via the SyncStatusAPI
// surfaced on space.Space. Two subscription scopes are supported:
//
//   - Account-wide via space.Service.SubscribeStatus (one cb sees every
//     space's rollup transitions; backed by Service's registry).
//   - Per-object via Space.SyncStatus().SubscribeObject (one cb per
//     objectId; backed by per-Tracker registry).
//
// The Service owns one Tracker per space (lazy, via For(spaceId)) and
// the account-wide subscriber registry. The Tracker holds the
// per-tree state machine and per-object subscriber registry. Each
// Tracker is wired into commonspace.Deps.SyncStatus when its space loads.
package syncstatus
