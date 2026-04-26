package space

// SyncStatusAPI exposes per-object and per-peer sync status inside
// this space, plus a space-level aggregate. Backed by
// internal/syncstatus.
//
// Placeholder — groomed in the sync-status subsystem pass.
type SyncStatusAPI interface {
	// TODO: Overall, Object(objectId), Peers, Subscribe
}
