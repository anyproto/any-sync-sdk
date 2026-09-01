package space

import "time"

// SyncState is the high-level state of a space or single object. Used
// in both SpaceSyncStatus.State and ObjectSyncStatus.State.
//
// Rollup priority (highest match wins, see Service.Status):
//
//  1. Error   — incompatible network / needs update
//  2. Offline — no responsible node reachable
//  3. Syncing — at least one tracked tree has pending heads
//  4. Synced  — every tracked tree converged with a responsible node
//  5. Unknown — bootstrap; no hooks fired yet, no peers polled yet
type SyncState uint8

const (
	SyncStateUnknown SyncState = iota
	SyncStateOffline
	SyncStateSyncing
	SyncStateSynced
	SyncStateError
)

// String returns a stable lowercase token for logging / wire mapping.
func (s SyncState) String() string {
	switch s {
	case SyncStateOffline:
		return "offline"
	case SyncStateSyncing:
		return "syncing"
	case SyncStateSynced:
		return "synced"
	case SyncStateError:
		return "error"
	default:
		return "unknown"
	}
}

// P2PState is the direct-peer sync state of a space: whether this
// device is connected to LAN or global peers that share it.
type P2PState uint8

const (
	P2PStateUnknown P2PState = iota
	// P2PStateNotPossible — p2p is disabled in config, or the device
	// has no usable network interface.
	P2PStateNotPossible
	// P2PStateNotConnected — a p2p layer is on but no direct peer
	// sharing this space is connected.
	P2PStateNotConnected
	// P2PStateConnected — at least one direct peer (LAN or global)
	// sharing this space has a live connection.
	P2PStateConnected
	// P2PStateRestricted — the OS denies local-network access (e.g.
	// iOS Local Network permission).
	P2PStateRestricted
)

// String returns a stable lowercase token for logging / wire mapping.
func (s P2PState) String() string {
	switch s {
	case P2PStateNotPossible:
		return "notpossible"
	case P2PStateNotConnected:
		return "notconnected"
	case P2PStateConnected:
		return "connected"
	case P2PStateRestricted:
		return "restricted"
	default:
		return "unknown"
	}
}

// SpaceSyncStatus is the per-space rolled-up sync state. Returned by
// Service.Status and delivered on Service.SubscribeStatus events.
//
// Counts:
//   - Total  = number of regular objects known locally (the per-space
//     `objects` collection). Excludes ACL / settings / spaceIndex /
//     members system — non-user-visible trees.
//   - Synced = Total - count of trees the tracker currently holds in
//     SyncStateSyncing. A tree that has never produced a status hook
//     is treated as Synced — no evidence of work needed.
//   - NetworkPeers = responsible sync nodes with a live connection.
//   - LocalPeers = local-network (LAN) peers sharing this space with a
//     live connection.
//   - GlobalPeers = internet-wide (relay / hole-punched) peers sharing
//     this space with a live connection. P2P summarizes LocalPeers and
//     GlobalPeers as one state.
type SpaceSyncStatus struct {
	SpaceId      string
	State        SyncState
	Synced       int
	Total        int
	NetworkPeers int
	LocalPeers   int
	GlobalPeers  int
	P2P          P2PState
	LastSyncedAt time.Time
}

// ObjectSyncStatus is the per-object sync state. Returned by
// SyncStatusAPI.Object and delivered on SubscribeObject events.
//
// Unknown objectIds return State == SyncStateUnknown. ObjectId always
// matches the request (or the tracked id for an event).
type ObjectSyncStatus struct {
	ObjectId   string
	State      SyncState
	LastSyncAt time.Time
}

// SyncStatusAPI exposes per-space sync state and per-object
// subscriptions for one space. Obtained via Space.SyncStatus().
//
// Account-wide subscription lives on space.Service (SubscribeStatus)
// — middleware rendering a space list should subscribe there, not
// loop over per-space handles.
type SyncStatusAPI interface {
	// Space returns the rolled-up state for this space. Cheap; safe
	// to call on every UI render.
	Space() SpaceSyncStatus

	// Object returns the state for a specific objectId. Unknown ids
	// return ObjectSyncStatus{State: SyncStateUnknown}.
	Object(objectId string) ObjectSyncStatus

	// SubscribeObject delivers an ObjectSyncStatus event for
	// objectId on every state flip. cb runs synchronously on the
	// dispatcher goroutine — keep it small or hand off.
	SubscribeObject(objectId string, cb func(ObjectSyncStatus)) (cancel func())
}
