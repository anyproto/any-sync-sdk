# Sync Status

## Vision

Caller-facing view of "what's the sync state of this space right now":

1. **Space-level aggregate** — single rollup state per space.
2. **Per-object state** — for any known objectId, is it synced / syncing / has local changes / errored.
3. **Recently-synced list** — last N objects to converge with a remote (useful for activity UI).
4. **Connected peers** (future) — network nodes + p2p, with connection state.

Consumed in-process by middleware. No transport here — middleware decides cadence and projects this to clients.

## What we already have

- **`syncstatus.StatusUpdater` hook** in any-sync (`commonspace.Deps.SyncStatus`). Currently wired to `NewNoOpSyncStatus()` in `internal/anysyncx/spacecache.go:90`. Fires on every head change / receive / apply, per tree, with senderId. This is the canonical inbound feed.
- **`object.AfterApply` hook** in our CRDT layer (`internal/spaceobjects/gate.go:74`). Already feeds eventbus + drainer; we can add a third fan-out for the tracker.
- **`nodeconf.NodeIds(spaceId)`** — responsible-node list for a space (network peers).
- **`pool.Pick(ctx, id)`** — non-dialing "is this peer currently connected?" check.
- **`HeadCache`** — converged per-space hash, already updated atomically with state writes.

## Hook semantics (any-sync)

```go
HeadsChange  (treeId, heads)                  // local heads moved (we wrote)
HeadsReceive (senderId, treeId, heads)        // remote announced heads
ObjectReceive(senderId, treeId, heads)        // remote delivered heads (pre-apply)
HeadsApply   (senderId, treeId, heads, allAdded) // ApplyChange landed
```

Match (heart's `core/syncstatus/objectsyncstatus/syncstatus.go`):
- `HeadsChange` → mark object `Syncing` with these heads pending.
- `HeadsApply(_, _, _, allAdded=true)` with senderId in `nodeconf.NodeIds` → object `Synced` (responsible node confirmed our heads landed there).
- Senders outside `NodeIds` (peers) → don't flip status; remember in tempSynced for later confirmation.

## Components

```
internal/syncstatus/
  tracker.go        per-space Tracker, owned by Service
  service.go        per-account Service, registry of trackers
  hooks.go          adapter implementing any-sync's syncstatus.StatusUpdater
  peers.go          network/p2p presence reader (pool.Pick poll + future p2p source)
  recent.go         bounded ring buffer of last-N applied objects
  events.go         per-tracker pub/sub (mb/v3 mailbox, same shape as eventbus)
```

### Tracker (per space)

```go
type Tracker struct {
    spaceId   string
    nodeIds   []string             // from nodeconf.NodeIds at construction
    trees     map[string]treeState // treeId → state
    recent    *ring                // last N applied
    peers     map[string]peerState
    dispatch  *mailbox             // status updates fan-out
}

type treeState struct {
    pendingHeads []string  // heads waiting for a responsible-node ack
    lastApplied  time.Time
    lastSender   string
    state        ObjectSyncState
}
```

### Service (per SDK)

- Constructed in `anysyncx.New` (or `sdk.Open` — see boot-ordering note below).
- `For(spaceId)` returns the per-space Tracker, building one on first call (mirrors how `headCache.observerFor` is wired at load).
- Implements `syncstatus.StatusUpdater` as a dispatcher: each method looks up the tracker by treeId/spaceId via a `spaceState`-driven init. The cleanest fit is one `StatusUpdater` *instance per space* (matches commonspace's per-space app graph) — `loadSpaceForCache` plucks `Tracker.For(id)` and passes it as `commonspace.Deps.SyncStatus`.

### Peer presence

- **Network**: every 1s (cheap), `pool.Pick` each `nodeconf.NodeIds(spaceId)`. Down → `ConnState=Down`; alive → `ConnState=Connected`. Set `LastSeenAt` on `peer.Peer.Context().Done()` close transitions.
- **P2P**: future. Slot is in the type; count is `0` until a p2p discovery source lands in `anysyncx`. Document the placeholder explicitly so callers don't read "0" as "broken".

### Recent

- Bounded ring (N=50 default; configurable). Push on `HeadsApply allAdded=true`. Each entry: `{ObjectId, At, FromPeer}`. Coalesce: if the same objectId is already at the head, bump its timestamp rather than duplicating.

## Public API (space/syncstatus.go)

```go
type SyncStatusAPI interface {
    // Space returns the rolled-up space state.
    Space(ctx context.Context) SpaceSyncStatus

    // Object returns the state for a specific objectId. Unknown ids
    // return SyncStateUnknown, not an error — middleware shouldn't
    // distinguish "never seen" from "missing".
    Object(ctx context.Context, objectId string) ObjectSyncStatus

    // Recent returns the last-applied objects, newest first. limit ≤ 0
    // returns the full ring (default cap 50).
    Recent(ctx context.Context, limit int) []ObjectSyncStatus

    // Peers returns connected peers — network nodes always, p2p when
    // available. v1: network only.
    Peers(ctx context.Context) []PeerInfo

    // Subscribe delivers status updates as they happen. The mailbox is
    // bounded and drops on overflow, matching eventbus semantics.
    Subscribe(ctx context.Context) (StatusSubscription, error)
}

type SpaceSyncStatus struct {
    State          SyncState
    NetworkPeers   int        // count of currently-connected responsible nodes
    P2PPeers       int        // 0 in v1
    LastSyncedAt   time.Time  // most-recent HeadsApply timestamp
    PendingObjects int        // count of trees in state != Synced
}

type ObjectSyncStatus struct {
    ObjectId   string
    State      SyncState
    LastSyncAt time.Time
    LastSender string  // empty when state == NotSynced
}

type PeerInfo struct {
    PeerId     string
    Kind       PeerKind     // PeerKindNetwork | PeerKindP2P
    ConnState  ConnState    // Connected | Down
    LastSeenAt time.Time
}

type SyncState uint8
const (
    SyncStateUnknown SyncState = iota
    SyncStateOffline   // no responsible node reachable
    SyncStateSyncing   // pending heads
    SyncStateSynced    // all known heads acked by a responsible node
    SyncStateError     // last sync attempt errored (transport + parse, not CRDT-level)
)
```

Update events carry the same record shape (one `ObjectSyncStatus` per affected object, plus an optional `SpaceSyncStatus` snapshot when the rollup transitions).

## Rollup rule (space state)

Highest priority wins:
1. **Offline** — zero `PeerInfo{Kind: Network, ConnState: Connected}`.
2. **Syncing** — any tree in `Syncing` *or* the network is reachable but our last `HeadsChange` is unconfirmed.
3. **Synced** — at least one responsible node connected and every known tree is in `Synced`.
4. **Unknown** — bootstrap state, never seen a successful sync yet.

`Error` is reserved for transport-level failures the peer manager surfaces (TBD; not in any-sync's StatusUpdater contract today).

## Boot ordering

`commonspace.Deps.SyncStatus` is passed in `loadSpaceForCache` (`spacecache.go:88`). The tracker has to exist *before* that call. Two options:

1. **Per-space lazy** — `App.syncStatusService.For(spaceId)` builds the tracker on first `loadSpaceForCache` for that id. Simple; the Service struct lives on `*anysyncx.App` alongside `headCache`.
2. **Pre-seeded** — `sdk.Open`'s eager-load loop (`sdk.go:115-125`) constructs trackers up-front from the tech-space List. Faster `Recent` answers for cold callers, but duplicates the lifecycle bookkeeping.

Recommend (1). The tracker has no expensive state to pre-warm.

## Subscriptions

Same shape as `eventbus.Dispatcher`:
- bounded `mb/v3` mailbox per subscriber
- `Mailbox()` exposes the full mb vocabulary (Wait for batched delivery, etc.)
- non-blocking sends; slow consumers drop and report via `Dropped()`
- closed when `Tracker` closes

One subscription = full firehose of status events for the space. v1 doesn't filter — callers re-filter client-side. (Mirrors `SubscribeProperties`.)

## Tests

- Unit: synthesize `StatusUpdater` calls, assert tracker state machine (HeadsChange → Syncing, HeadsApply allAdded with responsible sender → Synced, etc.).
- Integration: reuse `cold_sync_test.go` topology — alice writes, bob loads, both report `Synced` after convergence. Asserts the same end-state both sides report.
- Peer flap: stop responsible node, assert `Space → Offline`, restart, assert `→ Synced`.

## Open questions

1. **P2P slot now or later?** Adding `P2PPeers int` to `SpaceSyncStatus` and `PeerKindP2P` to the enum costs nothing today and prevents an API break later. Recommend including the slot, documenting "always 0 in v1".
2. **`Recent` cap** — 50 default? Per-space, in-memory only (lost on restart).
3. **`SyncStateError`** — heart conflates a few sources (incompatible version, storage quota, transport error). For the SDK we only have transport visibility. Worth modelling at all in v1, or drop until we have a concrete signal?
4. **Senders we don't recognize** — a writer-peer's `HeadsApply` confirms convergence-with-that-peer but not with a responsible node. Heart's solution is `tempSynced` + later confirmation when a node sender's `RemoveAllExcept` fires. Do we replicate that, or only count responsible-node senders for v1?
5. **Live presence vs polled** — `pool.Pick` is cheap but a 1s poll is still 1s/space. For accounts with many spaces, sharing a single peer-presence reader across spaces (keyed on `peerId`) is a small extra mile. Worth it now or revisit when we see contention?
