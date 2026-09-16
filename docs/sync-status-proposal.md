# Sync Status

The SDK turns any-sync's per-tree status hooks into a per-space rollup
and per-object state, consumed in-process by middleware. There is no
transport: middleware picks the cadence and projects events to its
clients.

Three read shapes:

1. **Account-wide feed** — `space.Service.SubscribeStatus(cb)`: one
   event per space whenever its rollup changes. For list views
   ("42/187 synced").
2. **Per-object feed** — `Space.SyncStatus().SubscribeObject(objectId, cb)`.
3. **Snapshots** — `Service.Status(spaceId)`, `SyncStatus().Space()`,
   `SyncStatus().Object(objectId)`.

## Public API

```go
// space.Service
Status(spaceId string) SpaceSyncStatus
SubscribeStatus(cb func(SpaceSyncStatus)) (cancel func())

// Space.SyncStatus()
type SyncStatusAPI interface {
    Space() SpaceSyncStatus
    Object(objectId string) ObjectSyncStatus
    SubscribeObject(objectId string, cb func(ObjectSyncStatus)) (cancel func())
}

type SpaceSyncStatus struct {
    SpaceId      string
    State        SyncState
    Synced       int       // Total minus trees currently Syncing
    Total        int       // regular objects known locally
    NetworkPeers int       // responsible sync nodes with a live connection
    LocalPeers   int       // LAN peers sharing the space, live
    GlobalPeers  int       // internet-wide peers sharing the space, live
    P2P          P2PState  // LocalPeers + GlobalPeers summarized
    LastSyncedAt time.Time // latest acknowledged apply
}

type ObjectSyncStatus struct {
    ObjectId   string
    State      SyncState
    LastSyncAt time.Time
}
```

`SyncState` declares `Unknown`, `Offline`, `Syncing`, `Synced`, `Error`.
The rollup produces only `Unknown`, `Syncing` and `Synced`; connectivity
is reported through the peer counts and `P2P`.

`P2PState`: `NotPossible` (p2p disabled, or no usable interface),
`NotConnected`, `Connected` (a live LAN or global peer),
`Restricted` (OS denies local-network access).

Callbacks run synchronously on the dispatching goroutine, like
`Members().Subscribe` and `Service.Subscribe`. Keep them small or hand
off.

## Hook semantics

The per-space `Tracker` is passed to any-sync as
`commonspace.Deps.SyncStatus` and implements `syncstatus.StatusUpdater`:

```
HeadsChange  (treeId, heads)                     // local write (synctree.AddContent)
HeadsReceive (senderId, treeId, heads)           // pre-apply head update; ignored
ObjectReceive(senderId, treeId, heads)           // tree pulled from a peer
HeadsApply   (senderId, treeId, heads, allAdded) // post-apply
```

- `HeadsChange` → tree `Syncing`, pending = heads.
- `ObjectReceive` → tracks a new tree as `Syncing` with its heads pending.
- `HeadsApply` with `allAdded` from a responsible sender → drops the
  acknowledged heads; the tree is `Synced` once pending is empty.
- A tree fetched whole from a responsible peer counts as a
  `HeadsApply(allAdded=true)` for its heads, so a tree that only ever
  arrives by push still reaches `Synced`.
- A diff round against a responsible peer that reports zero new and
  zero changed trees marks every tracked tree `Synced` and anchors the
  space-level sync time. Trees the tracker has never seen then read as
  `Synced` too.

**Responsible senders** are the space's sync nodes plus connected
direct peers (LAN and global), so a space synced purely peer-to-peer
still reaches `Synced`.

**Excluded trees** — the ACL tree, the settings tree and the
`spaceIndex` object never count toward the rollup.

## Rollup

- `Total` = rows in the per-space `objects` collection
  (`spaceobjects.Store.RegularObjectCount`).
- `Synced` = `Total − pending`, clamped at 0. A tree with no hook
  history counts as synced.
- `State`: any pending tree → `Syncing`; else `Total == 0` with no sync
  time yet → `Unknown`; else `Synced`.

`Object(id)` returns the tree's tracked state; for an untracked id,
`Synced` if a zero-diff round has been seen, otherwise `Unknown`.

## Components

`internal/syncstatus`:

- **Service** (per account; built in `anysyncx.New`, exposed as
  `App.SyncStatus()`). Owns one `Tracker` per space (lazy, `For`), the
  account-wide subscriber registry, and the wired sources: node ids,
  direct-peer ids, `Total`, peer counts, `P2P` state.
- **Tracker** (per space). Per-tree state machine and per-object
  subscriber registry.
- **Rollup loop.** Every tracker update, peer-store change and
  discovery-possibility change marks the space dirty. A 1s tick drains
  the dirty set, recomputes each rollup, and dispatches when it differs
  from the last emitted one (`LastSyncedAt` alone does not trigger an
  event).
- **Peer presence.** Counts use the non-dialing live-connection check
  (`pool.Pick`), read at rollup time.

## SDK vs middleware

| Concern                               | SDK | Middleware |
| ------------------------------------- | --- | ---------- |
| Hook ingestion, per-tree state        | ✓   |            |
| Debounced rollup, event dedupe        | ✓   |            |
| Peer presence                         | ✓   |            |
| Projection to wire (proto / JSON)     |     | ✓          |
| Replay to fresh client sessions       |     | ✓          |
| Cross-feature counters (files, etc.)  |     | ✓          |

## Not covered

- File / storage counters.
- "Missing ids" diff from headsync.
- Persistence of status across restarts: state rebuilds from hooks and
  diff rounds after start.
