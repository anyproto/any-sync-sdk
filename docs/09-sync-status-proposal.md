# Sync Status

## Vision

Caller-facing view of "what's the sync state of this space right now",
consumed in-process by middleware. The SDK ingests any-sync's per-tree
status hooks, runs a per-space rollup, and exposes:

1. **Account-wide firehose** — one event per space whenever its rollup
   transitions. Middleware paints a list view ("42/187 synced") off
   this. `space.Service.SubscribeStatus(cb)`.
2. **Per-object subscription** — one event per state flip for a single
   objectId. Middleware paints a per-object indicator off this.
   `Space.SyncStatus().SubscribeObject(objectId, cb)`.
3. **Point-in-time getters** — snapshot reads of either scope:
   `Service.Status(spaceId)`, `SyncStatus().Space()`,
   `SyncStatus().Object(objectId)`.

No transport here. Middleware decides cadence + projects to clients.

## What we already have

- **`syncstatus.StatusUpdater` hook** in any-sync (per-space, passed
  via `commonspace.Deps.SyncStatus` at `internal/anysyncx/spacecache.go:90`).
  Currently wired to `NewNoOpSyncStatus()`. Fires on every head
  change / receive / apply. This is the canonical inbound feed.
- **`spaceobjects.Store`** maintains the per-space `objects` shared
  dataset — one row per regular object. We read its size for `Total`.
- **`nodeconf.NodeIds(spaceId)`** — responsible-node id list for the
  "is this sender a node?" filter.
- **`pool.Pick(ctx, id)`** — non-dialing "is this peer currently
  connected?" check. Fed into the per-space `ConnectionStatus` map.
- **`nodeconf.NetworkCompatibilityStatus()`** — incompatible-version /
  needs-update signal that maps to `SyncStateError`.

## Hook semantics (any-sync v0.12.4)

Confirmed call sites in any-sync (v0.12.4):

```
HeadsChange  (treeId, heads)                     // synctree.AddContent (local write)
HeadsReceive (senderId, treeId, heads)           // pre-apply HeadUpdate — UNUSED in v1
ObjectReceive(senderId, treeId, heads)           // BuildSyncTree from non-responsible pull
HeadsApply   (senderId, treeId, heads, allAdded) // synchandler post-apply
```

Tracker reactions:

- `HeadsChange` → mark tree `Syncing(pendingHeads=heads)`.
- `HeadsApply(_, allAdded=true)` from responsible-node sender → drain
  matching heads, mark `Synced` once `pendingHeads` empties.
- `ObjectReceive` → ensure the tree is tracked (so a newly discovered
  tree shows up in the rollup).
- `HeadsReceive` → no-op (matches heart; nothing to do pre-apply).

`RemoveAllExcept` / `tempSynced` machinery from heart is **not**
replicated — it's driven by heart-internal triggers that don't fire
in the SDK's universe. v1 keeps the convergence model simple: only
responsible-node `HeadsApply` flips `Synced`.

## Public API

```go
// space.Service additions
type Service interface {
    // ...existing...

    // Status returns a snapshot of one space's rollup.
    Status(spaceId string) SpaceSyncStatus

    // SubscribeStatus delivers an event per space-rollup transition.
    // Account-wide; one cb sees every space.
    SubscribeStatus(cb func(SpaceSyncStatus)) (cancel func())
}

// space.Space adds (already declared):
//   SyncStatus() SyncStatusAPI

type SyncStatusAPI interface {
    // Space returns the rolled-up state for this space.
    Space() SpaceSyncStatus

    // Object returns the state for a specific objectId. Unknown ids
    // return SyncState=Unknown — middleware shouldn't distinguish
    // "never seen" from "missing".
    Object(objectId string) ObjectSyncStatus

    // SubscribeObject delivers ObjectSyncStatus events for one object
    // on every state flip. cb runs synchronously on the dispatcher
    // goroutine — keep it small.
    SubscribeObject(objectId string, cb func(ObjectSyncStatus)) (cancel func())
}

type SpaceSyncStatus struct {
    SpaceId      string
    State        SyncState
    Synced       int       // converged with a responsible node
    Total        int       // total regular objects known locally
    NetworkPeers int       // currently-connected responsible nodes
    LastSyncedAt time.Time // most-recent successful HeadsApply
}

type ObjectSyncStatus struct {
    ObjectId   string
    State      SyncState
    LastSyncAt time.Time
}

type SyncState uint8
const (
    SyncStateUnknown          SyncState = iota
    SyncStateOffline                    // no responsible node reachable
    SyncStateSyncing                    // pending heads
    SyncStateSynced                     // all known trees acked by a node
    SyncStateError                      // incompatible / needs-update
)
```

All callbacks run synchronously on the dispatch goroutine, matching
`Members().Subscribe` and `Service.Subscribe` — consistent with the
rest of the SDK.

## Rollup rule

Priority — highest match wins:

1. **Error** — `nodeconf.NetworkCompatibilityStatus()` is `Incompatible`
   or `NeedsUpdate`.
2. **Offline** — zero currently-connected responsible nodes.
3. **Syncing** — at least one responsible node connected *and* any
   tree is in pending state (`Synced < Total`).
4. **Synced** — at least one responsible node connected and every
   tracked tree converged.
5. **Unknown** — bootstrap; no hooks fired yet, no peers polled yet.

`Total` is the count of regular-object rows in the per-space `objects`
collection (`spaceobjects.Store`). Excludes ACL tree, spaceIndex,
settings, members system collection — not user-visible.

`Synced = Total - Pending`, where `Pending` is the number of tracked
trees currently in `Syncing`. A tree we've never seen a hook for is
treated as `Synced` (no evidence of work needed) — matches user
intuition and works correctly through cold restore as `ObjectReceive
→ HeadsApply` transitions land each new tree through `Syncing → Synced`.

## Components

```
internal/syncstatus/
  service.go      Per-account; registry of trackers; account-wide
                  subscribe registry; owns peer-presence reader and
                  per-space ConnectionStatus map.
  tracker.go      Per-space; implements syncstatus.StatusUpdater;
                  treeHeads state machine; per-object subscribe registry.
  rollup.go       Refresh-debounce queue + 1s tick (matches heart).
                  Composes SpaceSyncStatus and dispatches to subscribers
                  on edge transitions only.
  peers.go        Single-goroutine pool.Pick reader (1s tick across
                  the union of all loaded spaces' NodeIds). Writes
                  the ConnectionStatus map; nudges trackers via Refresh().
  subscribers.go  Sync-callback registry primitive used by both the
                  account-wide and per-object scopes.
```

### Tracker (per space)

```go
type treeState struct {
    pendingHeads []string
    state        SyncState // Syncing | Synced (never Unknown after first hook)
    lastApplied  time.Time
}

type Tracker struct {
    spaceId string
    nodeIds []string
    mu      sync.Mutex
    trees   map[string]treeState
    subs    *subscriberRegistry[ObjectSyncStatus] // keyed by objectId
}
```

### Service (per account)

- Constructed inside `anysyncx.New`, exposed via `App.SyncStatus()`.
- `For(spaceId)` lazily builds a `*Tracker` on first call. Returned
  trackers are reused — `loadSpaceForCache` plucks the tracker as
  `commonspace.Deps.SyncStatus`.
- Holds the account-wide `subscriberRegistry[SpaceSyncStatus]` and
  the per-space `ConnectionStatus` map.

### Rollup loop

Pattern adopted from heart's `spacesyncstatus`:

- `Refresh(spaceId)` enqueues an id in a dedupe set.
- 1s tick drains the set, recomputes each space's rollup, compares
  to last-emitted, dispatches to subscribers on inequality.
- Each per-space `Tracker` state change calls `service.Refresh(spaceId)`
  to schedule the rollup.

### Peer presence

Single goroutine on the Service. 1s tick over the union of
`nodeConf.NodeIds(s)` across every currently-loaded space. `pool.Pick`
each id — present → `Online`, absent → `ConnectionError`. On
transitions, write the per-space map and `Refresh(spaceId)`.

Sharing the reader across spaces means one tick regardless of space
count. Trackers don't own goroutines for this.

## Boot ordering

The per-space `Tracker` *is* the `commonspace.Deps.SyncStatus`
instance. `loadSpaceForCache` calls `app.SyncStatus().For(spaceId)`
and passes the result inline. No pre-seeding; lazy is fine — the
tracker has no expensive state.

## What lives inside the SDK vs outside

| Concern                                | SDK | Middleware |
| -------------------------------------- | --- | ---------- |
| `StatusUpdater` hook ingestion         | ✓   |            |
| Per-tree state machine                 | ✓   |            |
| 1s refresh-debounce rollup             | ✓   |            |
| Peer-presence reader (`pool.Pick`)     | ✓   |            |
| Dedupe identical events                | ✓   |            |
| Network-compatibility status read      | ✓   |            |
| Projection to wire (proto / JSON)      |     | ✓          |
| Replay to fresh client sessions        |     | ✓          |
| Cross-feature counters (files, etc.)   |     | ✓          |

## Tests

- Unit: synthesize `StatusUpdater` calls, assert tracker state machine
  (HeadsChange → Syncing, HeadsApply(responsible, allAdded) → Synced).
- Rollup: bound the in-test tick or expose a test-only `tickNow()` so
  edges fire deterministically.
- Integration: extend `cold_sync_test.go` topology — alice writes,
  bob loads, both report `State=Synced` and `Synced==Total>0` after
  convergence.
- Peer flap: stop responsible node, assert `State=Offline`; restart,
  assert `State=Synced`.

## Out of scope for v1

- P2P presence (slot reserved later if needed; not modelled now)
- File / storage counters
- "Missing ids" diff from headsync (heart's `UpdateMissingIds`)
- Session-replay for fresh subscribers (middleware concern)
- Persistence of any status state across restarts
