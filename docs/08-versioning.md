# Versioning & Re-indexing

## Vision
The SDK is a living library — new app versions add new dataset handlers, change existing handler logic, add system datasets, or modify indexing. When this happens, the any-store state (derived from replaying object tree changes through handlers) becomes stale and must be rebuilt.

Versioning is the mechanism that detects staleness and triggers re-indexing.

## Why We Need It

- **New handler added** — a dataset that was previously ignored now has a handler. Existing changes must be replayed so the new dataset appears in any-store
- **Handler logic changed** — the same change produces different any-store state. All existing records must be recomputed
- **Schema evolution** — a new version adds or renames fields; old data needs migration
- **New system datasets** — SDK adds a new built-in dataset (e.g., for search indexing); existing objects need to be re-indexed
- **Bug fixes in handlers** — silently corrupted state needs a rebuild

## Implemented today

**Version-driven re-index for compiled-in handler logic.**
`crdt.HandlerReg.Version` (public: `handler.Dataset.HandlerVersion`,
default 1) is the local version of a dataset's handler logic. Every apply
persists dataset→version onto the object's `_meta` row; a Controller
whose registered version differs from the stored one reports the dataset
stale at construction, and the object's next load rebuilds it: capture
local-scope values, rewind the watermark, wipe the materialized rows,
replay the whole tree through the current handlers, restore the local
values.

Bump `HandlerVersion` when a change to handler logic makes rows already
on disk wrong — a derived field that now holds a different shape, a stamp
computed differently. It is not the wire `DataVersion`: that one gates
PEERS, and bumping it parks the dataset's changes on every peer still
running older code. `HandlerVersion` never leaves the device.

Properties of the mechanism:

- **Lazy, per object.** The rebuild runs on load — the cost is the
  cold-restore cost, paid on first touch, on a path that opens the tree
  anyway.
- **Converged by a background sweep.** Lazy alone would leave the
  per-space `objects` collection mixing rows built by the old handler
  with rows built by the new one for as long as some object stays
  unopened, and a sort or filter over a rebuilt field would read both
  shapes. When a space store appears, a paced background sweep loads
  whatever the `_meta` scan reports stale. Nothing waits on it, and a
  store with nothing stale pays one scan.
- **Crash-safe.** The rewound watermark is persisted BEFORE the wipe and
  the stored versions are left stale until a replay re-applies something,
  so a crash anywhere in the middle simply rebuilds again on the next
  load. Once the replay has stamped versions, an interruption (a crash,
  or the sweep cancelled at shutdown mid-object) leaves the rebuild
  marked in flight on the `_meta` row; the next load resumes the replay
  from the persisted watermark and finishes the restore.
- **Not a deletion.** No `del` stamp, no Removed events: a `del` stamp is
  sticky and would evict the object from every consumer index for good.
  Live query subscriptions are not told about the wipe either — it never
  touches the subscribe engine — so a window keeps its rows and then
  sees the replay as one `updated` delta per change in the tree, with
  intermediate partial docs, converging when the replay ends.
- **Consumer cursors stay valid.** applySeq keeps climbing across a
  rebuild (the allocator seeds past the space's high-water mark), so a
  re-applied row simply surfaces as changed again. Only a wiped sdk.db,
  which restarts the axis at zero, mints a new space `Generation`.
- **Local-scope state survives.** Local values never entered the DAG, so
  no replay can reproduce them: they are captured before the wipe,
  persisted on the object's `_meta` row in the same upsert as the
  watermark rewind, and re-applied after the replay (bounded — past 20k
  leaves the excess is dropped with a warning). The persisted copy is
  what an interrupted rebuild restores from; it is cleared only when the
  restore has run. Account-scope values need no capture (the mirror
  replays its carrier records when the row re-materializes) and neither
  do read-tracking flags (the materializer recomputes them from unread
  entries).
- **Type objects rebuild without losing remote changes.** Wiping a type
  object drops its `shortIds` / `properties` / `datasets` collections, so
  the registry reads empty until the replay lands them. A remote change
  for that type arriving in the window fails the DataVersion gate's
  `KnownShortId` lookup and is parked, then drained once the defs are
  back — not dropped. Only local writes in the window see a transient
  `type_unknown` rejection.
- **Read state is not disturbed.** The replay re-applies every change in
  the tree, which would otherwise mark the whole object unread; read
  classification is suppressed for the duration, exactly as it is during
  a first-sight restore.
- **History rebuilds itself.** The per-object history collection goes
  with the wipe and the object is marked stale, so the existing lazy
  backfill re-indexes it.

**Runtime dataset schemas** (docs/17-user-datasets.md) cover the
runtime-defined half without a replay:

- **SchemaRev** — a fingerprint of the compiled declaration, stamped on
  each registration; a resident controller whose rev differs from the
  catalog's is stale and gets lazily evicted + rebuilt on first touch.
  Additive-only evolution means rebuilt registrations only need to APPLY
  future changes differently, never recompute stored ones.
- **Unknown-dataset parking** — changes for unregistered datasets park
  in `_detached` and drain once a registration exists (spec §8.2), so
  "new handler added" needs no wipe-and-rebuild for datasets whose
  changes arrived early.

**The CRDT version mark** (`space.CRDTVersion`, `techspace/crdtversion.go`)
is the account-wide scope — not a rebuild trigger but a compatibility
gate. The tech space's index object carries one system record
(`crdtVersion` dataset, record `crdtVersion`, field `version`) naming
the newest CRDT data-model version an SDK has written this account
with. The handler makes it monotonic on every replica: a lower value
is dropped on apply, so two devices racing to stamp it converge on
the maximum. `Open` reads it after the tech space is up: above the
SDK's own version it refuses with `space.ErrCRDTVersionNewer`
(`space.CRDTVersionNewerError` carries both numbers) and touches
nothing else; below or absent, it raises the mark in one synced
write. A newer mark arriving THROUGH SYNC while running — a restore on
a device with an older SDK, a second device upgraded first — flips the
account read-only: every user-authored synced write, tech space
included, fails with the same error, reads keep serving, and
`SDK.CRDTVersion()` reports `{Supported, Stored, Newer}` for the
embedder to show "upgrade required". Bump `space.CRDTVersion` when a
release writes data the previous release cannot read or would corrupt
by writing (a storage-model change like parts and modules, not a
handler-logic fix — those are `HandlerVersion`). The gate can only
protect releases that carry it: the version that introduced the mark
is the oldest one that refuses.

Still open: a forced `Reindex(objectId)` entry point and any-store
schema-level migrations.

## Version Scopes

Multiple version counters live in different parts of the system. Each scope answers a different question: "does this piece of state need to be rebuilt?"

Candidate scopes (to refine during grooming):

| Scope | Unit | Triggers rebuild of |
|-------|------|---------------------|
| **CRDT version** | whole account (tech-space mark) | Nothing — a newer mark refuses Open / turns the SDK read-only (implemented, above) |
| **SDK version** | whole SDK | Everything (migration) |
| **Handler version** | per dataset handler | All records in that dataset across all objects |
| **Dataset version** | per dataset registration | Records of that dataset |
| **Object-index version** | system object index | The index dataset only |
| **any-store schema version** | any-store DB | DB-level migrations (indexes, collections) |

## Re-indexing Flow

```
Controller construction (crdt/reindex.go):
  1. LoadAndSeedMeta reads the stored dataset→version map
  2. staleDatasets() diffs it against the registered HandlerReg.Versions
  3. Stale names hang off the Controller as its re-index verdict

Space store start (spaceimpl/service.go::storeFor):
  0. StartReindexSweep — scan _meta for this space's stale objects and
     load them in the background, paced; loading is the rebuild

Object load (spaceobjects/reindex.go, spaceobjects/store.go::loadObject):
  4. Capture local-scope leaves (per-object datasets by declared field,
     the shared objects row also by per-property scope from the registry)
     — or, for a rebuild already in flight, take the leaves persisted by
     the earlier attempt
  5. ResetForReindex — watermark to 0 and the leaves onto the `_meta`
     row, one upsert, persisted before anything is wiped
  6. Wipe — the object's row in each shared collection, every
     `<objectId>_*` collection, the space-level history rows
  7. ColdRestore replays the tree through the current handlers
     (read classification suppressed for the duration)
  8. Restore the captured local values via LocalSet; stamp the current
     handler versions and clear the in-flight mark (PersistVersions)

A load that finds the in-flight mark with versions already current skips
4–6 and runs 7–8 from the persisted watermark.
```

A dataset the object never wrote has no stored version and never
triggers a rebuild; a stored version for a dataset this controller
doesn't register is ignored, because a replay would not reproduce those
rows anyway. The compare is per (object, dataset); the wipe and replay
are per object, since one tree carries all its datasets.

## Source files
- `internal/crdt/reindex.go` — stale detection, rewind, version stamping
- `internal/spaceobjects/reindex.go` — capture, wipe, restore
- `internal/spaceobjects/store.go` — `loadObject` drives the sequence
- `internal/object/object.go` — `ColdRestore` / `replayLocked` replay the
  tree from the (rewound) watermark
- `any-sync/commonspace/object/tree/objecttree/` — `tree.Iterate` is the
  foundation for re-indexing
- `e2e/reindex_test.go` — the round trip: bump, rebuild, no rebuild
  without a bump

## Grooming Questions

### Version Granularity
1. Which version scopes do we actually need in v1? (Minimum: SDK version + handler version?)
2. Where are versions stored? (Dedicated system dataset in each object? Global in tech space? Local-only on device?)
3. Are versions per-device (re-index on each device independently) or synced (some other device already re-indexed, skip)?

### Re-indexing Strategy
4. ~~Full rebuild vs incremental~~ — **answered: always wipe and
   replay.** In-place migration needs migration code per version pair;
   the DAG is already the source of truth, so replaying it is both
   cheaper to write and impossible to get subtly wrong.
5. ~~When to run~~ — **answered: lazy, on object load.** Startup stays
   flat and the work rides a path that opens the tree anyway. An
   account-wide sweep would pay for objects nobody opens.
6. Large objects — can we re-index a million-record object without locking the UI?
7. ~~Failure handling~~ — **answered: no rollback, resume instead.**
   The rewound watermark is durable before the wipe and the stored
   versions stay stale until a replay re-applies something, so a crash
   mid-rebuild replays from the start of the tree on the next load; a
   crash after that resumes from the watermark, with the captured local
   leaves still on the `_meta` row.

### Handler Versioning
8. ~~How does a handler declare its version?~~ — **answered: a single
   integer on the registration** (`handler.Dataset.HandlerVersion` /
   `crdt.HandlerReg.Version`), not on the handler struct: a handler is
   pure behavior, its metadata lives with the registration. Zero means 1.
9. Multiple handlers for the same dataset across versions — coexist or hard replace?
10. Can the caller force a rebuild (`sdk.Reindex(objectId)`)? Still
    open — the machinery is in place, only the entry point is missing.

### Edge Cases
11. App downgrade — new version wrote data old handlers don't understand. What happens?
12. ~~Partial handler updates~~ — **answered: the whole object.** The
    compare is per dataset, but one tree carries every dataset of an
    object and a replay re-applies all of it, so a partial wipe would
    leave the untouched datasets double-applied.
13. Re-indexing while sync is active — what if new remote changes arrive mid-rebuild?

### Dependencies
14. Tightly coupled with CRDT (handlers) and Object (tree.Iterate). Affects startup time (Data Structure section).
