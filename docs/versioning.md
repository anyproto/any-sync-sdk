# Versioning & Re-indexing

The any-store state is derived: every object's rows are its tree replayed
through the registered handlers. When handler logic changes, rows already
on disk can be wrong. Versioning detects that and rebuilds them, and keeps
an older SDK from writing to an account a newer one has already shaped.

## Version counters

| Counter | Scope | Stored in | On mismatch |
|---|---|---|---|
| `HandlerVersion` | dataset registration, this device only | the object's `_meta` row | wipe and replay the object |
| `SchemaRev` | runtime dataset registration | the registration | evict and rebuild the controller, no replay |
| `DataVersion` | one change, on the wire | the change | a peer parks the change until it has the schema state it names (types-properties-proposal.md § Change-level `DataVersion`) |
| `space.CRDTVersion` | account | a tech-space system record | `Open` refuses; at runtime the account turns read-only |

There is no forced `Reindex(objectId)` entry point and no any-store
schema migration.

## Handler version

`handler.Dataset.HandlerVersion` (`crdt.HandlerReg.Version`; zero means 1)
is the version of a dataset's apply logic. A dataset served by the generic
schema handler (nil `Handler`, or a module without a bespoke handler)
persists `crdt.ComposeVersion(SchemaHandlerVersion, HandlerVersion)`, so a
bump on either the SDK side or the consumer side rebuilds it.

Bump `HandlerVersion` when a handler change makes rows already on disk
wrong: a derived field with a new shape, a stamp computed differently. Do
not bump `DataVersion` for this. `DataVersion` gates peers, and bumping it
parks the dataset's changes on every peer still running older code.
`HandlerVersion` never leaves the device.

One registration serves each dataset name, so a new handler version
replaces the old one outright.

### Detection

Every apply persists dataset→version on the object's `_meta` row. A
Controller reports a dataset stale when the stored version differs from the
registered one, in either direction: a downgrade rebuilds too. A dataset the
object never wrote has no stored version and never triggers a rebuild. A
stored version for a dataset the Controller does not register is ignored,
because a replay would not reproduce those rows.

The comparison is per (object, dataset); the rebuild is per object, because
one tree carries every dataset of the object and a replay re-applies all of
it.

### Rebuild

The rebuild always wipes and replays. The DAG is the source of truth, so
replaying it needs no per-version migration code.

- **Lazy, per object.** The rebuild runs inside the object's cache load,
  before the object reaches any caller, on a path that opens the tree
  anyway. A large object's first load pays the full replay.
- **Converged by a background sweep.** Lazy rebuilds alone would leave the
  per-space `objects` collection mixing rows built by the old and the new
  handler while some objects stay unopened, and a sort or filter over a
  rebuilt field would read both shapes. When a space store starts, a paced
  sweep loads every object the `_meta` scan reports stale. Nothing waits on
  it; a store with nothing stale pays one scan.
- **Crash-safe.** The rewound watermark is persisted before the wipe, and
  the stored versions stay stale until a replay re-applies something, so a
  crash mid-way rebuilds again on the next load. Once the replay has
  stamped versions, an interruption (a crash, or the sweep cancelled at
  shutdown) leaves the rebuild marked in flight on the `_meta` row; the
  next load resumes the replay from the persisted watermark and finishes
  the restore.
- **Not a deletion.** No `del` stamp and no Removed events: a `del` stamp is
  sticky and would evict the object from every consumer index. Live query
  subscriptions are not told about the wipe, so a window keeps its rows and
  sees the replay as one `updated` delta per change, with intermediate
  partial docs, converging when the replay ends.
- **Consumer cursors stay valid.** applySeq keeps climbing across a rebuild
  (the allocator seeds past the space's high-water mark), so a re-applied
  row surfaces as changed again. Only a wiped sdk.db restarts the axis at
  zero, which mints a new space `Generation`.
- **Local-scope values survive.** They never entered the DAG, so no replay
  reproduces them. They are captured before the wipe, persisted on the
  `_meta` row in the same upsert as the watermark rewind, and re-applied
  after the replay (at most 20,000 leaves; the excess is dropped with a
  warning). An interrupted rebuild restores from the persisted copy, which
  is cleared once the restore has run. Account-scope values need no capture
  (the account mirror replays its carrier records when the row
  re-materializes), and neither do read-tracking flags (the materializer
  recomputes them from unread entries).
- **Type objects keep remote changes.** Wiping a type object drops its
  `shortIds` / `properties` / `datasets` collections, so the registry reads
  empty until the replay restores them. A remote change for that type
  arriving in the window fails the DataVersion gate's `KnownShortId` lookup
  and is parked, then drained once the definitions are back. Local writes in
  the window see a transient `type_unknown` rejection.
- **Read state is untouched.** Replaying every change would mark the whole
  object unread; read classification is suppressed for the duration, as
  during a first-sight restore.
- **History re-indexes lazily.** The per-object history rows go with the
  wipe and the object is marked stale, so the history backfill rebuilds
  them.

### Flow

```
Controller construction (crdt/reindex.go):
  1. LoadAndSeedMeta reads the stored dataset→version map
  2. staleDatasets() diffs it against the registered HandlerReg.Versions
  3. Stale names hang off the Controller as its re-index verdict

Space store start (spaceimpl/service.go::storeFor):
  0. StartReindexSweep: scan _meta for this space's stale objects and
     load them in the background, paced; loading is the rebuild

Object load (spaceobjects/reindex.go, spaceobjects/store.go::loadObject):
  4. Capture local-scope leaves (per-object datasets by declared field,
     the shared objects row also by per-property scope from the registry),
     or, for a rebuild already in flight, take the leaves persisted by
     the earlier attempt
  5. ResetForReindex: watermark to 0 and the leaves onto the `_meta`
     row, one upsert, persisted before anything is wiped
  6. Wipe the object's row in each shared collection, every
     `<objectId>_*` collection, the space-level history rows
  7. ColdRestore replays the tree through the current handlers
     (read classification suppressed)
  8. Restore the captured local values via LocalSet; stamp the current
     handler versions and clear the in-flight mark (PersistVersions)

A load that finds the in-flight mark with versions already current skips
4–6 and runs 7–8 from the persisted watermark.
```

## Runtime dataset schemas

Runtime-defined datasets (docs/user-datasets.md) change without a replay:

- **SchemaRev** is a fingerprint of the compiled declaration, stamped on
  each registration. A resident Controller whose rev differs from the
  catalog's is stale and is evicted and rebuilt on first touch. Schema
  evolution is additive, so a rebuilt registration only applies future
  changes differently and never recomputes stored rows.
- **Unknown-dataset parking.** Changes for a dataset with no registration
  park in `_detached` and drain once one exists (crdt-spec.md §8.2), so
  a newly added handler needs no wipe for changes that arrived early.

## CRDT version mark

`space.CRDTVersion` (`internal/techspace/crdtversion.go`) is an account-wide
compatibility gate, not a rebuild trigger. The tech space's index object
carries one system record (dataset `crdtVersion`, record `crdtVersion`,
field `version`) naming the newest CRDT data-model version an SDK has
written this account with. The handler keeps it monotonic on every replica:
a lower value is dropped on apply, so devices racing to stamp it converge on
the maximum.

- **At `Open`**, after the tech space is up: a mark above the SDK's version
  refuses with `space.ErrCRDTVersionNewer` (`space.CRDTVersionNewerError`
  carries both numbers) and touches nothing else. Below or absent, the SDK
  raises the mark in one synced write.
- **At runtime**, a newer mark arriving through sync (a restore on a device
  with an older SDK, a second device upgraded first) makes the account
  read-only: every user-authored synced write, tech space included, fails
  with the same error, reads keep serving, and `SDK.CRDTVersion()` reports
  `{Supported, Stored, Newer}` so the embedder can show "upgrade required".

Bump `space.CRDTVersion` when a release writes data the previous release
cannot read or would corrupt by writing: a storage-model change, not a
handler-logic fix (that is `HandlerVersion`). The gate protects only
releases that carry it.

## Source files

- `internal/crdt/reindex.go`: stale detection, rewind, version stamping
- `internal/spaceobjects/reindex.go`: capture, wipe, restore, sweep
- `internal/spaceobjects/store.go`: `loadObject` drives the sequence
- `internal/object/object.go`: `ColdRestore` / `replayLocked` replay the
  tree from the rewound watermark
- `internal/techspace/crdtversion.go`: the CRDT version mark
- `e2e/reindex_test.go`: bump, rebuild, no rebuild without a bump
