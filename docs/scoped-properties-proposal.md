# Scoped properties

Scope is a fixed attribute of a **declaration** (a property definition or
a dataset schema field), never of a value. One taxonomy, `schema.Scope`,
covers both:

| scope | write route | version domain | syncs to |
|---|---|---|---|
| `synced` | the object's own DAG change | object tree orderId | everyone with space access |
| `derived` | handler only (`Sink.Derive`) | the triggering change's | computed convergently |
| `account` | carrier record in the tech space → per-device mirror | tech tree orderId | the same account's devices |
| `local` | `Object.LocalSet`, no DAG | local lexid allocator | this device only |

- A property declares `scope` at creation; the default is `synced`. Scope
  is pinned first-write-wins like `kind` (a schema-bearing field):
  changing it means defining a new property with a new propId. `derived`
  is reserved for the built-ins at row root (`id`, `author`, `spaceId`,
  `createdAt`, `modifiedAt`, `modifiedBy`).
- A dataset schema field may declare any of the four scopes, with the
  same enforcement.
- A value lives at its normal path (`{ownerId}.{propId}` on the objects
  row, or the dataset field) and is written from exactly one route.
  There is no priority merge or fallback stack: a shared and a personal
  "favorite" are two properties, and clients compose them. An override
  stack would need row migration, computed roots, projection stripping
  and shadowed-write events for no capability two properties lack.

## Why one `_ver` tree stays safe

A path is only ever written from its declared scope's version domain,
so versions from different domains never gate against each other inside
a record's single `_ver` tree. The tech-space `spaces` dataset relies on
the same property: synced fields and the local `localStatus` share one
`_ver` map.

Consequences:

- Record shape, query reads, the `_ver.id` creation marker, sorting and
  paging are scope-agnostic.
- A subscribe frame carries one versionId, from whatever change produced
  it. The client recipe `_ver.<op.path> = frame.versionId` holds for
  every scope.
- `ModifyResult.VersionId` is meaningful because a write call is
  single-scope.

## Enforcement

Scope resolves by `(ownerId, propId)` in the registry for properties and
by the dataset schema for dataset fields.

- **Writer side, strict.** `Properties().Set` resolves every patch key's
  scope, routes by it, and validates kind and scope before writing. The
  local and account routes skip dataset handlers, so writer-side
  validation is their guard; both are SDK-internal routes, not inbound
  network surface.
- **Apply side, per-op drop.** `SystemPropertiesHandler.BeforeCreate` /
  `BeforeModify` drop an inbound DAG op whose property is declared
  `account`, `local` or `derived` (reason `scope_mismatch`, same surface
  as a kind mismatch). `classifyFieldWrite` does the same for declared
  dataset fields. The DataVersion gate parks changes whose definitions
  haven't synced.
- **Mirror side.** The mirror resolves scope before applying. An op for
  an unknown propId is skipped, not dropped: the carrier keeps the value
  and the next re-mirror retries once the definition syncs.
- **No scope conflicts.** PropIds are `base58(xxh3-64(changeId))`, so
  concurrent creates mint different ids; the scope pin is the only rule
  needed.

## Write API

```go
Set(ctx, objectId, ownerId string, patch map[string]any) (ModifyResult, error)
```

- Routes by each key's declared scope. Keys that don't resolve fall
  through to the chosen route's strict validation, which produces the
  precise rejection (`unknown_property`, `type_unknown`,
  `scope_mismatch`).
- **One scope per call.** A patch spanning scopes is rejected with the
  per-scope key split. Routes commit in different version domains with
  no cross-route rollback.
- Local-scope writes return the locally minted lexid as `VersionId`;
  account-scope writes return the carrier tree's version.
- `Get(ctx, objectId)` returns the row verbatim. `SetType`,
  `AttachCollection` and `DetachCollection` are synced-only.

Dataset records: `ModifyBatch.Scope = ScopeLocal` routes a batch through
`Object.LocalSet`. Records must already exist (explicit ids, no
`Upsert`), `TraceIds` are rejected, and the `objects` dataset is refused
(local property values go through `Properties().Set`). Wrong-scope ops
surface as `ModifyResult.Rejections`; absent records as
`ErrStrictSkipAbsent` rejections. `ModifyMany` and `Delete` are
synced-only. `ScopeAccount` and `ScopeDerived` are rejected.

## Account transport

- The tech space hosts one **carrier object** per target space: derived
  from seed `builtin:accountValues/<spaceId>`, dataset `account_values`
  (`internal/accountvalues`). Bounded DAG, lifecycle 1:1 with the target
  space.
- One carrier record per target `(objectId, dataset, recordId)`, keyed
  `objectId:dataset:recordId` (a colon never appears in CIDs). The
  objects row is `<objId>:objects:<objId>`. Record fields are the
  account-scoped paths verbatim; the record's `_ver` (tech domain),
  including entries retained by `$unset`, is the version source.
- **Injected applies** (`Change.Injected`, `Object.InjectedSet`): no DAG
  and no dataset handler, like a local write, but the versionId is the
  carrier's instead of locally minted. Events and applySeq flow through
  the normal apply path. `classifyFieldWrite` admits the injected route
  only on declared account fields and undeclared heads of
  `DynamicScopeByKey` datasets. Injected applies always use the object's
  cached controller (`store.Get`); a second controller would persist
  stale in-memory watermarks and regress the feed and restore cursors.
- **`Diff(carrierRec, targetRec, scopeResolver)`** is pure: value
  present → `$set`; absent with a retained version → `$unset`; unknown
  propId → skip; already applied → no-op. Ops are grouped by each path's
  carrier version, one `InjectedSet` per version, so no batch claims a
  version it doesn't carry.
- **Mirror** (`internal/spaceimpl/accountmirror.go`), one per loaded
  space. Every trigger runs the same idempotent reconcile:
  - space load: full re-mirror of every carrier record;
  - carrier events: coalesced into full reconciles;
  - target row created: replay that object's carrier record;
  - target row tombstoned: delete that object's carrier records;
  - periodic tick: retries skipped unknown-definition values and keeps
    the carrier resident between head-sync rounds.
- **The carrier is the replay log.** A carrier value for an object not
  present locally waits in the carrier until the row appears. A local
  `Set` with account scope on an object this device doesn't hold is a
  caller error.
- **Read-your-writes.** `Set` writes the carrier, then runs the injected
  apply inline; the mirror's later apply of the same change gates to a
  no-op.
- **Coverage.** The mirror handles objects rows only. Dataset fields may
  declare `account` and are enforced, but have no transport:
  `ModifyBatch.Scope = ScopeAccount` is rejected.

## Removal

Every removal has a defined owner; nothing treats "unknown" as "delete",
which would race the schema-sync window.

- **Unset a value.** Synced: a CRDT `$unset`. Account: a carrier
  `$unset` whose retained `_ver` entry lets the re-mirror propagate it to
  devices that were offline. Local: a row unset.
- **Object deleted.** The mirror observes the tombstone and tombstones
  the object's carrier records. Any device may issue the same deletes;
  CRDT deletes are idempotent and tombstones sticky, so this converges.
  A racing account write loses to the carrier tombstone, and injected
  applies skip tombstoned rows, so nothing resurrects.
- **Space left or deleted.** `DropAccountValues` deletes the carrier
  tree. This violates an any-sync rule: derived trees must not be
  deleted, because delete + re-derive yields the same id with fresh
  history. It needs to become record-level GC (tombstone every carrier
  record, keep the empty derived tree).
- **Property definition removed.** Carrier values under the dead propId
  are orphans with the same read tolerance as synced orphans (docs/data-structure.md).
  The mirror does not GC them: it cannot tell "removed" from "not synced
  yet". Dead propIds cannot come back (content-addressed ids), and object
  delete or space leave reclaims them.

## Rebuilds and local values

A handler version bump rebuilds an object by wiping its materialized rows
and replaying the tree (`internal/spaceobjects/reindex.go`). Local-scope
leaves never entered the DAG, so they are captured onto the object's
`_meta` row before the wipe and re-applied after the replay; an
interrupted rebuild resumes from that row on the next load. Account
values need no capture: the mirror replays the carrier when the row
re-materializes.

## applySeq

Account and local writes never enter the target space's DAG, so they
have no AddSeq and would be invisible to a feed keyed on it.

- `applySeq` is a per-space, SDK-owned monotonic counter, allocated on
  every apply that mutates a record (DAG, mirror, local), stamped on the
  row as `_applySeq` and persisted in the same WriteTx as the stamp. A
  lazily persisted counter could regress after a crash and re-mint seqs
  a consumer cursor already passed.
- `Space.Changes()` (`ChangedSince`, `Subscribe`) is keyed on applySeq.
  AddSeq keeps its own job as the any-sync → any-store restore watermark
  (per-object `MaxAddSeq`). AddSeq answers "is any-store caught up with
  any-sync"; applySeq answers "is the consumer caught up with any-store".
- Boot replay of already-applied changes is a content no-op, so the feed
  stays quiet. A rebuild mints fresh seqs for rows whose content changed,
  so downstream indexers re-feed incrementally.
- Tombstones carry `_applySeq`, so deletions stream in the same order.
- Rows written before applySeq existed are backfilled `applySeq :=
  addSeq` once per space, and the allocator seeds past the maximum, so
  cursors persisted in AddSeq units stay valid.

## Consumer caveats

- Account and local properties in shared spaces mean "my value": queries
  filtering on them select per-account or per-device result sets.
- Agents and automations must not condition shared mutations on account
  or local values ("delete where my-review-status = rejected" deletes
  the shared object for everyone).
- A local search index covers what the local account sees, account and
  local values included; per-device index divergence is correct.

## Open items

- Record-level carrier GC on space leave (see Removal).
- Account transport for dataset records: key records by their real
  `(dataset, recordId)` and target per-object dataset collections in the
  mirror.
- Re-mirror fast path: a per-record applied watermark to skip unchanged
  carrier records in spaces with many account values.
