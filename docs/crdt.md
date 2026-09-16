# CRDT

Every object stores records in datasets. A record is an anyenc object,
edited with Mongo-style field operations (`$set`, `$inc`, `$addToSet`, …).
This doc is the overview; the complete specification (operations, change
format, apply algorithm, handlers, property scopes, events, conflict
examples) is **`crdt-spec.md`**.

## Per-field version gating

Each record stores its fields plus `_ver`, a map from field path to the
`versionId` last applied there:

```json
{
  "id": "block_1",
  "type": "text",
  "text": "Hello",
  "_ver": {
    "type": "!A1",
    "text": "!A1"
  }
}
```

**Apply rule:** for each field an edit touches, if `record._ver[path] >=
incomingVersionId` skip it; otherwise apply it and update `_ver[path]`.
Out-of-order delivery is therefore safe.

`_ver` is a tree: an entry is either a string version (covering everything
at and below that path) or an object with per-key entries plus an optional
`*` default for keys not listed.

VersionIds are **peer-local**: each peer's any-sync assigns its own `orderId`
for its view of the tree, and two peers may hold different versionIds for the
same logical change. Comparison happens only within one peer's Controller,
never across peers or trees.

## Mapping to the DAG

- **One DAG change is one write batch** on one dataset of one object.
- **`versionId` is the change's local any-sync `orderId`**, assigned by
  any-sync when the change is added to the tree. The SDK allocates no
  versionIds for synced writes. Device-local (`local`-scope) writes never
  enter the DAG and mint lexids instead (spec §9.1).
- **Local writes apply immediately**, offline included. Remote changes for
  the same fields merge through the gating rule.

## Operations

- **Supported:** `$set`, `$unset`, `$addToSet`, `$pull`, `$inc`, `$incGated`,
  `delete`. There are no list operations (`$push`).
- **`$inc`** is a commutative counter and doesn't touch `_ver`. **`$incGated`**
  is gated like `$set`; it is not convergent under arbitrary delivery order
  and exists for the LWW-on-result semantic.
- **No insert op.** Record creation is opt-in through a per-`RecordChange`
  `upsert` flag. Strict mode (the default) makes a modify on an absent record
  a no-op, matching Mongo's "update if exists". With `upsert: true` the batch
  creates the record; with a multi-field `$set` payload this is the create
  path. An `id` inside the payload is ignored. Without an insert op there is
  no dual-insert convergence problem and no "skip if exists" footgun.
- **Empty record id.** A `RecordChange` with an empty `id` (which requires
  `upsert: true`) gets `base58(xxh3-64(changeId))`, where `changeId` is
  any-sync's content-addressed change id. Further empty-id records in the
  same batch get that id plus `:<n>`. Callers create records without
  generating ids, and content addressing makes the ids globally unique.

Client workflow:

1. `Query(objectId, dataset).Filter(...).Sort(...).Limit(n).Subscribe(ctx,
   opts)` is a windowed live query returning `*QueryResult{Initial, Total,
   Sub}`; live deltas arrive on `Sub.Events()`. `Snapshot(ctx, opts)` returns
   the same shape once.
2. `Modify(ctx, ModifyBatch)` → `ModifyResult{VersionId, ChangeId, RecordIds,
   Rejections}`: one batch of ops, one versionId.
3. `Delete(ctx, DeleteBatch)` writes sticky tombstones.

## Trace IDs

A change may carry `traceIds`: opaque caller-supplied correlation tokens
(session ids, AI agent operation ids). They travel through any-sync in a
dedicated unencrypted field, so any-sync can index them for space-wide
queries ("every change tagged with trace X"). The CRDT layer also stamps
them onto each touched record as `_traces: {versionId → [traces]}`. After
every apply the map is pruned to the versionIds still referenced by `_ver`,
so it stays bounded by the record's live version count. Rules: spec §3.6.

## `_ver` structure rules

- **Collapsing needs authority.** A collapsed entry (a single string, or a
  `*` default) exists only where a write had whole-subtree authority: a
  whole-subtree `$set`/`$unset`, or `delete`.
  ```
  $set: { "a.b": {"some": "object"} }
  → _ver: { "a": { "b": "version" } }
  ```
  Siblings that happen to share a version are never folded into a `*` or
  collapsed: an invented default would claim versions for fields nobody
  wrote and gate out concurrent older writes to them, diverging peers.
  Compaction only makes lossless rewrites: it drops explicit entries equal to
  their level's `*`, and collapses a node whose `*` is present and whose
  entries all equal it.
- **Broad writes merge per leaf.** A `$set`/`$unset` at a path with finer
  `_ver` entries below it is not gated as a whole. Each existing leaf with a
  newer version keeps its value, older parts are replaced or removed, and the
  write stamps `*` at the subtree so unlisted keys take its version. A
  non-object payload lands only when nothing newer survives. Broad-replace
  versus per-leaf races converge in every delivery order.
- **Splitting preserves coverage.** Expanding a collapsed entry to write a
  finer leaf copies the inherited version as `*` onto every created
  intermediate, so unlisted deeper siblings keep their version.
- **Creation marker.** Every record carries `_ver.id`, the smallest versionId
  of any upsert that targeted the id. Each upsert applies `_ver.id =
  min(current, incoming)`, so the marker only moves down. This gives a
  deterministic cross-peer sort order under concurrent upserts; otherwise
  whichever upsert ran first would stamp a different marker on each peer and
  `ORDER BY _ver.id` would diverge. Strict modifies don't touch it, delete
  preserves it, and a delete on an absent record seeds it from the delete's
  version (still lowered by later upserts).
- **Auto-create.** A record created by its first upsert starts with
  `_ver = { "id": creationVersion }` plus entries for the fields the create
  wrote. Unwritten fields have no version: lookup returns `""`, the smallest
  version, with no fallback to `_ver.id`.
- **Delete (soft).** All fields are wiped except `id` and `_deletedAt`.
  `_ver` shrinks to `{ "id": creationVersion, "*": deleteVersion }`.

## Datasets

- **Handler-based.** Each dataset has a handler (schema and rules). A change
  for a dataset with no registered handler stays in any-sync and is parked,
  not applied, until a registration exists (spec §8.2).
- **Permissions.** any-sync's ACL decides who can write to the object at
  all; handlers enforce per-record rules (for example, "edit only your own
  chat message").
- **Versioning and re-indexing.** Handlers are versioned. When handler logic
  changes, the SDK re-walks the object tree and rebuilds the any-store state
  (`versioning.md`).

## AddSeq watermark

- **Three peer-local counters.** `versionId` (the local any-sync orderId)
  gates writes and serves as a sort key. `addSeq` (any-sync's local delivery
  counter) drives catch-up. `applySeq` (SDK-owned, per space) orders the
  consumer change feed and advances on every apply, including device-local
  writes. None of them match another peer's values.
- **Per object in the CRDT layer.** The `Controller` exposes `MaxAddSeq()` /
  `SetMaxAddSeq(seq)`. On restore the space layer reads each Controller's
  watermark, asks any-sync for heads with `LastAddSeq > watermark`, and
  replays the missing changes into the right Controllers.
- **Monotonic.** `ApplyChange` raises the watermark only when the incoming
  AddSeq is greater. A replay with a lower AddSeq still applies (the CRDT is
  idempotent) but doesn't lower the watermark.
- **Crash-safe.** Because apply is idempotent, a crash between apply and
  watermark persist is harmless: on restart the replay is a no-op through
  gating and sticky tombstones.
- **Per-record stamps.** The apply path stamps the change's AddSeq onto every
  record it writes as `_addSeq` (monotonic max, tombstones included; indexed
  on every collection; kept off the `Query.Subscribe` projection) and the
  change's applySeq as `_applySeq`. Both are peer-local storage metadata.
- **Per-object `_meta` row.** Written in the apply transaction: max AddSeq
  (`q`), max applySeq (`as`) and the space id (`sp`), indexed as `(sp, q)` and
  `(sp, as)` so change queries can scope to one space in the shared SDK DB.
- **Per-space boot watermark.** The space-level "highest `LastAddSeq` seen"
  row is written at the end of a boot catch-up pass and also on clean SDK
  close for every open, caught-up space. Without the close snapshot, every
  tree touched during a session sits above the boot value and the next boot
  force-loads all of them. A crash means no snapshot, and boot falls back to
  the idempotent replay. A space qualifies only after a successful boot
  catch-up or when created or derived in this session; joins and accepts
  don't qualify. A space with trees committed to storage but never
  materialized (the treesyncer's parked set) is skipped, since the boot replay
  is their only recovery. The watermark never moves backwards
  (`persistWatermarkIfAhead`).
- **Change index.** `Space.Changes()` (`ChangeIndexAPI`) serves consumer-side
  incremental indexers (full-text, vector search): `ChangedSince(since,
  limit)` for catch-up, ascending by applySeq; `MaxApplySeq()` for the cursor
  ceiling; `Subscribe(cb)` for a best-effort live feed of `ObjectChange{ObjectId,
  ApplySeq, Deleted}`; `Generation()` to detect a rebuilt store. The consumer
  owns the cursor. See `change-index-proposal.md`.

## Conflict rules

- **LWW by versionId** for every op except the commutative `$inc`,
  `$addToSet` and `$pull`.
- **Delete wins.** Tombstones are sticky: every modify on a tombstoned record
  is dropped regardless of version, and there is no resurrect. The drop is
  reported to the local writer as a whole-record rejection (`ErrRecordDeleted`
  in `ModifyResult.Rejections`), so an upsert that reuses a deleted id is
  distinguishable from a successful create. Deleting an already-deleted record
  is silent.
- **No conflict reaches the caller.** Everything auto-merges; the tombstone
  rejection reports a dropped write, not a conflict.

## Storage

- **`_ver` is stored inline** in the record, so queries pay little for it and
  sorting by versionId is cheap.
- **Compaction** is the lossless `_ver` rewrite after each apply; there is no
  snapshotting or pruning of old state.
- **`_ver` size** is bounded by the fields actually written; delete shrinks
  it to two entries.
- **Large datasets** in one object tree are fine, up to millions of records.

## Events

- **Recovery.** `Query.Subscribe` is the only recovery surface. On
  `ErrSubscriptionOverflow` (mailbox full) or `ErrSubscriptionDrifted` (held
  window depleted past its budget) the consumer resubscribes, and the new
  `QueryResult.Initial` reconciles state.
- **Own writes** arrive as events too; callers recognize them by the
  `VersionId` returned from the write. Per-session suppression is not
  supported.
- **Ordering.** Events follow apply order; cross-object order is not
  guaranteed.

Event shape and window semantics: spec §13. Open items: spec §17.
