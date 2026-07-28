# CRDT

## Vision
At the high level every object can store records to different collections (record sets). A record is an anyenc object. Basic mongo-like field-level operations: `$set`, `$inc`, `$addToSet`, etc.

## Proposal: Version-Gated Record Store
See full proposal: `proposal-version-gated-record-store-crdt.md`

### Core Idea — Per-Field Version Gating
Each record stores normal anyenc fields plus `_ver` — a map of per-field/per-path last applied `versionId`. VersionIds are **peer-local** — each peer's any-sync maintains its own `orderId` for its view of the tree, and two peers may hold different versionId strings for the "same" logical change. Comparison is always local (within one peer's Controller), never across peers, never across trees.

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

**Apply rule:** for each field touched by an edit, if `record._ver[path] >= incomingVersionId` → skip, else apply and update `_ver[path]`. This makes out-of-order delivery safe.

`_ver` is a tree: each entry is either a string version (collapsed — applies to everything at and below this point) or an object with explicit per-key entries plus an optional `*` default key for unenumerated siblings.

### Roles
- **Backend** owns sync + DB, assigns lexicographically sortable `versionId` per accepted batch
- **Client** keeps local state, queries with filters/sorts, sends edits, subscribes to event stream

### Change Event Format
```json
{
  "dataset": "blocks",
  "id": "block_1",
  "versionId": "!B7",
  "edits": [
    { "$set": { "backgroundColor": "#FFCC00" } }
  ]
}
```

### Client Workflow
1. `Query(objectId, dataset).Filter(...).Sort(...).Limit(n).Subscribe(ctx, opts)` — windowed live query. Returns `*QueryResult{Initial, Total, Sub}`; live deltas flow through `Sub.Events()`. Use `Snapshot(ctx, opts)` for a one-shot view with the same shape.
2. `Modify(edits…, opts…) → ModifyResult{VersionId, ChangeId, RecordIds}` — batch of `$set`/`$unset`/`$addToSet`/`$pull`/`$inc`/`$incGated` ops; one versionId for the batch. Defaults to strict update-if-exists. Pass an `upsert` option to auto-create the target record — that's the create path.
3. `Delete(ids…) → ModifyResult` — produces sticky tombstones.

### Future Directions (from proposal)
- **Field scopes** — account scope vs shared scope (separate datasets, namespaces, or per-field metadata)
- **Block editor** — dataset `blocks`, tree via `parentId`, ordering via lexicographic `position` field
- **Text CRDT** — integrate real text-merge CRDT per block
- **Schema validation + versioning** on backend

## Current any-sync & any-store Building Blocks

### anyenc — Record Encoding
`any-store/anyenc/` — binary format for JSON-like values. Arena + pool for GC-efficient allocation.

### Modifier Operations (any-store)
| Operation | Description |
|-----------|-------------|
| `$set` | Set field value |
| `$unset` | Remove field |
| `$inc` | Increment numeric field |
| `ModifierChain` | Compose multiple modifiers |

### Key-Value Storage (any-sync)
`any-sync/commonspace/object/keyvalue/` — alternative to object trees for simple KV:
- `ldiff.CompareDiff` for peer sync
- Last-Write-Wins (timestamp-based) conflict resolution

### State Building Pattern
```go
StateBuilder.Build(tree, oldState) → newState
```
Incremental: only processes changes after `LastIteratedId`. Supports snapshots.

### Source files
- `proposal-version-gated-record-store-crdt.md` — full proposal
- `any-store/anyenc/` — binary encoding
- `any-store/query/modifier.go` — $set, $inc, $unset
- `any-sync/commonspace/object/keyvalue/` — KV storage
- `any-sync/commonspace/settings/settingsstate/` — state builder pattern

## Key Decisions

### Version-Gated Model ↔ DAG Mapping
- **One DAG change = one `modifyRecords` batch**
- **`versionId` = any-sync `orderId`** (provided by any-sync, not a separate counter)
- **any-sync assigns the versionId**, not the client
- **Local-first flow**:
  1. Client applies change locally (tentative state)
  2. Client sends change to backend, gets back a `versionId`
  3. Meanwhile client may also receive an event for the same record/field from another source
  4. Client compares versionIds per field and keeps the newer one

### Operations (v1)
- **Supported**: `$set`, `$unset`, `$pull`, `$addToSet`, `delete`
- **`$inc`** — two modes needed: `$inc` (commutative/CRDT counter) and `$incGated` (version-gated like `$set`, non-convergent under arbitrary delivery). Exact naming TBD
- **No `insert` op** — record creation is opt-in via a per-`RecordChange` `upsert` flag. Strict mode (default) makes modifies on absent records no-ops, matching mongo's "update if exists" semantics. Setting `upsert: true` makes the batch auto-create the record; combined with a multi-field `$set` payload, this is the "create" path. `id` in the payload is silently ignored. Removing the explicit insert op eliminates the dual-insert convergence problem and the "skip if exists" footgun.
- **Empty record id → changeId sugar** — a `RecordChange` with an empty `id` gets its id from `Change.changeId` (any-sync's content-addressable, immutable DAG change id). The first empty-id record uses `changeId` verbatim; subsequent empty-id records in the same batch get `changeId:<index>`. (Separator is `:` rather than `/` so resolved ids are URL-path-safe.) Callers can "create a new record" without generating ids themselves — content-addressability gives them global uniqueness for free.
- **Deferred**: `$push` and other list ops

### Trace IDs
Every change carries optional `traceIds` — opaque caller-supplied correlation tokens (e.g. session IDs, AI agent operation IDs). They travel through any-sync in a dedicated non-encrypted field so any-sync can index them for space-wide queries ("give me every change tagged with trace X"). The CRDT layer also stamps them onto each touched record as `_traces: {versionId → [traces]}`, keyed by versionId for compaction. The map is GC'd after every apply down to the versionIds currently referenced by `_ver`, so storage stays bounded by the record's live version count. See spec §3.6 for the full rules.

### `_ver` Structure Rules
- **Collapsing is authority-bound (lossless only)**: a collapsed entry (single string, or a `*` default) may exist only where a write had explicit whole-subtree authority — a whole-subtree `$set`/`$unset`, or `delete`. Example:
  ```
  $set: { "a.b": {"some": "object"} }
  → _ver: { "a": { "b": "version" } }
  ```
  Sibling fields that merely happen to share a version are NEVER factored into a `*` or collapsed: inventing a default would claim versions for never-written fields and gate out concurrent older writes to them, diverging across peers. Compaction performs only lossless rewrites — dropping explicit entries equal to their level's `*`, and collapsing a node whose `*` is present and whose entries all equal it.
- **Broad writes merge per-leaf**: a `$set`/`$unset` at a path with finer `_ver` entries below it is NOT gated wholesale. Each existing leaf with a newer version survives with its value, older parts are replaced/removed, and the write stamps `*` at the subtree so unenumerated keys are claimed at its version. A non-object payload lands only when nothing survives. This keeps broad-replace vs per-leaf races convergent in every delivery order.
- **Splitting preserves coverage**: expanding a collapsed entry to write a finer leaf propagates the inherited version as a `*` onto every created intermediate, so coverage of unenumerated deeper siblings never silently drops to "no version".
- **Creation marker**: every record carries `_ver.id` = the smallest versionId of any upsert that ever targeted this id. Every upsert applies `_ver.id = min(current, incoming)`, so the marker only moves *downward*. This rule is what gives deterministic cross-peer sort order under concurrent upserts — without it, whichever upsert runs first would stamp a different marker on each peer and `ORDER BY _ver.id` would diverge. Strict modifies don't touch it, delete preserves it, and delete-on-absent seeds it from the delete's version (still subject to later min-lowering).
- **Auto-create**: a record auto-created by its first upsert modify starts with `_ver = { "id": creationVersion }` plus per-field entries for whatever fields the create touched. Fields not written keep no version (lookup returns `""`, the smallest version — no fallback through `_ver.id`).
- **Delete** (soft): wipe all fields, keep `id` and `deletedAt: timestamp`. `_ver` shrinks to `{ "id": creationVersion, "*": deleteVersion }` — the creation marker is preserved, and `*` defaults every other field to the delete version.

### Datasets
- **Handler-based** — each dataset has a handler (schema + rules). Changes for datasets without a registered handler are ignored
- **System vs user datasets** — exists, but belongs to Data Structure section
- **Permissions**:
  - ACL enforcement on any-sync side (who can write to this object at all)
  - Logical per-record permissions (e.g., "edit only your own chat message") handled by dataset handlers
- **Versioning & re-indexing** — handlers have versions. When the app adds a new handler or changes handler logic, the SDK must re-iterate the object tree and rebuild the any-store state. See `08-versioning.md`.

### AddSeq watermark
- **Two different peer-local counters.** `versionId` is the local any-sync orderId (used for gating and as a DB sort key); `addSeq` is the local delivery counter (used for catch-up). Both are maintained by the peer's local any-sync; neither is guaranteed to match another peer's values.
- **Per-object in the CRDT layer.** The `Controller` exposes `MaxAddSeq()` / `SetMaxAddSeq(seq)`. On restore, the space layer reads every Controller's watermark, asks any-sync for heads with `LastAddSeq > watermark`, and replays the missing changes into the right Controllers.
- **Monotonic.** ApplyChange bumps the watermark only when incoming AddSeq is strictly greater; a replay with a lower AddSeq still applies (CRDT is idempotent) but does not regress the watermark.
- **Crash-safe by construction.** Because the CRDT is idempotent, a crash between apply and watermark persist is harmless — on restart the replay becomes a no-op via gating and sticky tombstones.
- **Per-record `_addSeq`.** The apply path stamps the change's AddSeq onto every record it writes (root field `_addSeq`, monotonic max — a lower out-of-order replay never regresses it). Tombstones carry it too. An `_addSeq` index is ensured on every collection. This is storage metadata, kept off the `Query.Subscribe` wire projection; it surfaces only through explicit reads and the change-index query.
- **Per-object `_meta` index.** Each per-object `_meta` row already persists the object's max AddSeq atomically in the apply tx; it now also carries the `spaceId` (`sp` field, indexed as `(sp, q)`) so the change-index query can scope "objects in this space with AddSeq > N" against the shared SDK DB.
- **Per-space boot watermark persists at Close too.** The space-level "highest LastAddSeq seen" row (`spacesync`) is written at the end of a boot catch-up pass AND snapshotted on clean SDK Close for every open, caught-up space — session-live applies already materialized everything below `MaxLastAddSeq`, so without the close snapshot every tree touched during a session sits above the boot value and the next boot force-loads all of them for nothing. Crash ⇒ no snapshot ⇒ boot replay fallback (idempotent, same as the crash-safety bullet above). Allowlist-gated (absent = dirty = keep the boot replay): a space qualifies via a successful boot catch-up Run or by being created/derived this session (born clean); joins/accepts never qualify. A non-empty treesyncer parked set (trees committed to storage but never materialized — the boot replay is their only cross-restart recovery) also skips the space, and the write never regresses (`persistWatermarkIfAhead`).
- **Change-index surface.** `Space.Changes()` (`ChangeIndexAPI`) exposes `ChangedSince(since, limit)` (catch-up pull, ascending by AddSeq), `MaxAddSeq()` (cursor ceiling), and `Subscribe(cb)` (best-effort live feed of `(objectId, addSeq)`). Built for consumer-side incremental indexers (full-text / vector search): the consumer owns the cursor; the two paths reconcile because both order on AddSeq. See `12-change-index-proposal.md`.

### Conflict Rules
- **LWW by versionId** (v1) — acceptable for all ops except `$inc` (commutative counter) and the commutative set ops `$addToSet` / `$pull`
- **Delete wins absolutely** — tombstones are sticky. Every modify on a tombstoned record is dropped at the protocol level regardless of version. Explicit resurrect is out of scope for v1; callers that need it must layer their own mechanism on top
- **No conflict ever surfaces to the caller** — auto-merged

### Storage
- **`_ver` stored inline** in the same document — minimal query perf impact, and sorting by versionId is cheap
- **No snapshots, no compaction in v1**
- **`_ver` growth** — unbounded in v1. Future schema will limit fields. On delete, all fields except `id` and `isDeleted` are wiped, shrinking `_ver`
- **Large datasets in one object tree** — fine, can handle millions

### Event Flow
- **Event format** — similar to change format, possibly the same. A full spec (proper naming, structure) is a TODO
- **Own-write events** — future plan: session-based filtering, so the client doesn't receive its own writes in the originating session but does in other sessions. Decide later
- **Ordering** — not a required guarantee, but probably will arrive ordered in practice
- **Reconnection / recovery** — `Query.Subscribe` is the single recovery surface. On `ErrSubscriptionOverflow` (mailbox full) or `ErrSubscriptionDrifted` (held window depleted past the budget), the consumer resubscribes and the new `QueryResult.Initial` reconciles state.
- **Caller view** — SDK may wrap events into a simplified view for the client

## Full Spec
See **`05a-crdt-spec.md`** for the complete specification (operations, change format, event format, application algorithm, property scopes, conflict examples).

## Grooming Questions (open)

Most earlier questions are now resolved in the spec. Remaining:

1. **Local version IDs** — `local`-scope writes mint lexids (`NextVersion` of the path's current version) rather than DAG orderIds (spec §9.1); confirm the generator/prefix is final
2. **Final handler interface** — method names, error types, validation return shape (spec §8)
3. **Session model** — deferred; to be added when we build the session-based own-write filtering

### Dependencies
4. CRDT event format (spec §14) feeds directly into Data Structure event API. Property variant events are defined in spec §11 — resolves the cross-section blocker.
