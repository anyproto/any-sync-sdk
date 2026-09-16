# CRDT Full Spec

Specification of the version-gated record store CRDT. [crdt.md](crdt.md) is the overview.

- **Part I — Protocol:** how changes are stored in any-sync and applied to any-store.
- **Part II — External API:** how callers read, write and subscribe.
- **Part III — Reference:** conflict examples, scope limits, design notes.

---

# Part I — Protocol

## 1. Overview

A **record** is an anyenc document in a **dataset**, a named collection inside an **object** (an any-sync object tree). An object holds many datasets.

A write is a **change**: a batch of record edits stored as one any-sync DAG change. Each peer's any-sync gives every change a lexicographically sortable `versionId` (its local `orderId`). A versionId is meaningful only within one peer's view of one object tree; the CRDT never compares versionIds across peers or trees.

Each record carries a `_ver` map of per-field versionIds. The core rule:

> A field is updated only if the incoming `versionId` is strictly greater than the field's stored version.

Delivery order doesn't matter, and there is no multi-head or conflict state.

---

## 2. Terminology

| Term | Meaning |
|------|---------|
| **object** | any-sync object tree; the unit of encryption, ACL and sync |
| **dataset** | named record collection inside an object |
| **record** | one anyenc document in a dataset, identified by `id` |
| **change** | atomic batch of edits; one any-sync DAG change |
| **versionId** | local ordering key from any-sync (`orderId`), scoped to one peer's view of one object tree |
| **_ver** | per-record tree of the last-applied `versionId` per field path |
| **handler** | per-dataset hooks that validate changes and derive fields (§8) |
| **operation** | one edit inside a change (`$set`, `$inc`, …) |

---

## 3. Data Model

### 3.1 Record
```json
{
  "id": "block_42",
  "name": "Hello",
  "count": 7,
  "tags": ["a", "b"],
  "_ver": {
    "id":    "!A0",
    "name":  "!B3",
    "count": "!B3",
    "tags":  "!B3"
  }
}
```

- `id` is required and immutable.
- `_ver.id` is the record's creation version (§3.5).
- `_ver` ships with query results so clients can reconcile per-field state.
- Reserved names are `id` and every top-level name starting with `_`: `_ver`, `_deletedAt`, `_traces`, `_addSeq`, `_applySeq`. Input ops can't write them (§5.0).
- All other fields belong to the dataset (schema and handler, §8).

`_ver` is a tree. Each entry is either:
- a **string** versionId: the path and everything below it share that version; or
- an **object** of per-key entries plus an optional `*` default, the version of every key not listed. `*` can't be a field name (§5.0) and avoids anyenc's special empty-key encoding.

Lookup walks `_ver` along the path. If the walk leaves the enumerated keys, the `*` at the node where it left applies; with no `*`, the version is `""`, the "no version" sentinel that sorts below every real versionId. The node-local `*` is always enough because splitting a covered entry copies the inherited version onto every intermediate node it creates (§3.2).

### 3.2 `_ver` Collapsing Rules

A collapsed entry (a single string, or a `*` inside an object) may exist only where a write had authority over the whole subtree: a whole-subtree `$set`/`$unset`, or `delete`. A write at a path claims that path and everything below it, nothing else:

```
$set { "a.b.c": 1 }  at v5        →  _ver: { "a": { "b": { "c": "v5" } } }
$set { "a.b":   {c: 1, d: 2} } v5 →  _ver: { "a": { "b": "v5" } }
$set { "a":     {b: {c:1}} }   v5 →  _ver: { "a": "v5" }
```

Siblings that happen to share a version are never factored into a `*` or collapsed into their parent. An invented default would claim a version for fields nobody wrote, gate out concurrent lower-version writes to them on whichever peer compacted first, and diverge. Compaction only performs lossless rewrites, where every lookup returns the same version before and after:

1. Inside an object with a `*`, explicit string entries equal to the `*` are dropped.
2. An object whose `*` is present and whose entries all equal it collapses to that string.

**Broad writes over finer entries merge per leaf.** A `$set`/`$unset` at a path whose `_ver` entry is an object (finer writes exist below) is decomposed: each existing leaf with version ≥ the incoming version survives with its value; older parts are replaced, or removed where the incoming value doesn't cover them; the write stamps `*` on the subtree, claiming every unenumerated key. When nothing survives, the write has whole-subtree authority and the entry collapses to its version. A non-object payload lands only when nothing survives, since surviving newer leaves keep the position an object. This makes broad-replace vs per-leaf races converge in every delivery order. Gating a broad write against an aggregate such as the max leaf version depends on delivery order and is forbidden.

When a finer write splits a collapsed or `*`-covered entry, the inherited version is copied as a `*` onto every intermediate object the split creates, so the broad write still covers unenumerated deeper siblings.

### 3.3 Auto-creation via Upsert

There is no `insert` op. `RecordChange.upsert` controls creation:

- `false` (default, strict): when no record has the id, the record's modify ops don't apply, and the apply result carries an `ErrStrictSkipAbsent` rejection for it. Typos and stale ids can't create records.
- `true`: an absent record is created empty, stamped `_ver.id = versionId` (§3.5), and the ops run.

A create is usually `upsert: true` with a multi-field `$set` (§5.1). Concurrent creates of one id merge per field through gating.

`delete` ignores the flag: deleting an absent record writes a tombstone, so a delete beats a create it hasn't seen.

#### Empty record id

A `RecordChange` with an empty `id` gets one derived from the change's `changeId`:

```
DeriveRecordId(changeId) = base58(xxh3-64(changeId))   // up to 11 chars
```

`changeId` is any-sync's content-addressed DAG change id: globally unique and uniformly distributed. xxh3-64 keeps it uniform (P(collision) < 1e-6 up to ~6M derived ids in one storage namespace). The full changeId is 50–60 chars, too bulky for records keyed by many ids such as property rows. Implementation: `crdt.DeriveRecordId` in `internal/crdt/idderive.go`.

Resolution, per change, before validation:

1. An empty `id` requires `upsert: true`, otherwise `ErrEmptyIdRequiresUpsert`. A strict modify on a freshly derived id could never match a record.
2. The first empty-id record gets `DeriveRecordId(changeId)`.
3. Later empty-id records get `DeriveRecordId(changeId) + ":" + index`, where `index` counts empty-id records only (1, 2, …). `:` keeps ids safe in URL path segments.
4. An empty `id` with an empty `changeId` fails with `ErrMissingRecordId`.
5. Handlers see the resolved id.

On a shared dataset (the per-space `objects` row) every record resolves to the change's `objectId`.

### 3.4 Soft Delete

Delete keeps the record as a tombstone:
```json
{ "id": "block_42", "_deletedAt": "2024-05-06T12:53:20Z", "_ver": { "id": "!A0", "*": "!C9" } }
```
`_deletedAt` is a datetime taken from the change timestamp. Content fields are wiped. `_ver` shrinks to `id` (the preserved creation version, §3.5) and `*` (the delete version). `_traces` carry over as in §3.6. Sorting by `_ver.id` still places the tombstone at its creation point.

**Tombstones are sticky.** Every later modify on a tombstone is dropped regardless of version; the CRDT has no resurrection. An undelete feature belongs above the CRDT.

The local writer sees the drop: a non-delete modify (upsert included) on a tombstone adds a whole-record rejection (`OpIndex -1`, `ErrRecordDeleted`, re-exported as `space.ErrRecordDeleted`) to the apply result. The creation-marker min rule still applies. Deleting a tombstone is not a rejection; repeated deletes are idempotent.

### 3.5 `_ver.id` creation-version marker

`_ver.id` holds the version of the record's earliest upsert. Callers use it as an indexed creation-order key; chat datasets, for example, sort messages by `_ver.id` descending.

**Min rule.** Every upsert sets `_ver.id = min(current, incoming)`; the marker only moves down. Strict modifies leave it alone. Deleting a live record keeps its `_ver.id`; deleting an absent record seeds it with the delete version, which an older upsert can still lower. `min` is commutative and associative, so peers that receive concurrent upserts in different orders converge on the causally earliest one.

**Not a default.** Looking up an unenumerated field never falls back to `_ver.id`; it returns `""` unless a `*` applies. Per-field gating stays intact.

**Across delete.** A tombstone keeps `_ver.id`, so a deleted record sorts where it was created. An upsert that hits the tombstone is still dropped, but lowers the marker when older, so peers that saw the delete first agree with peers that saw the upsert first.

### 3.6 `_traces` — trace-id correlation map

`Change.TraceIds` are opaque correlation tokens chosen by the caller: a UI session id, an agent's operation id, a UUID. They travel inside the change payload (§6.4), the local version-history index makes a space's changes listable by trace, and the apply path stamps them on the record:

```json
{
  "id": "block_42",
  "name": "Hello",
  "_ver": { "id": "!A0", "name": "!B3" },
  "_traces": {
    "!A0": ["session-abc"],
    "!B3": ["ai-agent/op-42", "session-xyz"]
  }
}
```

The map is keyed by versionId, so a change touching N fields adds one entry. Traces of field X: `_traces[_ver[X]]`. Traces of the record: the union of all values.

Stamping runs in the same apply step, after the ops:

1. If the change's versionId appears anywhere in `_ver` (a leaf or a `*`) and `TraceIds` is non-empty: `_traces[versionId] = TraceIds`. The same versionId always carries the same traces, so rewriting is idempotent.
2. If it appears in `_ver` and `TraceIds` is empty: delete `_traces[versionId]`.
3. GC: drop every `_traces[v]` whose `v` no longer appears in `_ver`; remove `_traces` when it becomes empty.

The map stays bounded by the distinct versionIds live in `_ver`. When a later `$set` supersedes a field's version, that version's traces fall out unless another entry, typically `_ver.id`, still references it.

`$addToSet`, `$pull` and `$inc` don't update `_ver`, so their traces are not kept on the record.

Delete copies `_traces` onto the tombstone; GC then keeps the entries for the creation marker and the delete itself, so history queries still find deleted records.

Traces are a function of `_ver` and change content, so they converge like `_ver`, with the peer-local versionId caveat of §4.

---

## 4. Version IDs

- **Source:** any-sync's local tree storage assigns each change a `versionId` (its `orderId`). The SDK never mints versionIds for DAG changes; local-scope writes mint their own (§9.1).
- **Locality:** peer-local. Two peers may hold different strings for one DAG change. Never compare versionIds across peers, expose them as identities, or assume a change keeps its versionId over the network.
- **Shape:** opaque string, lexicographically ordered within one peer's view of one tree.
- **One change, one versionId:** every op in a change shares it.
- **Uses:** CRDT gating against the same peer's `_ver`, and ordering keys in the local store such as `_ver.id` (§3.5). The consumer change feed is keyed on applySeq, not versionId (§4a).
- **Convergence:** each peer gates its delivered changes against its own versionIds. Peers holding the same DAG reach the same logical record content, not the same `_ver` strings.

## 4a. AddSeq (delivery watermark)

`addSeq` is a monotonic `uint64` that any-sync's `spacestorage` assigns as changes arrive locally. It is space-wide in any-sync; the SDK tracks it per object because restore works per object. A versionId says where a change sits in the DAG; its addSeq says when this peer received it.

The CRDT Controller:

- `MaxAddSeq()` returns the highest AddSeq the object has applied.
- `SetMaxAddSeq(seq)` seeds the watermark on restore, once, before any `ApplyChange`.
- `ApplyChange` raises the watermark monotonically. Replaying a lower AddSeq applies idempotently and doesn't lower it.
- Zero means nothing observed yet. A change that fails as a whole (unknown dataset, empty `DataVersion`, empty-id rule) leaves the watermark alone. Op rejections don't fail a change: it commits and the watermark advances.
- The Controller persists the watermark inside the change's WriteTx, atomically with the records it covers, and stamps `_addSeq` on each written record.

### Restore / reindex contract

1. Read `MaxAddSeq()` for every Controller.
2. Ask any-sync for heads whose `LastAddSeq` is greater.
3. Fetch those objects' new DAG changes and replay them through `ApplyChange`.

Replaying an already-applied change is a no-op (gating and sticky tombstones), so a restore that re-reads changes at or below the watermark is harmless.

### ApplySeq (consumer-feed watermark)

`applySeq` is SDK-owned and per space. It is allocated inside the apply WriteTx for every apply that writes records: DAG changes, the account mirror's applies, and device-local writes. It is stamped on written records as `_applySeq` and persisted per object as `maxApplySeq` in the same WriteTx. Re-seeding reads the highest persisted value, so the allocator needs no counter row.

AddSeq answers "is any-store caught up with any-sync" and exists only for DAG changes. ApplySeq answers "is a consumer caught up with any-store": the change feed `Space.Changes()` is keyed on it, so non-DAG writes reach indexers.

- Allocation happens after the WriteTx is acquired. any-store has a single writer, so allocation order is commit order and an ascending cursor can't skip a late commit.
- Gaps from rolled-back transactions are normal.
- Replaying an applied change may re-stamp it; a consumer then re-reads the record once and never misses one.
- Per-object rows that predate applySeq are backfilled `applySeq := addSeq` once per space, and the allocator seeds past the historical maximum, so cursors held in AddSeq units stay valid.

---

## 5. Operations

### 5.0 Path rules (apply to every op with a field path)

Every op with a field path is checked before it applies. A violation is `ErrInvalidPath`, wrapped in `ErrValidation`. The local writer rejects the whole change (`ValidateChange`); on the inbound/replay path only the offending op, or the offending key of a multi-field op, is dropped (§7.2).

1. **Non-empty path.** `$set` and `$unset` omit it only in the multi-field form.
2. **No empty segments.** `["a", "", "b"]` is rejected, and so is the multi-field key `"a."`.
3. **No `.` inside a segment.** Single-path ops pass pre-split segments: `["meta", "color"]`, not `["meta.color"]`, which would clash with multi-field parsing.
4. **No `*` segment.** It is the `_ver` default key (§3.1); a field named `*` would corrupt gating for all its siblings.
5. **Top-level reservation.** The first segment may not be `id` or start with `_`.

Datasets add their own rules: field scope (§9) at the controller, then schema and handler checks (§8).

### 5.1 `$set`

**Single-path form:** `op.path` holds the segments, `op.payload` the value.
```
$set ["name"] → "Hello"
```

**Multi-field form:** `op.path` is empty and `op.payload` is an object keyed by dotted path.
```json
{ "$set": { "name": "Hello", "count": 7, "meta.color": "red" } }
```

Each entry of a multi-field `$set` is an independent gated single-path `$set` sharing the change's versionId. Validation is per entry as well (§7.2): an offending key is dropped and reported while its siblings apply.

For each path: if `_ver[path] < versionId`, write the value and set `_ver[path] = versionId`; otherwise skip. When finer entries exist below the path, the per-leaf merge of §3.2 applies instead.

### 5.2 `$unset`
```json
{ "$unset": { "description": "" } }
```
Gated like `$set`. When newer, removes the field and sets `_ver[path] = versionId`. The multi-field form takes an object whose keys are unset; values are ignored.

### 5.3 `$addToSet` (commutative)
```json
{ "$addToSet": { "tags": "urgent" } }
```
- Skipped when `_ver[field] ≥ versionId`: a newer `$set`/`$unset` replaced the whole field.
- Skipped when the field holds a non-array value.
- Otherwise adds the element if absent. `_ver[field]` is not updated, so concurrent adds all land.

### 5.4 `$pull` (commutative)
```json
{ "$pull": { "tags": "urgent" } }
```
Same checks as `$addToSet`; removes matching elements; doesn't update `_ver[field]`.

Concurrent `$addToSet` and `$pull` of the same element have no per-element ordering, so the result depends on delivery order.

### 5.5 `$inc` (commutative counter)
```json
{ "$inc": { "count": 1 } }
```
Adds the delta. Skipped when `_ver[field] ≥ versionId` (a newer `$set` owns the field), when the payload isn't a number, or when the field holds a non-number. Doesn't update `_ver[field]`, so concurrent increments all land.

### 5.6 `$incGated` (LWW-style increment)
```json
{ "$incGated": { "priority": 1 } }
```
`$set(field, current + delta)` with full gating; updates `_ver[field]`. Not convergent under arbitrary delivery (§15.6).

### 5.7 `delete`

No payload; the id comes from `RecordChange.id`.

- Absent record → a tombstone with `_deletedAt` from the change timestamp and `_ver = { "id": versionId, "*": versionId }`.
- Live record → replaced by a tombstone that keeps the existing `_ver.id`.
- Tombstone → no-op.

Tombstones are sticky (§3.4).

### 5.8 Operation Summary

| Op | Gated by `_ver[field]`? | Updates `_ver[field]`? | Commutative? |
|----|----------------------|----------------------|--------------|
| `$set` | yes | yes | no (LWW) |
| `$unset` | yes | yes | no (LWW) |
| `$addToSet` | yes | no | yes (set merge) |
| `$pull` | yes | no | yes (set merge) |
| `$inc` | yes (against `$set`) | no | yes (counter) |
| `$incGated` | yes | yes | no (LWW; not convergent, §15.6) |
| `delete` | sticky tombstone | `_ver` becomes `{ "id": creation, "*": v }` | n/a, delete wins |

Record creation is covered in §3.3.

---

## 6. Change Format

### 6.1 Change
One change = one any-sync DAG change = one `versionId`.

```
Change {
  spaceId:      string            // any-sync space id
  objectId:     string            // any-sync object tree id
  dataset:      string            // exactly one dataset per change
  dataVersion:  string            // schema/handler version peers gate on; required
  changeId:     string            // content-addressed DAG change id
  versionId:    string            // peer-local orderId (§4)
  addSeq:       uint64            // local delivery sequence (§4a)
  applySeq:     uint64            // per-space apply sequence, allocated at apply (§4a)
  timestamp:    int64             // change time; source of _deletedAt
  creator:      string            // identity that signed this change
  objectAuthor: string            // identity that signed the tree's root change
  traceIds:     [string, ...]     // correlation tokens (§3.6)
  records: [
    RecordChange {
      id:     string              // empty → derived from changeId (§3.3)
      upsert: bool                // create when absent (§3.3)
      ops:    [Operation, ...]    // applied in order
    },
    ...
  ]
}
```

An empty `dataVersion` fails with `ErrMissingDataVersion`; its meaning is defined in [types-properties-proposal.md](types-properties-proposal.md) § Change-level DataVersion.

`versionId` orders and gates. `changeId` is content-addressed: stable across peers and replays, with no coordination. It seeds empty-id derivation (§3.3) and must be set when any record has an empty id.

Two route flags mark changes that don't come from this object's DAG: `local` (a device-local write with a locally minted versionId) and `injected` (the account mirror, with the tech-space tree's versionId). See §9.1.

### 6.2 Operation
```
Operation {
  type:    "$set" | "$unset" | "$addToSet" | "$pull" | "$inc" | "$incGated" | "delete"
  path:    [string, ...]        // field path segments; empty for multi-field $set/$unset and for delete
  payload: anyenc.Value         // per §5
}
```

### 6.3 Multiple records, multiple datasets
A change touches any number of records in exactly one dataset. There are no atomic multi-dataset changes; `Space.ModifyMany` validates several batches together and writes one change per batch (§12.2).

### 6.4 Encoding
The payload is the `Data` of an any-sync tree change; any-sync signs and encrypts the outer change. The payload is an anyenc object with short keys:

```
{ d: dataset, v: dataVersion, t?: [traceId, ...], r: [record, ...] }
record: { i?: id, u?: upsert, o: [op, ...] }
op:     { t: type, p?: [segment, ...], v?: payload }
```

Payloads above a size threshold are S2-compressed; decoding accepts both forms. `versionId`, `changeId`, `addSeq`, the object and space ids, and the timestamp come from the any-sync envelope. Codec: `internal/object/wire.go`.

---

## 7. Application Algorithm

```
applyChange(change):
    // Whole-change failures: nothing applies, watermarks unchanged.
    if change.dataVersion == "": fail ErrMissingDataVersion
    handler = handlers[change.dataset]                 // else ErrUnknownDataset
    ids = resolveRecordIds(change)                     // §3.3

    // Content gate (§7.2): path rules (§5.0) + field scope (§9).
    // Offending ops, or keys of multi-field ops, are dropped and reported.
    for rc in change.records:
        rc.ops = contentFilter(rc.ops)

    in one WriteTx:
        change.applySeq = nextApplySeq()               // §4a
        for i, rc in change.records:
            applyRecord(ids[i], rc, change)
        persist maxAddSeq, maxApplySeq
    emit subscription events                           // after commit, §13

applyRecord(id, rc, change):
    rec = records[id]
    if rc has a delete op:
        if rec is tombstone: return                    // idempotent
        handler.BeforeDelete                           // error → reject record
        records[id] = tombstone(
            _deletedAt = change.timestamp,
            _ver       = { "id": rec?._ver.id or change.v, "*": change.v },
        )
        return

    if rec is absent:
        if not rc.upsert:
            reject record (ErrStrictSkipAbsent); return
        rec = { id, _ver: { "id": change.v } }         // §3.5
        records[id] = rec
        handler.BeforeCreate                           // error → reject record
        for op in rc.ops: applyOp(rec, op, change.v)
    else if rec is tombstone:
        reject record (ErrRecordDeleted)               // sticky, §3.4
        if rc.upsert: lowerCreationMarker(rec, change.v)
        return
    else:
        if rc.upsert: lowerCreationMarker(rec, change.v)
        for op in rc.ops:
            handler.BeforeModify(op)                   // error → reject op (per key for multi-field)
            applyOp(rec, op, change.v)
    apply handler-derived ops (Sink, §8)
    stamp _addSeq, _applySeq, _traces (§3.6); compact _ver (§7.1)

lowerCreationMarker(rec, v):
    if rec._ver.id is missing or v < rec._ver.id:
        rec._ver.id = v

applyOp(rec, op, v):
    switch op.type:
        case $set:
            if op.path is empty:                       // multi-field form
                for (key, value) in op.payload:
                    gatedSet(rec, v, splitDots(key), value)
            else:
                gatedSet(rec, v, op.path, op.payload)
        case $unset:
            if op.path is empty:
                for (key, _) in op.payload:
                    gatedUnset(rec, v, splitDots(key))
            else:
                gatedUnset(rec, v, op.path)
        case $addToSet, $pull:
            if getOrder(rec, op.path) >= v or field is not an array: skip
            add or remove the element                  // _ver NOT updated
        case $inc:
            if getOrder(rec, op.path) >= v or field is not a number: skip
            rec[op.path] = (rec[op.path] or 0) + op.payload   // _ver NOT updated
        case $incGated:
            if getOrder(rec, op.path) >= v or field is not a number: skip
            rec[op.path] = (rec[op.path] or 0) + op.payload
            setOrder(rec, v, op.path)

gatedSet(rec, v, path, value):
    (gate, subtree) = gateVersion(rec, path)
    if subtree is nil:                                 // one authoritative version
        if gate < v:
            writeValue(rec, path, value)
            setOrder(rec, v, path)
        return
    mergeReplace(rec, path, subtree, value, v)         // finer entries below, §3.2

gatedUnset(rec, v, path):
    (gate, subtree) = gateVersion(rec, path)
    if subtree is nil:
        if gate < v:
            removeValue(rec, path)
            setOrder(rec, v, path)
        return
    mergeReplace(rec, path, subtree, nil, v)

mergeReplace(rec, path, verSubtree, value, v):         // value nil = unset
    // Existing parts with version >= v survive with their values; older
    // parts are replaced by value, or removed where it doesn't cover them;
    // the merged _ver node gets `*: v` for every unenumerated key.
    // No survivors → write value outright and collapse _ver at path to v.
    // A non-object value lands only when nothing survives.
```

`gateVersion(rec, path)` walks `_ver` along the path and returns either one authoritative version (an explicit string, a collapsed ancestor, the local `*`, or `""` when untracked) or, when the entry at the path is an object, that subtree, meaning finer writes exist below and the per-leaf merge must run.

`getOrder(rec, path)` gates the commutative ops and `$incGated`. It walks like `gateVersion`, but when the entry at the path is an object it returns the maximum version below it. The aggregate is safe here: a position with finer entries below holds an object, and these ops skip non-array or non-number values anyway.

Versions compare as strings; `""` sorts before every real version (`space.CompareVersion`).

Local and injected changes (§9.1) skip the handler hooks; their writers validate them.

### 7.1 Collapse
After each apply, `_ver` is rewritten to canonical form using only the lossless rules of §3.2: drop explicit entries equal to their level's `*`, and collapse a node whose `*` is present and whose entries all equal it. Never invent a `*` or merge siblings that merely share a version. Correctness doesn't depend on compaction.

### 7.2 Write transaction
All records of one change apply in one any-store `WriteTx`, or a savepoint when the caller already holds one, as batch restore does. Events fire after the commit.

On the inbound/replay path validation never fails the change. It drops the smallest offending unit and records a rejection:

- **Content gate** (controller, before handlers): path rules (§5.0) and field scope (§9).
- **Handler gate** (§8): a `BeforeCreate` or `BeforeDelete` error rejects the record; a `BeforeModify` error rejects the op.

Inside a multi-field `$set`/`$unset` both gates shed individual keys, and the op is dropped only when every key fails. The rest of the change commits and the watermark advances. One bad historical change, or a rule tightened after a change was written (a field pinned, a status made terminal, a field moved to another scope), can't wedge restore or lose the change's other edits.

The local writer is strict: `ValidateChange` rejects a fresh change with any structurally invalid op before it enters the DAG.

---

## 8. Handlers

A handler is per-dataset behavior: lifecycle hooks the apply path calls inside the write transaction (§7.2). The dataset's name, wire `DataVersion`, indexes, field schema and read-tracking opt-in are declared next to the handler at registration (§8.1), not implemented by it.

```go
type Handler interface {
    Init(ctx context.Context) error    // once at registration; nil for stateless handlers

    BeforeCreate(ctx *ChangeCtx, rec *RecordChange, sink *Sink) error
    BeforeModify(ctx *ChangeCtx, rec *RecordChange, op *Op, sink *Sink) error
    BeforeDelete(ctx *ChangeCtx, rec *RecordChange, sink *Sink) error
}
```

Any hook may be a no-op; `DefaultHandler` is the embeddable no-op base. Errors follow §7.2: a `BeforeModify` error drops the op (per key for the multi-field form); a `BeforeCreate` or `BeforeDelete` error drops the whole `RecordChange`. Rejections wrap `ErrValidation` and appear in the apply result; the change still commits to the tree.

**ChangeCtx** carries:

- `Change`: the envelope (§6.1), including `Creator` (the signer of this change) and `ObjectAuthor` (the signer of the root change).
- `Before`: the record's state before the op, updated across ops of the same `RecordChange`; nil in `BeforeCreate`.
- `Get(dataset, id)`: reads another record of the same object inside the apply transaction. Hooks run on every replica and must not diverge, so a handler may read only fields that are immutable after creation (derived creation stamps) on records that are causal ancestors of the triggering change.
- `SelfIdentity`, `RecordId`: set only on the read-tracking classify path ([read-tracking-proposal.md](read-tracking-proposal.md)), so handler validation stays replica-independent.

**Sink** is how hooks write:

- `Derive(op)` queues an op on the same record, folded into the same store write and inheriting the change's versionId. Creation stamps such as `creator` and `createdAt` land this way and converge under the normal gate.
- `DeriveOnce(op)` does the same unless an op on that path is already queued; per-change stamps emitted from `BeforeModify` use it.
- `Project(dataset, rec)` queues a write to a record in another dataset of the same object, in the same transaction.

Fields a schema declares `derived` are writable only through `Sink`: the content gate rejects input ops on them.

**ObjectStamper** is the reverse direction, for the handler of a shared dataset (the per-space `objects` row). `StampObject(ctx, sink)` runs once per applied synced change on any other dataset of the object, after its records landed and only if the change wrote something. The derived ops apply to the object's row with the change's versionId as a strict update; an absent or tombstoned row is left alone. This is how `modifiedAt` / `modifiedBy` track writes to editor blocks, chat messages and runtime datasets. Local and account-route changes never trigger it, because their versions belong to other domains. It is SDK-internal; consumer datasets can't declare one.

### 8.1 Registration

Consumers register datasets at `sdk.Open` through `config.Config.Types`: each `handler.Type` owns zero or more `handler.Dataset`s.

```go
handler.Dataset{
    Name:           "chat_messages",     // unique across the whole catalog
    DataVersion:    "chat_messages-v2",  // stamped on every change; peers gate on it
    HandlerVersion: 2,                   // local; a bump rebuilds materialized rows
    Handler:        messagesHandler{},   // nil with a declared Schema → generic schema handler
    Schema:         …,                   // field classes (§9); zero value = Dynamic
    Indexes:        …,                   // ensured on the dataset's collection
    ReadTracking:   …,                   // optional unread tracking
}
```

Internally, and for the tech space's system datasets, the same bundle is a `crdt.HandlerReg`. A `HandlerVersion` bump makes the next load of each object wipe its materialized rows and replay its tree ([versioning.md](versioning.md)). A declared `Schema` with a nil `Handler` gets the generic schema handler, so the declaration alone is the behavior ([user-datasets.md](user-datasets.md)).

The handler set isn't fixed at compile time. It can differ across app versions, and datasets defined at runtime on type objects register late: the store's catalog carries them, and a controller picks them up by being rebuilt (evicted and reloaded). A live Controller's handler maps never change.

`config.Config.Modules` takes `handler.Module` entries, factories the store instantiates per collection. A module's canonical collection registers on every controller when the store opens, with the module's `DataVersion`. Each namespaced instance a type declares (`<typeId>_<key>`) is built by `Module.New(instance)` into its own `HandlerReg` when the catalog compiles the declaration, with the module's `HandlerVersion` composed in. One module, many registrations, identical behavior.

### 8.2 Unknown Datasets

A change for a dataset with no registered handler is already accepted into the any-sync tree but is not applied to any-store. The space layer's apply gate parks it in the per-space detached-changes collection (`<spaceId>__detached`), the same machinery the DataVersion gate uses, and the drain replays it once a registration exists. A runtime definition applying refreshes the catalog and wakes the drainer, which evicts a stale resident controller so the rebuilt registration applies the row. Until then the dataset is invisible to queries, nothing is lost, and the object's replay doesn't stall. See [user-datasets.md](user-datasets.md) § Runtime registration. A raw Controller with no gate returns `ErrUnknownDataset`.

### 8.3 Validation

Handler hooks are the second of the two apply-time gates in §7.2; content and scope validation runs first, at the controller. The common single-field rules (required fields, write-once or author-gated mutability, apply-time stamps, id rules, delete gates) need no custom handler: declare them on the dataset `Schema` and the generic schema handler enforces them ([user-datasets.md](user-datasets.md)). Custom handlers cover what a declaration can't express:

- Cross-field and shape rules beyond the schema: size limits, allowed op paths, "field A requires field B".
- Per-record permissions beyond the declared gates. The pattern: compare `ctx.Change.Creator` with a derived creation stamp on `ctx.Before`.

Inbound rejections drop the op or record and are recorded, never fatal (§7.2). Handlers that need stricter checks with caller-readable errors on local writes implement `LocalPreValidator` / `LocalPreValidatorMulti`, which run only on the local-write path before the change is created.

---

## 9. Property Scopes (protocol)

Every property and every declared dataset field has exactly one scope, fixed by its declaration. There are no per-value variants and no priority merge. Full design: [scoped-properties-proposal.md](scoped-properties-proposal.md).

### 9.1 One path, one write route
- `synced` — the regular CRDT pipeline on the object. `_ver` holds the object tree's versionIds.
- `account` — a carrier record in the account's private tech space. A per-device mirror writes converged values into the target record at the same paths (`injected` changes), stamping the tech tree's versionIds. Covers property values on `objects` rows.
- `local` — `Object.LocalSet` (`local` changes), no DAG, never synced. Versions are locally minted lexids: `NextVersion` of the path's current version.
- `derived` — written only by handlers (§8).

### 9.2 One `_ver` tree, disjoint domains
A record keeps a single `_ver` tree (§3). No two routes write the same path: a property's scope is pinned for its life (first write wins on the definition, like `kind`), and the apply path drops an op whose route doesn't match its field's scope, identically on every peer. The DataVersion gate parks changes whose schema hasn't synced, so the scope lookup never races the definition.

VersionIds from different scopes aren't comparable, and nothing compares them.

### 9.3 No computed root
The value at a path is the value its one scope last wrote. Events carry one versionId per change, in the domain of the route that produced it, so the client rule `_ver.<op.path> = event.versionId` works for every scope.

---

# Part II — External API

## 10. External API Overview

Callers query, write and subscribe, and `versionId` ties the three together. The caller is middleware ([common-context.md](common-context.md)):

- Methods take and return Go types; middleware serializes for its own protocol.
- There is no session awareness; middleware owns sessions.
- Middleware consumes events and pushes them over its own transport.

### 10.1 Consistency Model
`versionId` appears in:
- query results, through each record's `_ver` (§3);
- subscription events, one per event (§13);
- write results (`ModifyResult.VersionId`).

Callers use it to:
- recognize their own writes in the event stream;
- order events;
- reconcile optimistic in-memory state per field, walking `_ver` with the rules of §3.1–3.2;
- apply the same gating rule to derived state kept outside any-store.

`space.CompareVersion` compares two versions from this peer. Handler internals and re-indexing are not visible to callers.

---

## 11. Queries

```go
space.Query(objectId, dataset)   // one object's dataset
space.QueryObjects()             // the per-space objects collection

q.Filter(f).Sort(keys...).Limit(n).Offset(n).Projection(opts)
```

Terminals:

- `Iter(ctx)` — streaming iterator.
- `All(ctx)` — every match.
- `One(ctx)` — first match or `ErrNotFound`.
- `Count(ctx)` — match count.
- `Snapshot(ctx, opts)` — `*QueryResult` with an optional total (§13).
- `Subscribe(ctx, opts)` — the snapshot plus a live `Sub` (§13).

`Filter` accepts whatever `query.ParseCondition` accepts: a built `query.Filter`, a JSON string, or a map with Mongo-style operators. `Sort` accepts `query.ParseSort` input: `"name"`, `"-_ver.id"` for descending, or a built `query.Sort`. Parse errors surface on the first terminal call. `Aggregate` and `AggregateObjects` run aggregation pipelines over the same collections, snapshot only.

### 11.1 Projections

Records come back verbatim, with `_ver` and `_traces`, in the shape the SDK stores. Values of every scope sit at their normal paths.

Tombstones are excluded by default. `Projection(ProjectionOpts{IncludeDeleted: true})` returns tombstone rows from `Iter`, `All`, `One` and `Count`; `Snapshot` and `Subscribe` always skip them. A deleted object is purged rather than tombstoned, so it never appears in results; observe object deletion through a `deleted` removal (§13.3).

The `_ver` shape (§3.1–3.2) is the client contract; there is no separate accessor.

### 11.2 Record-level versionId
No record-level version is returned. The greatest version in `_ver` moves whenever a gated op lands, but it misses `$addToSet`, `$pull` and `$inc`, which don't touch `_ver`.

---

## 12. Writes

```go
Modify(ctx, ModifyBatch) (ModifyResult, error)
ModifyMany(ctx, []ModifyBatch) ([]ModifyResult, error)
Delete(ctx, DeleteBatch) (ModifyResult, error)

type ModifyBatch struct {
    ObjectId string
    Dataset  string
    Records  []RecordModify  // {Id, Upsert, Ops}
    TraceIds []string
    Scope    Scope           // ScopeSynced (default) or ScopeLocal
}

type ModifyResult struct {
    VersionId  VersionId     // this peer's version of the change (§4)
    ChangeId   string        // DAG change id, stable across peers
    RecordIds  []string      // resolved ids, input order
    Rejections []OpRejection // ops and records that didn't land (§7.2)
}
```

- Ops are the §5 operations, with a dotted `Path` string and a Go `Value`.
- Records are strict by default; `RecordModify.Upsert` creates absent records. A typical create is one multi-field `$set` with `Upsert: true`.
- `Modify` returns once any-sync has committed the change locally, online or not.
- `RecordIds[i]` is the resolved id. Empty ids become `base58(xxh3-64(ChangeId))`, with `:<index>` from the second empty id on (§3.3). Type and property creates read their id from `RecordIds[0]`.
- `Rejections` lists what the change carried but didn't apply; the change still committed.
- `Delete` writes sticky tombstones for `RecordIds`, including ids with no local record.
- `ScopeLocal` writes device-local fields on existing records without a DAG change (§9.1).

### 12.1 Property-scope Writes

One setter picks the write route from each property's declared scope (§9):

```go
Properties().Set(ctx, objectId, ownerId, patch) (ModifyResult, error)  // ownerId: the object's type or one of its collections
```

All keys in a patch must share one scope, because routes commit independently and can't roll back together; mixed-scope patches are rejected. `synced` properties go through the object's CRDT, `account` properties through the tech-space carrier record, `local` properties through `Object.LocalSet`. The returned `VersionId` is in the domain of the route that wrote.

### 12.2 Batches
One `ModifyBatch` is one change on one dataset of one object, with any number of records. `ModifyMany` takes several batches for the same object, possibly on different datasets: it validates all of them first and writes none if any fails, then writes one change per batch in input order. There is no cross-object atomicity. `Space.Upsert(UpsertBatch)` is the schema-driven bulk ingest ([user-datasets.md](user-datasets.md)).

---

## 13. Subscriptions and Events

Live queries are windowed:

```go
Query(objectId, dataset).Filter(...).Sort(...).Limit(n).Subscribe(ctx, opts)
QueryObjects().Filter(...).Sort(...).Limit(n).Subscribe(ctx, opts)
```

`Subscribe` returns `*QueryResult{Initial, Total, HasNext, Sub}`, where `Sub` is the live `QuerySubscription`. `Snapshot(ctx, opts)` returns the same shape without `Sub`. `QueryOpts.IncludeTotal` requests a one-time count.

### 13.1 Event Shape

One `SubscriptionEvent` per apply that touches the subscription's scope, emitted after the write transaction commits, so a query run on receipt sees the new state.

```
SubscriptionEvent {
  versionId: string             // version of the change that produced the event
  added:   [SubRecord, ...]     // records that entered the visible window
  updated: [SubRecord, ...]     // records in the window that changed
  removed: [RemovedRecord, ...] // records that left the visible window, with the reason
}

SubRecord {
  id:  string
  doc: anyenc.Value             // full post-apply value, deep-cloned
  ops: [EventOp, ...]           // the change projected to $set / $unset
}

RemovedRecord {
  id:     string
  reason: deleted | filtered-out | displaced
}
```

`ops` are projected: `$inc`, `$addToSet`, `$pull` and `$incGated` arrive as a `$set` of the post-apply value, or an `$unset` when the path is gone. A client can apply `ops` to a JSON mirror without a CRDT engine, or rerender from `doc`.

Deleting an object removes its local state without a CRDT apply. The resulting `deleted` removal carries an empty `versionId`; act on it directly rather than filtering it by version.

### 13.2 Window Semantics

- `Limit > 0` needs a `Sort`; `Subscribe` fails without one. The engine holds `Limit + 1` rows, the last being a hidden sentinel, so a single arrival at the top emits `added` for it and a `displaced` removal for the row it pushes out, without re-querying any-store.
- `Limit == 0` is unbounded; memory grows with the matching set.
- `Offset` applies to the initial snapshot only.

### 13.3 Removed Semantics

- `deleted` — the record was tombstoned or its object deleted; `Query.One` for the id returns `ErrNotFound`.
- `filtered-out` — an update made the record stop matching the filter; it still exists.
- `displaced` — an arrival or the record's own sort-key change pushed it past `Limit`; it still matches the filter.

Drop the record for good only on `deleted`. After the other two, `Snapshot` or `Query.One` still returns it.

### 13.4 Property Scopes in Events
Account- and local-scope writes produce events like synced ones: one per applied change, with the versionId of that change's domain. Consumers don't need a path's scope to apply the ops.

### 13.5 Ordering
- Events follow apply order, after commit.
- All record transitions of one change arrive in one event.
- There is no ordering across objects.

### 13.6 Own-write events
Callers receive their own writes and recognize them by matching `ModifyResult.VersionId` with `SubscriptionEvent.VersionId`. The SDK doesn't suppress them.

### 13.7 Backpressure + Recovery

Each subscription has a bounded mailbox (`QueryOpts.MailboxCapacity`, default 256, minimum 16). When it fills, the subscription closes with `ErrSubscriptionOverflow`. It closes with `ErrSubscriptionDrifted` when more than `DriftBudgetPercent` (default 30) of `Limit` records leave the held window without replacements; the engine never re-queries any-store to backfill.

Either error means: resubscribe, and the new snapshot reconciles state. Nothing is dropped silently.

### 13.8 Initial Snapshot Fence

`Subscribe` reads its snapshot while holding the engine lock. Applies that finish in the meantime wait for the lock and are delivered after the subscription is registered, so no event is missed.

---

## 14. What the External API Does NOT Expose

- Handler internals and invocation.
- The internal write flow: tree writes, re-indexing.
- The account mirror's carrier records; callers see values at their normal paths.

`_ver` is caller-facing and ships in the shape the SDK stores; there is no separate external representation.

The external API (queries, writes, events, versionId semantics) stays stable across SDK minor versions; protocol internals may change.

---

# Part III — Reference

## 15. Conflict Examples

### 15.1 Two devices rename the same record
```
Device A: $set(name="Alpha")
Device B: $set(name="Beta")   written after B received A's change
```
B's change descends from A's, so it orders after A's on every peer. Final state: `name = "Beta"`, `_ver.name` = B's version. A receives an event for B's change.

### 15.2 Out-of-order delivery
Device C holds `_ver.name = "v15"`. An old change arrives with `$set(name="Old")` at `v7`. Since `v7 < v15`, the op is skipped.

### 15.3 Delete races a creating `$set`
Device A creates `block_42` with a multi-field `$set`. Device B, not having seen the create, deletes `block_42`. Both reach every peer:
- `$set` has the lower versionId: it creates the record, then the delete replaces it with a tombstone.
- The delete has the lower versionId: it writes a tombstone, and the later `$set` hits the sticky tombstone and is dropped entirely.

Either way the delete wins.

### 15.4 `$addToSet` racing `$set` on the same field
Device A at v20: `$addToSet(tags, "urgent")`
Device B at v21: `$set(tags, ["done"])`
Device C receives them out of order.

- A first: `tags = ["urgent"]`, `_ver.tags` untracked (`""`). Then B at v21 applies: `tags = ["done"]`, `_ver.tags = "v21"`.
- B first: `tags = ["done"]`, `_ver.tags = "v21"`. Then A at v20 ≤ v21 is skipped.

Both orders converge to `["done"]`.

### 15.5 Concurrent `$inc`
```
Device A: $inc(views, 1) at v30
Device B: $inc(views, 1) at v31
```
Both apply in either order: `views = previous + 2`, `_ver.views` untouched.

A `$inc` never arrives before the change that created its record: the DAG delivers ancestors first (§17).

### 15.6 Concurrent `$incGated` (not convergent)

```
Device A: $incGated(count, 1) at vA
Device B: $incGated(count, 1) at vB
Device C: $incGated(count, 1) at vC    (vA < vB < vC)
```

`$incGated` is `$set(field, current + delta)` with version gating, not a CRDT counter. Different delivery orders reach different values:

- [vA, vB, vC]: 0 → 1 → 2 → 3
- [vC, vB, vA]: vC applies (0 → 1); vB and vA are gated out → 1
- [vB, vA, vC]: vB applies (0 → 1); vA is gated out; vC applies (→ 2)

Use `$inc` for counters. Use `$incGated` only when the absolute post-mutation value matters and last-writer-wins on the highest version is acceptable.

---

## 16. Out of Scope

- **Text CRDT** — rich text with per-character merging.
- **Per-element versions for sets** — concurrent `$addToSet` and `$pull` of one element depend on delivery order (§5.4).
- **Ordered lists / CRDT arrays** — only commutative set operations exist.
- **Atomic multi-dataset changes** (§6.3).
- **Tombstone pruning** — record tombstones are kept.
- **Own-write suppression** in events.

---

## 17. Design Notes

1. **Modify-before-create can't happen.** any-sync is a DAG: every change lists its causal parents in `prevIds`, and a receiver applies a change only after all of them. A modify of a record has the creating change in its ancestry, so it can't be delivered first by a correct peer. The strict-modify skip on an absent record is a backstop against buggy or adversarial peers, not a race to design for.

2. **`$inc` converges under DAG delivery.** `$inc` doesn't update `_ver`, which makes it commute with itself. A stale `$set` can't clobber an increment that saw it: the `$set` is in the `$inc`'s ancestry and always applies first. Divergence needs a `$set` and an `$inc` on the same field that are concurrent, which has no correct answer ("reset to 100" and "add 1" in parallel) and signals a schema mistake: a field is either a counter (only `$inc`) or a settable value (only `$set`). `$incGated` exists for the last-writer-wins case.

3. **`_ver.id` min-update is transactional.** `lowerCreationMarker` mutates the record even when every op is otherwise dropped (the sticky-tombstone case). It runs inside the change's `WriteTx`, so a crash can't persist the marker without the rest of the change.

4. **Duplicate `RecordChange.Id` in one change is allowed and applies in order.** Records apply in slice order; a second entry for the same id sees the first one's effects. Generated code, split validation and merged transport batches all produce such payloads. Empty-id records are never duplicates: they resolve to distinct ids (§3.3).
