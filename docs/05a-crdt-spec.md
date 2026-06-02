# CRDT Full Spec

Companion to `05-crdt.md`. Complete specification of the version-gated record store CRDT for SDK v1. Names are examples and may change, but shapes and semantics are binding.

The spec is organized in two parts:
- **Part I: Protocol** — how changes are stored in any-sync and applied to any-store. This is what you implement inside the SDK.
- **Part II: External API** — how the protocol is exposed to callers. This is the contract with users.

Part III covers examples, deferred features, and open items.

---

# Part I — Protocol

Everything in this part is about how the SDK stores and applies changes. Callers don't interact with these artifacts directly.

## 1. Overview

A **record** is an anyenc document stored in a **dataset**. A dataset is a named collection inside an **object** (any-sync object tree). An object can hold many datasets.

Writes are **changes** — batches of per-record edits. A change is one any-sync DAG change. Each peer's local any-sync maintains a lexicographically sortable `versionId` (= `orderId`) per change for its own view of the tree. VersionIds are **local to one peer's view of one object tree**: two peers may encode the "same" logical DAG change with different versionId strings, and versionIds from different trees are not comparable at all. The CRDT layer always compares versionIds locally, inside one Controller — never across peers.

Each record carries a hidden `_ver` map of per-field version IDs. Consistency rule:

> A field is updated only if the incoming `versionId` is strictly greater than the field's stored version.

Out-of-order delivery is safe. There is no multi-head / conflict state — everything merges deterministically.

---

## 2. Terminology

| Term | Meaning |
|------|---------|
| **object** | any-sync object tree; unit of encryption, ACL, and sync |
| **dataset** | named record collection inside an object |
| **record** | single anyenc document in a dataset, identified by `id` |
| **change** | atomic batch of edits, maps 1:1 to an any-sync DAG change |
| **versionId** | lexicographically sortable local ordering key maintained by any-sync (= local `orderId`). Scoped to **one peer's view of one object tree** — never compared across trees, never assumed to match across peers |
| **_ver** | per-field order map on each record, tracking last-applied `versionId` per field path |
| **handler** | code registered for a dataset that validates and applies changes |
| **operation** | one edit inside a change (`$set`, `$inc`, etc.) |

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

- `id` is required and immutable. The SDK rejects any op (including a multi-field `$set` payload) that tries to write `id` — the whole change is dropped at pre-apply validation
- `_ver.id` holds the record's **creation version** (§3.5) — stamped once, preserved across delete
- `_ver` is caller-facing: queries return it attached to each record so clients can reconcile their own in-memory state per field
- Reserved field names: `id` and any top-level name starting with `_`. `_ver`, `_deletedAt`, `_traces`, and any future protocol field use the underscore prefix. User ops cannot write to reserved names — writes are rejected at pre-apply validation, not silently dropped
- All other fields are defined by the dataset's handler

`_ver` is a tree. Each entry value is either:
- A **string** (a versionId) — collapsed: every subkey at and below this point shares this version
- An **object** with explicit per-key entries plus an optional `*` default key. The `*` default holds the version inherited by any sibling not enumerated in the object — this is how expansion preserves precision when a finer write splits a previously-collapsed subtree. `*` is chosen because it only ever appears inside `_ver` subtrees (never in the user-facing record body), is visually distinct from real field names, and avoids anyenc's special empty-key byte encoding.

Lookup descends `_ver` along the path; if it falls off into unenumerated territory, the closest ancestor's `*` default applies, otherwise the version is `""` (the empty string is the "no version" sentinel, smaller than any real versionId).

### 3.2 `_ver` Collapsing Rules

If every field under a subtree shares the same version, only the subtree root is tracked.

```
$set { "a.b.c": 1 }  at v5        →  _ver: { "a": { "b": { "c": "v5" } } }
$set { "a.b":   {c: 1, d: 2} } v5 →  _ver: { "a": { "b": "v5" } }
$set { "a":     {b: {c:1}} }   v5 →  _ver: { "a": "v5" }
```

When a broader write happens, it replaces any finer `_ver` entries under its path. When a finer write happens after a broader one, the finer path is expanded under the broader root only if the broader version is strictly older.

### 3.3 Auto-creation via Upsert
There is **no `insert` op**. Record creation is controlled by a per-`RecordChange` boolean flag `upsert`:

- `upsert: false` (default, **strict**) — if the target id has no record, every modify op in the batch is a silent no-op. Matches mongo's "update if exists" semantics and is the safe default that prevents accidental record creation from typos or stale ids.
- `upsert: true` — if the target id is absent, an empty record is allocated first and the ops then run normally. The fresh record is stamped with `_ver.id = versionId` as its creation-version marker (see §3.5).

To create a record, send a `RecordChange` with `upsert: true` and typically a multi-field `$set` op (§5.1) carrying the initial fields. Two concurrent creates targeting the same id merge per-field via gating — there is no dual-insert convergence problem because there is no "skip if exists" rule.

`delete` ignores the flag: a delete on an absent record always writes a tombstone so concurrent deletes win against unseen creates.

### 3.5 `_ver.id` creation-version marker

Every record carries `_ver.id = versionId-of-earliest-upsert`. It records "when was this record first materialized" and callers rely on it as an indexable query key — for example, chat datasets sort messages by `_ver.id DESC` to show newest-first conversation order without a separate creation-timestamp field.

**Min-update rule (mandatory for cross-peer convergence).** Every **upsert** sets `_ver.id = min(current, incomingVersion)`. The marker only moves *downward*, never upward. Strict (non-upsert) modifies and idempotent deletes on existing tombstones leave it alone. `delete` on a live record preserves the existing `_ver.id` (it's already ≤ the delete version in practice, because the upsert that created the record preceded the delete that wipes it). `delete` on an absent record seeds `_ver.id = deleteVersion` — a subsequent older upsert still lowers it via the min rule.

**Why min.** Two peers may receive concurrent upserts to the same id in different delivery orders. Without a deterministic update rule, each peer stamps `_ver.id` with whichever upsert ran first and they diverge. `min` is commutative and associative, so every peer converges to the same value: the smallest versionId across all upserts and initial-delete seeds on that id. The chosen marker is the causally-earliest upsert, which matches the intuitive "creation" for a chat-style sort.

**Not a fallback/default.** `_ver.id` is a marker, not a whole-record version. Looking up an unenumerated field does NOT fall back to `_ver.id` — it returns `""` unless a nested `*` default applies. This preserves per-field gating and avoids the convergence break discussed for whole-record assertions.

**Preserved across delete.** When a live record is tombstoned, the existing `_ver.id` stays on the tombstone. Sort by `_ver.id` therefore places a deleted record where it was created, not where it died. When a later concurrent upsert hits the tombstone, it's still gated out at the field level (sticky tombstone), but the min rule lowers `_ver.id` if needed so peers that saw the upsert first agree with peers that saw the delete first.

### 3.6 `_traces` — trace-id correlation map

Every `Change` carries an optional `TraceIds []string` — opaque correlation tokens picked by the caller (e.g. a UI session id, an AI agent's operation id, a BSON ObjectId, a UUID). They travel with the change through any-sync (stored in a dedicated, non-encrypted field on the wire so any-sync can index them space-wide) and are stamped onto the record via a reserved `_traces` map:

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

**Keyed by versionId** purely for compaction: one change that touches N fields produces one `_traces[versionId]` entry, not N. To answer "which traces touched field X?" the caller reads `_ver[X]` to get a versionId and then looks up `_traces[versionId]`. To answer "which traces touched this record?" they union the map's values.

**Stamping rules** (run inside the same apply modifier, after the ops):

1. If `ch.VersionId` ends up referenced anywhere in the record's `_ver` (any leaf or `*` default) **and** `len(ch.TraceIds) > 0`: write `_traces[ch.VersionId] = ch.TraceIds`. Same versionId ⇒ same change ⇒ same traces, so overwrite is idempotent.
2. If `ch.VersionId` is referenced in `_ver` and `len(ch.TraceIds) == 0`: delete `_traces[ch.VersionId]` (explicit clear semantic — empty means "no trace for this version").
3. **GC**: walk `_ver`, collect every string version value, and drop any `_traces[v]` whose versionId is no longer present. If `_traces` becomes empty, remove the field entirely.

The GC rule keeps the map bounded by the number of **distinct versionIds currently live on the record** — not the historical total. When a later `$set` overwrites a field (bumping `_ver[field]` to a newer version), the superseded version's trace falls out unless it's still anchored somewhere else (typically the `_ver.id` creation marker).

**Commutative ops don't trace.** `$addToSet`/`$pull`/`$inc` don't update `_ver[field]`, so their versionId never lands in `_ver` and their traces aren't retained. This is a known limitation tied to those ops being non-LWW — per-element trace tracking is future work if needed.

**Tombstones preserve traces.** Delete copies the existing `_traces` onto the tombstone, then GC prunes to whatever survives in the shrunken `_ver = { id: creationVer, *: deleteVer }` — typically the creation-marker entry plus the delete's own trace entry. History queries across deleted records therefore still find them.

**Convergence.** Traces are a deterministic function of `_ver` (which versionId landed) plus the change's content (TraceIds), so two peers applying the same set of changes converge to the same `_traces` regardless of order, modulo the peer-local versionId caveat from §4.

**Name reservation.** `_traces` uses the underscore-prefix rule from §3.1 — user ops cannot write to it. No validation relaxation needed.

#### Empty id → `base58(xxh3-64(changeId))` sugar

When a `RecordChange` carries an empty `id`, the CRDT layer auto-assigns it from the enclosing `Change.changeId` via:

```
DeriveRecordId(changeId) = base58(xxh3-64(changeId))   // up to 11 chars
```

`changeId` is any-sync's content-addressable, immutable DAG change id — globally unique by construction and uniformly distributed. Hashing it with xxh3-64 preserves the uniformity (collision space 2⁶⁴; P(collision) < 1e-6 up to ~6M derived ids per storage namespace), and base58 encodes the 8-byte digest in a compact ~11-char form. Using the full changeId verbatim was rejected because it's ~50–60 chars, making storage-heavy records (e.g. per-object property records carrying many property-id keys) unnecessarily bulky. Implementation: `crdt.DeriveRecordId` in `crdt/idderive.go`, using `github.com/zeebo/xxh3` (≈2.5× faster than `cespare/xxhash` on CID-length inputs) and `github.com/mr-tron/base58` (the latter is the base58 library any-sync already depends on).

Resolution rules (per change, before validation):

1. **Empty `id` requires `upsert: true`.** A strict modify on an empty id is rejected with `ErrEmptyIdRequiresUpsert` before any state mutation — the resolved id would be freshly derived from `changeId`, so no existing record could possibly match and the operation would always be a no-op. Rejecting it loudly surfaces a programming error that would otherwise silently drop writes.
2. The **first** empty-id record in the batch gets `DeriveRecordId(changeId)`.
3. Subsequent empty-id records get `DeriveRecordId(changeId) + "/" + index`, where `index` counts empty-id records only (1, 2, 3, …). Explicit-id records don't affect the counter.
4. If a record has an empty `id` AND the enclosing `changeId` is also empty → `ErrMissingRecordId`.
5. Handlers see the **resolved** id in `RecordChange.id` during validation, so id-pattern rules apply to the final value.

Typical usage: a caller issuing a "create a new record" writes `RecordChange{Upsert: true, Ops: [{$set, multiField}]}` with no `Id`. The resulting record lands under `DeriveRecordId(changeId)`, and subsequent modifies target it by that derived id.

### 3.4 Soft Delete
Delete keeps the record visible as a tombstone:
```json
{ "id": "block_42", "_deletedAt": 1715000000, "_ver": { "id": "!A0", "*": "v9" } }
```
All other fields are wiped. `_ver` shrinks to two entries: `id` (the preserved creation version, §3.5) and `*` (the delete version as the default for every other field). Sorting by `_ver.id` still places the tombstone at its creation point.

**Tombstones are sticky.** Every subsequent modify on a tombstoned record is dropped at the protocol level regardless of version — there is no way to resurrect a record at the CRDT layer in v1. This gives "delete wins absolutely" without depending on handlers to reject undeletes. Higher-level resurrect (e.g. user undelete UI) is out of scope for v1; callers that need it must layer their own mechanism on top.

---

## 4. Version IDs

- **Source**: each peer's local any-sync tree storage maintains a `versionId` (= local `orderId`) for every change it has observed. The SDK never invents real versionIds.
- **Locality**: versionIds are **peer-local**. Two peers may hold different versionId strings for the same logical DAG change, and neither is authoritative. Do not compare versionIds across peers, serialize them to clients as identities, or assume a change's versionId is stable over the network.
- **Shape**: opaque string, lexicographically comparable within one peer's view. "Newer" ⇔ greater string when compared locally.
- **One change = one versionId (locally)**: every operation in a batch shares the same local versionId when a peer applies it.
- **Two uses inside the SDK**:
  1. CRDT gating, always comparing local versionIds against the same peer's `_ver` map.
  2. As the ordered index key for efficient change queries against the local store (Phase 2: "give me changes in apply order").
- **Convergence argument**: each peer applies its locally-delivered changes under locally-consistent gating. Two peers holding the same DAG end up with the same *logical* record content — not with the same `_ver` strings. Cross-peer events exchange DAG changes, not versionIds; the receiver assigns its own local versionIds on apply.

## 4a. AddSeq (delivery watermark)

Both `versionId` and `addSeq` are **peer-local** counters, but they answer different questions and have different shapes:

- **versionId** (= local any-sync orderId): lexicographically sortable string. Used for CRDT gating (locally) and as the ordered index key for change queries. Peer-local and scoped to one object tree.
- **addSeq**: monotonic `uint64` assigned by any-sync's `spacestorage` as changes are received locally. Space-wide in any-sync's implementation (see `any-sync/commonspace/spacestorage/spacestorage.go`), but the SDK tracks it **per object** because the restore contract is per-object:

Per-object tracking in the CRDT Controller:

- `Controller.MaxAddSeq()` returns the highest AddSeq observed by that object.
- `Controller.SetMaxAddSeq(seq)` seeds the watermark on restore (once, before any `ApplyChange`).
- `ApplyChange` bumps the watermark monotonically after a successful apply. A replay with a lower AddSeq still applies (CRDT is idempotent) but does **not** regress the watermark.
- Zero means "no changes observed yet". A failed `ApplyChange` (e.g. unknown dataset, validation rejection, empty-id rule violation) does not touch the watermark.

### Restore / reindex contract

1. Read `MaxAddSeq()` for every Controller.
2. Ask any-sync for heads whose `LastAddSeq` is greater.
3. For each such object, fetch the new DAG changes and replay them through `ApplyChange`.
4. Controllers bump their watermarks as changes land, and the space layer persists them atomically with the any-store write tx (Phase 2).

The CRDT's idempotency means the contract tolerates crash-after-apply-before-watermark-persist: on restart the replay sees the same changes again and they become no-ops via gating and sticky tombstones.

### Why not a separate per-record `_seq`?

Deferred. The two use cases (incremental UI updates, "what changed in batch N") are better served by the Phase 2 event stream. Adding `_seq` later is a backward-compatible schema change if a concrete query pattern demands it.

---

## 5. Operations

### 5.0 Path rules (apply to every op with a field path)

Every op that targets a field path is validated *before* the apply phase runs — a malformed path fails the whole change with `ErrInvalidPath` (wrapped in `ErrValidation`). Rules:

1. **Path must be non-empty.** `$set` and `$unset` can have an empty `path` only when the multi-field form is in use (payload is an object of dotted keys).
2. **No empty path segments.** `["a", "", "b"]` is rejected. In the multi-field form, a dotted key like `"a."` splits into `["a", ""]` and is also rejected.
3. **No `.` inside a path segment.** A single-path op must supply its path as already-split components — `["meta.color"]` is rejected because it would silently clash with multi-field parsing. Write `["meta", "color"]` instead.
4. **Top-level reservation.** The first segment may not be `id` (immutable) and may not start with `_` (all protocol-owned fields live under the underscore prefix: `_ver`, `_deletedAt`, and any future system field). Users choose their own field names from outside those namespaces.

Handlers can layer additional validation on top (e.g. schema shape, permissions).

### 5.1 `$set`

Two forms:

**Single-path form** — `op.path` is a non-empty dotted path, `op.payload` is the value:
```
$set "name" → "Hello"
```

**Multi-field form** — `op.path` is empty, `op.payload` is an object whose keys are dotted paths and whose values are the values to assign:
```json
{ "$set": { "name": "Hello", "count": 7, "meta.color": "red" } }
```

Each entry of a multi-field `$set` is applied as an independent gated single-path `$set` sharing the change's versionId. Use the multi-field form to "create" a record atomically (it's the closest analogue to the removed `insert` op): if the target id doesn't exist, the record is auto-created and every field lands as a single batch.

For each path, if `_ver[path] < versionId`, write value and set `_ver[path] = versionId`. Else skip that path.

Writes targeting the immutable `id` field (single-path or as a key in the multi-field payload) are silently ignored.

### 5.2 `$unset`
```json
{ "$unset": { "description": "" } }
```
Gated like `$set`. If newer, remove field and set `_ver[path] = versionId`. Multi-field form mirrors `$set`: empty `op.path`, payload object whose keys (dotted paths) are unset; values are ignored.

### 5.3 `$addToSet` (commutative)
```json
{ "$addToSet": { "tags": "urgent" } }
```
1. If `_ver[field] ≥ versionId` → skip (a newer `$set`/`$unset` superseded the whole field)
2. Else: add element if not present. **Do not update `_ver[field]`**.

Rationale: sets are commutative; multiple concurrent adds should all land. The check against `_ver[field]` preserves correctness when a `$set` replaces the whole field.

### 5.4 `$pull` (commutative)
```json
{ "$pull": { "tags": "urgent" } }
```
Same check as `$addToSet`. If allowed, remove matching element(s). Do not update `_ver[field]`.

**Known limitation** (v1): concurrent `$addToSet` + `$pull` of the same element has no per-element ordering guarantee. Per-element version tracking is future work.

### 5.5 `$inc` (commutative counter)
```json
{ "$inc": { "count": 1 } }
```
Unconditionally add delta to the field. **Does not update `_ver[field]`**. Purely commutative.

**Edge case**: if `_ver[field] ≥ versionId` → skip `$inc`. Avoids clobbering a recent `$set` with a stale increment.

### 5.6 `$incGated` (LWW-style increment)
```json
{ "$incGated": { "priority": 1 } }
```
Equivalent to `$set(field, currentValue + delta)` with full version gating. Updates `_ver[field]`.

### 5.7 `delete`

Payload is unused (the record id comes from `RecordChange.id`).

- If record absent → create a tombstone with `deletedAt = change.timestamp` and `_ver = { "id": versionId, "*": versionId }` (the delete seeds both its own creation marker and the default)
- If record exists (live) → replace it with the tombstone shape above, preserving the existing `_ver.id` as the tombstone's creation marker; all other fields are dropped
- If record is already a tombstone → idempotent no-op
- **Sticky tombstones**: every subsequent modify on a tombstoned record is dropped at the protocol level regardless of version. There is no insert to race against, and no version of any modify can resurrect a tombstone in v1.

### 5.8 Operation Summary

| Op | Gated by `_ver[field]`? | Updates `_ver[field]`? | Commutative? |
|----|----------------------|----------------------|--------------|
| `$set` | yes | yes | no (LWW) |
| `$unset` | yes | yes | no (LWW) |
| `$addToSet` | yes | no | yes (set merge) |
| `$pull` | yes | no | yes (set merge) |
| `$inc` | yes (against `$set`) | no | yes (counter) |
| `$incGated` | yes | yes | no (LWW; non-convergent under arbitrary delivery — see §15) |
| `delete` | sticky tombstone | collapses `_ver` to `{ "*": v }` | N/A — wins absolutely |

**Auto-create** (no explicit row): controlled by `RecordChange.upsert`. When `upsert: true`, any modify op targeting a non-existent id auto-creates an empty record `{id, _ver: {}}` first, then applies. When `upsert: false` (the default, strict), modifies on absent records are no-ops. `delete` ignores the flag. There is no `insert` op.

---

## 6. Change Format (wire)

### 6.1 Batch structure
One batch = one any-sync DAG change = one `versionId`.

```
Change {
  spaceId:    string            // any-sync space ID
  objectId:   string            // any-sync object ID
  dataset:    string            // dataset name inside the object
  changeId:   string            // DAG change identity (distinct from versionId)
  versionId:  string            // peer-local any-sync orderId — used for gating AND as a local sort key
  addSeq:     uint64            // any-sync local delivery sequence; §4a
  traceIds:   [string, ...]     // opaque caller-supplied correlation tokens; §3.6
  records: [
    RecordChange {
      id:     string            // record ID; empty → derived from changeId, see §3.3
      upsert: bool              // if true, auto-create the record when absent; §3.3
      ops:    [Operation, ...]  // ordered list of operations for this record
    },
    ...
  ]
}
```

`changeId` and `versionId` are **distinct**:
- `versionId` is the any-sync orderId — lexicographically comparable within one tree, used for gating.
- `changeId` is the any-sync **content-addressable, immutable** DAG change id (a hash of the change's contents). It is globally unique, cannot change across replays, and requires no coordination. That uniqueness — combined with the hash derivation described in §3.3 — makes it safe to use as a derived record id.

The CRDT layer uses `changeId` for observability and as the seed for empty-id resolution (§3.3). It MUST be provided when the batch contains a `RecordChange` with an empty `id`.

### 6.2 Operation
```
Operation {
  type:    "$set" | "$unset" | "$addToSet" | "$pull" | "$inc" | "$incGated" | "delete"
  path:    [string, ...]        // dotted field path; empty for multi-field $set/$unset and for delete
  payload: anyenc.Value         // operation-specific; see §5
}
```

### 6.3 Multiple records, multiple datasets
One batch touches many records but belongs to **exactly one dataset**. Multi-dataset atomic writes are out of scope for v1.

### 6.4 Encoding
A change is marshalled into the `Data` field of an any-sync `TreeChange`. The encoding is `anyenc`. any-sync signs and encrypts the outer `TreeChange`; the CRDT protocol is agnostic to that.

---

## 7. Application Algorithm

```
applyChange(change):
    handler = handlers[change.dataset]      // ErrUnknownDataset if missing
    resolvedIds = resolveRecordIds(change)  // §3.3: empty id → changeId[/idx]
                                            //  ErrMissingRecordId if both empty
    for i, rc in change.records:            // pre-validate every op first
        rcResolved = rc with id=resolvedIds[i]
        for op in rc.ops:
            handler.validate(rcResolved, op) // any error → drop the WHOLE change
    for i, rc in change.records:
        for op in rc.ops:
            applyOp(resolvedIds[i], rc.upsert, op, change)

applyOp(recordId, upsert, op, change):
    if op.type == delete:
        if records[recordId] is tombstone: return       // idempotent
        preservedIdVer = records[recordId]?._ver.id or change.v
        records[recordId] = tombstone(
            deletedAt = change.ts,
            _ver      = { "id": preservedIdVer, "*": change.v },
        )
        return                                           // delete ignores `upsert`

    rec = ensureRecord(recordId, upsert, change.v)
    if rec is nil: return                    // sticky tombstone OR strict-absent

ensureRecord(id, upsert, version):
    r = records[id]
    if r is tombstone:
        if upsert: lowerCreationMarker(r, version)       // keep §3.5 min-rule
        return nil
    if r exists:
        if upsert: lowerCreationMarker(r, version)       // keep §3.5 min-rule
        return r
    if not upsert: return nil                            // strict — skip
    r = { id, _ver: { "id": version } }                  // §3.5 creation-version marker
    records[id] = r
    return r

lowerCreationMarker(rec, version):
    // Min-rule: _ver.id only moves downward.
    if rec._ver.id is missing or version < rec._ver.id:
        rec._ver.id = version

    switch op.type:
        case $set:
            if op.path is empty:            // multi-field form
                for (key, value) in op.payload:
                    gatedSet(rec, change.v, splitDots(key), value)
            else:
                gatedSet(rec, change.v, op.path, op.payload)
        case $unset:
            if op.path is empty:
                for (key, _) in op.payload:
                    gatedUnset(rec, change.v, splitDots(key))
            else:
                gatedUnset(rec, change.v, op.path)
        case $addToSet:
            if compareVersion(getOrder(rec, op.path), change.v) >= 0: skip
            else: addToSet(rec, op.path, op.payload)        // _ver NOT updated
        case $pull:
            if compareVersion(getOrder(rec, op.path), change.v) >= 0: skip
            else: pullFromSet(rec, op.path, op.payload)     // _ver NOT updated
        case $inc:
            if compareVersion(getOrder(rec, op.path), change.v) >= 0: skip
            else: rec[op.path] = (rec[op.path] or 0) + op.payload   // _ver NOT updated
        case $incGated:
            // treat as $set(path, currentValue + delta) — gated, updates _ver
            if compareVersion(getOrder(rec, op.path), change.v) < 0:
                rec[op.path] = (rec[op.path] or 0) + op.payload
                setOrder(rec, change.v, op.path)

gatedSet(rec, v, path, value):
    if path[0] is reserved (id, _ver): return               // immutable / protocol-owned
    if compareVersion(getOrder(rec, path), v) < 0:
        writeValue(rec, path, value)
        setOrder(rec, v, path)

gatedUnset(rec, v, path):
    if path[0] is reserved: return
    if compareVersion(getOrder(rec, path), v) < 0:
        removeValue(rec, path)
        setOrder(rec, v, path)
```

`compareVersion(a, b)` returns `< 0` if `a < b`, `0` if equal, `> 0` if `a > b`. The empty string compares less than any non-empty version.

### 7.1 Collapse
After each write that adds a finer `_ver` entry, walk up and collapse siblings that share the same version into their parent. Implementations may defer this for efficiency; correctness does not depend on aggressive collapsing.

### 7.2 Write transaction
All ops within one change are applied atomically (one any-store `WriteTx` in Phase 2). Events fire **after** the transaction commits. Validation is two-phase: every op in the change is validated first; if any handler rejects, the entire change is dropped before any mutation runs.

---

## 8. Handlers

A handler owns a dataset. It defines:

```go
type Handler interface {
    Dataset() string                   // dataset name
    Version() int                      // handler version (for re-indexing)
    Validate(op Operation) error       // permission / shape check
    Apply(tx WriteTx, change Change) error
}
```

### 8.1 Registration
Handlers are registered at SDK init:
```go
sdk.RegisterHandler(handler)
```
Registration is not compile-time — the set of handlers can differ across app versions. (Ties into Versioning section.)

### 8.2 Unknown Datasets
Changes arriving for datasets with no registered handler are **persisted in any-sync** (they were already accepted into the tree) but **not applied** to any-store. When a handler is later registered, the Versioning flow triggers a replay.

Dataset is effectively invisible to queries until a handler exists, but data is preserved.

### 8.3 Validation
Each handler may enforce:
- Shape rules (schema validation; permissionless in v1, handler-defined later)
- Per-record permissions (e.g., "only the author of a chat message can edit it") — identity comes from the signing key on the change
- Reject invalid changes: validation errors logged; change dropped from any-store state but stays in the tree

---

## 9. Property Variants (protocol)

The `objects` system collection stores one record per object with four property scopes: `auto`, `base`, `account`, `device` (see `06-data-structure.md`).

### 9.1 Where property writes come from
- `base` — written via the regular CRDT pipeline on the **object itself** (one built-in dataset per object, handled by the `baseProperty` handler). Records use empty `id` in changes; the handler translates them into an update on the `objects` collection of the space
- `account` — written to a dedicated dataset in the **rewriteObject** in tech space; handled by the `rewriteObject` handler
- `device` — written locally, no any-sync change involved

### 9.2 Per-variant `_ver`
The `objects` record holds three independent `_ver` maps, one per overridable variant:
```json
{
  "id": "objectId",
  "name": "device name",
  "_device":  { "name": "device name" },
  "_account": { "name": "account name" },
  "_base":    { "name": "base name" },
  "_o_device":  { "name": "vLocal1" },
  "_o_account": { "name": "vTech7"  },
  "_o_base":    { "name": "vObj42"  }
}
```
- `_o_device` is keyed by **local** version IDs (§9.4)
- `_o_account` is keyed by tech space's `versionId`
- `_o_base` is keyed by the object's own `versionId`

Version IDs from different scopes are **not comparable** — each `_o_*` lives in its own version space.

### 9.3 Computed root
After any variant changes, the handler recomputes affected root fields by picking the winning variant per priority `device > account > base`. The computed value lives at the root and is what queries see by default.

### 9.4 Device version IDs
Device writes never touch any-sync. The SDK keeps a local monotonic counter per (space, object) stored in any-store. Keys in `_o_device` use the counter with a distinct prefix (e.g., `"d#42"`).

---

# Part II — External API

This part defines what callers see. The protocol in Part I is the implementation; this is the contract.

## 10. External API Overview

The external API exposes the protocol as a small, stable surface: **queries**, **writes**, and **subscriptions**. `versionId` is the shared consistency primitive tying all three together.

**The caller is middleware, not an end-user client.** The SDK is a Go library consumed in-process. Middleware handles transport (gRPC/REST), sessions, client-local auth, and product logic. See `00-common-context.md` for the full stack. Implications:

- API methods take Go types (not JSON/protobuf). Middleware serializes for its own wire protocol.
- No session-awareness in v1 (confirmed deferred, since middleware already has sessions)
- "Callback to the client" is not the SDK's job — middleware consumes SDK events and pushes them over its own transport

### 10.1 Consistency Model
`versionId` is **caller-facing**. It appears in:
- Query results (per record, indicating the latest change version that touched the record)
- Subscription events (per event, indicating the change version that produced it)
- Write return values (the final versionId any-sync assigned)

Callers use versionIds to:
- Recognize their own writes in the event stream (match returned versionId against incoming events)
- Order events if their UI needs strict sorting
- Reconcile their own optimistic in-memory state with the SDK's any-store. The client (or the middleware consuming the SDK) holds its own state, applies optimistic writes, and needs per-field version info to know which fields have been server-confirmed vs still local-only. Query results carry `_ver` attached to the record; clients walk it using the shape documented in §3.
- Apply their own version-gating rule on top of the SDK when maintaining derived state outside any-store

What callers never see:
- Handler internals
- Re-indexing machinery

---

## 11. Queries

```go
Query(objectId, dataset).Filter(...).Sort(...).Limit(n).Offset(n)
QueryObjects().Filter(...).Sort(...).Limit(n).Offset(n)
```

Terminal calls:

- `Iter(ctx) -> Iterator` — streaming.
- `All(ctx) -> []anyenc.Value` — materialise all.
- `One(ctx) -> anyenc.Value` — first match or `ErrNotFound`.
- `Count(ctx) -> int` — match count.
- `Snapshot(ctx, opts) -> *QueryResult` — point-in-time view + optional total (§13).
- `Subscribe(ctx, opts) -> *QueryResult` — initial view + live `Sub` (§13).

`Filter` accepts anything `query.ParseCondition` accepts (already-built `query.Filter`, JSON string, or map literal with mongo operators). `Sort` accepts anything `query.ParseSort` accepts (`"name"`, `"-_ver.id"` for descending, or already-built `query.Sort`). Parse errors are stashed eagerly and surfaced on the first terminal call.

### 11.1 Projections

By default the `_device` / `_account` / `_base` variant fields (for the `objects` collection) are stripped and tombstones are excluded. The caller sees the clean record with `_ver` attached in the same tree shape used internally — clients that mirror SDK state per field parse `_ver` using the same lookup rules as the SDK (the shape is documented in §3.1 and §3.2).

Opt-in flags:

- **`WithRawVariants()`** — include the raw `_device` / `_account` / `_base` variant fields for `objects` collection records (normally the caller only sees the computed root values).
- **`WithTombstones()`** — include tombstones in the result.

Defining a stable client-facing version accessor (single-path lookup, walker) is deferred to a later iteration. For now the tree shape documented in §3 is the contract.

### 11.2 Record-level versionId
The record-level versionId is the greatest versionId in the record's `_ver` tree (after collapsing). Useful when a client only cares about "has this record changed since I last looked" rather than per-field reconciliation.

---

## 12. Writes

Two methods, both accept raw operations.

```go
Modify(objectId, datasetName, id, ops, opts...) -> (ModifyResult, error)
Delete(objectId, datasetName, id) -> (ModifyResult, error)

type ModifyResult struct {
    VersionId string   // peer-local lexid stamped on the records
    ChangeId  string   // any-sync DAG change id (content-addressable, stable across peers)
    RecordIds []string // per-record id, aligned to input order
}
```

- `ops` is a list of operations from §5 — callers write `$set`, `$addToSet`, `$inc`, etc. directly
- `Modify` defaults to strict (no record creation). Pass `WithUpsert()` (or equivalent option) to enable auto-creation — this is the "create" path. A typical create is `Modify(id, [{$set: multiFieldPayload}], WithUpsert())`
- `VersionId` is synchronous — any-sync is offline-first and commits locally before returning
- `ChangeId` is the any-sync DAG hash; use it for tracing and cross-peer correlation
- `RecordIds[i]` is the resolved id of record `i`. For records the caller submitted with empty Id, the resolved value is `base58(xxh3-64(ChangeId))` (with `:<index>` for the second-and-later empty ids in a batch; `:` rather than `/` so the id is safe in URL path segments). This is the propId / shortId convention — property creates read it from `RecordIds[0]`

### 12.1 Property-scope Writes

```go
SetDeviceProperty(objectId, fields) -> (ModifyResult, error)
SetAccountProperty(objectId, fields) -> (ModifyResult, error)
```

`base`-scope property writes go through regular `Modify()` targeting the base-property dataset.

### 12.2 Batches
A single `Modify` can target multiple records (as one batch) if the API supports it. Final API shape for multi-record batches is in §17.

---

## 13. Subscriptions and Events

Single surface: windowed live queries.

```go
Query(objectId, dataset).Filter(...).Sort(...).Limit(n).Subscribe(ctx, opts)
QueryObjects().Filter(...).Sort(...).Limit(n).Subscribe(ctx, opts)
```

Returns `*QueryResult{Initial, Total, Sub}` where `Sub` is the live `QuerySubscription`. Both call shapes also support `Snapshot(ctx, opts)` for a point-in-time read with the same result shape (no live `Sub`).

### 13.1 Event Shape

One `SubscriptionEvent` per CRDT apply that touches the sub's scope; emitted **after** the any-store write tx commits (read-your-writes safe).

```
SubscriptionEvent {
  versionId: string             // per-change DAG order; for fence-and-replay
  added:   [SubRecord, ...]     // records that entered the visible window
  updated: [SubRecord, ...]     // records already in the window, changed
  removed: [RemovedRecord, ...] // records that left the visible window, tagged with cause
}

SubRecord {
  id:  string
  doc: anyenc.Value             // full post-apply value, deep-cloned
  ops: [EventOp, ...]           // projected $set / $unset ops from the change
}

RemovedRecord {
  id:     string
  reason: deleted | filtered-out | displaced
}
```

`ops` is post-projection: every `$inc` / `$addToSet` / `$pull` / `$incGated` has been merged on the SDK side and ships as a `$set` against the post-apply value (or `$unset` when the path went away). A thin client can apply `ops` against a JSON-like local mirror without a CRDT engine. `doc` carries the full post-apply state for clients that prefer to rerender from scratch.

`removed` carries each id that left the visible window, tagged with **why** so clients can tell "the object is gone" from "it left my result set but still exists":

- `deleted` — the record was tombstoned; gone from the database. A `Query.One` now returns `ErrNotFound`.
- `filtered-out` — an update changed a field so the query filter no longer matches; the record still exists.
- `displaced` — a higher-priority arrival (or the record's own sort-key change) pushed it past the `Limit` boundary; it still matches the filter, just sits outside the visible window.

Branch on `deleted` to drop the object for good; the other two mean a `Snapshot`/`Query.One` would still return it.

### 13.2 Window Semantics

The window tracks filter + sort + limit incrementally.

- When `Limit > 0`, the engine internally holds `Limit + 1` rows; the largest-tuple row is the *sentinel* — kept but not visible to the consumer. Single new arrivals at the top of the sort cause an `Added` (the new row) + `Removed` (the previous bottom-visible row that demoted to sentinel) without any any-store re-query.
- `Limit == 0` is unbounded; RAM grows with the matching set.
- `Offset` applies to the initial snapshot only; the live window has no offset.

### 13.3 Removed Semantics

Each `RemovedRecord` carries the id plus a `reason` so consumers can tell "the object is gone" from "it left my result set but still exists":
- `deleted` — the record was tombstoned in the DB; a `Query.One` for the id now returns `ErrNotFound`.
- `filtered-out` — an update changed a field so the filter no longer matches; the record still exists.
- `displaced` — a higher-priority arrival (or the record's own sort-key change) pushed it past `Limit`; it still matches the filter, just sits outside the visible window.

Branch on `deleted` to drop the object for good. For `filtered-out` / `displaced`, a `Snapshot` / `Query.One` with the id still returns the doc (filtered-out ⇒ doesn't match the active filter; displaced ⇒ does).

### 13.4 Property Variants in Events
For the `objects` collection: `SubRecord.Doc` carries the merged post-apply value (computed root). Variant-level (`_device` / `_account` / `_base`) introspection is not exposed in v1; an advanced channel is deferred.

### 13.5 Ordering
- Events are emitted in `applyChange` order (strictly after tx commit).
- Within one change, all record transitions ship in a single `SubscriptionEvent`.
- Cross-object ordering is not guaranteed.

### 13.6 Own-write events
Caller receives their own writes as events. They recognize them by matching the returned versionId on the change against `SubscriptionEvent.VersionId`. Session-based suppression is deferred.

### 13.7 Backpressure + Recovery

Per-sub mailbox is `mb/v3`-bounded (default 256, min 16). There is no silent-drop policy: on overflow the engine closes the sub with `ErrSubscriptionOverflow`. A second close mode is `ErrSubscriptionDrifted` — fired when more than `DriftBudgetPercent` (default 30) of `Limit` records leave the held window without replacements (the engine never re-queries any-store on the hot path to backfill).

Either error is the only recovery contract: the client resubscribes and the new snapshot reconciles state. There is no `Dropped()` counter or partial-delivery mode.

### 13.8 Initial Snapshot Fence

`Subscribe`'s snapshot read runs UNDER the engine's mutex; apply events that fire during the read queue on the lock and process correctly after the new sub is registered. No per-event `VersionId` dedupe needed.

---

## 14. What the External API Does NOT Expose

- Handler registration / lifecycle
- Internal write flow (tree writes, reconciliation, re-indexing)
- `_ver_device` / `_ver_account` / `_ver_base` internal structure (callers use `WithRawVariants()` if they need the raw scopes, otherwise they see computed root values)
- Device-counter version IDs (callers see final versionIds in events, even for device writes which use their own internal counter)

Note that `_ver` itself IS caller-facing — it ships with each queried record in the same tree shape the SDK stores it. Clients use the documented lookup rules (§3) to walk it. This is the full contract; there's no "internal vs external" representation split for `_ver` in v1.

The stability contract: the external API (shape of queries, writes, events, versionId semantics) does not change between SDK minor versions. Protocol internals can change freely.

---

# Part III — Reference

## 15. Conflict Examples

### 15.1 Two devices rename same record
```
Device A: $set(name="Alpha")  at vA (sent first, accepted at v10)
Device B: $set(name="Beta")   at vB (sent after seeing v10, accepted at v11)
```
Both apply. Final state: `name = "Beta"`, `_ver.name = "v11"`. Device A gets an event for v11 and updates local state.

### 15.2 Out-of-order delivery
Device C already has `_ver.name = "v15"`. An old event arrives with `$set(name="Old")` at `v7`. Since `v7 < v15`, op is skipped. State stays consistent.

### 15.3 Delete races a creating `$set`
Device A creates `block_42` via a multi-field `$set` (recall: there is no `insert` op). Device B (having not seen the create) deletes `block_42`. Both reach every peer:
- If the `$set` has lower versionId: `$set` auto-creates the record, then delete replaces it with a tombstone → final state is tombstone.
- If the delete has lower versionId: delete writes a tombstone first. The `$set` arrives later, `ensureRecord` sees the sticky tombstone and returns nil → the entire `$set` is dropped, including any merge of unrelated fields.

Either way, **delete wins absolutely**. There is no version of any modify that can resurrect a tombstoned record at the protocol level in v1.

### 15.4 `$addToSet` racing `$set` on the same field
Device A at v20: `$addToSet(tags, "urgent")`
Device B at v21: `$set(tags, ["done"])`
Device C receives them out of order.

- If A arrives first: `tags = ["urgent"]`, `_ver.tags = nil`. Then B at v21 > nil → applies → `tags = ["done"]`, `_ver.tags = "v21"`
- If B arrives first: `tags = ["done"]`, `_ver.tags = "v21"`. Then A at v20 ≤ v21 → skipped

Both orderings converge to `["done"]`.

### 15.5 Concurrent `$inc`
```
Device A: $inc(views, 1) at v30
Device B: $inc(views, 1) at v31
```
Both apply regardless of order. Final `views = previous + 2`. `_ver.views` untouched.

`$inc` requires a previous value to compose with — a `$inc` that arrives before the creating `$set` would compose against `0` and then be overwritten when the create lands. In practice this race does not happen because any-sync's DAG enforces causality between the create and any modify.

### 15.6 Concurrent `$incGated` (intentionally non-convergent)

```
Device A: $incGated(count, 1) at vA
Device B: $incGated(count, 1) at vB
Device C: $incGated(count, 1) at vC    (vA < vB < vC)
```

`$incGated` is "$set(field, current+delta) with version gating", **not** a CRDT counter. Different delivery orders reach different final values for the same set of ops:

- Apply [vA, vB, vC] (canonical): 0 → 1 → 2 → 3
- Apply [vC, vB, vA]: vC applies (0→1), then vB and vA are gated against vC and skipped → 1
- Apply [vB, vA, vC]: vB applies (0→1), vA gated against vB → skip, vC applies on top of 1 (→2)

Use `$inc` for counters. Use `$incGated` only when the absolute post-mutation value matters and you accept LWW semantics on whichever op wins the version race.

---

## 16. Deferred / Out of Scope (v1)

- **Text CRDT** — rich text with per-character merging
- **Per-element versioning for sets/arrays** — would fix `$addToSet`/`$pull` race
- **Snapshots / compaction** — no pruning of old state
- **Schema validation** (beyond what handlers implement ad-hoc)
- **Cross-dataset atomic writes** in one batch
- **Session-based own-write filtering**
- **Ordered lists / CRDT arrays** — only commutative set operations are supported

---

## 17. Open Items

### Protocol
1. Device version ID prefix — `"d#"`, `"~d-"`, or other (§9.4). This is a local-counter namespace for device-scoped writes that never touch any-sync; unrelated to any-sync orderIds.
2. Final handler interface — method names, error types (§8)

### External API
3. Final write method naming (`Modify` vs `Update`; no `Insert`/`Create` — auto-creation makes them unnecessary)
4. Multi-record batch writes — is `Modify` plural-friendly?
5. Record-level versionId on query results — is it always the max of `_ver`, or the versionId of the most recent change that touched any field?
6. Event batcher tuning — `mb` buffer size, overflow policy

### Cross-cutting
7. How to represent ordered lists if/when needed (not v1)

### Design decisions from the Phase 1 review

8. **Modify-before-create races — protocol-impossible.** any-sync is a Git-like DAG: every change carries a `prevIds` chain pointing to the changes it causally depends on, and a receiver cannot apply a change until all of its `prevIds` are already applied. If a peer authored a modify referencing some record, the creating change is in that modify's ancestry by construction. A modify delivered before its creating change can only happen if the delivering peer is buggy or adversarial — it's not a race the CRDT needs to tolerate as a legitimate case. The CRDT layer's "strict modify on absent = silent skip" behavior is a defensive backstop for the pathological case, not a first-class edge case.

9. **`$inc` convergence under DAG delivery — accepted.** `$inc` doesn't update `_ver`, which is what gives it commutativity with itself. The "stale `$set` delivered after an `$inc` clobbers the increment" scenario cannot happen under DAG delivery for causally-ordered ops: if `$inc` exists at version vInc, its author had already seen the `$set` at version vSet in its state, so vSet is in vInc's `prevIds` chain; no receiver ever applies vInc before vSet. The only way to get apparent divergence is if `$set` and `$inc` are **concurrent** on the same field (two peers that hadn't seen each other's writes), which is a semantically ill-defined user pattern ("reset to 100 AND add 1 in parallel" has no correct answer) and a schema-design anti-pattern: a field is either a counter (only `$inc`) or a settable value (only `$set`). `$incGated` remains available for the LWW-on-post-mutation-value semantic when you explicitly want it.

10. **Transactional atomicity of `_ver.id` min-update — Phase 2 requirement, noted in code.** `lowerCreationMarker` mutates the record even when the op is otherwise skipped (sticky-tombstone case). Phase 2's any-store integration must run it inside the same `WriteTx` as the rest of the change so a crash between the mutation and the commit can't leak inconsistency. A code comment at the function's docstring spells this out so it's hard to miss during wiring.

11. **Handler-writable derived-field namespace — design deferred.** The `_` prefix is reserved at the path-validation layer, so a future `_h.*` sub-namespace is already unreachable from user ops. When Phase 2 designs the handler-hook interface, the current plan is a per-record `AfterApply(rec, change)` callback that runs inside the same tx and is allowed to write to `_h.*`. Re-indexing (running `AfterApply` over existing records when handler logic changes) is a separate question for that design round.

12. **Duplicate `RecordChange.Id` in one batch — allowed, applied sequentially.** A batch's `Records` slice has strict internal order; two `RecordChange` entries targeting the same id apply one after the other, with the second seeing the first's effects. This is intentional: it matches how generated code, split validation phases, and merged transport batches produce payloads. Two empty-id records in the same batch are not duplicates — they auto-suffix to distinct ids per §3.3.
