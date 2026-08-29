# Version History — research & proposal

Status: research write-up, not groomed. Companion artifacts:
`internal/crdt/history_replay_bench_test.go` (feasibility benchmarks +
the filtered-replay soundness test referenced throughout).

## 1. Requirements

1. **See changes** — list the history of an object / dataset / record:
   who changed what, when, with op-level detail.
2. **Object at a past state** — view an object as it was at version X.
3. **Dataset / record at a past state** — same, scoped down.
4. **Diffs** — object-, dataset-, and record-level, between two
   versions, and "what did change C do".
5. **Traces** — changes and diffs filtered by caller-supplied trace ids
   ("everything AI-operation-X did", "diff of what session Y changed").

## 2. Raw material (what already exists)

The design leans on the fact that **any-store state is a pure,
order-tolerant function of the change set**: replaying any
causally-closed subset of changes through the CRDT converges to the
same logical state regardless of order (`docs/05a-crdt-spec.md` §4, §7).

| Building block | Where | What it gives us |
|---|---|---|
| Full change log, kept forever | any-sync `objecttree/storage.go` — `changes` collection; no per-change GC (only whole-tree `Delete`); in-memory tree reduction never touches storage | Every change is locally replayable: raw signed bytes, `Id` (content-hash CID, cross-peer stable), `PrevIds`, `SnapshotId`, `OrderId` (peer-local lexid, linear extension of the DAG), `AddSeq`, `Timestamp`, author identity |
| Ordered scans | `Storage.GetAfterOrder(orderId, iter)` (forward-only, lexid order), `Get(id)` | Change-feed reads without building a tree |
| **History tree builder** | `objecttree.BuildHistoryTree` / `BuildNonVerifiableHistoryTree` with `HistoryTreeParams{Heads, IncludeBeforeId, BuildEmptyData}` | Reconstructs the read-only tree "as of heads H" (exact causal past — back-walks from H and drops non-ancestors) or "just before change X". `BuildEmptyData` loads DAG structure without payloads/decryption. Tested (`objecttree_test.go:1561-1731`). **Not yet used by the SDK** |
| Historical decryptability | `readKeysFromAclState` derives **all** epochs' read keys | Old changes stay decryptable across key rotations for members with read access |
| Deterministic replayer | `Object.replayLocked` → `Controller.ApplyChange`; idempotent, per-op salvage of historically-invalid ops | Point it at any change source and it materializes a projection |
| Scratch stores | any-store `Config.InMemory`; a context-carried `WriteTx` is reused by nested ops, so a whole replay commits once | Cheap throwaway projection targets; `Close()` teardown |
| Per-field provenance | `_ver` (last-writer versionId per field), `_ver.id` creation marker, `_traces` (GC'd to live versions) | "Who last touched field F" — but **no past values** anywhere in the projection |
| Change decoding | `internal/object/wire.go` Codec | Raw payload → `crdt.Change{Dataset, Records[{Id, Upsert, Ops}], TraceIds, DataVersion}` |
| Dirty feed | `Space.Changes()` ChangeIndexAPI | Object-level "changed since cursor" — no content, no history |

What does **not** exist: past values in the projection (LWW apply is
destructive), a diff engine (any-store has only boolean deep `Equal` —
`anyencutil.Equal`), any per-record/per-trace change index, reverse
iteration, and any SDK wiring of `BuildHistoryTree`.

## 3. Version model

### 3.1 The version handle is `ChangeId`

`OrderId`/versionId is peer-local **and unstable over time on one
device** (lexids are rebalanced on tree rebuilds). The content-hash
`ChangeId` is the only identifier that is stable across peers and
restarts, so it is the public version handle. `OrderId` is used
internally as the ordering key for listing and replay, never
serialized to callers as an identity.

```go
type Version = string // ChangeId (CID) — cross-peer stable
```

### 3.2 Cut semantics: causal past, uniformly

"State at version X" = the projection of **exactly X's ancestors**
(X's causal past, inclusive) — what `BuildHistoryTree(Heads:[X],
IncludeBeforeId:true)` produces.

The rejected alternative — local-linearization prefix (`OrderId ≤
OrderId(X)`) — reflects what this device's screen showed at the time,
but:

- it differs across devices (different delivery orders), so a shared
  "look at version X" link shows different content per device;
- lexid rebalancing means it can differ **on the same device across
  time** — a history view that changes between two openings is a UX
  defect, not a feature.

Causal cuts are stable everywhere, match git intuition, and cost one
metadata-only DAG walk (`BuildEmptyData` — no decryption) to compute an
ancestor set. Consequence to accept: a historical view may include
concurrent changes the user hadn't seen at the time, or omit ones they
had. That is inherent to any cross-device-stable semantics.

The same cut applies at every scope (object / dataset / record) —
one semantics, no special cases.

## 4. Architecture: two cooperating engines

No MVCC, no per-record value logs, no inverse ops (see §10). The
feature is built from:

- **Engine A — on-demand causal replay** for *states and diffs*
  (requirements 2, 3, 4). Zero write-path cost; cost proportional to
  the causal past being viewed. Benchmarked in §8.
- **Engine B — persistent history index** for *listing and filtering*
  (requirements 1, 5). Tiny metadata rows written in the same tx as
  the apply on the warm path; skipped and lazily backfilled on the
  cold-restore path.

### 4.1 Engine A: on-demand replay into a scratch store

"View object O at version X":

1. `BuildNonVerifiableHistoryTree({Storage: O's tree storage, Heads:
   [X], IncludeBeforeId: true})` — gives the causal past, with
   decryption via the object's existing key machinery. Changes keep
   their live `OrderId`s, so `_ver` maps come out identical to what the
   live projection held when only those changes existed.
2. Open an in-memory any-store (`Config.InMemory`), construct a fresh
   `crdt.Controller` with the object's current handler set.
3. Open ONE outer `WriteTx`, iterate the history tree
   (`IterateRoot` → decode via the object Codec → `ApplyChange` with
   the tx-carrying ctx), commit once.
4. Wrap the scratch store in the existing read-only query surface
   (`Query(objectId, dataset)...Iter/All/One/Count` bound to the
   scratch DB) and hand it to the caller. `Close()` on release.

Dataset scope: identical, but skip changes whose payload dataset
differs (one change = exactly one dataset, so this is a clean filter).

Notes:

- Replay order doesn't matter for correctness (order-tolerant CRDT);
  topological iteration order is fine.
- Strip `_applySeq` from returned records: it is allocated per apply
  and depends on how many applies the scratch controller ran — a
  replay artifact, not state. (Proven necessary by the equivalence
  test.)
- Handler-derived fields are recomputed by the **current** handler
  set. If handler logic changed since the historical change was
  written, derived fields reflect today's logic — same contract as
  re-indexing (docs/08).

### 4.2 Engine A fast path: record-filtered replay

Per-record history at chat scale ("timeline of message M in a
1M-change object") cannot afford full-object replay per version. The
fast path: fetch from the index (§4.4) only the changes whose payload
touches record M, intersect with the ancestor set of the requested
cut, and replay just those onto a scratch value.

**Soundness.** CRDT gating is strictly record-local: each record's
`_ver` is compared only against ops targeting that record; sticky
tombstones and the `_ver.id` min-rule are per-record; version
comparison is plain string comparison, never a DAG lookup. Changes
that don't touch M cannot influence M's bytes.
`TestFilteredReplayMatchesFullReplay`
(`internal/crdt/history_replay_bench_test.go`) verifies byte-identical
records (including `_ver`) between full and filtered replay over a
randomized 20k-change workload with creates, mixed ops, and deletes.
The delivery-order / branch-shape fuzz
(`internal/crdt/history_filtered_fuzz_test.go`) extends this:
soundness (filtered subsequence of order D vs full replay of D) holds
under arbitrary permutations; cross-order convergence is asserted
across causally valid interleavings of concurrent branches. Two
convergence caveats the fuzz surfaced, asserted around rather than
hidden:

- **Tombstone `_ver` residue is delivery-order-dependent**: a sticky
  delete arriving before a higher-versioned concurrent edit keeps the
  edit's version in `_ver.*`; visible state (deleted) still converges.
  Harmless for history (diffs classify by `_deletedAt`), but historic
  views of tombstones may differ in `_ver` bytes across devices.
- **`$inc` concurrent with `$set` on the same field diverges by
  construction** (inc applies relative to whatever value was current
  at its apply time). Pre-existing projection-level property, not a
  history-engine defect — flagged for grooming (05a spec follow-up).

**Caveats that bound the fast path:**

- Handler hooks must not read *other records* during apply. Today they
  only stamp envelope-derived fields (author/createdAt), which is
  record-local. Make this an explicit invariant; any future handler
  that breaks it must flag its dataset `DisableFilteredReplay`, forcing
  the slow path.
- Apply paths that write sibling rows (e.g. shared `objects` dataset,
  property variants) produce correct state for the *filtered* record;
  siblings are simply absent from the scratch. Record-scope queries
  only — enforced by the API shape (the fast path returns one record,
  not a queryable store). The one exception to the record-local
  argument is the `objects` row itself: its `modifiedAt` is also
  stamped by changes on the object's other datasets
  (`crdt.ObjectStamper`), which neither the row's filtered subsequence
  nor a dataset-scoped slow-path view includes (`DisableFilteredReplay`
  still scopes the replay to the requested dataset). Such views carry
  the `modifiedAt` of the row's own last change; only a full-object
  view (no dataset scope) reproduces the object-level value. DiffRange
  accounts for it by counting shared rows as touched whenever the delta
  has a per-object-dataset change.

**Ancestor set amortization.** A record timeline UI walks versions
newest→oldest. Compute the ancestor set once per view (one
`BuildEmptyData` walk, no decryption), cache it against the pagination
cursor, and test membership per candidate change. Per-version cost is
then just the filtered replay (~1.6 ms measured for a 59-change
record; §8).

### 4.3 Persisted materializations as a view cache

Replaying into a **disk-backed** scratch store costs the same as
in-memory (§8: within noise on both machines — one batched commit +
checkpoint, sequential writes). That makes persisting a
materialization essentially free at build time, and it composes
unusually well with the causal-cut choice:

- **Cache entries are immutable.** `causal(ChangeId)` never changes,
  so a materialization keyed by `(objectId, version, projectionKey)`
  never needs invalidation — only eviction. `projectionKey` =
  DataVersion + registered handler versions, so a re-index naturally
  orphans stale entries instead of serving them.
- **Hits are ~5000× cheaper than cold replay** for a 100k-change
  object: open file + read ≈ 0.1–0.2 ms vs 0.6–1.2 s (§8). A user
  scrubbing back and forth between versions pays the replay once per
  version.
- **Incremental build.** To view Y when a cached X with
  `X ∈ causal(Y)` exists: copy X's file, replay only
  `causal(Y) \ causal(X)` on top (the projection rows carry their
  `_ver` maps, so resuming is exactly the cold-restore-from-watermark
  pattern). A timeline walk (v1, v2, v3…) becomes O(delta) per step
  instead of O(history).
- **It is only a cache.** One file per entry in a cache directory,
  deletable at will, always reconstructible from the DAG; LRU by size
  cap. No correctness obligations, no write-path coupling.

**Proactive anchors for big objects.** The demand-driven cache can be
extended with *anchor* entries placed deliberately along the history
axis of large objects (change count from `_meta` above a threshold):
materializations at roughly every K-th change, so that ANY future
history call — `ViewAt`, per-version record timelines, diffs — replays
from the nearest cached ancestor instead of from the root. Mechanics
are identical to cache entries (immutable, keyed by cut heads +
projection key, evictable); only the trigger differs:

- **Stepping stones, not background jobs**: seed anchors as a
  by-product of history work already running — the first `ViewAt` on a
  big object persists intermediate cuts it passes through anyway; the
  history-index backfill (§4.4), which already walks every change, can
  drop anchors at its batch boundaries for near-free. No idle workers
  (mobile battery, §4.4 rationale applies).
- **Reuse condition** is `anchorHeads ⊆ ancestors(Y)` — checked with
  the same metadata-only walk the record fast path already needs
  (§4.2), so a timeline walk touches the DAG structure once and then
  pays only O(delta) replays per step.

One trap to avoid: it is tempting to seed an anchor by simply copying
the **live projection** (it already materializes `causal(current
heads)`, no replay needed). But the live rows also carry non-DAG state
— `local`- and `account`-scope property values, `_applySeq` — that a
pure DAG replay would not produce, so a copied anchor would leak
"today's" local values into historical views. Either strip
non-`synced`-scope paths on copy (the schema knows each path's scope),
or build anchors by replay only. Related contract decision: `ViewAt`
presents the **synced** scope only — local/account-scope values have
no history in this object's DAG, so historical views exclude them
rather than showing misleading current values.

Note the naming collision to keep out of the API: these anchors are
purely **local projection checkpoints** — unrelated to any-sync tree
snapshots (`IsSnapshot` changes), which are wire-level, affect sync
and the history horizon (§9), and are a separate future decision.

This subsumes the "materialized checkpoints" idea (§10) in a
demand-driven form: entries appear because someone actually used
history on the object, not on a write-path schedule. v1 can ship
without any of it (pure in-memory views) and add the disk cache, then
anchors, behind the same `ViewAt` signature when needed.

### 4.4 Engine B: persistent history index

Change rows live in PER-OBJECT collections mirroring the
`<objectId>_<dataset>` projection layout, with **OrderId as the
primary key**: lexids are monotonic per tree, so inserts are
append-ordered (no random-CID btree splits), descending listings are
reverse PK scans, and an object's history is dropped by the same
`<objectId>_*` purge sweep that drops its dataset collections. The
double-underscore suffix keeps the namespace disjoint from datasets
("_"-prefixed dataset names are rejected at registration):

- `<objectId>__history` — one row per change, PK `id = orderId`:
  `{id: o, c: changeId, ds, author, ts, n: recordCount, prev: [..],
  traces: [..], recs: [{rec, k}], recIds: [..]}`. `recIds` is the
  flat multikey-indexed mirror of `recs` (SYN-88 — replaced the
  earlier separate `<objectId>__history_recs` collection): the
  record filter is plain query conditions on the change listing —
  `{"recIds": R}` multikey membership plus the usual ds/author keys —
  so one scan shape serves every object-scoped filter combination.
  Halves the per-object collection count and the per-change row
  writes. No migration (pre-release): DBs written under the old
  layout should be recreated.
- `_history_traces` — space-level (the trace query is space-wide by
  requirement), one row per (change, trace), PK
  `id = sp + SEP + traceId + SEP + o + SEP + objectId` (OrderIds are
  only unique per tree, hence the object tail — which also rides the
  pagination cursor): `{id, o, obj, c: changeId}`. SEP is a space
  (0x20) — the only printable byte sorting below the whole lexid
  alphabet (which starts at '!'), required because lexids of
  different lengths can be prefix-related and a separator sorting
  above any lexid char would invert bytewise key order vs OrderId
  order (pinned by TestListByTraceOrderWithPrefixRelatedOrderIds;
  earlier rows used NUL, which broke id-rendering tooling).
- `_history_meta` — space-level per-object index state:
  `{id: objectId, sp, stale}`.

**Write paths:**

- **Warm path** (live applies, local writes): append rows inside the
  same `WriteTx` as the projection apply. Measured cost is a few tiny
  upserts per change on top of N record upserts + the `_meta`
  watermark that already happen — noise next to decrypt/decode.
- **Cold restore / re-index** (millions of ops, the protected perf
  path): **skip the index entirely**; mark the object
  `historyIndexStale` in `_meta`. Optional optimization if backfill
  proves hot: cold restore already decrypts+decodes every change, so
  it can emit a cheap sidecar stream of `(changeId, orderId, dataset,
  recordIds, traceIds)` for the backfiller to consume without a second
  decryption pass.
- **Backfill**: lazy, on first history query touching a stale object —
  bounded batches (~1000 changes per tx) with a progress callback /
  `ErrHistoryIndexBuilding`. No background workers burning mobile
  battery for a feature never opened.

**Per-dataset opt-out.** Chatty machine-written datasets (presence-like
state) would leak disk through a permanent index for history nobody
asks for. Add `SkipHistory bool` to the dataset registration/schema;
skipped datasets write no index rows and are invisible in history
views (matches the user's mental model of "document history").
DAG changes still retain everything — flipping the flag later just
requires a backfill.

**OrderId staleness.** Index rows key on `orderId` (it is the primary
key). VERIFIED non-issue in current any-sync (v0.13): storage OrderIds
are write-once — `updateHeads` only assigns where `OrderId == ""`,
loads restamp from storage verbatim, and no renumber/rebalance path
exists for persisted changes. Should a future any-sync introduce
rebalancing, handle it like the schema gate handles generations:
persist the tree's rebuild generation, mark the object's index
order-stale on mismatch, and rebuild via the ordinary backfill
(ChangeIds are the stable join key; no decryption needed).

## 5. Diff engine

A small structural differ over anyenc (`Object.Visit` +
`anyencutil.Equal` — nothing exists today, ~150 lines):

```go
type FieldDiff struct { Path []string; Before, After *anyenc.Value } // nil = absent
type RecordDiff struct { Id string; Kind Added|Removed|Changed|Deleted; Fields []FieldDiff }
type DatasetDiff struct { Dataset string; Records []RecordDiff }
type DiffResult struct { Base, Version Version; Datasets []DatasetDiff }
```

- **Record diff**: deep-compare two reconstructed record values,
  emitting leaf-level `FieldDiff`s. `_ver`/`_traces`/`_applySeq`/`_addSeq`
  are excluded (peer-local bookkeeping, not content).
- **Dataset diff**: iterate both scratch stores' collections in id
  order (merge-join); classify added / removed / changed / tombstoned;
  per-record field diffs for changed ones.
- **Object diff**: dataset diffs across the union of datasets.
- **"What did change C do"** has two answers, both exposed:
  - *intent*: the decoded op list (`$set name=...`) — free, from the
    payload; this is also the `ListChanges` detail view;
  - *effect*: `Diff(base: C.PrevIds-state, version: C)` — what actually
    landed after gating (an op may have been gated out by a newer
    concurrent write).
- **Concurrent versions** (neither ancestor of the other): diff the two
  causal pasts directly (two-way). No three-way/merge-base UI — the
  CRDT already merged; the question "what differs between A and B" has
  a direct answer. (A "what did this author intend" view is the
  per-change diff above.)

Implementation note: diffing two full scratch stores costs two replays.
When `base` is an ancestor of `version` (the common "compare v5 to
v9"), one replay suffices: materialize `base`, snapshot the touched
records (the changes in between name every touched record id — decode
them first), continue applying to `version`, then diff only the touched
records' before/after copies.

## 6. Traces

- The record-level `_traces` map is **not** a history mechanism — it is
  GC'd down to versionIds still live in `_ver`. History-grade trace
  queries come from `_history_traces` (§4.4), populated at apply time
  from the decoded payload. E2EE means this local index is the *only*
  viable spot — server-side indexing would require moving traceIds out
  of the encrypted payload.
- Note: `docs/05a-crdt-spec.md` §3.6 envisions traceIds "on the wire in
  a dedicated non-encrypted field so any-sync can index them
  space-wide". That is **not implemented** (the codec puts `t:` inside
  the encrypted payload) and this proposal does not need it. Keep the
  wire change as a separately-decidable follow-up with an explicit
  privacy trade-off (trace tokens visible to nodes).
- Query surface: `ListChanges(filter{TraceId})` across an object (index
  scan on `(sp, tr, o)` gives space-wide too); "diff of what trace T
  did" = the per-change *effect* diffs of T's changes (aggregating a
  single state-diff across interleaved foreign changes is ill-defined;
  per-change diffs are exact and honest).

## 7. Public API sketch

Per-space surface, mirroring `Changes()`:

```go
Space.History() HistoryAPI

type HistoryAPI interface {
    // Requirement 1 + 5. Backed by the index; descending OrderId
    // (causal parents never appear after children; timestamps are
    // author-supplied and display-only). Pagination by opaque cursor
    // (orderId underneath). Triggers lazy backfill; may return
    // progress via opts callback.
    ListChanges(ctx, objectId string, f HistoryFilter, limit int, cursor string) (ChangeList, error)

    // Requirement 2 + 3 (object/dataset scope). Causal cut at version.
    // Returns the standard read-only query surface bound to an
    // in-memory scratch projection. Caller must Close.
    ViewAt(ctx, objectId string, version Version) (HistoricalView, error)

    // Requirement 3 (record scope), chat-scale fast path: filtered
    // replay ∩ ancestor set. One record value, not a store.
    RecordAt(ctx, objectId, dataset, recordId string, version Version) (*anyenc.Value, error)

    // Requirement 4. base=="" means version's PrevIds (per-change
    // effect diff). Scope narrows via filter (dataset / recordIds).
    Diff(ctx, objectId string, base, version Version, f DiffFilter) (DiffResult, error)
}

type HistoryFilter struct {
    Dataset  string   // only this dataset
    RecordId string   // only changes touching this record (needs Dataset)
    TraceId  string   // only changes carrying this trace
    Author   string   // identity filter
    Coalesce *CoalesceOpts // §7.1
}

type ChangeMeta struct {
    Version   Version   // ChangeId; for coalesced entries: head of group
    Author    string
    Timestamp int64     // author clock — display-only
    Dataset   string
    TraceIds  []string
    Touched   []TouchedRecord // dataset + recordId + op summary
    Truncated bool            // §9: history horizon reached
    GroupSize int             // 1 unless coalesced
}
```

### 7.1 Coalescing

Raw changes are keystroke-grained; UIs need human-scale versions. The
SDK groups (middleware can't — it would have to page through thousands
of raw changes to build one screen): consecutive changes collapse into
one entry iff they form a **linear chain** (each the sole parent of the
next), share the **same author**, and fall within a **time window**
(default ~5 min). Never group across merges/branches — concurrent work
stays visibly separate. The group's handle is its head ChangeId, which
composes perfectly with causal cuts (viewing the handle includes the
whole group). Grouping is computed at list time from `PrevIds` +
index rows; nothing extra is stored.

## 8. Feasibility numbers

`internal/crdt/history_replay_bench_test.go`, two workload profiles
bracketing the real shapes:

- **editor** — 1 create per ~100 changes (1k records under 100k
  changes), 99% single-field `$set`/`$inc` edits: few records edited
  many times; replay cost is LWW gating on existing rows.
- **chat** — ~75% creates (every message a new ~120-char-text record),
  25% recent-skewed edits/reactions: ~0.75 records per change; replay
  cost is row inserts and scratch-store growth.

Measures Controller apply only — decrypt (AES) + anyenc decode add on
top, but cold-restore experience puts them in the same order of
magnitude, not 10×. Two machines: desktop (Ryzen 9 9950X, NVMe) and
laptop (ThinkPad, Ryzen 7 PRO 5850U) as the representative low end.

| Scenario | Desktop | Laptop |
|---|---|---|
| editor: full replay, one tx per change | ~45k changes/s (100k → 2.2 s) | ~23k changes/s (100k → 4.4 s) |
| editor: full replay, **one outer tx**, in-memory | ~165–226k changes/s (1k → 4.4 ms, 100k → 0.6 s) | ~84–115k changes/s (1k → 8.9 ms, 100k → 1.2 s) |
| **chat**: full replay, one outer tx, in-memory | ~114k changes/s (100k → 0.88 s) | ~62k changes/s (100k → 1.6 s) |
| editor, **disk-backed** scratch (commit + checkpoint) | ≈ in-memory (within noise) | ≈ in-memory (~3% slower) |
| Single record filtered (59 of 100k changes) | **1.6 ms** | **4.2 ms** |
| Cache hit: open persisted view + read record | 0.10 ms | 0.22 ms |

The chat profile is ~45% slower per change (insert-bound) and its
scratch projection is proportionally larger (~75k records per 100k
changes) — the memory guardrail in §9 and the record-scope fast path
matter most exactly there. A million-change chat extrapolates to
~9 s / ~16 s full-object replay — reinforcing that chat history UX
should ride `RecordAt`/timelines (milliseconds) and anchors (§4.3),
not full-object `ViewAt`.

Read: on-demand `ViewAt` is interactive (≪100 ms) for objects up to
~10k changes even on the laptop, and acceptable (~1.2 s + decode
overhead) at 100k. Persisting the materialization is free at build
time and turns repeat views into sub-millisecond opens (§4.3).
Million-change objects (chats) are exactly the ones where users want
*record* history, and the filtered path is ~1000× cheaper there.
Checkpointing (§10) stays deferred until telemetry shows p95 objects
where full-object `ViewAt` actually hurts.

## 9. Failure modes & constraints

- **History horizon.** Today the SDK never writes tree snapshots, so
  full history is always local. If snapshots later optimize sync, a
  fresh device may lack pre-snapshot changes: `BuildHistoryTree` hits a
  missing `PrevId` → surface `ErrHistoryTruncated` / `Truncated: true`
  on the oldest listable entry ("earlier changes not available on this
  device"). Any future per-change GC design must route through the
  same contract. Version history is a **best-effort-depth** feature by
  contract, never a durability promise.
- **Clock skew.** Timestamps are author-supplied. List order is
  OrderId (causal-consistent); timestamps are labels. UIs must expect
  non-monotonic dates.
- **Deleted objects.** Whole-object delete drops the tree — no history
  for deleted objects (consistent with cascade-delete semantics).
- **Memory.** `ViewAt` holds the reconstructed projection in RAM.
  Fine for documents; for million-record datasets the record/dataset
  scopes are the intended tools. Guardrail: cap scratch size, return
  `ErrViewTooLarge` and require narrower scope.
- **ACL edge.** Members can decrypt all epochs they have read access
  to; a member removed and re-added, or datasets under future scoped
  keys, may have gaps → same `Truncated` contract per change range.
- **Unknown datasets.** Changes for unregistered handlers are parked
  (schema gate) and invisible in the projection; history views built
  with the current handler set skip them the same way. Index rows are
  still written (dataset name is known), so history *listing* shows
  them; `ViewAt` reflects what the projection would show.

## 10. Alternatives considered and rejected

- **Per-record value logs / inverse ops at apply time** — instant
  record history, but permanent ~2× value-storage amplification and a
  tax on cold restore (the protected perf path); redundant with the
  DAG, which already stores everything. Rejected; the filtered-replay
  fast path serves the same need at read time.
- **Local-prefix cut semantics** — unstable across devices *and*
  across rebuilds on one device (lexid rebalance). Rejected (§3.2).
- **Write-path materialized checkpoints** (persisted every-K-changes
  copies maintained on apply) — superseded by the demand-driven view
  cache (§4.3): same replay-bounding effect, zero write-path cost,
  entries only for versions someone actually viewed, plain eviction
  instead of a maintenance protocol.
- **Server-side trace index (unencrypted traceIds on the wire)** — not
  needed for any listed requirement; privacy cost (correlation tokens
  visible to nodes). Deferred as an explicit product decision.
- **MVCC / time-travel in any-store** — engine keeps only current
  state + WAL snapshots of "now"; retrofitting MVCC is a storage-engine
  project with permanent write-amplification, duplicating what the DAG
  already provides. Rejected.
- **Raw-changes-only API (no index)** — listing filtered by
  record/trace would decrypt-and-scan entire trees per query.
  Rejected; the index is metadata-only and tiny.

## 11. Changes needed in any-sync / any-store

- **any-sync: none required.** `BuildHistoryTree` (+
  `BuildEmptyData`), `Storage.Get/GetAfterOrder`, and full-epoch key
  derivation cover everything. Nice-to-haves, not blockers: a
  descending `GetAfterOrder` twin (we page via the SDK index instead);
  exposing a rebuild-generation signal for §4.4's OrderId re-stamp
  (can be derived by comparing stored vs live OrderIds lazily).
- **any-store: none.** InMemory mode, context-tx composition, and the
  typed query package already suffice. (A structural diff util could
  live in `anyenc/anyencutil` eventually, but it is SDK-shaped for now
  because of the `_ver`-aware exclusions.)
- **Wire codec (SDK-internal, compatible):** nothing — traceIds stay
  in the encrypted payload; the index is built after decode.

## 12. Suggested v1 scope & build order

1. **Diff engine** (pure functions over anyenc) + unit tests — no
   dependencies, everything else consumes it.
2. **Engine A**: history-tree → scratch-controller replay; `ViewAt`
   (object/dataset), `Diff`. Reuses cold-restore internals.
3. **Filtered-replay fast path** + promotion of
   `TestFilteredReplayMatchesFullReplay` into a delivery-order fuzz;
   `RecordAt`.
4. **Engine B index**: warm-path rows, `SkipHistory` opt-out,
   stale-flag + lazy backfill; `ListChanges` with filters and cursor
   pagination.
5. **Traces + coalescing** on top of the index.
6. Defer: the persisted view cache (§4.3 — add behind the same
   `ViewAt` signature when repeat-view telemetry warrants), server-side
   traces, any per-change GC story.
