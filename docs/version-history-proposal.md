# Version History

`Space.History()` lists an object's changes, reconstructs objects,
datasets and records as of a past version, and diffs versions. Public
surface: `space/history.go`; engine: `internal/history`.

## 1. Requirements

1. **Change list** — who changed what and when, per object, filterable
   by dataset, record, trace id and author.
2. **Object / dataset at a past version.**
3. **Record at a past version**, cheap enough for chat-scale objects.
4. **Diffs** between two versions, and the effect of a single change.
5. **Traces** — changes filtered by caller-supplied trace ids.

## 2. Building blocks

State in any-store is a pure, order-tolerant function of the change
set: replaying any causally-closed subset of changes through the CRDT
yields the same logical state regardless of order
(`docs/crdt-spec.md` §4, §7). History is built on that plus:

| Building block | Provides |
|---|---|
| any-sync tree storage | Every change kept locally: signed bytes, `Id` (content-hash CID, stable across peers), `PrevIds`, `OrderId` (peer-local lexid), `AddSeq`, `Timestamp`, author. No per-change GC. |
| `objecttree.BuildNonVerifiableHistoryTree` | Read-only tree for the exact causal past of given heads (`IncludeBeforeId: true`), decrypted with the object's keys. |
| ACL read keys for all epochs | Old changes stay decryptable across key rotations for members with read access. |
| `crdt.Controller.ApplyChange` | Deterministic, idempotent replay into any store. |
| any-store `Config.InMemory` + context-carried `WriteTx` | Throwaway scratch projections committed in one transaction. |

The live projection holds no past values: LWW apply is destructive.
Everything historical comes from replaying the DAG.

## 3. Version model

### 3.1 The version handle is `ChangeId`

```go
type Version = string // ChangeId (CID)
```

`OrderId` is peer-local; the content-hash `ChangeId` is the only
identifier stable across peers and restarts, so it is the public
handle. `OrderId` is used internally as the ordering key for listing
and replay and is never exposed as an identity.

### 3.2 Cut semantics: causal past

"State at version X" is the projection of exactly X's causal past,
inclusive. The same cut applies at every scope (object, dataset,
record).

A causal cut is identical on every device, so a shared "version X"
link shows the same content everywhere. The cost: a historical view
may include concurrent changes the user had not seen at that moment,
or omit ones they had. A local-delivery-order prefix (`OrderId ≤
OrderId(X)`) would match what one screen showed but differs across
devices.

## 4. Architecture

Two engines, no MVCC, no per-record value logs, no inverse ops:

- **Engine A — on-demand causal replay** serves states and diffs
  (requirements 2–4). No write-path cost; cost is proportional to the
  causal past being viewed.
- **Engine B — persistent history index** serves listing and filtering
  (requirements 1, 5). Small metadata rows written in the apply
  transaction; skipped on cold restore and backfilled lazily.

### 4.1 Engine A: replay into a scratch store

`ViewAt(objectId, X)`:

1. Build the history tree for heads `[X]` under the live tree's lock.
   Changes keep their stored `OrderId`s, so `_ver` maps match what the
   live projection held when only those changes existed.
2. Open an in-memory any-store and a fresh `crdt.Controller` with the
   object's current handler set.
3. Iterate the tree, decode each change, `ApplyChange` inside one outer
   `WriteTx`, commit once.
4. Return a `HistoricalView` (`Record`, `Records`, `Datasets`) over the
   scratch store. The caller must `Close` it.

Rules:

- Replay order does not affect the result; topological iteration is
  used.
- The scratch controller has no apply-sequence allocator, so no
  `_applySeq` is stamped.
- Handler-derived fields are recomputed by the current handler set:
  if handler logic changed, derived fields reflect today's logic (same
  contract as re-indexing, docs/versioning.md).
- A view materializing more than `DefaultMaxViewRecords` (500k)
  distinct records aborts with `ErrViewTooLarge` (§9).

### 4.2 Engine A fast path: record-filtered replay

`RecordAt(objectId, dataset, recordId, X)` replays only the changes
that touch the record (from the index, §4.4) and lie in X's causal
past, onto a scratch value. It returns one record value, not a store.

**Soundness.** CRDT gating is record-local: a record's `_ver` is
compared only against ops targeting that record, sticky tombstones and
the `_ver.id` min-rule are per-record, and version comparison is a
string comparison, never a DAG lookup. Changes that do not touch record
M cannot influence M's bytes. `TestFilteredReplayMatchesFullReplay`
checks byte-identical records (including `_ver`) between full and
filtered replay; `internal/crdt/history_filtered_fuzz_test.go` checks
the same under arbitrary delivery permutations and branch-heavy DAGs.

Known cross-order differences, independent of history:

- **Tombstone `_ver` residue**: a sticky delete arriving before a
  higher-versioned concurrent edit keeps the edit's version in `_ver.*`.
  Visible state (deleted) converges; historical tombstones may differ
  in `_ver` bytes across devices. Diffs classify by `_deletedAt`, so
  they are unaffected.
- **`$inc` concurrent with `$set`** on the same field diverges by
  construction (docs/crdt-spec.md §5.5).

**Constraints on the fast path:**

- Apply hooks must be record-local (read no other records). A dataset
  whose handler reads other records during apply sets
  `DisableFilteredReplay`, which routes `RecordAt` through full replay.
- Apply paths that write sibling rows produce correct state for the
  filtered record only; siblings are absent from the scratch value.
- The `objects` row is the one exception to record locality: its
  `modifiedAt` / `modifiedBy` are also stamped by changes on the
  object's other datasets (`crdt.ObjectStamper`). A filtered or
  dataset-scoped view of that row carries the stamps of the row's own
  last change; only a full-object view reproduces the object-level
  values. `DiffRange` counts shared rows as touched whenever the delta
  contains a change on a per-object dataset.

### 4.3 Historical views are synced-scope only

`local`- and `account`-scope values have no history in the object's
DAG, so views exclude them rather than showing current values. This is
automatic for replayed views: those values never enter the DAG.

**Persisted view cache (not built).** Views are in-memory only. A
causal cut never changes, so a materialization keyed by `(objectId,
version, DataVersion + handler versions)` would never need
invalidation, only eviction, and a view at Y could start from a cached
X in Y's causal past and replay only the difference. Such a cache must
be built by replay, never by copying the live projection: live rows
carry `local`/`account` values and `_applySeq` that a DAG replay does
not produce. These checkpoints are local projections and unrelated to
any-sync tree snapshots.

### 4.4 Engine B: persistent history index

Collections:

- `<objectId>__history` — per object, one row per change, primary key
  `id = OrderId`:
  `{id, c: changeId, ds, author, ts, n: recordCount, prev: [...],
  traces: [...], recs: [{rec, k: [opKinds]}], recIds: [...]}`.
  `recIds` is a sparse multikey index mirroring `recs`, so "changes
  touching record R" is `{"recIds": R}` on the same scan that serves the
  dataset and author filters.
  - OrderIds are monotonic per tree, so inserts are append-ordered and
    descending listings are reverse primary-key scans.
  - The object's history is dropped by the same `<objectId>_*` purge
    that drops its dataset collections. The double underscore keeps the
    name disjoint from datasets (`_`-prefixed dataset names are rejected
    at registration).
- `_history_traces` — per space, one row per (change, trace id), primary
  key `spaceId SEP traceId SEP OrderId SEP objectId`, row
  `{id, o, obj, c}`. OrderIds are unique only per tree, hence the object
  tail, which also rides the pagination cursor. `SEP` is a space
  (0x20): the only printable byte below the lexid alphabet (which
  starts at `!`). Lexids of different lengths can be prefix-related, so
  a separator sorting above any lexid character would break the key
  order. Trace ids must not contain a space.
- `_history_meta` — per space, per-object index state
  `{id: objectId, sp, stale}`.

**Write paths:**

- **Warm path** (live applies and local writes that produce DAG
  changes): rows are written by the apply hook inside the apply
  `WriteTx`. Changes without a DAG identity (`Local`, `Injected`) and
  `SkipHistory` datasets write no rows. An index write failure never
  fails the apply; it marks the object stale.
- **Cold restore and resumed rebuilds**: rows are skipped (cold restore
  is the protected perf path) and the object is marked stale.
- **Backfill**: the first history query on a stale object, or on an
  object with no index state, walks the full tree and writes rows in
  batches of 1000 changes per transaction. It runs synchronously inside
  that query.

**`SkipHistory`** on a dataset registration keeps chatty
machine-written datasets (presence-like state) out of the index: no
rows, invisible in listings. The DAG still has every change, so
flipping the flag later only needs a backfill.

**OrderId stability.** any-sync assigns a stored change's `OrderId`
once and never rewrites it, so index rows stay valid. If any-sync ever
renumbered stored changes, the object's index would need marking stale
and backfilling; `ChangeId` remains the join key.

## 5. Diff engine

Structural diff over anyenc (`internal/history/diff.go`):

```go
type FieldDiff   struct { Path []string; Before, After *anyenc.Value } // nil = absent
type RecordDiff  struct { Id string; Kind DiffKind; Fields []FieldDiff } // added | removed | changed | deleted
type DatasetDiff struct { Dataset string; Records []RecordDiff }
type DiffResult  struct { Base, Version Version; Datasets []DatasetDiff }
```

- **Record diff**: deep compare, leaf-level `FieldDiff`s. `_ver`,
  `_traces`, `_applySeq` and `_addSeq` are excluded (peer-local
  bookkeeping). `deleted` means tombstoned at the newer version;
  `removed` means physically absent.
- **Dataset / object diff**: merge by record id across the union of
  datasets.
- **Effect of change C**: `Diff(base: "", version: C)` diffs C against
  its parents — what actually landed after gating, excluding ops gated
  out by newer concurrent writes. The listing's `Touched` entries give
  the op kinds per record.
- **Base is an ancestor of version** (the common case, and always for
  effect diffs): `DiffRange` does one replay and one tree walk. Base
  changes apply as they stream, the delta is buffered, the records it
  touches are snapshotted at base, then the delta applies and only
  those records are diffed.
- **Concurrent versions**: two views, two-way diff of the two causal
  pasts. No merge-base view; the CRDT already merged.

## 6. Traces

- The per-record `_traces` map is not a history mechanism: it is pruned
  to versionIds still live in `_ver`. History trace queries use
  `_history_traces`, filled at apply time from the decoded change.
- Trace ids travel inside the encrypted change payload, so nodes cannot
  index them and the local index is the only trace index.
- `ListChanges` with `TraceId` is object-scoped; the trace collection is
  space-level, so a space-wide listing needs no layout change. "What
  trace T did" is the per-change effect diffs of T's changes: a single
  state diff across interleaved foreign changes is ill-defined.

## 7. Public API

```go
Space.History() HistoryAPI

type HistoryAPI interface {
    // Newest first (descending OrderId: parents never after children).
    // Opaque cursor; "" = exhausted. Backfills a stale index first.
    ListChanges(ctx, objectId string, f HistoryFilter, limit int, cursor string) (ChangeList, error)

    // Object at version; synced scope only; caller must Close.
    ViewAt(ctx, objectId string, version Version) (HistoricalView, error)

    // One record at version via the fast path (§4.2). nil = absent;
    // a deleted record returns its tombstone (`_deletedAt` set).
    RecordAt(ctx, objectId, dataset, recordId string, version Version) (*anyenc.Value, error)

    // base == "" diffs version against its parents.
    Diff(ctx, objectId string, base, version Version, f DiffFilter) (DiffResult, error)
}

type HistoryFilter struct {
    Dataset  string
    RecordId string        // requires Dataset
    TraceId  string
    Author   string
    Coalesce *CoalesceOpts // nil = raw changes
}

type ChangeMeta struct {
    Version   Version // ChangeId; for groups, the newest member
    Author    string  // signer of this change
    Timestamp int64   // author clock, Unix seconds, display-only
    Dataset   string
    TraceIds  []string
    Touched   []TouchedRecord // dataset, recordId, op kinds
    Truncated bool            // reserved, see §9
    GroupSize int             // 1 unless coalesced
}
```

Errors: `ErrVersionNotFound`, `ErrHistoryTruncated`, `ErrViewTooLarge`.

### 7.1 Coalescing

Raw changes are keystroke-grained. With `Coalesce` set, consecutive
changes collapse into one entry when they form a linear chain (each
the sole parent of the next), share an author, and fall within
`Window` of each other (default 5 minutes). Groups never span merges
or branches. The group's handle is its newest `ChangeId`, so viewing it
includes the whole group. Grouping is computed at list time from the
index rows' `prev`; nothing extra is stored. The SDK does it because a
caller would otherwise page through thousands of raw changes to build
one screen.

## 8. Performance

Controller apply only, in-memory scratch, one outer transaction
(`internal/crdt/history_replay_bench_test.go`), on a Ryzen 7 PRO 5850U
laptop:

| Scenario | Time |
|---|---|
| Editor-shaped object (1k records, 100k changes), full replay | ~1.2 s |
| Chat-shaped object (~75k records, 100k changes), full replay | ~1.6 s |
| One record (59 of 100k changes), filtered replay | 4.2 ms |

Decryption and decode add a comparable amount. `ViewAt` is interactive
for objects up to ~10k changes and acceptable at 100k. Million-change
chats are the objects where users want record history, and `RecordAt`
serves those in milliseconds; full-object views there are what
`ErrViewTooLarge` bounds.

## 9. Failure modes and constraints

- **History horizon.** The SDK writes no tree snapshots, so full
  history is local. A missing change in the causal past (ACL gap, or a
  future snapshot/GC horizon) surfaces as `ErrHistoryTruncated` from
  `ViewAt` / `Diff`. `ChangeMeta.Truncated` is reserved for that horizon
  and always false today. Version history is best-effort depth, never a
  durability promise.
- **Clock skew.** Timestamps are author-supplied labels; list order is
  causal. UIs must expect non-monotonic dates.
- **Deleted objects** have no history: deletion drops the tree.
- **Memory.** Views live in RAM, bounded by `DefaultMaxViewRecords`
  distinct records; beyond it `ErrViewTooLarge` asks for a narrower
  scope.
- **ACL.** A member removed and re-added, or future scoped keys, can
  leave undecryptable ranges; they surface as `ErrHistoryTruncated`.
- **Unknown datasets.** Changes for unregistered handlers are parked by
  the schema gate and absent from the projection; replayed views skip
  them the same way. Index rows are still written, so listings show
  them.
