# Version history — SDK implementation plan

Source: `docs/version-history-proposal.md` on branch
`worktree-version-history-research` (not on main yet). Build order follows
§12 of the proposal.

## 0. Port research artifacts

- Cherry-pick / copy from `worktree-version-history-research`:
  - `docs/version-history-proposal.md`
  - `internal/crdt/history_replay_bench_test.go` (benchmarks +
    `TestFilteredReplayMatchesFullReplay`)
- The bench test was written alongside branch-only crdt changes
  (`internal/crdt/handler.go` etc.) — check it compiles against main and
  trim/adapt if it depends on unrelated branch work (read-tracking, etc.).

## 1. Diff engine (no dependencies)

- New package (e.g. `internal/history/diff`): pure structural differ over
  anyenc using `Object.Visit` + `anyencutil.Equal` (~150 lines).
- Types: `FieldDiff{Path, Before, After}`, `RecordDiff{Id, Kind, Fields}`,
  `DatasetDiff`, `DiffResult` (§5).
- Exclude `_ver`, `_traces`, `_applySeq`, `_addSeq` from comparison.
- Record diff → dataset diff (merge-join two stores in id order) → object
  diff (union of datasets). Unit tests.

## 2. Engine A — on-demand causal replay (`ViewAt`, `Diff`)

- Wire `objecttree.BuildNonVerifiableHistoryTree({Heads:[X],
  IncludeBeforeId:true})` — first SDK use of the history-tree builder.
- Replay: in-memory any-store (`Config.InMemory`) + fresh
  `crdt.Controller` with the object's current handler set; ONE outer
  `WriteTx`; iterate tree → decode via object Codec → `ApplyChange`;
  commit once. Reuses cold-restore internals.
- `ViewAt(objectId, version)` returns the standard read-only query
  surface bound to the scratch store; caller `Close()`s.
- Dataset scope = filter changes by payload dataset.
- Contracts: strip `_applySeq` from returned records; **synced scope
  only** (no local/account values in historical views); derived fields
  recomputed by current handlers.
- Per-change *effect* diff: `Diff(base=PrevIds-state, version=C)`; ancestor
  optimization — one replay, snapshot touched records, continue to
  `version`, diff before/after.
- Guardrail: cap scratch size → `ErrViewTooLarge`.

## 3. Filtered-replay fast path (`RecordAt`)

- Fetch record-touching changes from the index (§4.4), intersect with
  ancestor set (metadata-only `BuildEmptyData` walk, cached per
  view/pagination cursor), replay onto a scratch value.
- Returns one record value, not a store (API shape enforces record scope).
- Promote `TestFilteredReplayMatchesFullReplay` into a fuzz across
  delivery orders and branch-heavy DAGs (required before shipping, §4.2).
- Add the handler invariant: apply hooks must not read other records;
  escape hatch `DisableFilteredReplay` on dataset registration → slow path.

## 4. Engine B — persistent history index (`ListChanges`)

- Collections in the space DB, space-scoped with `sp` like `_meta`:
  - `_history` {id: changeId, sp, obj, o, ds, author, ts, traces, n};
    indexes `(sp, obj, o)`, `(sp, o)`.
  - `_history_recs` {id: changeId+"/"+recId, sp, obj, ds, rec, o};
    index `(sp, obj, ds, rec, o)`.
  - `_history_traces` {id: changeId+"/"+traceId, sp, tr, o, obj};
    index `(sp, tr, o)`.
- Warm path: rows appended in the SAME WriteTx as the projection apply.
- Cold restore/re-index: skip index, set `historyIndexStale` in `_meta`;
  lazy backfill on first history query (~1000-change batches, progress
  callback / `ErrHistoryIndexBuilding`). No background workers.
- `SkipHistory bool` on dataset registration (chatty machine datasets).
- OrderId staleness: persist rebuild generation in `_meta`; on mismatch
  re-stamp `o` lazily via metadata-only walk (ChangeId is the join key).
- `ListChanges(objectId, filter, limit, cursor)`: descending OrderId,
  opaque cursor (orderId underneath), filters Dataset/RecordId/TraceId/
  Author.

## 5. Traces + coalescing

- Trace queries ride `_history_traces`; "diff of trace T" = per-change
  effect diffs (no aggregate state-diff).
- Coalescing at list time: linear chain + same author + time window
  (~5 min default); never across merges; group handle = head ChangeId;
  `GroupSize` in `ChangeMeta`.

## 6. Public surface

- `Space.History() HistoryAPI` with `ListChanges` / `ViewAt` / `RecordAt`
  / `Diff` per §7; `Version = ChangeId` (never expose OrderId).
- Errors: `ErrHistoryTruncated` (+ `Truncated` flag), `ErrViewTooLarge`,
  `ErrHistoryIndexBuilding`.
- Deleted objects: no history (tree dropped). Unknown datasets: listed
  (index rows exist) but absent from `ViewAt` projections.

## Deferred (explicitly out of v1)

- Persisted view cache + proactive anchors (§4.3) — add later behind the
  same `ViewAt` signature.
- Server-side (unencrypted) trace ids on the wire.
- Any per-change GC / snapshot horizon work (only the `Truncated`
  contract is in scope).

## Dependencies on other repos

- any-sync: none. any-store: none. (§11)
- `../any-history` worktree consumes this branch via
  `go mod edit -replace github.com/anyproto/any-sync-sdk=../any-sync-sdk-history`.
