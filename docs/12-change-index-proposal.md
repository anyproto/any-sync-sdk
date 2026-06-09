# Change Index — AddSeq surface for FTS / vector indexers

Branch: `feat/addseq-change-index`

**Status: implemented.** Public surface `Space.Changes()` (`space.ChangeIndexAPI`).
Per-record `_addSeq` stamping + per-space `_meta` scoping in `internal/crdt`;
live feed + query in `internal/spaceobjects`; wiring in `internal/spaceimpl`.
Covered by unit tests (`internal/crdt`, `internal/spaceobjects`) and an
end-to-end test (`e2e/change_index_test.go`).

## Goal

Let an external FTS + vector-search indexer track *what changed* in a
space and re-index incrementally. **The indexer lives entirely on the
consumer side** — the SDK ships no FTS/vector logic, owns no embedding
or search index, and stores no cursor. Its job is to expose a small,
robust surface (a live callback, a "changed since N" query, and
`_addSeq` visibility) that a consumer's indexer drives. The consumer
persists its own cursor and decides when/how to re-index.

Three capabilities, matching the original proposal:

1. **Persist `_addSeq` on every record** (per-object datasets *and* the
   per-space `objects` property rows), indexed.
2. **Public read surface** — query object ids with `_addSeq > cursor`,
   and read `_addSeq` back on property records.
3. **Lightweight live callback** — on every applied change, hand the
   indexer `(objectId, addSeq)` so it can mark the object dirty without
   polling.

## What AddSeq is (grounding)

- `AddSeq` is any-sync's **per-space monotonic delivery counter**
  (`docs/04-object.md`: "per-space monotonic counter for incremental
  sync"). any-sync stamps it on `StorageChange.AddSeq` for every change
  — local writes (`object.go:415`, from `AddContent` result) and inbound
  replays alike. It already rides on `crdt.Change.AddSeq`.
- It is **peer-local** (do not compare across peers) and **per-space**
  (each space has its own sequence). A single cursor `addSeq > N` is
  therefore meaningful *within one space*, which is exactly the
  indexer's unit of work.
- Per object, the max addSeq is **already persisted atomically** in the
  shared `_meta` collection: `PersistMeta` writes `{q: maxAddSeq, hv: …}`
  keyed by objectId inside the same `WriteTx` as the record mutations
  (`controller.go:626`, `meta.go:57`). `Controller.maxAddSeq` is
  per-object across **all** datasets (any `ApplyChange` bumps it), so
  `_meta.q` is the authoritative "this object last changed at addSeq N"
  signal — including pure base-content changes that never touch a
  property value.

The two existing watermark scopes:
- per-object row keyed by `objectId` → `q` = object's max addSeq.
- per-space row keyed by `space:<spaceId>` → coarse catch-up hint.

## Design decisions

### D1 — "object changed" = any dataset, sourced from `_meta`

An FTS/vector indexer needs to re-index when *any* indexable content
changes — block text in the base dataset, not just the `name` property.
The per-space `objects` collection only moves on property-value writes,
so it is **not** a sufficient dirty signal. `_meta.q` (max across all
datasets) is. The cross-object "changed since" query is therefore backed
by `_meta`, not by `objects`.

### D2 — `_meta` is space-scoped by adding `spaceId`, because the SDK DB is shared by default

`Config.Storage.Topology` defaults to **Shared** — one `sdk.db` for all
spaces (`README.md` storage layout, `config/config.go`). So `_meta`
holds rows for objects from *every* space, and addSeq numbering differs
per space. A raw `_meta WHERE q > N` would mix spaces and compare
incomparable counters.

Fix: stamp `sp: <spaceId>` onto each per-object `_meta` row in
`PersistMeta`, add an index on `(sp, q)`, and scope every public query
by `sp == thisSpaceId`. The `space:<id>` watermark rows have no `sp`
field and are naturally excluded. Old rows written before this change
lack `sp`; they get backfilled lazily on the object's next change (and a
one-shot backfill pass can be added if a populated DB must be migrated —
see Open Questions).

> Threading spaceId: `Controller` currently knows only `objectId`. Add a
> `spaceId` field set at construction. `object.Object` already holds
> `o.spaceId`; the Store constructs the controllers, so the value is in
> hand. Blast radius is the `NewController` / `NewControllerWithShared`
> call sites — see Impact below.

### D3 — `_addSeq` stamped in the CRDT apply path, kept out of subscription events

Stamp `_addSeq` directly in `recordModifier.Modify` (and the tombstone /
sibling paths), **not** via the handler `sink.derived` mechanism. That
keeps it off the `derivedOps` wire projection, so it never shows up as a
spurious `$set` in `Query.Subscribe` events. It is storage metadata,
surfaced only through explicit read opts and the change-index query.

`_addSeq` advances **monotonically** on the record: stamp
`max(existing, ch.AddSeq)`. An out-of-order replay (drain of a parked
change, cold-restore re-apply) can carry a lower addSeq than what the
record already holds; the CRDT still applies it idempotently but the
record's `_addSeq` must not regress.

`_addSeq` is reserved-by-prefix (`_*` names are already rejected for
user writes by `validateOpPaths`, per `reserved_fields_and_paths`), so
no new validation is needed to protect it — only a named constant.

### D4 — naming: avoid `space.Indexer`

`space.Indexer` already names the tech-space space-index seam. The new
surface is called **ChangeIndex** (`Space.Changes() ChangeIndexAPI`) to
avoid collision.

## Implementation plan

### Part 1 — stamp `_addSeq` on records (`internal/crdt`)

1. `controller.go`: add `const AddSeqField = "_addSeq"` next to the
   other reserved fields; add `spaceId string` to `Controller` and a
   constructor param/field (D2).
2. `apply.go`: add `stampAddSeq(a, rec, ch.AddSeq)` helper that sets
   `_addSeq = max(existing, addSeq)` on the record **root** (not the
   variant subdoc). Call it from:
   - `recordModifier.Modify` — create, modify, and the upsert-on-tombstone
     branches (every path that returns `(rec, true, nil)`).
   - `newTombstone` — carry the latest addSeq onto the tombstone so a
     delete is itself a "change since N".
   - `applySibling` — siblings are real records on other collections.
3. Built-in index: ensure an ascending `_addSeq` index on **every**
   collection the controller opens. Add an `ensureBuiltinIndexes` call
   alongside `ensureHandlerIndexes` in `collectionForWrite`,
   `collectionForRead`, and the shared-collection branch of
   `registerHandler`. (One `IndexInfo{Fields: ["_addSeq"]}`.)

### Part 2 — `_meta` space scoping + change-index query (`internal/crdt`)

4. `meta.go`: add `const metaSpaceIdKey = "sp"`. `PersistMeta` gains a
   `spaceId` param and writes `sp` when non-empty.
   `Controller.PersistMeta` passes `c.spaceId`. Update the call site in
   `ApplyChangeWithResult` (`controller.go:627`).
5. `meta.go`: ensure index `(sp, q)` on `_meta` (idempotent
   `EnsureIndex` at first meta-collection open in
   `NewControllerWithShared`).
6. New `meta.go` query helper:
   ```go
   type ObjectSeq struct { ObjectId string; AddSeq uint64 }
   func QueryChangedObjects(ctx, coll, spaceId string, since uint64, limit int) ([]ObjectSeq, error)
   ```
   Filter `sp == spaceId && q > since`, sort `q ASC`, optional limit for
   cursor paging. Excludes `space:` rows (no `sp`).
   Plus `func MaxObjectAddSeq(ctx, coll, spaceId) (uint64, error)` for
   the current cursor upper bound (or reuse the per-space watermark).

### Part 3 — live callback registry (`internal/spaceobjects`)

7. Add a `changeRegistry` to `Store` reusing the
   `syncstatus/subscribers.go` registry shape (a small generic
   add/remove/dispatch with `hasSubscribers`). Event type:
   `ObjectChange{ObjectId string; AddSeq uint64}`.
8. `gate.go afterApplyFor`: after the existing engine/​drainer fan-outs,
   if `changeRegistry.hasSubscribers()`, dispatch
   `ObjectChange{ch.ObjectId, ch.AddSeq}`. Gated by `hasSubscribers` so
   the cold-restore / catch-up path stays free when nobody listens
   (mirrors the `engine.HasSubscribers()` guard already there).
   - Keep `cb` cheap / hand-off documented (runs under the apply path);
     same contract as the syncstatus firehose.
9. Store methods: `SubscribeChanges(cb) (cancel func())`,
   `ChangedObjects(ctx, since, limit)`, `MaxAddSeq(ctx)` — the last two
   delegate to the Part 2 helpers against the Store's `_meta` collection
   and `s.spaceId`.

### Part 4 — public surface (`space` + `internal/spaceimpl`)

10. `space/changeindex.go`: new interface
    ```go
    type ObjectChange struct { ObjectId string; AddSeq uint64 }
    type ChangeIndexAPI interface {
        // MaxAddSeq is the current space-wide cursor upper bound.
        MaxAddSeq(ctx context.Context) (uint64, error)
        // ChangedSince returns objects with addSeq > since, ascending,
        // capped at limit (0 = no cap). Page by passing the last
        // returned AddSeq as the next `since`.
        ChangedSince(ctx context.Context, since uint64, limit int) ([]ObjectChange, error)
        // Subscribe fires cb on every applied change in this space.
        // cb runs synchronously on the apply path — keep it small or
        // hand off. Returns an idempotent cancel.
        Subscribe(cb func(ObjectChange)) (cancel func())
    }
    ```
11. `space/space.go`: add `Changes() ChangeIndexAPI` to the `Space`
    interface.
12. `internal/spaceimpl/changeindex.go`: `changeIndexAPI{parent}`
    delegating to the Store; wire `newChangeIndexAPI(s)` into
    `newSpace`; add `func (s *spaceImpl) Changes() …`.
13. `space/properties.go`: surface `_addSeq` on `PropertiesAPI.Get`
    output. Either always include it, or fold it under
    `PropertyReadOpts.IncludeMeta` next to `_ver` / `_traces`. **Recommend
    always-include** — it is a single cheap number an indexer always
    wants. (Decide during review.)

### Part 5 — tests + docs

14. crdt: `_addSeq` stamped on create/modify/delete/sibling; monotonic
    (lower replay does not regress); index ensured.
15. meta: `QueryChangedObjects` filters by space, excludes `space:`
    rows, paginates by `q`; old rows without `sp` are skipped until
    backfilled.
16. spaceobjects: callback fires once per applied change with the right
    `(objectId, addSeq)`; no fire when no subscribers; cancel works.
17. spaceimpl/e2e: two objects, write to each, `ChangedSince(0)` returns
    both ordered; `ChangedSince(firstSeq)` returns only the later;
    `Subscribe` observes live writes; `Properties().Get` shows `_addSeq`.
18. Update `docs/05-crdt.md` (AddSeq section) and `README.md` status.

## Impact / blast radius

- `NewController` / `NewControllerWithShared` signature gains `spaceId`
  (or a `SetSpaceId` setter to avoid churn). Run `sverklo_impact` before
  editing — call sites are the Store and crdt tests.
- `PersistMeta` signature gains `spaceId`. Callers: `controller.go`,
  `Controller.PersistMeta`, and `PersistSpaceMaxAddSeq` is unaffected
  (separate fn). `spacesync` reads via `LoadSpaceMaxAddSeq` — unchanged.
- `Space` interface gains one method — every `Space` implementation /
  mock must add `Changes()`. Only `spaceImpl` implements it in-tree.
- apply hot path: one extra `_addSeq` set + one `_meta` field per change
  (the `_meta` upsert already happens). Negligible.

## Open questions

1. **Backfill of pre-existing `_meta` rows** (missing `sp`). For a fresh
   DB, non-issue. For an existing DB, add a one-shot pass that reads each
   object's spaceId (from the Store's known object set) and stamps `sp`,
   or accept lazy backfill on next change. Which do we need for v1?
2. **`_addSeq` on `PropertiesAPI.Get`** — always-include vs gated by
   `IncludeMeta` (Part 4.13).
3. **Cursor semantics across restart** — the consumer-side indexer owns
   the cursor. It persists its last-seen `addSeq` and resumes with
   `ChangedSince(lastSeq)`, treating `_addSeq` as the only ordering key.
   `ChangedSince(0)` after a cold start returns everything the catch-up
   pass has replayed into `_meta`. The SDK stores nothing on the
   indexer's behalf. The live `Subscribe` callback is best-effort
   notification only — a consumer that drops events (crash, slow cb)
   recovers by re-running `ChangedSince(itsLastCursor)`; it must not
   assume the callback is a durable queue.
4. **PerSpace topology** — with `Topology=PerSpace`, `_meta` is already
   space-isolated and `sp` is redundant-but-harmless. No special-casing
   needed; the `sp` filter still holds.
