# Scoped Properties — scope on the declaration, not on the value

Status: **accepted** (2026-06-12). Supersedes the variant-bag model in
`05a-crdt-spec.md` §9 ("Property Variants") and `06-data-structure.md`
§"Storage — Proposal 2" / §"Conflict Resolution"; those sections are
rewritten as part of slice 1. Expert-reviewed (two consults; key
findings folded in below).

## Decision

**Scope is a fixed attribute of a declaration** — of a property
definition, or of a dataset schema field — never a per-value overlay.
One unified taxonomy everywhere (`schema.Scope`):

| scope | write route | version domain | syncs to |
|---|---|---|---|
| `synced` | object's own DAG change | object tree orderId | everyone with space access |
| `derived` | handler-only (sink.Derive) | triggering change's | (computed convergently) |
| `account` | carrier record in tech space → per-device mirror | tech tree orderId | same account's devices |
| `local` | `Object.LocalSet`, no DAG | local lexid allocator | this device only |

- A property declares `scope` at creation; default `synced`; pinned
  first-write-wins exactly like `kind` (schema-bearing field). Changing
  scope = define a new property = new propId. `derived` is reserved for
  built-ins (`id/author/spaceId/createdAt/modifiedAt/modifiedBy`, at
  row root).
- Dataset schema fields already declared `synced|derived|local`; they
  may now also declare `account` (e.g. a per-account `read` flag on
  `chat_messages`). Same enforcement, same transport.
- **No priority merge, no fallback stack, no `_base`/`_account`/
  `_device` record fields.** A value lives at its normal path
  (`record[{typeId}][{propId}]`, or the dataset field), written from
  exactly one route. Wanting both a shared and a personal "favorite"
  means two properties; clients compose if they care.

### Rejected: the variant-bag model (docs' "Proposal 2")

Per-record `_base`/`_account`/`_device` bags + computed root + priority
`device > account > base` was rejected because ~80% of its complexity
(row migration, computed-root derivation hooks, projection stripping,
shadowed-write event edge cases, three-domain `_ver` presentation) paid
for the override stack alone. The half-built machinery on main
(`RecordChange.Variant`, wire `x` key, `variantTarget`, event variant
prefixing, `ProjectionOpts.IncludeVariants`/`PropertyReadOpts`) is
**deleted** in slice 1.

This also supersedes 06's §"Not Expanding This Pattern to All CRDT
Data": that rejection targeted the override model (shadow-variant GC,
merge consistency). A scope-declared dataset field has no shadowing —
it's just a field with a different write route — so extending scopes to
dataset fields is now in scope and resolves real product needs
(read/unread, per-account pins, last-seen) without bespoke side objects.

## Why `_ver` stops being a problem

Domain safety comes from **path-disjointness**: a given path is only
ever written from its one declared scope's version domain, so versions
from different domains never gate against each other inside the single
per-record `_ver` tree. This generalizes a shipped precedent — the
tech-space `spaces` dataset already mixes synced fields and a local
field (`localStatus`) on the same records, one `_ver` map, mixed-domain
versionIds on one event stream.

Consequences:
- Record shape, query reads, `_ver.id` creation marker, sort/paging:
  **unchanged**.
- Subscribe frames: one versionId per frame (the domain of whatever
  change produced it — already true for local writes today). The
  documented client recipe `_ver.<op.path> = frame.versionId` holds
  verbatim. No shadowed writes, no derived root ops, no empty-frame
  suppression.
- `ModifyResult.VersionId` is meaningful per call because a write call
  is single-scope (below).

## Enforcement matrix (who rejects what, where)

Scope resolution = registry lookup by (typeId, propId) for properties;
dataset schema `ScopeOf(field)` for dataset fields.

- **Writer-side, strict** (keeps bad changes out of the DAG): the
  routing layer resolves every patch key's scope; `Set()` routes by it;
  kind+scope validated before any write. Local/account routes don't run
  dataset handlers, so writer-side validation is their primary guard —
  acceptable because those routes are SDK-internal APIs, not inbound
  network surface.
- **Apply-side, defensive per-op drop** (convergent guard against
  malicious/buggy peers): `SystemPropertiesHandler.BeforeCreate/
  BeforeModify` drop an inbound DAG op whose target propId is declared
  `account`/`local`/`derived` — same machinery and rejection surface as
  kind mismatch. `classifyFieldWrite` does the equivalent for declared
  dataset fields. The DataVersion gate already parks inbound changes
  whose schema hasn't synced, closing the unknown-definition window for
  the DAG route.
- **Mirror-side** (account route): the watcher resolves scope before
  applying; ops for **unknown propIds are skipped, not dropped
  permanently** — the state-based re-mirror retries them once the
  target space's definitions sync (expert "Hole A"). No parking
  machinery needed; the re-mirror IS the retry.
- **FWW scope conflicts don't exist**: propIds are
  `base58(xxh3-64(changeId))` — content-addressed, concurrent creates
  mint different ids. The only needed rule is the immutability pin
  (scope joins `kind` in `schemaBearingFields`).

## Write API

`PropertiesAPI` (replaces `SetBase`/`SetAccount`/`SetDevice`):

```go
Set(ctx, objectId, typeId string, patch map[string]any) (ModifyResult, error)
```

- Auto-routes: resolves each key's declared scope, issues the write on
  that scope's route.
- **Single-scope per call**: a patch spanning scopes is rejected with
  an error listing the split (expert pushback accepted — avoids
  unrollbackable partial writes across version domains; one call = one
  version domain = clean ModifyResult).
- `Set` on local-scoped props returns the locally-minted lexid as
  VersionId (uniform optimistic-UI recipe across scopes).
- `Get(ctx, objectId)` returns the row verbatim (no opts, nothing to
  strip). The membership writes — `SetType` /
  `AttachCollection` / `DetachCollection` — are synced-only.

## Account transport (slice 4 — landed 2026-06-12)

- Tech space hosts **one derived carrier object per target space**
  (seed: `builtin:accountValues/<spaceId>`, dataset `account_values`,
  package `internal/accountvalues`); bounded DAG, 1:1 space lifecycle,
  trivial GC on leave. The names cover internals only — values sit at
  their normal `{typeId}.{propId}` paths in carrier records AND target
  rows; no new user-visible namespace exists.
- One carrier record per **(objectId, dataset, recordId)**, record key
  `objectId:dataset:recordId` (colon never appears in CIDs) — the
  objects row is the degenerate case (`<objId>:objects:<objId>`).
  Record fields are the account-scoped paths; the record's own `_ver`
  (tech domain) is the version source, including retained entries for
  unset paths. v1 mirror coverage: objects rows only; dataset-field
  transport is a mechanical follow-up on the same key/Diff path.
- **Injected applies** (`Change.Injected` + `Object.InjectedSet`): no
  DAG, dataset handler skipped (like Local), but the versionId is
  caller-supplied (the tech tree's) instead of locally minted; events
  + applySeq flow through the one apply path. classifyFieldWrite
  permits the injected route on declared account fields and undeclared
  heads of DynamicScopeByKey datasets only. Injected applies always go
  through the object's CACHED controller (store.Get) — a second bare
  controller would persist stale in-memory maxApplySeq/maxAddSeq
  mirrors into _meta and regress the feed/restore watermarks.
- **`Diff` is a pure function** — `Diff(carrierRec, targetRec,
  scopeResolver) []InjectedBatch`: value present ⇒ $set; absent with a
  retained version ⇒ $unset; unknown propId ⇒ skip (Hole A — the next
  re-mirror retries); already applied ⇒ no-op. Ops are GROUPED BY each
  path's retained carrier version, one InjectedSet per distinct
  version — never a max-version over-claim. Both mirror paths (live
  event + on-load state re-mirror) share it.
- **State-based full re-mirror on space load** (no cross-space replay
  cursors); live path is a windowed Query.Subscribe on the carrier
  dataset whose overflow/drift recovery IS "re-run the re-mirror and
  resubscribe". Fast-path per-record watermark skip: follow-up.
- **No orphan rows / replay-log semantics**: a remote-originated
  carrier value for an object not present locally just WAITS in the
  carrier (the carrier record is the replay log); the row-created hook
  (afterApply creation-marker detection on the objects dataset) and
  the on-load re-mirror are the two replay triggers. A LOCAL
  `Set(account)` call addressing an object whose row doesn't exist on
  this device errors, symmetric with the local route (caller bug, not
  a sync state).
- **Read-your-writes**: Set's account branch writes the carrier, then
  runs the injected apply inline for this device; the watcher's later
  double-apply gates to a no-op.

## Removal / GC

Every remove path has a defined owner; nothing relies on "unknown ⇒
delete" (which would race the schema-sync window):

- **Unset a value** (`$unset` via Set): synced — normal CRDT op;
  account — carrier-record `$unset` with retained `_ver` entry, so the
  re-mirror propagates the unset to devices that were offline; local —
  sidecar + row unset.
- **Object deleted** (target row tombstoned): the watcher observes the
  tombstone and **deletes the object's carrier records in tech space**
  (all `(objectId, *, *)` keys) and its local sidecar rows. Every
  device may issue the same carrier delete — CRDT deletes are
  idempotent and tombstones sticky, so this converges. A racing account
  write loses to the sticky carrier tombstone; injected applies already
  short-circuit on tombstoned target rows, so nothing resurrects.
- **Space left/deleted**: drop the whole per-space carrier object in
  tech space + the space's sidecar rows.
- **Property definition removed**: carrier values under the dead propId
  become orphans, same policy as base-value orphans (docs 06 §"read
  tolerance") — deliberately NOT GC'd by the mirror, because the mirror
  cannot distinguish "removed" from "definition not synced yet" (the
  Hole-A skip rule). Bounded: dead propIds can't be resurrected
  (content-addressed ids), and the object-delete / space-leave paths
  reclaim them eventually.

## Local route (slice 3 — landed)

`Properties.Set` routes local-scoped patches through `Object.LocalSet`
(strict mode — the row must exist; no local-domain creation markers).
Writer-side strict validation (unknown property / kind) replaces the
handler pass local writes skip. The controller's head-level class check
gets `HandlerReg.DynamicScopeByKey`: undeclared heads on the objects
dataset carry per-property scopes the controller can't see, so the
direction check is skipped for them (declared derived heads stay
enforced); per-prop enforcement lives in the handler (DAG route) and
`Set` (local/account routes).

**Dataset records (landed 2026-07-03).** The local route is public for
dataset records too: `ModifyBatch.Scope = ScopeLocal` routes the batch
through `Object.LocalSet` (`spaceimpl.modifyLocal`) — the same
materialization techspace uses for `localStatus` / `identities`,
opened to declared local-scope dataset fields (e.g. a read-tracking
`unread` flag on `chat_messages`). Single-scope per call, like
`Properties.Set`. Strict by construction: explicit record ids, no
Upsert (local fields annotate synced records, never create them), no
TraceIds. Scope enforcement is the apply layer's `classifyFieldWrite`
(route=local): wrong-scope ops surface as `ModifyResult.Rejections`,
absent records as `ErrStrictSkipAbsent` rejections. `ModifyMany` and
`Delete` stay synced-only. Contract test:
`e2e/local_scope_records_test.go` (the dataset-record sibling of
`local_scope_test.go`).

**Sidecar deferred.** The expert-recommended local sidecar collection
(`(objectId, dataset, recordId) → {values, vers}` as the durable source
of truth, row value a materialization) only matters for the
wipe-and-rebuild re-index path — machinery that does not exist yet
(docs/08 is all open questions). Local values are durable in any-store
today. The sidecar lands WITH the rebuild machinery; until then a
handler-version-bump wipe (if implemented naively) must not be shipped
without it.

## applySeq (slice 2) — consumer-feed watermark

Account/local writes never enter the target space's DAG ⇒ no AddSeq ⇒
invisible to `ChangedSince`/`_addSeq > since` (the `any` indexer's
freshness signal would silently go stale on those writes).

- New per-space, SDK-owned monotonic `applySeq`: minted on every apply
  that actually mutates a record (DAG, mirror, local), stamped on the
  row as `_applySeq`, **persisted in the same WriteTx** as the stamp
  (lazily-persisted counters can regress on crash and re-mint seqs a
  consumer cursor already passed).
- `Space.Changes()` feed and `ChangedSince` re-key onto applySeq.
  **AddSeq keeps its real job**: the any-sync→any-store restore
  watermark (per-object `MaxAddSeq`, `IterateAfterAddSeq`). Two
  reconciliation links, each keyed by its upstream's coordinate:
  AddSeq answers "is any-store caught up with any-sync"; applySeq
  answers "is the index caught up with any-store".
- Boot replay of already-applied changes gates to content no-ops ⇒ no
  bump ⇒ quiet feed. Handler-version rebuilds mint fresh seqs for rows
  whose content changed ⇒ downstream indexers re-feed incrementally
  (today's answer is `rm <data-dir>/index`).
- Tombstones carry `_applySeq` like they carry `_addSeq` (deletion
  streaming).
- Existing rows: seed `_applySeq` from `_addSeq` lazily (first stamp
  wins) or at first open; consumers' cursors reset per-space via the
  feed's documented "cursor is local" contract.

## Migration

**None.** Values already sit at their final paths; everything existing
is synced-scope; definitions without a `scope` field read as `synced`.

## Documented footguns (docs slices, both repos)

- Account/local props in shared spaces mean **"my value"** in queries —
  filters on them select per-account/per-device result sets.
- Agents/automations must not condition **shared** mutations on
  account/local-scoped values ("delete where my-review-status =
  rejected" deletes the shared object for everyone).
- The local search index intentionally indexes what the local account
  sees (account/local values included) — per-device index divergence is
  correct.

## Implementation slices (SDK, this branch)

Slices 1–4 are landed on feat/scoped-properties; remaining follow-ups
are listed at the end of this section.

1. **Scope on declarations + enforcement + cleanup** — `schema.
   ScopeAccount` (+ ParseScope); unify `anytype`/`spaceindex` mini-enums
   onto schema.Scope; `PropertyDraft/Def.Scope` (+ space-package alias);
   `typetype` pins `scope` (schemaBearingFields) and validates labels on
   create; `PropInfo.Scope` through LiveRegistry/Stub; handler drops
   wrong-route ops (new `scope_mismatch` validation reason); auto-routing
   `Set()` (synced route live; account/local return clear not-implemented
   until slices 3/4); **delete variant machinery**; docs 05a §9 + 06
   rewrite.
2. **applySeq** — counter + stamping + feed re-key + tests (crash
   semantics, rebuild re-feed).
3. **Local scope end-to-end** — Set() local route via LocalSet on the
   objects row; sidecar source of truth + rebuild re-injection;
   classifyFieldWrite allows declared-local dataset fields via LocalSet
   (already does) and the objects carve-out.
4. **Account scope end-to-end** — carrier object + dataset, injected
   apply path (explicit versionId), watcher (live + on-load re-mirror +
   row-creation pull + leave-space GC); account dataset fields ride the
   same carrier keying.
5. **Docs final pass** — 02-tech-space (carrier), 04/05 cross-refs,
   README status.

## `any` server follow-ups (separate repo, after SDK tags)

- Property create/read endpoints + CLI gain `scope` (wire strings =
  schema.Scope labels; `x-scope` discovery unchanged and now shares the
  vocabulary).
- `Properties.Set` replaces per-scope endpoints 1:1; docs 03/08/09.
- Indexer/chunkers swap `_addSeq` → `_applySeq` (docs/13 § freshness +
  the "index reflects local view" note).

## Follow-up ledger (post slices 1–4)

- **Carrier GC must not tree-delete (any-sync rule on derived trees).**
  any-sync forbids deleting DERIVED objects — deletion is allowed only
  for ordinary (non-derived) trees — because a derived tree's id is
  deterministic: delete + re-derive yields the SAME identity with
  fresh history, i.e. history replacement. The carrier is derived
  (`builtin:accountValues/<spaceId>`), so `DropAccountValues`'s
  `tree.Delete` on space leave is invalid under the protocol. Replace
  with record-level GC: tombstone every carrier record (CRDT deletes,
  converge normally) and leave the empty derived tree in place;
  storage reclamation is whatever any-sync ever offers for derived
  trees. The per-object delete GC (DeleteAccountValuesForObject) is
  already record-level and unaffected.

- **Dataset-field account transport**: declaration + enforcement are
  live; the mirror handles the objects rows only. Extending it = key
  records by their real (dataset, recordId) and target per-object
  dataset collections in mirrorRecord.
- **Local sidecar** for local-scope durability — lands WITH the
  wipe-and-rebuild re-index machinery it serves (docs/08).
- **Re-mirror fast path**: per-carrier-record applied-watermark skip
  for spaces with very many overridden objects.
- **Carrier residency**: the mirror keeps the carrier object resident
  while reconciling; a TTL-evicted carrier delays remote account
  values until the next event/reconcile (same residency semantics as
  the tech index object).
- **`any` server follow-ups**: scope on property endpoints/CLI,
  `_addSeq` → `_applySeq` in chunkers/indexer, docs 03/08/09/13, the
  two shared-space footguns documented in client recipes.
