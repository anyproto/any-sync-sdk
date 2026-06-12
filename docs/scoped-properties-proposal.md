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
  built-ins (`any.id/author/spaceId/createdAt`).
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
  strip). `AttachType`/`DetachType` unchanged (synced-only).

## Account transport (slice 4)

- Tech space hosts **one derived carrier object per target space**
  (seed: `builtin:accountVariants/<spaceId>`); bounded DAG, 1:1 space
  lifecycle, trivial GC on leave.
- One carrier record per **(objectId, dataset, recordId)** — the
  objects row is the degenerate case (dataset `objects`, recordId =
  objectId). Record fields are the account-scoped paths; the record's
  own `_ver` (tech domain) is the version source, including retained
  entries for unset paths.
- Per-device watcher: live-subscribes to the carrier dataset and
  applies converged values into the target rows as **injected applies**
  — no DAG, handler-skipping like LocalSet, but with the caller
  (mirror) supplying the tech versionId instead of minting a local one.
  Gating is the standard `_ver.<path>` compare; monotone per peer
  because the carrier's tech tree is.
- **State-based full re-mirror on space load** (no cross-space replay
  cursors): per carrier record, leaf-merge (value bag + retained
  `_ver`) against the target row — value absent + newer version ⇒
  `$unset`, so unsets propagate to devices that were offline. Fast-path
  skip via per-record max-version watermark.
- **No orphan rows**: an account value for an object whose row doesn't
  exist locally waits; the row-creation path pulls pending carrier
  records, so `_ver.id` stays purely object-tree domain.

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

## Device route (slice 3)

Existing `Object.LocalSet` + `NextLocalVersion`, gated per-prop instead
of per-schema-field (the `objects` dataset's `classifyFieldWrite` can
only see top-level heads; per-prop scope checks live writer-side and in
the handler). `device_variants` sidecar is NOT needed in this model? —
it is: local values live in any-store rows that DAG rebuild wipes.
Decision kept from expert review: **local-scope source of truth is a
small local sidecar collection** (`(objectId, dataset, recordId) →
{values, vers}`); the row value is a materialization; rebuild re-injects
sidecar + re-mirrors account state after replaying the DAG.

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
