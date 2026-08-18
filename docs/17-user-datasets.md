# User-Defined Dataset Schemas (SYN-147)

A dataset schema is declarative and definable **at runtime, per space,
by clients**. A schema alone expresses what previously required a
compiled-in Go handler: required fields, write-once vs author-mutable
fields, author-only delete, apply-time stamps, record-id rules,
indexes-adjacent behaviors, and a search-extraction annotation. The
SDK enforces the declaration generically on the apply path; bespoke
handlers remain only for cross-field rules (e.g. payloads'
"networkSign requires rootCid").

Related docs: [05a-crdt-spec.md](05a-crdt-spec.md) §7–8 (apply gates,
handler registration), [types-properties-proposal.md](types-properties-proposal.md)
(the property machinery this reuses), [06-data-structure.md](06-data-structure.md)
(the `type` meta-type's datasets).

## Declaration vocabulary

`schema.Dataset` / `schema.Field` (re-exported through `handler` and
`space`) carry the behavioral declaration. Zero values preserve
pre-existing behavior throughout.

Per field:

| Declaration | Meaning |
|---|---|
| `Scope` | existing write/sync class (synced/derived/local/account); zero = synced |
| `Schema` | value shape (recursive JSON-Schema subset); nil = unconstrained |
| `Required` | must be present in the create payload; create-only check |
| `MutableBy` | post-create write rule: `never` (zero) / `author` / `any` |
| `Stamp` | apply-time derived value: `creator` / `createTime` / `modifyTime`; forces derived scope |

Per dataset:

| Declaration | Meaning |
|---|---|
| `Dynamic` | free-form keyspace next to declared fields (existing) |
| `IdRule` | `auto` (zero: ids derived from the change, the empty-id sugar) / `user` (caller ids, pattern + max length constrained) |
| `DeleteBy` | record-delete gate: `anyone` (zero) / `author` |
| `SkipHistory` | keep out of the version-history index (existing) |
| `Search` | `{title, text}` field mapping, surfaced as `x-search` for external indexers; SDK-opaque |

Declaration well-formedness (`schema.ValidateDatasetDecl`, shared by
every entry path so a bad declaration can neither register nor sync):
stamps force derived scope; at most one field per stamp kind;
`Required` excludes `Stamp` and requires synced scope; any
`MutableBy: author` field or `DeleteBy: author` requires a
`Stamp: creator` field (the authorship fact must live ON the record so
apply-time checks read only `ctx.Before` — zero extra reads); id
constraints only under `IdRule: user`.

## The generic schema handler

`crdt.SchemaHandler` interprets a declaration. A `handler.Dataset`
with a declared `Schema` and `Handler: nil` gets one automatically —
compiled-in types use the same enforcement as runtime ones.

The declaration compiles once at construction into a flat rule table;
the accept path of `BeforeModify` is one map lookup with zero
allocations (benchmark-asserted — cold restore replays millions of
ops through these hooks).

Enforcement:

- **BeforeCreate** (whole-record drop): id rule; required fields
  (satisfied only by whole-field content-adding ops — dotted keys
  don't count); declared value shapes; stamps derived from
  `Change.Creator` / `Change.Timestamp` via the sink (zero DB reads).
- **BeforeModify** (per-op drop; multi-field ops salvage per key via
  the controller's probe): mutability gate, value shapes, `modifyTime`
  bump on accepted mutable writes. Writes descending below the
  declared shape (under a scalar, into an undeclared closed-object
  property) are rejected.
- **BeforeDelete**: the `DeleteBy: author` gate.
- **PreValidateMulti**: the strict local mirror with caller-readable
  errors (spec §7.2 disposition split).

### Convergence rules

Every verdict is **arrival-order independent** (the replica-determinism
contract of spec §8):

- **Write-once = create-only.** A `MutableBy: never` field is writable
  ONLY in the record's creating change. There is no late fill: a
  presence-based rule ("reject iff the field already has a value")
  would accept/reject concurrent fills in opposite orders on different
  replicas. Fields that must stay settable declare `MutableBy`.
- **Author gates read the creator stamp** — a derived-at-create,
  immutable record fact, so the verdict is identical wherever the
  record exists.
- **`DeleteBy: author` rejects deletes of never-created records**
  (no creation marker yet). Convergent because an author's own
  create→delete is self-causally ordered — a delete arriving before
  its record's create can only be a non-author's, which BOTH orderings
  reject.
- **Stamps come from the change envelope**, never from replica state.

### The IdRule: user contract

A caller-supplied record id must have a **single writer** (it doubles
as the upsert idempotency key). Concurrent creates of the same id by
different members take arrival-order-dependent creation verdicts
(required checks, the creator stamp behind author gates) and are
outside the convergence guarantee. One ingest writer per dataset — or
per id range — keeps the contract trivially true.

## Definitions on the type object

The `type` meta-type owns a third built-in dataset, **`datasets`**
(next to `properties` and `shortIds`), registered compiled-in like any
other. A definition is CRDT records:

- **Head record** (one per dataset; id minted client-side, unique):
  `def:"dataset"`, `collection` (the dataset name), `dynamic`,
  `idRule`/`idPattern`/`idMaxLen`, `deleteBy`, `skipHistory` — all
  pinned first-write; `displayName`, `description`, and the
  `search.title`/`search.text` string leaves stay mutable (the
  `format.ui`/`format.filter` model).
- **Field record** (one per field; id derived from the change):
  `def:"field"`, `dataset` (owning head id), `key`, `kind`, `scope`,
  `stamp`, `required`, `mutableBy`, `items`/`properties` — pinned;
  `name`, `description` mutable.

Record-per-field is what makes concurrent edits merge for free:
creates are stateless-accepted content-addressed records, pinning is
stateless per-path, and distinct field adds converge as distinct
records — exactly the `properties` dataset model. Cross-record
consistency (orphan fields, author rules without a creator stamp) is
resolved at catalog compile, never in hooks: hooks that read sibling
records would take arrival-order-dependent verdicts.

`AddDataset` writes the head and all initial fields in **one change**
(the head id is minted client-side so field records can reference it),
so a crash can never strand an orphan head.

Every definition create/delete projects a shortId row into the type's
existing `<typeId>_shortIds` collection (with a `src: "datasets"`
discriminator), so the DataVersion gate covers dataset-schema state
with no gate changes and no extra lookups.

### Compile: records → declaration

`types.CompileDatasetDefs` folds a type's records in one storage pass,
deterministically on converged records: tombstones and orphan fields
skipped; duplicate field keys / duplicate names within the type
resolve to the smallest creation `_ver.id` (sound within one tree:
orderId VALUES are peer-local but their relative order converges);
a fold that fails `ValidateDatasetDecl` is emitted **Invalid** —
visible through `Types().Datasets` (with the reason) so it can be
repaired or removed, but never registered.

### Evolution rules

Validation always runs against the **current** compiled schema, so
evolution is additive-only with pinned behavior:

- Add datasets and fields freely at runtime; edits sync and apply like
  any space data.
- `kind`, `scope`, `stamp`, `required`, `mutableBy`, the id rule, the
  delete gate, and the collection name are pinned for the life of the
  definition — remove and re-add under a new key to change them.
- **Additive fields cannot be `Required`** — fresh devices replay the
  dataset's own history against the current schema; a required field
  added later would reject every historical create. Required fields
  exist only from `AddDataset`.
- **`RemoveDatasetField` re-validates the remaining declaration** and
  refuses removals that would invalidate it (e.g. the creator stamp of
  an author-gated dataset). Already-invalid definitions stay removable.
- `RemoveDataset` does NOT clean up record data (the `RemoveProperty`
  stance); subsequent writes drop once peers apply the removal.

## Runtime registration

Controllers' handler maps stay immutable after construction; runtime
schemas reach them through a store-level **catalog**:

- The catalog is a copy-on-write snapshot (`atomic.Pointer`), built
  once at store open (one indexed scan of live `__type__` rows + one
  compile per type object) and refreshed ONLY when a `datasets` change
  applies. Controller construction reads it with one atomic load — no
  storage scans, no declaration compiles (handlers are pre-built per
  snapshot and shared; `SchemaHandler` is read-only after
  construction). Cross-type name conflicts resolve to the smallest
  DefId — content-addressed and identical on every replica (`_ver.id`
  is never compared across trees); built-ins and config-registered
  names always win.
- **Lazy, demand-driven eviction** — nobody is evicted proactively on
  schema apply. Each registration carries a `SchemaRev` fingerprint of
  its compiled declaration; a resident controller whose rev differs
  from the catalog's (dataset added, field added/removed, definition
  removed) is stale. Local touch (`Modify`/`Upsert`/`Query`) drops and
  reloads it; user-facing write paths retry once on the resulting
  `object.ErrClosed`, so the eviction never surfaces as a caller error.
- **Inbound**: the apply gate parks a change whose dataset this
  controller doesn't currently carry (unknown OR stale rev) into the
  `_detached` collection — the spec §8.2 persist-but-skip, realized.
  When the definition applies, the catalog refreshes and the drainer
  wakes; the drain evicts exactly the parked objects' stale
  controllers before replaying. Rows whose dataset is still
  unregistered are skipped before the payload decode. Stale-rev parks
  with no missing shortId pairs nudge the drainer at park time (the
  schema may already be fully applied — nothing else would wake it).

## DataVersion & gating

Changes on a runtime dataset stamp `typeId:latestShortId` — the owning
type's whole schema state (properties AND dataset definitions share
one shortId stream). A peer that hasn't applied the writer's schema
state parks the data change until it has: schema-then-data arrival
order is guaranteed by the existing detached-changes machinery in
either direction. Definition writes themselves stamp the hardcoded
`typeDatasetHandler-v1` (never gated on their own state).

## Generic batch upsert

`Space.Upsert(UpsertBatch)` is the schema-driven ingest endpoint —
one call serves any `IdRule: user` dataset:

- per page (default 500 records): ONE `$in` query for current values,
  in-memory structural diff, one DAG change;
- absent → create (screened client-side by the same rules the handler
  enforces: per-record rejections, never a page abort);
- present → diff **declared-mutable fields only**; identical records
  skipped; a differing write-once field rejects the record
  (`ErrImmutableFieldChanged` — explicit, silent skips would hide data
  loss); author-mutable fields pre-checked against the local identity;
- modifies emit single-path `$set` per changed field (per-op handler
  granularity);
- apply-time rejections fold into `UpsertResult.Rejections`; counters
  advance only after the page write commits;
- re-running the same batch is a no-op (all skipped, no changes).

Not transactional against concurrent writers — the read-diff-write
window resolves by per-path LWW; the deployment model is one ingest
writer per dataset (see the IdRule contract above).

## Discovery

`Space.Datasets()` includes runtime datasets. Each `DatasetSchema`
carries the owning `TypeId` (consumer indexers gate on it) and the
JSON Schema document grows the behavioral keywords: standard
`required`, per-field `x-mutable-by` / `x-stamp`, dataset-level
`x-delete-by`, `x-id` / `x-id-pattern` / `x-id-max-length`, and
`x-search {title, text}` — defaults omitted. `Types().Datasets(typeId)`
returns the management view (definition ids, invalid state, display
fields).

## Out of scope / limitations (v1)

- **No computed fields.** Apply hooks must be replica-deterministic; a
  user-facing expression form is a versioned-determinism problem.
  Stamps cover the security-relevant derivations; other derivation
  hooks stay built-in-only.
- **Deep shape/size validation** stays garbage-tolerant per spec §7.2:
  declared value shapes are enforced, byte caps and exotic layouts are
  a consumer concern.
- `SkipHistory` on a runtime dataset defined after the history index
  opened applies from the next index open.
- No handler-version re-index machinery beyond SchemaRev-driven
  registration refresh (docs/08-versioning.md remains the vision).

## API surface

```go
// definitions (space.TypesAPI)
AddDataset(ctx, typeId, DatasetDraft) (datasetDefId, error)
AddDatasetField(ctx, typeId, datasetDefId, DatasetFieldDraft) (fieldDefId, error)
RemoveDataset(ctx, typeId, datasetDefId) error
RemoveDatasetField(ctx, typeId, fieldDefId) error
PatchDataset(ctx, typeId, defId, DatasetDefPatch) error   // mutable leaves
Datasets(ctx, typeId) ([]DatasetDef, error)

// data (space.Space) — plus the existing Modify/Query surface
Upsert(ctx, UpsertBatch) (UpsertResult, error)
```
