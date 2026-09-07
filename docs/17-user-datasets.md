# Parts, modules and user-defined datasets

A type declares what its objects carry beyond properties as **parts**:
display units, each owning one or more **datasets**. A dataset is
served by a **module** — the built-in `records` module (a
schema-enforced dataset the client fully describes) or a compiled-in
module the embedder registers (an editor, a chat). Everything is
declarative and definable **at runtime, per space, by clients**: a
records schema alone expresses what previously required a compiled-in
Go handler (required fields, write-once vs author-mutable fields,
author-only delete, apply-time stamps, record-id rules, a
search-extraction annotation); a module declaration instantiates the
module's compiled-in behaviour on a collection of the type's own.

Related docs: [05a-crdt-spec.md](05a-crdt-spec.md) §7–8 (apply gates,
handler registration), [types-properties-proposal.md](types-properties-proposal.md)
(the property machinery this reuses), [06-data-structure.md](06-data-structure.md)
(the `type` meta-type's datasets and rendering metadata),
[bundles.md](bundles.md) (bundle roots declaring parts).

## Model

```
Type
 ├─ properties            (docs/06)
 ├─ weight, layout        (rendering metadata, docs/06)
 └─ parts[]               display units, keyed
     ├─ name/icon/pos/hidden/ui/uses
     └─ datasets[]        keyed; module + shared; the declaration
```

- A **part** is what a client renders: a key (slug, pinned), the
  display slice (`name`, `icon`, `pos`, `hidden`), the widget descriptor
  `ui` (`{type, config}`, the x-format shape, written whole, opaque to
  the SDK) and `uses` — keys of other datasets of the same type the
  part renders without owning.
- A **dataset** is keyed inside its type (slug, pinned) and names a
  `module` (pinned) and whether it is `shared` (pinned). A `records`
  dataset carries the behavioural declaration below; a module-served
  dataset carries none — the module owns the schema, and field
  declarations on it are refused (`ErrModuleOwned`).
- A **module** (`handler.Module`, registered via `config.Config.Modules`)
  is a factory: `New(ModuleInstance) handler.Dataset` returns the
  handler, schema, indexes, read-tracking and history flags for one
  collection. A module may own a shared **canonical collection**
  (`Canonical`); `SharedOnly` refuses namespaced instances so an object
  carries at most one collection of the module. A module may also
  declare a namespace on the objects row (`Properties`) — see
  docs/06 § Module namespaces. A **reserved** module (`Reserved`,
  requires `SharedOnly`) is refused to runtime declarations —
  `AddPart`, `AddDataset`, a bundle's `Parts` — with
  `space.ErrModuleReserved`; only a bundle install the consumer makes
  with the `space.SystemInstall()` ensure option, or a registered
  type's static part, may declare it. Draft-time only: the compile
  keeps an applied declaration valid, since a peer that admitted it
  was the consumer's own install. The install root is also the
  module's **only carrier**: a local write attaching a user type that
  declares a reserved module to any row but the type's own
  (`Objects().Create` types, `AttachType`, an `any.types` op through
  `Modify`) is refused with `handler.ErrValidationReservedCarrier`
  (reason `reserved_carrier`), so a client cannot mint a second
  instance by attaching the type. There is no `SystemInstall` escape:
  the consumer's install attaches the type through its root alone. A
  registered type's static declaration is not a carrier — attachable
  by the consumer's design. Two limits: the guard reads this device's
  catalog snapshot, so a declaration not yet compiled locally is not
  reserved to it; and inbound apply stays read-tolerant, so a peer
  that lands the type on another row is applied, after which that row
  keeps writing (only additions are checked).

### Static parts on registered types

A registered type (`config.Config.Types`) declares its parts at boot
(`handler.Type.Parts`) in the shape a runtime declaration takes: each
part names entries of the type's `Datasets` (`PartDataset.Name` — the
collection is the dataset's name; the module reads as `records` on the
generic schema handler, none for a bespoke handler) or module datasets
(`PartDataset.Module` + `Shared` / `Key`, the collection rule below).
The module datasets enter the catalog as if a type object had
declared them: a shared one adds the type to the canonical collection's
owner set, a namespaced one registers `<typeId>_<key>` on every
controller with the module's `DataVersion` (registered types mint no
schema state to gate on), the module namespace on the objects row
follows (`Store.ModuleGrants`), and discovery lists them under
`Owners = [typeId]`. Static declarations are validated at `sdk.Open`
(`ValidateExternalTypes` / `ValidateExternalModules`: slug keys, each
static dataset named by exactly one part once parts are declared,
the collection rule, minted names free) and never mutate at runtime —
`AddPart` & co. answer `ErrTypeRegistered`; `Types().Parts` /
`Datasets` return the compiled view with the keys as ids. A type
with `Datasets` and no `Parts` reads back one implicit part per
dataset, keyed by the dataset name. `handler.Type.Hidden` keeps the
type out of default listings (`TypeInfo.Hidden`).

### The collection rule

```
shared: true   →  <module canonical>            editor_blocks
shared: false  →  <typeId>_<key>                bafyrei…_segments
```

- `shared` is legal only for a module with a canonical collection, and
  a shared dataset's key IS the canonical name (it defaults to it when
  left empty). A type declares at most one shared dataset per module.
  `records` is never shared.
- A shared dataset is the canonical collection itself, one per module
  per space: two types that both share `editor` give an object carrying
  both a single body, and retyping keeps it. Two types with namespaced
  editor datasets give two collections; nothing merges.
- Namespaced names cannot collide: type ids are content-addressed CIDs
  without `_`, so `<typeId>_<key>` splits at the first `_` and no two
  types can produce the same collection. There is no cross-type name
  ownership to settle any more — the runtime catalog needs no
  first-writer resolution, and bundles need no name preflight.
- Keys are slugs (`[a-z][a-z0-9_]*`, ≤ 64 bytes, `schema.ValidateSlug`):
  a collection segment and a wire path segment, nothing that needs
  quoting.

### Ownership and the write gate

A write to a collection is admitted at local write time when the object
carries **any** type whose parts declare it — the one owner of a
namespaced collection, any owner of a canonical one
(`Store.DatasetOwners`, `spaceImpl.checkDatasetMembership`). No type is
attached on write: a write to a collection none of the object's types
declare fails with `space.ErrDatasetNotDeclared`. Inbound apply stays
read-tolerant, as before: canonical collections register statically on
every controller (a peer applies an inbound `editor_blocks` change
without the declaring type's definitions), namespaced instances go
through the catalog and park until the declaring type's schema state
arrives (below). Removing a shared dataset withdraws that type's
ownership of the canonical collection and nothing else.

## Declaration vocabulary (records datasets)

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
| `Stamp` | apply-time derived value: `creator` / `createTime` / `modifyTime`; forces derived scope, and the time stamps force kind `datetime` (the value is handler-produced, so a declared kind would only be a way to get it wrong) |

Per dataset:

| Declaration | Meaning |
|---|---|
| `Dynamic` | free-form keyspace next to declared fields (existing) |
| `IdRule` | `auto` (zero: ids derived from the change, the empty-id sugar) / `user` (caller ids, pattern + max length constrained) |
| `DeleteBy` | record-delete gate: `anyone` (zero) / `author` |
| `SkipHistory` | keep out of the version-history index (existing) |
| `Search` | `{title, text}` field mapping plus an optional `scope` slug (which index scope the entries land under), surfaced as `x-search` for external indexers; SDK-opaque. `text` names one or more field keys (SYN-179) — the indexer joins the mapped values into one body; on the wire a single key rides as a bare string, multiple as an array (single-element arrays canonicalize to the string on marshal) |

Declaration well-formedness (`schema.ValidateDatasetDecl`, shared by
every entry path so a bad declaration can neither register nor sync):
stamps force derived scope; at most one field per stamp kind;
`Required` excludes `Stamp` and requires synced scope; any
`MutableBy: author` field or `DeleteBy: author` requires a
`Stamp: creator` field (the authorship fact must live ON the record so
apply-time checks read only `ctx.Before` — zero extra reads); id
constraints only under `IdRule: user`; a `search.text` mapping names at
least one field key with no empty or duplicate keys (mapped keys are
not required to be declared fields — the annotation stays opaque).

## The generic schema handler

`crdt.SchemaHandler` interprets a declaration. A `handler.Dataset`
with a declared `Schema` and `Handler: nil` gets one automatically —
compiled-in types, module instances that return a nil handler, and
`records` datasets all use the same enforcement.

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
other. A type's parts are CRDT records there:

A bundle root that declares `Parts` is a type object implementing
itself (`any.types = ["__type__", rootId]`, `typeId == objectId`): its
definitions live on the root exactly like this, and evolve through
`Types().AddPart` / `AddDataset` / `AddDatasetField` / `PatchDataset` /
`PatchDatasetField` with `typeId = rootId`. See `bundles.md § Bundle
parts`.

- **Part record** (one per part; id minted client-side, unique):
  `def:"part"`, `key` (slug, pinned); `name`, `icon`, `pos`, `hidden`,
  `ui` (an object, created whole) and `uses` (an array of dataset keys)
  mutable.
- **Head record** (one per dataset; id minted client-side, unique):
  `def:"dataset"`, `key` (slug), `module`, `shared`, `part` (the owning
  part record id), `dynamic`, `idRule`/`idPattern`/`idMaxLen`,
  `deleteBy`, `skipHistory` — all pinned first-write; `displayName`,
  `description`, and the `search.title`/`search.text`/`search.scope`
  leaves stay mutable (leaves mutate, a broad `search` replace is
  pinned). `title`/`scope` are scalar strings; `text` is a bare field
  key or a non-empty array of unique keys (SYN-179). The head carries
  no `collection`: the collection is computed at compile from the
  collection rule.
- **Field record** (one per field of a `records` dataset; id derived
  from the change): `def:"field"`, `dataset` (owning head id), `key`,
  `kind`, `scope`, `stamp`, `required`, `mutableBy`, `items`/`properties`
  — pinned; `name`, `description` and the opaque `x-format` descriptor
  (every path under it, any value — the same bag a property definition
  carries, docs/06 § "The `x-format` descriptor") mutable via
  `PatchDatasetField`. The descriptive slice never enters the schema
  revision: editing it re-registers nothing.

Record-per-definition is what makes concurrent edits merge for free:
creates are stateless-accepted records, pinning is stateless per-path,
and distinct adds converge as distinct records — exactly the
`properties` dataset model. Cross-record consistency (orphans, author
rules without a creator stamp, the collection rule, module existence)
is resolved at catalog compile, never in hooks: hooks that read sibling
records would take arrival-order-dependent verdicts, and a handler
cannot see the module catalog (a peer without a module still stores
the declaration).

`AddPart` writes the part, its heads and their fields in **one change**
(the ids are minted client-side so records can reference each other),
so a crash can never strand an orphan. `AddDataset` on an existing part
writes the head and its fields the same way.

Every definition create/delete projects a shortId row into the type's
existing `<typeId>_shortIds` collection (with a `src: "datasets"`
discriminator), so the DataVersion gate covers dataset-schema state
with no gate changes and no extra lookups.

### Compile: records → parts and declarations

`types.CompileTypeParts` folds a type's records in one storage pass,
deterministically on converged records (`CompileDatasetDefs` is the
flat dataset view of the same fold):

- tombstoned records are skipped;
- duplicate part keys fold into one: the display slice from the
  smallest creation `_ver.id`, datasets and `uses` unioned;
- a head whose part is absent, or a field whose head is absent, is an
  orphan and is skipped;
- duplicate dataset keys name the same collection by construction:
  the smallest creation `_ver.id` is the definition's identity and
  display, fields union by key across every head (smallest `_ver.id`
  per key), and a disagreement on a pinned leaf marks the definition
  invalid — sound within one tree: orderId VALUES are peer-local but
  their relative order converges;
- the collection rule decides the collection; an unknown module or a
  shared violation marks the definition invalid; a type declaring two
  shared datasets of one module keeps the smallest `_ver.id` and marks
  the rest invalid;
- a module-served dataset carries no fields: field records under it
  are orphans;
- a `records` fold that fails `ValidateDatasetDecl` is emitted
  **Invalid** — visible through `Types().Datasets` (with the reason)
  so it can be repaired or removed, but never registered.

### Evolution rules

Validation always runs against the **current** compiled schema, so
evolution is additive-only with pinned behavior:

- Add parts, datasets and fields freely at runtime; edits sync and
  apply like any space data.
- A part's key; a dataset's key, module, shared flag and part; a
  field's `kind`, `scope`, `stamp`, `required`, `mutableBy`; the id
  rule and the delete gate are pinned for the life of the definition —
  remove and re-add under a new key to change them. A dataset never
  moves between parts.
- **Additive fields cannot be `Required`** — fresh devices replay the
  dataset's own history against the current schema; a required field
  added later would reject every historical create. Required fields
  exist only from `AddPart` / `AddDataset`.
- **`RemoveDatasetField` re-validates the remaining declaration** and
  refuses removals that would invalidate it (e.g. the creator stamp of
  an author-gated dataset). Already-invalid definitions stay removable.
- `RemoveDataset` / `RemovePart` do NOT clean up record data (the
  `RemoveProperty` stance); subsequent writes drop once peers apply the
  removal, and a shared dataset's removal only withdraws this type's
  ownership.

## Runtime registration

Controllers' handler maps stay immutable after construction; runtime
declarations reach them through a store-level **catalog**:

- The catalog is a copy-on-write snapshot (`atomic.Pointer`), built
  once at store open (one indexed scan of live `__type__` rows + one
  compile per type object) and refreshed ONLY when a `datasets` change
  applies. Controller construction reads it with one atomic load — no
  storage scans, no declaration compiles (registrations are pre-built
  per snapshot and shared; handlers are read-only after construction).
  A namespaced dataset gets one registration: the generic schema
  handler over its declaration for `records`, the module's
  `New(instance)` output otherwise, with the module's `HandlerVersion`
  composed in. A shared dataset adds its type to the canonical
  collection's owner set and registers nothing — the canonical
  collection is on every controller from store open, with the module's
  `DataVersion`.
- **Lazy, demand-driven eviction** — nobody is evicted proactively on
  schema apply. Each registration carries a `SchemaRev` fingerprint of
  its compiled declaration (for a module instance: the module and key,
  since the schema is the module's); a resident controller whose rev
  differs from the catalog's (dataset added, field added/removed,
  definition removed) is stale. Local touch (`Modify`/`Upsert`/`Query`)
  drops and reloads it; user-facing write paths retry once on the
  resulting `object.ErrClosed`, so the eviction never surfaces as a
  caller error.
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

Changes on a namespaced dataset stamp `typeId:latestShortId` — the
declaring type's whole schema state (properties, parts AND dataset
definitions share one shortId stream). A peer that hasn't applied the
writer's schema state parks the data change until it has: schema-then-
data arrival order is guaranteed by the existing detached-changes
machinery in either direction. Changes on a module's canonical
collection stamp the module's opaque `DataVersion`, as compiled-in
datasets always have (the gate lets opaque strings through — the
declaring type's state is not the right gate for a collection several
types own). Definition writes themselves stamp the hardcoded
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

`Space.Datasets()` lists every collection the store hosts. Each
`DatasetSchema` carries `Owners` — the declaring types: one for a
registered-type or namespaced dataset, every type sharing the module
for a canonical collection (empty while nothing declares it), none for
space-level built-ins — plus `Module` and `Shared`. Consumer indexers
gate on `Owners` (an object may hold the dataset when it carries one
of them). The JSON Schema document grows the behavioral keywords:
standard `required`, per-field `x-mutable-by` / `x-stamp`,
dataset-level `x-delete-by`, `x-id` / `x-id-pattern` / `x-id-max-length`,
and `x-search {title, text, scope}` (`text`: a field key or an array of
keys; a single key marshals as the bare string) — defaults omitted.
Each field node also carries its descriptive slice when set: standard
`description` and the opaque `x-format` bag, verbatim. That holds for
compiled-in datasets too — a `handler.Field` declares the same
`Description` / `XFormat`, and every SDK-declared dataset (tech space,
`payloads`, the `objects` row's `any.*` fields) ships with a description
on each field and a descriptor where one fits (docs/06 § The `x-format`
descriptor). `Types().Parts(typeId)` and
`Types().Datasets(typeId)` return the management views (definition
ids, invalid state, display fields, the computed `Collection`, the
full value shape, the descriptor).

## Out of scope / limitations (v1)

- **No computed fields.** Apply hooks must be replica-deterministic; a
  user-facing expression form is a versioned-determinism problem.
  Stamps cover the security-relevant derivations; other derivation
  hooks stay built-in-only.
- **Deep shape/size validation** stays garbage-tolerant per spec §7.2:
  declared value shapes are enforced, byte caps and exotic layouts are
  a consumer concern.
- `SkipHistory` on a dataset defined after the history index opened
  applies from the next index open.
- No handler-version re-index machinery beyond SchemaRev-driven
  registration refresh (docs/08-versioning.md remains the vision).
- A module's `SharedOnly` is the only per-object cardinality rule: an
  object carrying two types with namespaced datasets of one module
  carries two collections of it.

## API surface

```go
// definitions (space.TypesAPI)
Patch(ctx, typeId, TypePatch) error                       // name/description/icon/weight/layout
Parts(ctx, typeId) ([]PartDef, error)
AddPart(ctx, typeId, PartDraft) (partId, error)           // part + datasets + fields, one change
PatchPart(ctx, typeId, partId, DatasetDefPatch) error     // display slice, ui (whole), uses
RemovePart(ctx, typeId, partId) error                     // part + its datasets
AddDataset(ctx, typeId, partId, DatasetDraft) (datasetDefId, error)
AddDatasetField(ctx, typeId, datasetDefId, DatasetFieldDraft) (fieldDefId, error) // records only
RemoveDataset(ctx, typeId, datasetDefId) error
RemoveDatasetField(ctx, typeId, fieldDefId) error
PatchDataset(ctx, typeId, defId, DatasetDefPatch) error   // mutable leaves
PatchDatasetField(ctx, typeId, fieldDefId, DatasetDefPatch) error
Datasets(ctx, typeId) ([]DatasetDef, error)               // flat, with Collection

// modules (config.Config.Modules)
handler.Module{Name, Canonical, SharedOnly, Reserved, DataVersion, HandlerVersion, Properties, New}

// static parts (config.Config.Types)
handler.Type{…, Parts: []handler.Part{{Key, Name, Icon, Pos, Hidden, UI, Uses,
    Datasets: []handler.PartDataset{{Name} | {Module, Shared, Key}}}}, Hidden}

// bundles declaring a type (space.EnsureBundleRequest)
EnsureBundleRequest{…, Parts, Properties /* XKey required, deterministic ids */, Layout, Weight, Hidden}
Bundles().Ensure(ctx, req, space.SystemInstall())   // the consumer's own install: may name a reserved module

// data (space.Space) — plus the existing Modify/Query surface
Upsert(ctx, UpsertBatch) (UpsertResult, error)
```
