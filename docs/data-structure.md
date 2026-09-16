# Data Structure

Data is read through any-store queries and live subscriptions. Layout
in the local DB:

- **Space list** — the tech space's space index (docs/tech-space.md).
- **`<spaceId>_objects`** — one row per object in the space, holding its
  properties.
- **`<objectId>_<dataset>`** — the records of each dataset an object
  carries.

## Object Properties

Every object has one row in its space's `objects` collection,
`id` = any-sync objectId. The row holds the derived fields (`id`,
`author`, `spaceId`, `createdAt`, `modifiedAt`, `modifiedBy`), the
universal `any` properties (name, description, icon, tags, `type`,
`collections`) and the values of every property its type and
collections define.

### Property scopes

Scope is a fixed attribute of a property definition (and of a declared
dataset field), from the `schema.Scope` taxonomy. It is pinned at
creation like `kind`: changing scope means defining a new property
with a new propId. Design and rationale:
[`scoped-properties-proposal.md`](scoped-properties-proposal.md).

| Scope | Write route | Version domain | Syncs to |
|------|------|------|------|
| **derived** | Handler-stamped: `id`, `author`, `spaceId`, `createdAt`, `modifiedAt`, `modifiedBy` | Triggering change | Computed convergently on every peer |
| **synced** | The object's own CRDT change | Object tree | Everyone with access |
| **account** | Carrier record in the tech space, mirrored per device | Tech tree | This account's devices |
| **local** | `Object.LocalSet`, no DAG | Local lexid | This device only |

There is no override stack: a property lives in exactly one scope. A
shared and a personal "favorite" are two properties.

**Timestamps are `datetime` instants.** Filter literals must be
instants too: `{"createdAt": {"$gte": {"$date": "2026-01-01T00:00:00Z"}}}`.
A bare number does not error: any-store compares across types by type
rank, and instants rank above numbers and strings, so `$gte` matches
every row and `$lt` / `$eq` match none. Rows built before `datetime`
existed hold numbers until their re-index completes
(docs/versioning.md); during that window a `-modifiedAt` sort
returns two type-grouped blocks.

**`modifiedAt` / `modifiedBy` are object-level.** Every synced write to
the object moves them: a property write, a write or record delete in
any of its datasets (editor blocks, chat messages, runtime datasets).
`author` stays the creator.

- The `objects` handler is a `crdt.ObjectStamper`: a change on another
  dataset stamps the object's row in the same transaction with the
  change's VersionId, LWW like any field. The pair moves together, so a
  converged row names the signer of the change that set the time.
  Concurrent writers resolve by VersionId, not wall clock.
- Local- and account-route writes never move them.
- A stamp never creates a row. If a content change applies before the
  row's creating change (a parked create draining later), the row
  carries the creating change's stamps until the next synced write.
- Deleting the objects row does not move them; a tombstone keeps its
  last stamps.
- `modifiedBy` is an account id: resolve it through `Space.Members()`
  or `SDK.Identities()`; it can name an account since removed from the
  space. Absent means the row predates the field and awaits its rebuild
  (docs/versioning.md), or the signer is unknown.
- `modifiedAt` is indexed (the default list sort); `modifiedBy` is not,
  so filtering by it scans the collection.

### Storage

Values of every scope sit at `{ownerId}.{propId}` in the object's row;
there are no scope-specific fields. One `_ver` tree per record holds
all version domains: no two routes write the same path (scope pinning
plus apply-side route enforcement, CRDT spec §9).

```json
{
  "id": "objectId",
  "author": "accountId", "createdAt": { "$date": "…" },
  "modifiedBy": "accountId", "modifiedAt": { "$date": "…" },
  "any":            { "name": "Heat" },
  "{movieTypeId}":  { "Y9Hxx5xmYmF": ["personA"], "EwyHGrtTdxB": 1995 },
  "_ver":           { "id": "…", "any": { "name": "…" } }
}
```

The derived built-ins are the exception: `id`, `author`, `spaceId`,
`createdAt`, `modifiedAt` and `modifiedBy` sit at the row root, although
`Types().Properties("any")` lists them. Filter and sort them by bare
name (`{"modifiedBy": …}`, `sort: ["-modifiedAt"]`); `any.modifiedBy`
matches nothing.

In a shared space, account- and local-scope values are this
account's / this device's values, so queries filtering on them return
per-account / per-device results. Consequences:

- Automations must not condition shared mutations on account/local
  values ("delete where my-review-status = rejected" deletes the shared
  object for everyone).
- A consumer-side search index built on this device indexes the local
  view; per-device divergence is correct.

### Handlers / enforcement

- **`SystemPropertiesHandler`** serves the synced route. Besides kind
  validation it enforces route-vs-scope: an inbound DAG op targeting a
  propId declared `account` / `local` / `derived` is dropped per op.
  This is convergent: the DataVersion gate parks changes whose schema
  has not synced, so the scope lookup never races the definition.
- **Account mirror** (tech-space watcher, see
  `scoped-properties-proposal.md` § Account transport): applies
  converged carrier values into target rows as injected applies stamped
  with tech versionIds. Unknown propIds are skipped and retried by the
  state-based re-mirror on space load.
- **Local route**: `Object.LocalSet`, validated writer-side.
- **Write API**: `Properties().Set(ctx, objectId, ownerId, patch)`
  resolves each propId's scope and writes on that route. A patch
  spanning more than one scope is rejected: routes commit independently
  across version domains, with no cross-route rollback.

Every route emits its events through the same subscription stream, one
versionId per event from that route's domain.

### Scopes on dataset fields

Dataset schema fields use the same taxonomy, including `account` (e.g.
a per-account `read` flag on chat messages). The account carrier keys
records by `(objectId, dataset, recordId)`; the objects row is the case
where dataset is `objects` and recordId is the objectId. A scoped field
has no shadowing: it is a field with a different write route.

## Types, Properties & Data Schemas

### Type and collections

```
Space
 └─ Object
     ├─ any.type          exactly one type: what the object IS
     ├─ any.collections   any number of collections: what it is filed UNDER
     └─ the datasets its type declares
```

A **type** is functional: property definitions, parts (the datasets its
objects carry) and a layout. A **collection** is categorizing: property
definitions only. Values live at `{ownerId}.{propId}` for the type and
every collection alike.

- **`any.type`** is a scalar LWW register; retyping is a `$set`
  (`Properties().SetType`). An object with a row always has one:
  `Objects().Create` and `Derive` refuse without a type, and a write
  clearing it (the field or the whole `any` container) is refused with
  `type_required` / `space.ErrTypeRequired`.
- **`any.collections`** is a set edited with `$addToSet` / `$pull`
  (`Properties().AttachCollection` / `DetachCollection`); it may be
  empty.
- Membership is structural and shared, so all three writes go through
  the object's own CRDT (synced scope).
- Trees that never write a row (a payloads carrier, a tech-space
  carrier, a definition or bundle root interrupted between tree creation
  and its first change) have no type and no row. They match no member
  query, stay out of head-sync until a change lands, and are reclaimed
  or completed by the next attempt.

Types and collections are objects in the space. A definition object
carries a reserved **marker** in `any.type` instead of a type:
`__type__` on a type, `__collection__` on a collection.

Every object implements the built-in `any`. Two more built-ins describe
definitions:

- **`type`**, granted to rows carrying `__type__`:
  - `xkey` — the type's programmatic handle;
  - `layout` — the rendering descriptor `{type, config}`, written whole,
    opaque to the SDK;
  - `hidden` — keeps the type out of default listings and pickers (a
    bundle root uses it when it only hosts its records, docs/bundles.md
    § Bundle-declared definitions);
  - `meta` — consumer flags, one scalar per key, written per key so
    concurrent writers merge, opaque to the SDK;
  - datasets `properties`, `shortIds` and `datasets` (parts and their
    dataset definitions, docs/user-datasets.md).
- **`collection`**, granted to rows carrying `__collection__`: the same
  `xkey` / `hidden` / `meta` and the `properties` / `shortIds` datasets.
  No layout, no `datasets`.

The marker differs from the namespace name because `_`-prefixed
top-level fields are protocol-owned and cannot be a storage namespace.
The handler grants `type.*` / `collection.*` off the marker alone; a row
that names the meta id as its type gets neither, which keeps `xkey`
unwritable on non-definitions.

`Types().Patch` rewrites `any.name` / `any.description` / `any.icon` and
`type.layout` / `type.hidden` in one change, and patches `type.meta` per
key (nil unsets). `Collections().Patch` does the same over
`collection.*`.

Nested writes on the objects row (`{ownerId}.{propId}.{key…}`) are
admitted only under an object-kind property, `$set` only, merging per
leaf. Below any other kind the op is dropped. `$inc`, `$addToSet` and
`$pull` address the property itself.

A definition object **implicitly implements itself**: its row carries
only the marker, yet it may hold `{ownId}.{propId}` values and the
records of the datasets it declares. A bundle root keeps its own data
this way. It never matches a member query: `{"any.type": typeId}`
returns the objects of that type, never the type object.

**Registered definitions.** Registered types (`config.Config.Types`)
carry their shape statically: `handler.Type.Parts` declares parts
(static datasets by name, module datasets by module; docs/user-datasets.md § Static
parts on registered types) and `handler.Type.Hidden` the listing flag.
`Types().Parts` / `Datasets` return the compiled view; runtime mutators
return `ErrTypeRegistered`. Registered collections work the same way
(§ Collections). A bundle may declare either kind on its root (parts,
properties with ids derived from `(rootId, xKey)`, layout, hidden), so
one install converges on one definition across devices
(docs/bundles.md).

Built-ins exist as **derived objects** with well-known ids in every
space, so query and UI code treat them like user definitions.

#### Membership and the local write pre-flight

A local write to `{ownerId}.{propId}` is admitted when ownerId is a
member of the object after this change:

- the universal `any`;
- its type (the row's, or the one this change sets);
- its collections (the row's plus those this change adds);
- the meta namespace its marker grants;
- its own id, when it carries a marker;
- the module namespaces those members grant (§ Module namespaces).

Anything else rejects the whole write with `type_not_implemented`.

Slots are strict. A known collection id written to `any.type`, or a
known type id or marker added to `any.collections`, is refused with
`wrong_slot` (`space.ErrWrongSlot`). An id this device cannot resolve
(a definition not synced yet) passes: offline-first outranks slot
hygiene. The check runs in the handler pre-flight, so a raw `$set`
through `Modify` is covered too.

Inbound apply has **no membership guard**. `any.type` and
`any.collections` are themselves concurrent CRDT values, so gating
applies on them would make the outcome apply-order-dependent. A value
written concurrently with a retype or detach lands as an orphan on
every peer and converges (§ Orphan values).

#### Querying membership

`{"any.type": typeId}` returns the objects of a type;
`{"any.collections": collectionId}` the members of a collection (array
fields match element-wise). Neither returns the definition object, so
client filters need no marker exclusion. `any.type` has a dense index on
the `objects` collection, `any.collections` a sparse one.

#### Collections

```go
// space.CollectionsAPI, from Space.Collections()
List(ctx) ([]CollectionInfo, error)                 // meta `collection` + registered + user; Hidden included
Get(ctx, collectionId) (CollectionInfo, error)      // a type id → ErrNotACollection
Create(ctx, CollectionCreateParams) (id, error)     // Name, Description, IconCID, XKey, Hidden, Meta
Patch(ctx, collectionId, CollectionPatch) error     // registered → ErrTypeRegistered; a type id → ErrNotACollection
Delete(ctx, collectionId) error                     // not implemented, like Types().Delete
```

`Create` writes `any.{name,description,icon}`,
`collection.{xkey,hidden,meta}` and `any.type = "__collection__"` in one
change.

The listings are disjoint: `Types().List` returns the built-ins (`any`,
`spaceIndex`, `type`, `collection`), registered types and user types,
never a collection; `Collections().List` the reverse.

Property definitions have one surface, `space.PropertyDefsAPI`,
embedded in both `TypesAPI` and `CollectionsAPI` and addressed by owner
id: `Properties`, `AddProperty`, `RemoveProperty` and `PatchProperty`
accept a type id or a collection id through either accessor. The
type-only surface (`Parts`, `AddPart`, `AddDataset*`, `Patch`,
`Datasets`, `Types().Get`) returns `ErrNotAType` for a collection id.

A **registered collection** is declared in
`config.Config.Collections []handler.Collection{Id, Name, Description,
IconCID, Properties []PropertyDecl, Hidden}` and validated at
`sdk.Open` (unique ids, disjoint from `Types` and reserved ids). It
appears in `Collections().List` / `Get` with `BuiltIn` set, validates
writes like a registered type, and never changes at runtime
(`ErrTypeRegistered`).

#### Module namespaces

A registered module (`handler.Module.Properties`, docs/user-datasets.md) may declare
values on the objects row under its own name, e.g. `chat.unreadCount`,
`chat.notifyMode`. The namespace is never a membership value: the
pre-flight grants it to a row whose type declares a dataset of that
module (`Store.ModuleGrants`), at runtime or through
`handler.Type.Parts`. Read tracking writes a module collection's unread
counters there. It uses the same registry overlay as a registered
type's properties, so `Properties().Set(objectId, "<module>", …)` routes
by declared scope. A module reserved for the consumer's own installs is
registered `Reserved` (docs/user-datasets.md § Model).

### Property ids

A user-defined property's id derives from the change that created its
definition record:

```
propId = base58(xxh3-64(changeId))   // up to 11 chars
```

This is the CRDT layer's empty-id resolution (CRDT spec §3.3,
`crdt.DeriveRecordId`). The same string is the definition's record id
and the field key for values on objects. Bundle-declared properties
derive their ids from `(rootId, xKey)` instead (docs/bundles.md).

Built-in properties (on `any`, `type`, `collection`, `spaceIndex`) use
human-readable ids such as `name`, `description`, `icon`, which never
collide with generated ids.

Two concurrently created "Rating" properties get two ids and are fully
independent: neither is canonical. Consolidating same-named properties
is a user or agent concern; the SDK has no rule for it.

### Property record shape (in the space's `objects` system collection)

The CRDT dataset name is `objects`; `SystemPropertiesHandler` is wired
through the Controller's shared-collection override, so every object's
writes land in its one row of the per-space collection. Values are
namespaced by owner id (the type or a collection; a short id for
built-ins), then keyed by propId:

```json
{
  "id": "objectId",
  "any":              { "name": "Heat", "description": "…",
                        "type": "{movieTypeId}", "collections": ["{watchedCollectionId}"] },
  "{movieTypeId}":    { "Y9Hxx5xmYmF": ["personA","personB"], "EwyHGrtTdxB": 1995 },
  "{watchedCollectionId}": { "e5vwLLgBRiM": 8.2 }
}
```

The human `name` lives only on the definition; renaming never touches
stored values.

### Orphan values

Values outside the object's current schema are kept, never wiped, and
converge on every peer:

- **Retype / detach / re-attach.** `SetType` and `DetachCollection`
  leave the old `{ownerId}.*` bag in place; setting or attaching that
  owner again reveals it unchanged. The bag is plain LWW data,
  independent of the membership fields.
- **Concurrent detach and write.** Inbound validation does not consult
  `any.type` / `any.collections`, so the value lands as an orphan on
  every peer. An apply-side membership guard, or parking such writes,
  would be non-convergent (§ Membership and the local write pre-flight).
- **Removed definitions.** `RemoveProperty` leaves per-object values in
  place; later writes to that propId drop per op via the
  unknown-property rule.
- **No kind conflicts per propId.** A propId's kind is pinned for life;
  changing a kind means removing and re-adding, which mints a new
  propId. A live propId validates identically on all peers, and stale
  values linger only under dead propIds, which can never be reused.
- **Clients ignore unknowns.** `Properties().Get()` returns the raw
  record; the SDK does not project by current schema. A client must
  ignore any propId absent from the owner's current schema and any
  namespace the object no longer has.
- The built-in namespaces need no guard: `any` is universal and never a
  membership value, and `type` / `collection` are granted off the
  marker.

### Property definitions (inside a definition object's `properties` dataset)

One record per property:

| Field | Value | Mutability |
|---|---|---|
| `id` | `base58(xxh3-64(changeId))` | Immutable (record id) |
| `kind` | `string` / `number` / `boolean` / `null` / `array` / `object` / `datetime` | Pinned |
| `scope` | `synced` / `account` / `local` (default `synced`) | Pinned |
| `items` | Sub-shape for arrays | Pinned |
| `properties` | Sub-shapes for objects | Pinned |
| `name` | Human label | Mutable |
| `description` | String | Mutable |
| `x-key` | Caller-side mapping key; not unique, not enforced | Mutable |
| `meta` | Consumer flags, string → string | Mutable per key |
| `x-format` | Opaque descriptor object | Mutable at every path |
| `required` | `[]string`; stored, not enforced | Mutable |

Nested shapes are stored inline:

```json
{ "id": "Y9Hxx5xmYmF", "name": "credits", "kind": "object",
  "properties": {
    "writer": { "kind": "string" },
    "budget": { "kind": "number" }
  } }

{ "id": "3XEMVWA6EK", "name": "actors", "kind": "array",
  "items": { "kind": "string" } }
```

### The `x-format` descriptor

`x-format` holds everything descriptive about a property beyond its
`kind`: semantic slug, icon, ordering key, option set, relation targets,
per-format config. Dataset fields carry the same bag (docs/user-datasets.md). The SDK
stores it opaquely:

```json
{ "id": "3XEMVWA6EK", "name": "Stage", "kind": "array", "items": { "kind": "string" },
  "x-format": { "type": "choice", "pos": "a0",
                "config": { "multiple": false },
                "options": { "lead": { "name": "Lead", "color": "grey", "pos": "a0" } } } }
```

- **Two structural rules.** It is an object, at creation and on any
  later whole-bag `$set`, and a creation writes it whole (dotted
  `x-format.*` keys in a creation change are rejected). No key inside is
  known to the SDK.
- **Every path is CRDT-mutable** via `PatchProperty`, with any JSON
  value. A nested object's keys are edited independently (two authors
  adding two options both land); a leaf is replaced whole. Which members
  are nested and which are leaves is the consumer's design (e.g. a
  filter stored as one JSON-text leaf so two conditions never merge into
  garbage).
- **Built-ins carry descriptors too.** The properties of `any`, `type`,
  `collection` and `spaceIndex`, and every SDK-declared dataset field
  (tech-space datasets, `bundles`, `payloads`, the objects row's derived
  root fields), declare a `description`, plus an `x-format` where the
  consumer vocabulary names the value (display text, instants, flags).
  System values (identities, ids, CIDs, numbers, enums, opaque objects)
  carry a description only. `Types().Properties` and discovery return
  them like user definitions. Synced `any.*` values are described on the
  `any` type's properties, not on the `objects` discovery document.
- **`kind` is the guarantee, `x-format` is a hint.** Values are
  validated against `kind` at apply on every peer, never against the
  descriptor. Whether a value fits the slug (a link is a well-formed
  `any://` URI, a `date` is midnight UTC) and the leaf-only patch rule
  are checked by the consumer at its write boundary. Reference integrity
  is lazy, at read time.
- A definition without `x-format` renders from `kind`. Registered types
  declare descriptors through `handler.PropertyDecl.Description` /
  `.XFormat`.

### Immutability rules

- **Pinned** (`kind`, `scope`, `items`, `properties`, and `key` on
  records that carry it): an edit to an existing record is dropped at
  apply on every peer, and `PatchProperty` rejects it before writing. To
  change them, define a new property.
- **Mutable**: `name`, `description`, `x-key`, `required`, `meta.<k>`,
  `x-format` and every path under it. Edits merge per path; no shortId
  is minted.

Adding or removing a definition mints a new shortId for its owner;
property writes are stamped with owner shortIds and parked on peers
that lack that schema state (types-properties-proposal.md).

### Read tolerance

- Unknown propIds in a record are returned as-is.
- Values violating the current definition are returned as-is.
- The client decides how to render out-of-spec values. There is no
  `valid` flag and no rejection on read.

### Dataset schemas

Datasets are declared on types as parts, served by the built-in
`records` module (a runtime, client-described schema) or by compiled-in
modules: docs/user-datasets.md.

The SDK enforces no record or value size limits; consumers limit sizes
at their write boundary.

### Open questions

- **Coarse DataVersion over-gates.** A property write is stamped with
  the owner's latest shortId, not the schema state of the props it
  touches. A write to a stable `propX` pinned to a shortId minted by an
  unrelated `propY` addition parks on a peer that has not received that
  addition. It converges once delivered; per-prop pairs would remove the
  dependency.
- **Value scans match orphans.** "Clients ignore unknowns" covers
  rendering, not queries: `find({"{deadPropId}": x})` can match orphan
  data. A fix would be a query-layer filter, not a storage change.
- **Broken references** are not surfaced; a read-time `_refs`
  annotation is a candidate.

## Queries and subscriptions

### Query API

`Space.Query(objectId, dataset)` and `Space.QueryObjects()` return a
`space.Query` builder over any-store:

- `Filter(any)` — a built `query.Filter`, a JSON string or a map with
  Mongo-style operators, parsed eagerly; errors surface on the terminal
  call.
- `Sort`, `Limit`, `Offset`, `Projection` (which reserved fields appear).
- Terminals: `Iter`, `All`, `One`, `Count`, `Snapshot`, `Subscribe`.
- Results are `*anyenc.Value`.

Pagination is offset-based; cursor pagination for large result sets is
an open question.

### Subscriptions / Event Flow

- **`Query.Subscribe(ctx, opts)`** returns `*QueryResult{Initial, Total,
  HasNext, Sub}` for the chained filter / sort / limit. `Snapshot(opts)`
  returns the same shape without `Sub`. There is no raw event firehose.
- **Engine**: `internal/subscribe.Engine`, one per loaded space, owned by
  `spaceobjects.Store`. `engine.mu` serializes register, close and
  per-event apply; the after-apply hook calls `engine.OnApply` only when
  `HasSubscribers()`.
- **Mailbox**: `github.com/cheggaaa/mb/v3` per subscription, capacity
  `MailboxCapacity` (default 256, minimum 16). Consumers read with
  `sub.Events().Wait(ctx)` or `WaitOne(ctx)`.
- **Scope**: the shared `objects` dataset (`QueryObjects`) or one
  `(objectId, dataset)`. Members queries do not support `Subscribe`
  (`ErrSubscribeUnsupported`).
- **Events**: `SubscriptionEvent{VersionId, Added, Updated, Removed}`.
  `Added` / `Updated` are `[]SubRecord{Id, Doc, Ops}`: the full
  post-apply value (deep-cloned, safe to retain) and the projected
  `$set` / `$unset` ops. `Removed` is `[]RemovedRecord{Id, Reason}`:
  - `RemoveDeleted` — tombstoned, gone;
  - `RemoveFilteredOut` — no longer matches the filter, still exists;
  - `RemoveDisplaced` — pushed past `Limit`, still matches.
- **Window**: with `Limit > 0` the engine holds `Limit + 1` rows; the
  extra row is a hidden sentinel, so a single arrival at the top of the
  sort needs no re-query. `Limit == 0` is unbounded; memory grows with
  the matching set.
- **Snapshot fence**: the initial read runs under `engine.mu`, so events
  applied during the read queue behind it and process after the
  subscription is registered. The snapshot callback must not load
  objects or write to the DAG: both need `engine.mu` and deadlock.
- **Overflow and drift** close the subscription: a full mailbox with
  `ErrSubscriptionOverflow`; more than `DriftBudgetPercent` (default 30)
  of `Limit` records leaving the window without replacement with
  `ErrSubscriptionDrifted`. Resubscribing is the only recovery.
- **Total**: `IncludeTotal` runs one `Count` at snapshot time; it is not
  maintained live.

### Consistency

- Reads see the caller's own writes.
- Events fire after the any-store transaction commits, never before.
- any-store reads have snapshot isolation.

## Storage Topology

All SDK data lives in one any-store DB. `config.Storage.Topology`
declares `StoragePerSpace`, but it is not implemented: the field is not
read.

| | Shared DB | Per-space DB |
|---|---|---|
| Cross-space transactions | Yes | No |
| Space deletion | Row and collection sweeps | File delete |
| Writers | One (any-store single writer) | One per space |

Per-space DBs would be selected by a `dbRouter` (scope → DB). Open: its
scope key (spaceId, tech, device), and whether the tech space gets its
own DB. Choosing needs performance testing.

Local-scope values live in the same DB as synced data, versioned by
locally minted lexids in the same `_ver` tree (CRDT spec §9). They are
not backed up and do not survive an app reinstall.
