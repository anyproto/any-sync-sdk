# Data Structure

## Vision
Main interface for data retrieval: any-store queries + subscription to event flow. System collections: `spaces` (global), `objects` (per-space system collection for properties), plus per-object datasets. Some property values can be overridden at device level (local) or account level (tech space).

## Object Properties

Every object has properties — a map with required fields (`id`, `name`, `description`, `author`, optionally `type` and `collections`). All objects in a space share one system collection `objects`: **one record per object**, `id` = any-sync objectId.

### Property scopes (decision 2026-06-12 — scope on the declaration)

Scope is a fixed attribute of the property DEFINITION (and of declared
dataset schema fields), from the unified `schema.Scope` taxonomy. A
property lives in exactly one scope, pinned at creation like `kind`;
changing scope = defining a new property (new propId). Full design and
rationale: [`scoped-properties-proposal.md`](scoped-properties-proposal.md).

| Scope | write route | version domain | syncs to | overridable |
|------|------|------|------|------|
| **derived** | handler-stamped (`id`, `author`, `spaceId`, `createdAt`, `modifiedAt`, `modifiedBy` — the times are `datetime` instants, the identities account ids) | triggering change | (computed convergently) | no (read-only) |
| **synced** | the object's own CRDT change | object tree | everyone with access | n/a — no override stack |
| **account** | carrier record in tech space + per-device mirror | tech tree | this account's devices | n/a |
| **local** | `Object.LocalSet`, no DAG | local lexid | this device only | n/a |

> **Reading the time stamps.** They are `datetime` instants, not epoch
> numbers: a filter literal has to be one too (`{"createdAt": {"$gte":
> {"$date": "2026-01-01T00:00:00Z"}}}`). A bare number does not error —
> any-store decides cross-type comparisons by type rank and instants
> rank above numbers and strings, so `$gte` matches every row whatever
> the date, and `$lt` / `$eq` match none. A filter that forgets the
> wrapper returns a wrong answer, not an empty one.
>
> Sorting (`-modifiedAt`) is unaffected in a converged store. While a
> re-index is in flight the collection can hold both shapes at once, so
> that window sorts as two type-grouped blocks — it closes when the
> space's sweep finishes (docs/08-versioning.md).
>
> **`modifiedAt` / `modifiedBy` are object-level.** They move on every
> synced write to the object — a property write and a write to any of
> its datasets (editor blocks, chat messages, runtime datasets) alike,
> a record delete included — so `-modifiedAt` orders objects by their
> latest change, whatever it touched, and `modifiedBy` names the
> account that signed that change (`author` stays the creator). The
> `objects` handler is a `crdt.ObjectStamper`: a change on another
> dataset stamps the object's row in the same transaction, with the
> change's VersionId (LWW like any field). One change stamps the pair
> and it moves together, so a converged row pairs the time with the
> signer of that very change; concurrent writers resolve by VersionId,
> not by clock — the row names the ordering-max change's signer, which
> need not be the one whose wall clock is latest. Local- and
> account-route writes never move them. A row that does not exist yet
> is not created by a stamp — it gets them when it is created
> (order-dependent: a content change applied before the row's creating
> change, a parked create draining later, leaves the row with the
> creating change's stamps until the object's next synced write);
> deleting the objects row itself does not move them, and a tombstoned
> row keeps its last stamps.
>
> `modifiedBy` is an account id: resolve it through `Space.Members()`
> for a current member or `SDK.Identities()` account-globally; it can
> name an account since removed from the space. `modifiedAt` is
> indexed (the default list sort), `modifiedBy` is not — filtering by
> it scans the collection. An absent `modifiedBy` means the row was
> built before the field existed and awaits its rebuild
> (docs/08-versioning.md), or the latest change's signer is unknown —
> never "nobody modified it".

There is **no per-value override stack and no priority merge** — the
earlier auto/base/account/device variant model (priority `device >
account > base`, per-record `_base`/`_account`/`_device` bags, computed
root) was rejected: ~80% of its complexity (row migration, root
derivation, projection stripping, shadowed-write event semantics,
three-domain `_ver` presentation) paid for the override stack alone.
Wanting both a shared and a personal "favorite" means two properties;
clients compose if they care.

### Storage

Values of every scope sit at their normal `{ownerId}.{propId}` paths in
the object's row — no reserved scope fields, no migration from the
pre-scope layout (everything that existed was synced-scope). One `_ver`
tree per record; version domains coexist because no two routes ever
write the same path (the scope pin + apply-side route enforcement keep
them disjoint — see CRDT spec §9).

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

The derived built-ins are the one exception to the `{ownerId}.{propId}`
layout: `id`, `author`, `spaceId`, `createdAt`, `modifiedAt` and
`modifiedBy` sit at the row root, not under `any.*`, although
`Types().Properties("any")` lists them. Filter and sort them by their
bare names (`{"modifiedBy": …}`, `sort: ["-modifiedAt"]`);
`any.modifiedBy` matches nothing.

In a shared space, account- and local-scoped values reflect THIS
account/device — queries filtering on them select per-account /
per-device result sets ("my value" semantics). Two documented
footguns: agents must not condition SHARED mutations on account/local
values, and the local search index intentionally indexes the local
view.

### Handlers / enforcement
- **`SystemPropertiesHandler`** serves the synced route only. Besides
  kind validation it enforces route-vs-scope: an inbound DAG op whose
  target propId is declared `account`/`local`/`derived` is dropped
  per-op (convergent — the DataVersion gate parks changes whose schema
  hasn't synced, so the lookup never races the definition).
- **Account mirror** (tech-space watcher; see the proposal §"Account
  transport"): applies converged carrier values into target rows as
  injected applies stamping tech versionIds; unknown propIds are
  skipped and retried by the state-based re-mirror on space load.
- **Local route**: `Object.LocalSet`; validation is writer-side
  (`Properties.Set` resolves scopes to route anyway).
- Write API: one auto-routing `Properties.Set` — a patch resolving to
  more than one scope is rejected (routes commit independently across
  version domains; no cross-route rollback).

### Scopes on dataset fields

Dataset schema fields use the same taxonomy and may declare `account`
(e.g. a per-account `read` flag on `chat_messages`) alongside the
existing `synced`/`derived`/`local`. The account carrier keys records
by `(objectId, dataset, recordId)` — the objects row is the degenerate
case. This supersedes the earlier "Not Expanding This Pattern to All
CRDT Data" decision: that rejection targeted the override/variant model
(shadow-variant GC, merge consistency); a scope-declared field has no
shadowing — it is just a field with a different write route.

## Types, Properties & Data Schemas

Refines and extends the "Object Properties" section above. Covers what types are, how property definitions are stored, how schemas evolve under CRDT, and what's deferred to later versions.

### Type and collections

```
Space
 └─ Object
     ├─ any.type          one type, or none — what the object IS
     ├─ any.collections   any number of collections — what it is filed UNDER
     └─ owns the datasets its type declares (Mongo-like record collections)
```

An object has exactly ONE type and any number of collections. A **type** is functional: property definitions, parts (the datasets its objects carry) and a layout. A **collection** is categorizing: property definitions only — no parts, no layout. Values live at `{ownerId}.{propId}` on the object's row for the type and for every collection alike.

`any.type` is a scalar LWW register: retyping is a `$set` (`Properties().SetType`), clearing it an `$unset` (`UnsetType`), and it may be absent — the object then has no parts and renders as properties. `any.collections` is a set, edited with `$addToSet` / `$pull` (`Properties().AttachCollection` / `DetachCollection`). Membership is structural and shared, so all four writes route through the object's own CRDT (synced scope).

Both kinds are objects in the space, and both carry a reserved **marker** in `any.type`: `__type__` on a type object, `__collection__` on a collection object. The marker is what identifies a definition — a definition object has no type of its own.

Every object implements the built-in `any` — universal properties (name, description, icon, tags, `type`, `collections`, id, author, createdAt, modifiedAt, modifiedBy). Two more built-ins describe definitions:

- `type` — the meta-type namespace, granted to rows carrying `__type__`. Contributes `xkey` (the type's programmatic handle), `layout` (the rendering descriptor — `{type, config}`, the x-format shape, written whole, opaque to the SDK), `hidden` (keeps the type out of default listings and pickers; a bundle root asks for it when it only hosts its records — docs/bundles.md § Bundle-declared definitions) and `meta` (the open bag of consumer flags — one scalar per single-level key, written per key so concurrent writers merge; opaque to the SDK), plus the `properties`, `shortIds` and `datasets` datasets (`datasets` holds the type's parts and their dataset definitions — docs/17-user-datasets.md).
- `collection` — the meta-collection namespace, granted to rows carrying `__collection__`. Contributes the same `xkey` / `hidden` / `meta` and the same `properties` / `shortIds` datasets, and nothing else: a collection has no layout and no `datasets`.

Marker and namespace are different strings because a `_`-prefixed top-level field is protocol-owned, so the marker cannot double as a storage namespace; the handler grants `type.*` / `collection.*` off the marker alone, and a row that merely names the meta id as its type reaches neither. Keeping `xkey` there rather than on `any` is what makes it unwritable on a row that is not a definition. `Types().Patch` rewrites the display and rendering metadata (`any.name` / `any.description` / `any.icon`, `type.layout` / `type.hidden`) in one change and patches `type.meta` per key (a nil value unsets); `Collections().Patch` does the same over `collection.*`. Nested writes on the objects row — `{ownerId}.{propId}.{key…}` — are admitted only under an object-kind property, `$set` only, and merge per leaf; below any other kind the op is dropped (`SystemPropertiesHandler`), and the container ops (`$inc`, `$addToSet`, `$pull`) address the property itself.

A definition object **implicitly implements itself**: its row carries only the marker, yet it may hold `{ownId}.{propId}` values and the records of the datasets it declares. That is how a bundle root keeps its own data on the root (favourites entries, an app's layouts) with no extra flag. It never matches a member query — `{"any.type": typeId}` returns the objects of that type, never the type object.

Registered types (`config.Config.Types`) carry the type shape statically: `handler.Type.Parts` declares their parts (static datasets by name, module datasets by module — docs/17 § Static parts on registered types) and `handler.Type.Hidden` their listing flag; `Types().Parts` / `Datasets` return the compiled view and the runtime mutators answer `ErrTypeRegistered`. Registered collections (`config.Config.Collections`) carry the collection shape the same way — see § Collections. A bundle may declare either kind on its root — parts, properties with ids derived from `(rootId, xKey)`, layout, hidden — so one install converges on one definition across devices (docs/bundles.md).

Built-ins exist as **derived objects** in every space (well-known ids, uniform with user definitions — no "built-in vs user" fork in query/UI code).

The membership fields are a fresh on-disk and wire shape — accepted without migration as a pre-release decision (new accounts); no rebuild version was bumped, since a replay could not translate the old list.

#### Membership and the local write pre-flight

A local write to `{ownerId}.{propId}` is admitted when ownerId is one of the object's members AFTER this change: the universal `any`, its type (the row's, or the one this change sets), its collections (the row's plus the ones this change adds), the meta namespace its marker grants, its own id when it carries a marker, and the module namespaces those members grant. Anything else rejects the write whole with reason `type_not_implemented` — "set it as any.type or add it to any.collections".

Slots are strict. A KNOWN collection id written to `any.type`, or a KNOWN type id (or a marker) added to `any.collections`, is refused with reason `wrong_slot` (`space.ErrWrongSlot`). An id this device cannot resolve — a definition that has not synced yet — passes: offline-first outranks slot hygiene. The check lives in the handler's pre-flight, so a raw `$set` through `Modify` is covered, not just the typed API.

Inbound apply stays read-tolerant: there is **no apply-side membership guard**. `any.type` and `any.collections` are themselves concurrent CRDT values, so gating applies on them would make the outcome apply-order-dependent (non-convergent). A value written concurrently with a retype or a detach lands as orphan on every peer and converges — see §"Types & Property Lifecycle".

#### Querying membership

`{"any.type": typeId}` returns the objects of that type; `{"any.collections": collectionId}` the members of that collection (any-store matches an array field element-wise). Neither ever returns the definition object itself, which carries a marker rather than its own id — client filters need no marker exclusion. `any.type` carries a dense index on the per-space `objects` collection (every object has a type or a marker), `any.collections` a sparse one.

#### Collections

```go
// space.CollectionsAPI, from Space.Collections()
List(ctx) ([]CollectionInfo, error)                 // meta `collection` + registered + user; Hidden included
Get(ctx, collectionId) (CollectionInfo, error)      // a type id → ErrNotACollection
Create(ctx, CollectionCreateParams) (id, error)     // Name, Description, IconCID, XKey, Hidden, Meta
Patch(ctx, collectionId, CollectionPatch) error     // registered → ErrTypeRegistered; a type id → ErrNotACollection
Delete(ctx, collectionId) error                     // declared, not implemented — mirrors Types().Delete
```

`Create` mints the collection object and writes `any.{name,description,icon}`, `collection.{xkey,hidden,meta}` and `any.type = "__collection__"` in one change.

The two listings are disjoint over objects: `Types().List` returns the built-ins (`any`, `spaceIndex`, `type`, `collection`), every registered type and every user type object, never a collection object; `Collections().List` the mirror.

Property definitions are ONE surface, `space.PropertyDefsAPI`, embedded by both `TypesAPI` and `CollectionsAPI` and addressed by the owning definition's id: `Properties(ownerId)` / `AddProperty(ownerId, draft)` / `RemoveProperty` / `PatchProperty` accept a type id or a collection id through either accessor. The type-only surface (`Parts`, `AddPart`, `AddDataset*`, `Patch`, `Datasets`, …) answers `ErrNotAType` on a collection id, and `Types().Get` does the same.

A **registered collection** is the compiled-in twin: `config.Config.Collections []handler.Collection{Id, Name, Description, IconCID, Properties []PropertyDecl, Hidden}`, validated at `sdk.Open` (unique ids, disjoint from `Types` and the reserved ids). It surfaces through `Collections().List` / `Get` with `BuiltIn` set, its properties validate on write exactly like a registered type's, and its declarations never mutate at runtime (`ErrTypeRegistered`).

#### Module namespaces

A registered module (`handler.Module.Properties`, docs/17) may declare values on the objects row under its own name — `chat.unreadCount`, `chat.notifyMode`. The namespace is never a membership value: the local write pre-flight grants it to a row when the row's type declares a dataset of that module (`Store.ModuleGrants`) — at runtime or statically through `handler.Type.Parts` — and the read-tracking service writes a module collection's unread counters there. Same registry overlay as a registered type's properties, so `Properties().Set(objectId, "<module>", …)` routes by the declared scope. A module the consumer keeps for its own installs is registered `Reserved` (docs/17 § Model).

### Property ids

Every user-defined property has a property id derived from the changeId of the change that created its definition record:

```
propId = base58(xxh3-64(changeId))   // up to 11 chars
```

This is the default empty-id resolution the CRDT layer already produces (see CRDT spec §3.3 / `crdt.DeriveRecordId`). Properties use it directly as their record id, and the same string is used as the field key under which values are stored on objects (see below).

Built-in properties (on `any`, `type`, `collection`) use **hardcoded human-readable ids** like `"name"`, `"description"`, `"icon"`. They never collide with user property ids — 11-char base58 never produces those strings.

### Property record shape (in the space's `objects` system collection)

One record per regular object, `id = objectId`. Values are namespaced by the owning definition's id — the object's type or one of its collections (a short id for built-ins) — and within each namespace keyed by `propId`:

The CRDT-side dataset name on regular objects is also `objects` — the `properties.SystemPropertiesHandler` is wired against the per-space `objects` collection via the Controller's shared-collection override, so every regular object's writes coalesce into one row in that collection. Type objects do NOT register this handler; their own `any.name`/`any.description`/etc. live on per-type-object storage and don't appear in the per-space `objects` collection.

```json
{
  "id": "objectId",
  "any":              { "name": "Heat", "description": "…",
                        "type": "{movieTypeId}", "collections": ["{watchedCollectionId}"] },
  "{movieTypeId}":    { "Y9Hxx5xmYmF": ["personA","personB"], "EwyHGrtTdxB": 1995 },
  "{watchedCollectionId}": { "e5vwLLgBRiM": 8.2 }
}
```

Storage path is `{ownerId}.{propId}` — one shape for the type and every collection. The human `name` ("actors", "rating", …) lives only on the property definition record; renaming never touches stored values.

Retyping and detaching leave the old namespace's values in place: they become orphan data, read-tolerant.

### Property definitions (inside a definition object's `properties` dataset)

One record per definition. Shape:

| field | kind | mutability |
|---|---|---|
| `id` | string = `base58(xxh3-64(changeId))` | immutable (record id) |
| `type` | enum: `string` / `number` / `boolean` / `array` / `object` | first-write-wins on the record |
| `name` | string (human label) | CRDT-mutable |
| `description` | string | CRDT-mutable |
| `x-key` | string (caller-side mapping key) | CRDT-mutable |
| `x-refType` | string (typeId) | superseded by `x-format.relation` (never implemented) |
| `meta` | object (string → string) | CRDT-mutable consumer flags |
| `x-format` | object — the opaque descriptor | CRDT-mutable, every path (see below) |
| `enum` | array | additive (client-soft) |
| `items` | object | recursive sub-shape (arrays) |
| `properties` | object | recursive sub-shape (objects) |
| `required` | array<string> | CRDT-mutable |
| constraints… | various | loosening allowed (client-soft) |

`id` is the stable machine identifier used for storage. `name` is the human label. **`x-key`** is an optional mutable annotation a caller can use to map a property to a stable code-side identifier (e.g. generated client code references properties by `x-key` rather than by id). Not enforced, not unique — just metadata.

Concurrent creates of two "Rating" properties produce two different ids; both exist fully as independent properties. Reconciling same-named properties is a **user/agent concern**, not an SDK CRDT rule.

### Recursive shape (JSON-Schema-subset)

Full nesting is supported and stored inline. Example object-typed property:

```json
{
  "id": "Y9Hxx5xmYmF",
  "name": "credits",
  "type": "object",
  "properties": {
    "writer":  { "type": "string", "x-refType": "{personTypeId}" },
    "studio":  { "type": "string" },
    "budget":  { "type": "number" }
  },
  "required": ["writer"]
}
```

Array-typed:

```json
{ "id": "3XEMVWA6EK", "name": "actors", "type": "array",
  "items": { "type": "string", "x-refType": "{personTypeId}" } }
```

any-store's dotted-path `$set` handles deep edits (`$set: {"properties.editor": {"type":"string"}}`). Concurrent additions of different sub-fields merge; edits to the same sub-field follow field-level LWW.

**`x-refType`** — superseded before it was ever implemented. Its use case ("this value points to objects of this type") is a consumer convention inside `x-format` (`{"type": "relation", "relation": {"targetTypes": […]}}`). Kept in the table for historical context only.

### The `x-format` descriptor

`x-format` is everything descriptive about a property beyond its structural `kind` — the semantic slug, icon, ordering key, option set, relation targets, per-format config. The same bag sits on a dataset field (docs/17). The SDK stores it **opaquely**:

```json
{ "id": "3XEMVWA6EK", "name": "Stage", "kind": "array", "items": { "kind": "string" },
  "x-format": { "type": "choice", "pos": "a0",
                "config": { "multiple": false },
                "options": { "lead": { "name": "Lead", "color": "grey", "pos": "a0" } } } }
```

- **Two structural rules, nothing else.** It is an object — at create and on any later whole-bag `$set` — and a creation writes it whole (dotted `x-format.*` keys in a creation change are rejected, so there is exactly one creation shape to validate). No key inside is known to the SDK.
- **Every path under it is CRDT-mutable** — the slug included — with any JSON value, via `PatchProperty`. Members follow the documented per-path LWW: a nested object's keys are edited independently (two authors adding two options both land), a single leaf replaces whole. Which members are nested and which are single leaves is the consumer's design (e.g. a filter is stored as one JSON-text leaf so two conditions never field-merge into garbage).
- **Built-ins carry the same slice.** The hardcoded properties of `any`, `type`, `collection` and `spaceIndex` and every SDK-declared dataset field (the tech-space datasets, `bundles`, `payloads`, the `objects` row's derived root fields) declare a `description`, and an `x-format` where the consumer vocabulary names the value — display text, instants, flags. `Types().Properties` and discovery return them exactly like a user definition's; system values (identities, ids, CIDs, numbers, enums, opaque objects) carry a description alone. The synced `any.*` values are described on the `any` type's properties, not on the `objects` discovery document — they are not row-root heads.
- **`kind` is the guarantee, `x-format` is a hint.** Values are validated against `kind` at apply on every peer, never against the descriptor. Whether a value fits the slug — a link is a well-formed `any://` URI, a `date` lands on midnight UTC — is checked by the consumer at its write boundary (the `any` server), and the leaf-only patch rule ("a set targets a leaf, never a container") is enforced there too. Reference integrity stays lazy/read-time.
- A definition without `x-format` renders structurally from `kind`. Registered (built-in) types declare theirs through `handler.PropertyDecl.Description` / `.XFormat`, surfaced by `Types().Properties()` exactly as written.

### Immutability rules

**CRDT-hard (enforced at apply on every peer, convergent)**

- **Per-record first-write-wins on `id` and `type`.** `id` is immutable (it's the record id); `type` is locked by the first write to the record. Each property is its own island — no cross-record binding, no silent-ignore rules between different property records.

**Client-soft (SDK refuses to emit; honest clients comply, misbehaving peers bounded by read-tolerance)**

- Changing a sub-field's primitive type.
- Changing `items.type`.
- Removing sub-fields from object-typed properties.
- Narrowing enums (removing values).
- Tightening numeric / string constraints.

**Freely mutable**

- `name`, `description`, `x-key`, `meta.<k>`, `x-format` and every path under it, sub-field additions, enum additions, constraint loosening.

### Same-name properties are not a conflict

Two properties with the same `name` ("Rating") are two different ids, each with its own independent definition and value column. Neither is canonical; neither is shadow. Clients see both; UIs can hint "N definitions share this name"; agents or humans consolidate if needed. The SDK has no opinion.

### Read tolerance

- Unknown property ids in a record → returned as-is.
- Values violating the current definition → returned as-is.
- Client decides how to render / handle out-of-spec values.
- No `valid` flag; no rejection; no `waitType` for properties.

### Why no versioning for properties

- Properties are small and bounded; a hostile or buggy write can't run away.
- Per-record immutability of `id` and `type` + client-soft rules at write time + read tolerance together provide enough protection without a version / stamping mechanism.
- Dropped from the design: `breaking` flag, `currentSchemaVersion`, `schemaVersion` stamping on data changes, `waitType`, re-validation cascades.

### Data schemas (v1)

Dynamic, type-defined data schemas for user datasets are **deferred**. In v1:

- Only **hardcoded SDK-side schemas** exist for built-in system datasets (`properties`, `objects`, `members`, future `chatMessages`, …). These are Go code, shipped with the binary, versioned with the SDK.
- User datasets owned by types have **no schema enforcement** in v1.
- A type object MAY have a `datasets` dataset — a plain registry of dataset names it owns. Records: `{ id, name, description? }`. No schema payload.

Rationale: data is unbounded and may have special apply semantics (chat ordering, TTL, dedup). Dynamic schemas there need migration / versioning / coordination that v1 shouldn't carry.

### Size / structural limits

Not enforced at the CRDT layer in v1. API layer owns this.

### Self-describing

The property-definition shape is expressible in the same JSON-Schema-subset we use for everything else. Embedded as a Go constant for the `type` built-in. No format fork between built-in and user types.

### Open points

- Concrete built-in set beyond `any` / `type` / `collection` — candidates: `relation`, `file`, `member`. Not in v1.
- Built-in dataset catalog API for introspection — deferred.
- Reference-integrity surfacing (broken refs) — deferred, probably a `_refs` annotation at read/query time.
- Whether derived built-in type objects materialize on first touch or exist from space creation — both work; pick during implementation.

---

## Current any-store Capabilities

### Query System
```go
// Create query
query := collection.Find(filter)

// Chain options
query = query.Sort("-updatedAt").Limit(20).Offset(0)

// Execute
iter, err := query.Iter(ctx)
for iter.Next() {
    doc := iter.Doc()
    // doc.Value() → *anyenc.Value
}
```

### Filter Parsing (MongoDB-style)
```go
filter := query.ParseCondition(`{"status": {"$in": ["active", "pending"]}, "count": {"$gt": 5}}`)
```

Supported comparisons: `$eq`, `$gt`, `$gte`, `$lt`, `$lte`, `$ne`, `$in`, `$nin`, `$exists`, `$all`, `$elemMatch`, `$and`, `$or`, `$not`, `$nor`, `$regex`, `$size`, `$type`.

### Modifiers
```go
modifier := query.ParseModifier(`{"$set": {"name": "foo"}, "$inc": {"counter": 1}}`)
collection.UpdateOne(ctx, filter, modifier)
```

### Indexes
```go
collection.EnsureIndex(ctx, IndexInfo{
    Name:   "idx_status",
    Fields: []string{"status", "-createdAt"},
    Unique: false,
    Sparse: false,
})
```
CBO (Cost-Based Optimizer) picks best index automatically.

### Transactions
```go
tx, _ := db.WriteTx(ctx)
defer tx.Rollback()
coll := tx.Collection("objects")
coll.Insert(ctx, doc)
tx.Commit()
```

### No Built-in Subscriptions
any-store has no native pub-sub. SDK needs to build event flow on top:
- `durability.OnWriteEvent()` callback exists but is for crash recovery, not user subscriptions
- SDK must implement change tracking + notification layer

## Proposed System Collections

| Collection | Scope | Contents |
|------------|-------|----------|
| `spaces` | global | Space metadata, join status, sync state |
| `objects` | global or per-space | Object metadata, type, heads |
| `accountSettings` | global (tech space) | Account-level preferences |
| `spaceSettings` | per-space | Space-level preferences |

### Settings Layering
```
Query result = merge(deviceLocal, accountLevel, defaults)
```
- **Device-local** — stored in local any-store DB, not synced
- **Account-level** — stored in tech space, synced across devices
- Merge strategy: device-local overrides account-level overrides defaults

## Source files
- `any-store/query.go` — Collection.Find, Query interface
- `any-store/query/filter.go` — filter parsing
- `any-store/query/modifier.go` — modifier parsing
- `any-store/query/sort.go` — sort parsing
- `any-store/internal/qplanner/` — CBO query planner
- `any-store/tx.go` — transactions

## Key Decisions

### Query API
- **Typed layer over any-store** — SDK wraps any-store's query API to enable validation, rewriting, and tracking. Not a raw passthrough
- **Query wrapper** — returns something like a wrapper around `anystore.Query` with helpers: `projection()`, `iterate()`, `all()`, `one()`, etc.
- **Return values** — keep `anyenc.Value` as the unit; it already has typed getters and `.String()` → JSON
- **Pagination** — TBD; start with what any-store offers (offset), revisit if needed
- **Cross-collection queries** — probably needed eventually, **not in v1**
- **Property scopes in queries** — nothing to hide: values of every scope sit at their normal paths and reflect the local account/device view

### Subscriptions / Event Flow
- **Single API: `Query.Subscribe(ctx, opts)`** — windowed live queries. Chain `Filter / Sort / Limit` then call `Subscribe` for a live `*QueryResult{Initial, Total, Sub}`, or `Snapshot(opts)` for the same shape without a live `Sub`. There is no separate raw-event firehose.
- **Engine** — `internal/subscribe.Engine`, one per loaded space, owned by `spaceobjects.Store`. Single `engine.mu` serializes register / close / per-event apply; the apply path's afterApply hook calls `engine.OnApply(ev, postValue)` gated on `engine.HasSubscribers()`.
- **Batcher** — `github.com/cheggaaa/mb/v3`, per sub. Bounded capacity (default 256, min 16). Each consumer reads via `sub.Events().Wait(ctx)` (batches naturally) or `WaitOne(ctx)`.
- **Granularity** — per-sub `Scope` is either the shared `objects` dataset (`Space.QueryObjects()`) or an explicit `(objectId, dataset)`. Members feed is NOT yet plumbed into the engine (`MembersQuery.Subscribe` returns `ErrSubscribeUnsupported`) — separate change.
- **Event payload** — `SubscriptionEvent{VersionId, Added, Updated, Removed}`. `Added`/`Updated` are `[]SubRecord{Id, Doc, Ops}` carrying the full post-apply value (deep-cloned, safe to retain) AND the projected `$set`/`$unset` ops; `Removed` is `[]RemovedRecord{Id, Reason}`. `Reason` is `RemoveDeleted` (tombstoned, gone from the DB), `RemoveFilteredOut` (update broke the filter match, still exists), or `RemoveDisplaced` (pushed past the `Limit` boundary, still matches). Branch on `RemoveDeleted` to tell "object is gone" from "left my window but a `Snapshot`/`Query.One` would still return it".
- **Window semantics** — when `Limit > 0` the engine internally holds `Limit + 1` rows; the largest-tuple row is the *sentinel* (kept but not visible to the consumer), so single new arrivals at the top of the sort absorb cleanly without an any-store re-query. `Limit == 0` is unbounded; RAM grows with the matching set.
- **Snapshot fence** — initial snapshot read runs UNDER `engine.mu`. Apply events that fire during the read queue on the lock and process correctly after the new sub is registered. No `VersionId` dedupe needed.
- **Overflow / drift** — overflow = mailbox full; sub closes with `ErrSubscriptionOverflow`. Drift = more than `DriftBudgetPercent` (default 30) of `Limit` records left the held window without replacement; sub closes with `ErrSubscriptionDrifted`. Both signal "resubscribe to recover" — the resubscribe path is the only recovery contract (no silent drops, no per-event count).
- **Total** — `QueryOpts.IncludeTotal` runs a single `Count(filter)` at snapshot time; not maintained on the live stream. Call `Snapshot` again for a refreshed count.
- **Scope-route events** — account/local writes flow through the same windowed event stream as synced ones, one versionId per event from that change's own domain.

### Storage Topology
- **Flexible via `dbRouter`** — scope → DB instance. Allows starting with one shared DB (simpler, enables cross-space transactions) and moving to per-space DBs if performance dictates
- **Tradeoffs**:
  - Shared DB: cross-space transactions possible, but space deletion is harder, and any-store has a single writer (contention risk)
  - Per-space DB: isolated, easier deletion, parallel writers; no cross-space transactions
- **Needs performance testing** before locking in
- **Device data** — lives in the same DB as synced data; local-scope versions are locally-minted lexids in the same `_ver` tree (resolved — see CRDT spec §9).
- **Device data persistence** — not backed up, does not survive app reinstall

### System Collections
- **`spaces`** — same thing as the tech space's space index. Not a separate local projection
- **`objects`** — decision deferred. Global collection (one across all spaces) is more usable since any-sync objectIds are globally unique, but virtual objects (future) complicate this. Revisit later
- **Extensibility** — adding new system collections later is cheap; we don't need to enumerate them all upfront
- **Writability** — system collections are **not directly writable** by the caller. Writes go through dedicated SDK methods:
  - Regular CRDT batch for `base`-scope changes
  - Separate methods for `device`-scope and `account`-scope changes
- **Events** — same flow as user collections. Caller can narrow to `properties`, `members`, etc.

### Consistency
- **Read-your-writes** — yes, guaranteed
- **Event ordering** — `any-sync receives change → applies to any-store → commits → dispatches event`. Events never arrive before the state is committed
- **Transactional reads** — any-store provides snapshot isolation out of the box

## Grooming Questions (open)

### Query API
1. Exact shape of the query wrapper — final list of helpers on `Query` (`projection`, `iterate`, `all`, `one`, `count`, `explain`?)
2. Pagination semantics once offset-based starts hurting (10k+ results) — add cursors? Continue using offsets with an index?

### Events (resolved — see §"Subscriptions / Event Flow" above)
3. ~Change shape inside `{spaceId, objectId, []changes}`~ → `SubscriptionEvent{VersionId, Added, Updated, Removed}` is the wire shape; signing/encryption metadata never leaves the engine.
4. ~`mb` tuning — default buffer size, overflow policy?~ → default 256, min 16; overflow ⇒ close-with-sentinel-error.
5. ~Subscription filter inside a document set?~ → `Query.Subscribe` takes the full chained `Filter`/`Sort`/`Limit`/`Offset`; no separate dataset narrowing knob.

### Property Events (cross-section)
6. ~Event delta representation for property variants~ → resolved by scope-on-declaration: there are no variants; one event per applied change, ops at normal paths, versionId from the producing route's domain.
7. ~Multiple variants of one property in one batch~ → impossible by construction (one property = one scope = one route; `Properties.Set` is single-scope per call).

### Storage Topology
8. ~Local `versionId` for device-scope writes~ → resolved: locally-minted lexids (`NextVersion`), disjoint by path from synced versions.
9. `dbRouter` interface sketch — what's the scope key (spaceId? "device"? "tech"?)?
10. When we move to per-space DBs, how does tech space integrate? Its own DB, or alongside regular spaces?

### System Collections
11. Global vs per-space `objects` — unblocker for this decision is understanding "virtual objects" (what they are, when they'll arrive). Defer to a later grooming.
12. Do we enumerate a minimum set of system collections for v1? (`spaces`, `objects`, `members` at least?)

### Write Methods
13. ~What do the dedicated write methods look like for `device` and `account` scopes?~ → resolved: no per-scope methods. A single auto-routing `Properties.Set(ctx, objectId, ownerId, patch)` resolves each propId's declared scope and writes on that route (`space/properties.go`); mixed-scope patches are rejected.
14. ~Are device/account writes atomic with the event emission, or eventually consistent?~ → resolved by scope-on-declaration (§"Property Types", CRDT spec §9): `synced` commits through the object's CRDT, `account` through the tech-space carrier mirrored per-device, `local` straight into the device row. Each route emits one event in its own version domain; there is no cross-route atomicity (callers issue one `Set` per scope).

### Types & Property Lifecycle
16. **What happens to property values when the type changes or a collection is detached?** Resolved — orphan-resurrection, no SDK projection, client ignores unknown.

    - **Retype / detach / re-attach — orphan-resurrection.** `SetType` to another type, `UnsetType` and `DetachCollection` never wipe the `{ownerId}.*` value bag; setting or attaching that owner again reveals it unchanged. Convergent and free — the value record is plain LWW data, independent of the membership fields. No wipe, no GC.
    - **Concurrent detach vs write — no apply-side membership guard.** Inbound property validation deliberately does NOT consult `any.type` / `any.collections` (nil preflight). A value written concurrently with a retype or a detach lands as orphan on *every* peer and converges. An apply-side membership guard is **forbidden**: both fields are themselves concurrent CRDT values, so gating on them makes the outcome apply-order-dependent (non-convergent). Parking such writes has the same defect — same conclusion.
    - **Wrong-kind writes are impossible per propId.** `propId = base58(xxh3-64(changeId))` (see §"Property ids") — unique per definition. A propId's kind is pinned for life; "changing" a kind means remove + re-add, which mints a *new* propId. So a live propId always validates identically on all peers, and a removed propId's writes drop cleanly everywhere via the unknown-property rule. Stale values linger only under dead propIds, which can never be resurrected (a new definition gets a new hash). There is no convergence hole here.
    - **No read projection — client ignores unknown.** `Properties.Get()` returns the raw record verbatim; the SDK does **not** filter by current schema (clients read near-raw for speed, that's the point). **Contract: a client MUST ignore any propId not present in the owner's current schema, and any namespace the object no longer has.** That hides orphan garbage at the render layer with zero SDK-side projection cost.
    - **Required fields / events.** No `required` keyword in v1, so orphan values raise no validation error. Membership stays a generic `$set` / `$unset` on `any.type` and `$addToSet` / `$pull` on `any.collections`; no synthetic per-field events for the namespace.
    - **Typed API.** `SetType` / `UnsetType` / `AttachCollection` / `DetachCollection` are the sanctioned mutation path; the slot and membership rules also cover a raw `Modify` on the two fields, since they run in the handler pre-flight (§"Membership and the local write pre-flight").

    **Still open:**
    - **P1 — coarse DataVersion over-gates (liveness, not correctness).** A property write is stamped with the owner's *single latest* schema version, not the versions the written props actually need. A write to a stable `propX` gets pinned to a shortId minted by an unrelated `propY` addition, so a peer that hasn't yet received that `propY`-add **parks the write** until it does (indefinitely if that unrelated change is slow/lost). Converges once delivered. Fix: per-prop (multi-pair) DataVersion so a write depends only on the schema it touches. Self-healing, so not blocking.
    - **Value-scan queries leak orphans.** "Client ignores unknown" covers field rendering, not value scans: `find({"{deadPropId}": x})` (or a cross-field value scan) can still match orphan data. If it ever matters, that's a query-layer filter, not a storage change.
    - **Property-definition deletion.** Deleting a property definition leaves per-object values behind (no cascade in `PropertyHandler.BeforeDelete`); future writes drop via the unknown-property rule. Same orphan policy as a retype — confirm we want them identical.

    The built-in namespaces need no guard: `any` is universal and never named in a membership field, and `type` / `collection` are granted off the marker alone, so neither can be detached.

### Dependencies
15. ~Event format for property variants~ → resolved with question 6 (scope-on-declaration; CRDT spec §9).
