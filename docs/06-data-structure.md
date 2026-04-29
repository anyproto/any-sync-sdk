# Data Structure

## Vision
Main interface for data retrieval: any-store queries + subscription to event flow. System collections: `spaces` (global), `objects` (per-space system collection for properties), plus per-object datasets. Some property values can be overridden at device level (local) or account level (tech space).

## Object Properties (from `object-properties.txt`)

Every object has properties — a map with required fields (`id`, `name`, `description`, `author`, optionally `[]types`). All objects in a space share one system collection `objects`: **one record per object**, `id` = any-sync objectId.

### Property Types
| Type | Source | Sync | Overridable |
|------|--------|------|-------------|
| **auto** | constants from any-sync / common context (`id`, `author`, `spaceId`) | N/A | no (read-only) |
| **base** | stored in the object CRDT itself (changes use empty record `id`). Applied by a built-in `baseProperty` handler. Hardcoded rule: an object can only update its own record | synced with the object | yes |
| **account** | derived object in tech space; special handler applies changes to the `objects` collection of the appropriate space | synced across account devices | yes |
| **device** | device-level record in local DB | not synced | yes |

Final set: **auto / base / account / device**.

### Conflict Resolution
Any overridable field can exist in multiple types simultaneously — e.g., `isFavorite` can be `base`, `account`, and `device`. **Priority (common to all properties)**: `device > account > base`. Rationale:
- `base` — the default, set by anyone with write access
- `account` — "I want this different for my account, across all my devices"
- `device` — "on this specific device, override everything else"

`auto` sits outside the priority — it's read-only and cannot be overridden.

One priority for all properties (not per-property). Simpler, still lets clients read any individual variant when needed.

### Storage — Proposal 2 (chosen)
```json
{
  "id": "objectId",
  "name": "device name",                    // computed per priority device > account > base
  "isFavorite": true,
  "_device":  { "name": "device name" },
  "_account": { "name": "account name", "isFavorite": true },
  "_base":    { "name": "base name" }
}
```
- **Pros**: simple queries (`{isFavorite: true}`), per-variant access still possible, special filters possible when needed
- **Cons mitigated**: any-store has s2 compression, so duplicated values won't inflate storage significantly

Rejected:
- **Proposal 1** (namespaces only) — query complexity is unacceptable
- **Proposal 3** (any-store views) — requires a new feature in any-store, shouldn't block v1

### Handlers
- **`SystemPropertiesHandler`** (was named `baseProperty` in earlier drafts) — built-in, runs on regular objects. Wired against the per-space `objects` collection via the Controller's shared-collection override: every write to dataset `objects` lands in one row keyed by the change's ObjectId. The "object can only update its own record" rule is enforced at the apply layer (`ch.ObjectId` is what the row id is set to, regardless of the inbound `RecordChange.Id`).
- **`rewriteObject` handler** — built-in, lives in tech space. Watches the account-level rewrite object and applies `account` variants to the corresponding space's `objects` collection
- Together these two handlers keep the computed root value in sync across `_device` / `_account` / `_base` writes

### Not Expanding This Pattern to All CRDT Data (v1)
We considered making the rewrite/override pattern a general CRDT feature (any record can have local/account/general variants). Decided against it:
- Consistency is harder across many datasets
- Garbage collection becomes complex (when can a variant be dropped?)
- No product need — properties are the only case that requires this right now

Revisit later if a concrete use case appears.

## Types, Properties & Data Schemas

Refines and extends the "Object Properties" section above. Covers what types are, how property definitions are stored, how schemas evolve under CRDT, and what's deferred to later versions.

### Structure

```
Space
 └─ Object
     ├─ implements N types (coexist, no inheritance in v1)
     └─ owns N datasets (Mongo-like record collections)
```

A **type** is an object with `type = type`. Its own shape is hardcoded in the SDK. Every type object implements two built-ins:
- `any` — universal properties (name, description, icon, id, author, createdAt)
- `type` — the meta-type; contributes the `properties` and (optionally) `datasets` datasets

Built-ins are expected to exist as **derived objects** in every space (well-known ids, uniform with user types — no "built-in vs user" fork in query/UI code).

### Property ids

Every user-defined property has a property id derived from the changeId of the change that created its definition record:

```
propId = base58(xxh3-64(changeId))   // up to 11 chars
```

This is the default empty-id resolution the CRDT layer already produces (see CRDT spec §3.3 / `crdt.DeriveRecordId`). Properties use it directly as their record id, and the same string is used as the field key under which values are stored on objects (see below).

Built-in properties (on `any`, `type`) use **hardcoded human-readable ids** like `"name"`, `"description"`, `"icon"`. They never collide with user property ids — 11-char base58 never produces those strings.

### Property record shape (in the space's `objects` system collection)

One record per regular object, `id = objectId`. Values are namespaced by `typeId` (or short id for built-ins), and within each namespace keyed by `propId`:

The CRDT-side dataset name on regular objects is also `objects` — the `properties.SystemPropertiesHandler` is wired against the per-space `objects` collection via the Controller's shared-collection override, so every regular object's writes coalesce into one row in that collection. Type objects do NOT register this handler; their own `any.name`/`any.description`/etc. live on per-type-object storage and don't appear in the per-space `objects` collection.

```json
{
  "id": "objectId",
  "any":             { "name": "Heat", "description": "…" },
  "{movieTypeId}":   { "Y9Hxx5xmYmF": ["personA","personB"], "EwyHGrtTdxB": 1995 },
  "{reviewTypeId}":  { "e5vwLLgBRiM": 8.2 }
}
```

Storage path is `{typeId}.{propId}`. The human `name` ("actors", "year", …) lives only on the property definition record — renaming never touches stored values.

The list of types an object implements lives at `any.types: [typeId, …]`. Adopting = appending its id; dropping = removing it (values in that namespace become orphan data, read-tolerant).

### Property definitions (inside a type object's `properties` dataset)

One record per definition. Shape:

| field | kind | mutability |
|---|---|---|
| `id` | string = `base58(xxh3-64(changeId))` | immutable (record id) |
| `type` | enum: `string` / `number` / `boolean` / `array` / `object` | first-write-wins on the record |
| `name` | string (human label) | CRDT-mutable |
| `description` | string | CRDT-mutable |
| `x-key` | string (caller-side mapping key) | CRDT-mutable |
| `x-refType` | string (typeId) | CRDT-mutable |
| `x-kind` | string | CRDT-mutable |
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

**`x-refType`** — value metadata, not validation. Says "this id points to an object implementing this type". Works on `string` fields and `items` of arrays. Reference integrity (existence / type-implementation check) is lazy / read-time, not CRDT-apply-time.

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

- `name`, `description`, `x-key`, `x-kind`, `x-refType`, sub-field additions, enum additions, constraint loosening.

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

- Object-level `types` list at `any.types` vs a dedicated field — pick during implementation.
- Concrete built-in set beyond `any` + `type` — candidates: `relation`, `file`, `member`. Not in v1.
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
- **Property variants in queries** — default projection **hides** `_device` / `_account` / `_base`. Caller can opt-in via a flag. Projection control may live at API level, not SDK level

### Subscriptions / Event Flow
- **Two event layers** — internal CRDT events (raw ops, used by handlers / property recompute / versioning) and user events (simplified `inserted`/`updated`/`deleted` with new field values). SDK callers see only user events by default. See CRDT spec §7.
- **Batcher** — use `github.com/cheggaaa/mb/v3`. Each subscriber gets its own `mb` instance (no shared fan-out)
- **Granularity** — subscribe to a single document (object) or a set of documents by `ids`. No per-query live subscriptions in v1
- **User event payload** — `{spaceId, objectId, dataset, records: [{id, type, fields?}]}`. Delta fields only; caller re-reads any-store for full state if needed
- **Backpressure** — handled by `mb` limits (built-in)
- **Sessions** — deferred. When added, probably at the API layer, not SDK core
- **Variant-level events** — opt-in advanced channel for `_device`/`_account`/`_base` internal events (debugging, settings UI)
- **System collections** — same event flow. Caller can narrow subscriptions to e.g. properties-only or members-only

### Storage Topology
- **Flexible via `dbRouter`** — scope → DB instance. Allows starting with one shared DB (simpler, enables cross-space transactions) and moving to per-space DBs if performance dictates
- **Tradeoffs**:
  - Shared DB: cross-space transactions possible, but space deletion is harder, and any-store has a single writer (contention risk)
  - Per-space DB: isolated, easier deletion, parallel writers; no cross-space transactions
- **Needs performance testing** before locking in
- **Device data** — lives in the same DB as synced data. Open question: do we need a local `versionId` for consistency between device-variant writes and sync events?
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

### Events
3. Change shape inside `{spaceId, objectId, []changes}` — reuse the CRDT change format verbatim, or strip signing/encryption metadata?
4. `mb` tuning — default buffer size, overflow policy (block vs oldest-drop)?
5. Subscription filter inside a document set — does the caller pass e.g. `{datasetName: "properties"}` to narrow, or do they read the full change list and filter client-side?

### Property Events (cross-section)
6. Events must carry property changes even though each variant (`_device` / `_account` / `_base`) has its own `_o` version. How is the event delta represented — as a change to the computed root, as a change to one of the `_*` variants, or both? This affects CRDT event format too.
7. Order of events when multiple variants of the same property change in one batch — is this even possible in one batch?

### Storage Topology
8. Local `versionId` for device-scope writes — do we need one to keep consistency between device-variant state and sync events? Or can we reuse the synced `versionId` somehow?
9. `dbRouter` interface sketch — what's the scope key (spaceId? "device"? "tech"?)?
10. When we move to per-space DBs, how does tech space integrate? Its own DB, or alongside regular spaces?

### System Collections
11. Global vs per-space `objects` — unblocker for this decision is understanding "virtual objects" (what they are, when they'll arrive). Defer to a later grooming.
12. Do we enumerate a minimum set of system collections for v1? (`spaces`, `objects`, `members` at least?)

### Write Methods
13. What do the dedicated write methods look like for `device` and `account` scopes?
    - `sdk.Properties.SetDevice(spaceId, objectId, {isFavorite: true})`?
    - `sdk.Properties.SetAccount(...)`?
14. Are device/account writes atomic with the event emission, or eventually consistent?

### Dependencies
15. Event format for property variants must be agreed with the CRDT section (question 6 above is the cross-section one to resolve)
