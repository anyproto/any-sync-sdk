# Data Structure

## Vision
Main interface for data retrieval: any-store queries + subscription to event flow. System collections: `spaces` (global), `objects` (per-space system collection for properties), plus per-object datasets. Some property values can be overridden at device level (local) or account level (tech space).

## Object Properties

Every object has properties — a map with required fields (`id`, `name`, `description`, `author`, optionally `[]types`). All objects in a space share one system collection `objects`: **one record per object**, `id` = any-sync objectId.

### Property scopes (decision 2026-06-12 — scope on the declaration)

Scope is a fixed attribute of the property DEFINITION (and of declared
dataset schema fields), from the unified `schema.Scope` taxonomy. A
property lives in exactly one scope, pinned at creation like `kind`;
changing scope = defining a new property (new propId). Full design and
rationale: [`scoped-properties-proposal.md`](scoped-properties-proposal.md).

| Scope | write route | version domain | syncs to | overridable |
|------|------|------|------|------|
| **derived** | handler-stamped (`id`, `author`, `spaceId`, `createdAt`) | triggering change | (computed convergently) | no (read-only) |
| **synced** | the object's own CRDT change | object tree | everyone with access | n/a — no override stack |
| **account** | carrier record in tech space + per-device mirror | tech tree | this account's devices | n/a |
| **local** | `Object.LocalSet`, no DAG | local lexid | this device only | n/a |

There is **no per-value override stack and no priority merge** — the
earlier auto/base/account/device variant model (priority `device >
account > base`, per-record `_base`/`_account`/`_device` bags, computed
root) was rejected: ~80% of its complexity (row migration, root
derivation, projection stripping, shadowed-write event semantics,
three-domain `_ver` presentation) paid for the override stack alone.
Wanting both a shared and a personal "favorite" means two properties;
clients compose if they care.

### Storage

Values of every scope sit at their normal `{typeId}.{propId}` paths in
the object's row — no reserved scope fields, no migration from the
pre-scope layout (everything that existed was synced-scope). One `_ver`
tree per record; version domains coexist because no two routes ever
write the same path (the scope pin + apply-side route enforcement keep
them disjoint — see CRDT spec §9).

```json
{
  "id": "objectId",
  "any":            { "name": "Heat" },
  "{movieTypeId}":  { "Y9Hxx5xmYmF": ["personA"], "EwyHGrtTdxB": 1995 },
  "_ver":           { "id": "…", "any": { "name": "…" } }
}
```

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
| `x-refType` | string (typeId) | superseded by `format` (never implemented) |
| `x-kind` | string | CRDT-mutable |
| `format` | object `{type, ui, filter}` | `format.type` first-write-wins; `format.ui` / `format.filter` CRDT-mutable |
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

**`x-refType`** — superseded by `format` before it was ever implemented. Its use case ("this value points to objects of this type") is now expressed as `format: {type: "links", filter: {"type": {"$in": ["…"]}}}`. Kept in the table for historical context only.

### Property formats

`format` annotates a property with a value convention beyond its structural kind:

```json
{ "id": "3XEMVWA6EK", "name": "related", "kind": "array",
  "format": { "type": "links", "ui": "multiselect",
              "filter": "{\"type\":{\"$in\":[\"page\"]}}" } }
```

- `format.type` — `links` (array of `any://<objectId>` URI strings), `date` (`2006-01-02` string), `datetime` (RFC 3339 string), `tags` (array of tag record ids — reserved until the space-level tag table lands). Pinned by the first write, like `kind`, because it constrains the kind (`links`/`tags` ⇒ `array` of `string`; `date`/`datetime` ⇒ `string`).
- `format.ui` — presentation hint (`select` / `multiselect` / `link` / `links`). Opaque string to the SDK; CRDT-mutable leaf.
- `format.filter` — mongo-style condition over candidate objects, stored as its JSON **text** (a string leaf, so concurrent edits replace each other as a unit instead of field-merging two conditions). Opaque to the SDK; CRDT-mutable leaf.

Validation split: the SDK enforces only structure at definition-write time (known `format.type`, the format→kind coupling, `ui`/`filter` are strings; format must be created as a whole object — dotted `format.*` creation keys are rejected). Semantics — ui vocabulary, filter syntax, and whether values actually match the format (a datetime parses, a link is a well-formed `any://` URI) — are a consumer concern (the `any` server validates them at its API boundary). Like `x-refType` before it, a format is value **metadata, not apply-time value validation**: reference integrity stays lazy/read-time.

### Immutability rules

**CRDT-hard (enforced at apply on every peer, convergent)**

- **Per-record first-write-wins on `id` and `type`.** `id` is immutable (it's the record id); `type` is locked by the first write to the record. Each property is its own island — no cross-record binding, no silent-ignore rules between different property records.
- **`format.type` is pinned at sub-path granularity.** Edits to `format.type` and broad replaces of the whole `format` object drop at apply (a broad replace could smuggle a type change past a handler that has no prior state); the `format.ui` / `format.filter` leaves stay writable.

**Client-soft (SDK refuses to emit; honest clients comply, misbehaving peers bounded by read-tolerance)**

- Changing a sub-field's primitive type.
- Changing `items.type`.
- Removing sub-fields from object-typed properties.
- Narrowing enums (removing values).
- Tightening numeric / string constraints.

**Freely mutable**

- `name`, `description`, `x-key`, `x-kind`, `format.ui`, `format.filter` (string leaves only), sub-field additions, enum additions, constraint loosening.

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
13. ~What do the dedicated write methods look like for `device` and `account` scopes?~ → resolved: no per-scope methods. A single auto-routing `Properties.Set(ctx, objectId, typeId, patch)` resolves each propId's declared scope and writes on that route (`space/properties.go`); mixed-scope patches are rejected.
14. ~Are device/account writes atomic with the event emission, or eventually consistent?~ → resolved by scope-on-declaration (§"Property Types", CRDT spec §9): `synced` commits through the object's CRDT, `account` through the tech-space carrier mirrored per-device, `local` straight into the device row. Each route emits one event in its own version domain; there is no cross-route atomicity (callers issue one `Set` per scope).

### Types & Property Lifecycle
16. **What happens to property values when a user removes or adds a type on an object?** The current note ("values in that namespace become orphan data, read-tolerant") is a one-liner; we need a real answer covering the points below. Code-state anchors are inlined so we know what's already implemented vs. open design:

    **Current state in code (2026-05-11):**
    - No dedicated `AttachType` / `DetachType` API — both are declared in `space/properties.go:37` but return "not implemented" in `internal/spaceimpl/properties.go:139`. Today `any.types` is written as a freeform `$set` array during `objects.bootstrap()` and `typesAPI.Create()`.
    - `SystemPropertiesHandler` (`internal/properties/system.go:59`) validates by `Registry.LookupKind(typeId, propId)` only — it does **not** consult `any.types`. Writes for a type not in the object's `any.types` list apply blindly.
    - `Properties.Get()` (`internal/spaceimpl/properties.go:36`) returns the full record verbatim — no filter/projection by current `any.types`. Orphan values are visible to callers as-is.
    - No cleanup / GC / `dropType` / `removeType` code exists anywhere in the tree. "Read tolerance" is documented intent, not active behavior.
    - `any` and `type` built-ins are synthesized on read but **not guarded** against removal from `any.types` at the handler level.
    - `RemoveProperty` (`internal/spaceimpl/types.go:425`) is "not implemented"; `PropertyHandler.BeforeDelete` marks the shortId but does not cascade to per-object values.
    - Tests: `sdk_test.go:183` (`TestSDK_TypesAndProperties`) covers type binding at create only; no remove / re-add / orphan-value tests exist.

    **Decision (2026-06-02) — orphan-resurrection, no SDK projection, client ignores unknown.**

    Resolved after tracing the gate + handler + propId model:

    - **Drop / re-add — orphan-resurrection.** Detaching a type (`$pull any.types`) never wipes the `{typeId}.*` value bag; re-attaching reveals it unchanged. This is convergent and free — the value record is plain LWW data, independent of `any.types`. No wipe, no GC on detach.
    - **Concurrent drop vs write — no apply-side membership guard.** Inbound property validation deliberately does NOT consult `any.types` (`internal/properties/system.go:169`, nil preflight). A value written concurrently with a detach lands as orphan on *every* peer and converges. An apply-side membership guard is **forbidden**: `any.types` is itself a concurrent CRDT value, so gating on it makes the outcome apply-order-dependent (non-convergent). Parking such writes (an earlier proposal) has the same defect — same conclusion.
    - **Wrong-kind writes are impossible per propId.** `propId = base58(xxh3-64(changeId))` (see §"Property ids") — unique per definition. A propId's kind is pinned for life; "changing" a kind means remove + re-add, which mints a *new* propId. So a live propId always validates identically on all peers, and a removed propId's writes drop cleanly everywhere via the unknown-property rule. Stale values linger only under dead propIds, which can never be resurrected (a new definition gets a new hash). There is no convergence hole here.
    - **No read projection — client ignores unknown.** `Properties.Get()` returns the raw record verbatim; the SDK does **not** filter by current schema (clients read near-raw for speed, that's the point). **Contract: a client MUST ignore any propId not present in the type's current schema.** That hides orphan garbage under dead/detached propIds at the render layer with zero SDK-side projection cost. This replaces the vague "read tolerance" note with an explicit client obligation.
    - **Required fields / events.** No `required` keyword in v1, so orphan values raise no validation error. Attach/detach stays a generic `$set`/`$pull` on `any.types`; no synthetic per-field events for the namespace.

    **Still open:**
    - **P1 — coarse DataVersion over-gates (liveness, not correctness).** A property write is stamped `LatestShortId(typeId)` (`internal/spaceimpl/properties.go:118`) — the type's *single latest* schema version, not the versions the written props actually need. A write to a stable `propX` gets pinned to a shortId minted by an unrelated `propY` addition, so a peer that hasn't yet received that `propY`-add **parks the write** until it does (indefinitely if that unrelated change is slow/lost). Converges once delivered. Fix: per-prop (multi-pair) DataVersion so a write depends only on the schema it touches. Self-healing, so not blocking.
    - **Value-scan queries leak orphans.** "Client ignores unknown" covers field rendering, not value scans: `find({"{deadPropId}": x})` (or a cross-field value scan) can still match orphan data. If it ever matters, that's a query-layer filter, not a storage change.
    - **Built-in types (`any`, `type`).** No handler guard against removal from `any.types` today. Decide: enforce non-removability at apply time (convergent), SDK-write-time only (client-soft), or leave it (the read-side synthesizer masks it).
    - **Property-definition deletion.** Deleting a property definition leaves per-object values behind (no cascade in `PropertyHandler.BeforeDelete`); future writes drop via the unknown-property rule. Same orphan policy as type-drop — confirm we want them identical.
    - **Test gap.** No detach → concurrent-write → re-attach convergence test exists (`sdk_test.go` covers bind-at-create only). Add one to lock the orphan-resurrection guarantee and guard against a future apply-side membership guard regressing it.

    **Dedicated API.** Whatever we decide, `AttachType` / `DetachType` should be the only sanctioned mutation path — freeform `$set` on `any.types` makes some of the policies above (e.g. cascade-wipe, built-in guard) un-enforceable without inspecting every op.

### Dependencies
15. ~Event format for property variants~ → resolved with question 6 (scope-on-declaration; CRDT spec §9).
