# Types & Properties — Proposal

> Historical proposal. The shipped model is one type per object at
> `any.type` plus any number of collections at `any.collections` —
> see [06-data-structure.md](06-data-structure.md) § Type and
> collections. Read "the types an object implements" below as "its
> type and its collections".

## System — structure & abilities

**Space** — a container of objects.

**Object** — belongs to a space. Can hold **many datasets**.

**Dataset** — a MongoDB-like collection of records, scoped to its object. One object → N named datasets.

**Record** — a document inside a dataset.

### Abilities

**On records**
- Create (implicit on first write)
- Update fields: `$set`, `$unset`, `$inc`, `$addToSet`, `$pull`
- Delete (tombstoned)
- Address nested fields by dotted path

**On datasets**
- Query: filter, sort, limit, offset (Mongo-style operators)
- Indexes
- Read within a transaction

**On objects**
- Create / derive / delete
- Hold multiple datasets
- Take one type and any number of collections (a type brings handlers for certain datasets)

**On spaces**
- Create / join / delete
- Enumerate objects

**Cross-cutting**
- Subscribe to changes (per document or id-set) — `inserted / updated / deleted` events
- Offline-first; automatic CRDT merge, no conflict surfaces
- Read-your-writes; events arrive after commit

---

## Proposal — types & properties

**`properties`** — per-space system dataset, one record per object (`id` = objectId). Holds namespaced property values for the types the object implements.

**Type** — an object with `type = type`. Type-object's own properties are **hardcoded**. Built-ins (`any`, …) ship with the SDK.

A type defines:
- **Its properties** — one record per property in a `properties` dataset on the type object. Fields: `key`, `kind`, and optionally `x-format` (the opaque descriptor — semantic slug, options, relation targets, config — see docs/06 § "The `x-format` descriptor"; its `options` member is the concrete realization of the deferred `enum` keyword, owned by the consumer). More fields (e.g. `required`, `default`) may be added later, when a concrete need appears.
- **Optionally, versioned data schemas** for the object's datasets.

An object has **one type** and any number of **collections**, whose namespaces coexist. No extension/inheritance in v1.

### Property record shape

Namespace key = `typeId` for user types, short id for built-ins.

```json
{
  "id": "objectId",
  "any": { "name": "Movie name", "description": "..." },
  "{movieTypeId}":  { "actors": ["actorId1"], "year": 2004 },
  "{reviewTypeId}": { "score": 5.6 }
}
```

Type-owned datasets on the object also use `typeId` as the dataset key.

### Property key conflicts — LWW

If two peers add the same key under the same type with different value shapes, **LWW** decides. SDK applies all keys as-is; clients must be ready to see mixed values.

### Change-level `DataVersion`

Every CRDT `Change` carries a `DataVersion` string that pins the change to a specific schema/handler version. The meaning depends on the dataset.

| Change target                                  | `DataVersion` meaning                                                                    |
| ---------------------------------------------- | ---------------------------------------------------------------------------------------- |
| Property values on the per-space `objects` row | `ownerId:shortId` pairs, `;`-separated: the latest shortId of each type or collection owning a touched property. No known owner → the hardcoded `systemPropertyHandler-v1`. |
| Namespaced dataset declared by a type          | `typeId:latestShortId` of the declaring type (docs/17-user-datasets.md § DataVersion & gating). |
| Module canonical collection                    | The module's opaque `DataVersion`, compiled into the SDK.                                |
| Definition datasets on a type object           | Hardcoded handler version, e.g. `typePropertyHandler-v1`. Bumped by the SDK handler, not derived from the DAG. |

**Empty `DataVersion` is invalid — the change is rejected.**

The string format is opaque to the CRDT layer (shortIds for types, handler-chosen identifiers like `chat-v1` for data). Only equality matters — **versions are not comparable**, just known or unknown.

### ShortId — derivation

`shortId = base58(truncate(hash(changeId), 8 bytes))` — the leading 8 bytes of a hash over the ChangeId of the last "important" change to the type object's `properties` dataset.

**"Important" change** — any change that:
- adds a property record, or
- removes a property record.

Re-adding a previously-removed property (same `key`, any `kind`) counts as an addition and mints a new shortId. All other edits (future cosmetic fields, display-only metadata) leave the shortId unchanged. Classification is owned by the `typePropertyHandler`.

**Genesis** — before any important change has landed, the shortId is empty. Writes stamped with empty `DataVersion` are invalid (see above), so concrete data can't flow until the type has its first property.

### Schema evolution rules

The `typePropertyHandler` (the built-in handler for type objects' `properties` dataset) enforces:

- **Add a property** — allowed. Mints a new shortId.
- **Remove a property** — allowed. Mints a new shortId. Existing record data in any-store is **not** cleaned up; subsequent writes touching that property are dropped op-by-op (unknown-property rule).
- **Modify an existing property's schema-bearing fields** (`kind`, `items`, `properties`) — **rejected at write time**. Kinds are pinned for life. To change a property's shape, remove it and re-add it as a new shortId; old-writer ops then either match (same kind by coincidence) or drop cleanly.
- **Modify display-only fields** (`name`, `description`, `x-key`, `meta.<k>`, and every path under the opaque `x-format` descriptor — per-path `$set`/`$unset` via `PatchProperty`; option adds of distinct keys converge) — allowed, does not mint a shortId. First-write-wins covers the whole schema-bearing fields (`kind`, `scope`, `items`, `properties`) and nothing below them.

Effect: old-writer data against the current schema always type-matches on still-present properties (kind never changed) or drops cleanly on removed properties. No snapshot of historical schemas needed; **validation always runs against the current (latest merged) schema**.

### Known-shortIds set

Each type object owns a local collection of known shortIds — one row per important change:

```
{ shortId, versionId, changeId }
```

Used only for the detached-changes gating decision ("do I have the schema state this change was written against?"). No compiled schema or property-record snapshot is stored — the schema is derived on demand from the type's current `properties` dataset. Append-only in v1; no GC.

Opaque handler-version strings don't parse as `ownerId:shortId` pairs and pass the gate unconstrained.

Runtime dataset definitions (docs/17-user-datasets.md) ride this exact
machinery: the type's `datasets` dataset projects rows into the SAME
shortIds collection (a `src: "datasets"` discriminator, `defId` in
place of `propId`), so one `typeId:latestShortId` stamp gates data
changes against the type's whole schema state — property definitions
and dataset definitions alike — with no gate changes.

### Decision rule in `ApplyChange`

1. `DataVersion` empty → **reject** (whole change).
2. Every `ownerId:shortId` pair in the known-shortIds set, or an opaque handler-version string → **apply**. Each op is validated per-op against the current schema; ops that fail (kind mismatch, unknown property) are silently dropped, others apply.
3. `DataVersion` not known → **detach** (see below).

No max/min comparison — known or unknown, nothing else.

### Validation atomicity

- **Pre-apply, per-op**: each op in a `RecordChange` is validated against the current schema before being applied.
- **Per-op**: ops that fail schema validation are dropped silently; other ops in the same change still apply. If all ops in a record change are dropped, that record is skipped but the Change as a whole still commits (watermark advances, causality preserved).
- **Per-key inside a multi-field `$set`/`$unset`**: dropping granularity goes one level finer than the op. A key that fails field-class (scope) or path validation is shed individually; the op's surviving keys still apply, and the op is dropped wholesale only when every key offends. This keeps a create that bundled a since-reclassified field (e.g. a synced field later moved to `local` scope) from disappearing on replay — the record materializes from its still-valid keys.
- **Path syntax validation** (reserved `_*` prefix, empty path segments, dots inside segments) is separate and aborts the whole change — path issues indicate a protocol-level bug.
- **Writer-side responsibility**: the SDK's write API prevalidates both path syntax and schema before submitting to the DAG. Per-op drops on receive are defensive — they exist for cross-peer bugs and removed-property replays, not for programmer mistakes in the local client.

### Dataset ownership

- **One type owns one namespaced dataset**: a namespaced dataset has exactly one declaring type and gates on that type's shortId. A module's canonical collection is shared by several types and gates on the module's opaque `DataVersion` instead.

### Detached-changes collection

One per space. Schema:

```
{ id: changeId, objectId, versionId, dataVersion }
```

Index on `dataVersion`.

**Populate** — on rule (3) above, insert a row. The change body is **not** stored — re-fetched from any-sync's local `changes` collection (keyed by changeId) on re-apply. That collection already holds every change the peer has received, so fetch is a cheap primary-key lookup.

**Drain** — every time the registry gains a new row with shortId `X`:

1. `find({dataVersion: X})` in the detached collection.
2. For each hit, fetch the change from any-sync's `changes` collection and call `ApplyChange`.
3. Re-apply in **ascending `versionId`** order so DAG causality is preserved when one registry insert unblocks many detached changes.
4. On successful apply, delete the detached row.

Detached changes whose `DataVersion` never arrives stay in the collection indefinitely. That's safe (never applied, never cause damage) — GC is a future operational concern, not a correctness one.

### Deferred

- Type extension / templating.
- Per-property conflict policies beyond LWW.
- Detached-collection GC (stale fabricated entries).

---

## Schema format — decision

- **Format**: JSON-Schema-like minimal subset. v1 record fields: `kind` (on every node), `items` (on arrays), `properties` (on objects). Other keywords (`enum`, `required`, `additionalProperties`, `default`) are deferred; added when a concrete need appears.
- **Supported kinds** in v1: `string`, `number`, `boolean`, `null`, `array`, `object`, `datetime`.
  `datetime` is any-store's native instant (unix millis, memcmp-orderable, index-keyable,
  `{"$date": …}` in JSON) — the shape the date operators compute on. `kind` is always
  explicit — nothing is defaulted from the `x-format` descriptor — and pinned for the
  life of a property.
- **Recursive validation**: arrays with an `items` sub-schema check every element; objects with a `properties` map check every field and reject unknown fields. Arrays/objects without these keywords pass a shallow kind check only (any element / any shape). Progressive disclosure — simple schemas stay simple.
- **Validator**: custom, operates natively on `*anyenc.Value`. No conversion between anyenc and `interface{}`/JSON on the validation path — validation runs on every write op, so the hot path must be allocation-free for scalar success cases. Schema is compiled once from property records.
- **Duplicate keys in input**: last-wins. Writers resolve conflicts client-side before committing property changes; the validator doesn't police it.
- **Where schemas live**:
  - **Properties dataset on type objects** — shape is hardcoded in Go (a built-in dataset handler validates `key`, `kind` directly). No JSON Schema applies here.
  - **Data datasets on user objects** — validated against a JSON Schema synthesized from the type object's property records.
- **Absent field (`$unset`)**: always valid in v1. Without a `required` keyword, removing a field is a no-op from the schema's perspective.

Validation always runs against the **current** schema — see "Schema evolution rules" above for why this is safe (kinds are pinned, removals drop cleanly).

### Still open

- Prior art review from other local-first systems (Automerge, Yjs, Jazz, DXOS, …) — not blocking, but worth a pass before the validator design freezes.
