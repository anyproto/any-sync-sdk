# Types & Properties

How type and collection schemas are versioned, and how data changes are
gated on the schema state their writer had. The object model (one type per
object, collections, the `objects` row) is in
[data-structure.md](data-structure.md); runtime datasets are in
[user-datasets.md](user-datasets.md).

## Model

- Every object has one type (`any.type`) and any number of collections
  (`any.collections`).
- A type or collection is an object whose `properties` dataset holds one
  record per property, keyed by propId: `kind`, optional `scope`, `items`
  and `properties`, display fields (`name`, `description`, …), and the
  opaque `x-format` descriptor.
- Property values live on the object's row in the per-space `objects`
  collection at `{ownerId}.{propId}`; built-in owners use short ids (`any`).
  A type's namespaced datasets are collections named `<typeId>_<key>`.
- Two definitions with the same `name` are distinct properties with
  distinct propIds. Values under one path converge per field like any CRDT
  write.
- There is no type inheritance or templating.

## Change-level `DataVersion`

Every CRDT `Change` carries a `DataVersion` string naming the schema or
handler state its writer used. An empty `DataVersion` is invalid: the
Controller rejects the change (`crdt.ErrMissingDataVersion`).

Schema state is encoded as `typeId:shortId` pairs separated by `;`
(`internal/types/dataversion.go`), where the id may be a type or a
collection. An owner with no shortId yet contributes no pair. Handler
versions are opaque strings, conventionally `<name>-v<n>`; only equality
matters, never order.

| Change | `DataVersion` |
|---|---|
| `Objects().Create` bootstrap row | a pair per owner the create names (type, collections, `InitialProperties` owners); `systemPropertyHandler-v1` when none has a shortId |
| `Properties().Set`, synced route | a pair for the owner written; `systemPropertyHandler-v1` when it has no shortId |
| Membership writes (`SetType`, `AttachCollection`, `DetachCollection`, `Derive`), local-scope property writes | `systemPropertyHandler-v1` |
| Runtime namespaced dataset | a pair for the declaring type's latest shortId; `typeDatasetHandler-v1` when the type has none |
| Registered type's static namespaced module instance | the module's `DataVersion` (registered types mint no schema state) |
| Module canonical collection | the module's `DataVersion` (`config.Config.Modules`) |
| Registered dataset (`handler.Dataset` in `Type.Datasets`), tech-space system dataset | the registration's `DataVersion` |
| Definition datasets on a type object | `typePropertyHandler-v1` (properties), `typeDatasetHandler-v1` (dataset definitions) |
| Other built-in datasets | `shortIds-v1`, `payloads-v1`, `bundles-v1`, `identityKeys-v1` |

### ShortId — derivation

`shortId = base58(xxh3-64(changeId))` (`crdt.DeriveRecordId`), over the
ChangeId of an important change to the type object's definitions.

An important change:

- creates a property record, including a concurrent duplicate create of a
  deterministic property id that lands as a modify on other replicas;
- deletes a property record;
- creates or deletes a dataset-definition record (part, dataset head,
  field).

Other edits leave the shortId unchanged. The `typePropertyHandler` and
`typeDatasetHandler` classify changes and project the rows.

### Known-shortIds set

Each type or collection object keeps a `<ownerId>_shortIds` collection, one
row per important change:

```
{
  id:       shortId,
  changeId: string,
  propId:   string,      // property rows
  defId:    string,      // dataset-definition rows
  src:      "datasets",  // dataset-definition rows only
  kind:     string,      // property add rows
  removed:  true         // removal rows
}
```

The row's `_ver.id` carries its versionId; the latest shortId is the row
with the greatest `_ver.id`. Property and dataset-definition
rows share one stream, so a single `typeId:latestShortId` stamp covers the
type's whole schema state. The set is append-only with no GC, and stores no
compiled schema: the schema is derived on demand from the current
definitions.

### Schema evolution rules

The `typePropertyHandler` enforces:

- **Add a property**: allowed; mints a shortId.
- **Remove a property**: allowed; mints a shortId. Stored values stay on
  object rows; later ops touching the property are dropped (unknown
  property).
- **Schema-bearing fields** (`key`, `kind`, `scope`, `items`, `properties`,
  with their subtrees) are pinned after the first write. An op editing them
  on an existing record is dropped, and `PatchProperty` refuses such a patch
  up front. To change a shape, define a new property (a new propId).
- **Display fields** (`name`, `description`, `x-key`, `required`,
  `meta.<k>`, every path under `x-format`) are mutable per path and mint no
  shortId. A `$set` of the whole `x-format` must be an object.

Kinds never change, so data written against any earlier schema either
matches a still-present property or drops on a removed one. Validation
always runs against the current merged schema; no historical snapshots are
kept.

### Gate

Every inbound or replayed change passes the DataVersion gate
(`spaceobjects.gateFor`) before the Controller applies it:

1. Each `typeId:shortId` pair in `DataVersion` is looked up in that owner's
   known-shortIds set (`KnownShortId`, an id lookup). Unknown pairs are
   missing.
2. The object's Controller must hold a current registration for the
   change's dataset: one exists and, for a runtime dataset, its `SchemaRev`
   matches the catalog's.
3. No missing pair and a current registration: the change applies.
   Otherwise it parks in the detached-changes collection.

ShortIds have no order: a pair is known or not.

### Validation atomicity

Applying a change is tolerant per op (crdt-spec.md §7.2):

- An op that fails content validation (path syntax, field class) or handler
  validation (unknown property, kind mismatch, pinned field) is dropped and
  recorded as a rejection; the rest of the change commits and the watermark
  advances. A record whose ops all drop is skipped.
- Inside a multi-field `$set`/`$unset`, an offending key is shed on its own
  and the surviving keys apply; the op drops only when every key fails. A
  create that bundled a since-reclassified field still materializes from its
  valid keys on replay.

Local writes are strict. `ValidateChange` refuses a change with an illegal
path or a field-class violation, and the `objects` handler's `PreValidate`
refuses the whole write on its first membership, type, unknown-property or
kind violation. Nothing invalid enters the DAG; inbound drops cover
cross-peer bugs and definitions that changed after the write.

### Detached-changes collection

One per space, `<spaceId>__detached`:

```
{
  id:        changeId,
  spaceId:   string,
  objectId:  string,
  addSeq:    number,
  orderId:   string,                   // any-sync orderId, reused as VersionId on replay
  timestamp: number,
  payload:   binary,                   // wire-format change bytes
  pending:   ["typeId:shortId", ...],  // missing pairs; empty when parked for a registration
  dataset:   string
}
```

**Park.** The gate upserts the row and the change does not apply. A row
parked only for a missing or stale registration wakes the drainer at once,
since the schema may already be present.

**Drain.** A per-space worker runs off the apply path. Applies to
definition datasets wake it (a dataset-definition apply refreshes the
runtime catalog first), and bursts of wake-ups coalesce into one pass. A pass scans the whole
collection. A row is ready when every pending pair is known and its dataset
is registered. Each ready row is decoded and applied through its object,
bypassing the gate, with `orderId` as the VersionId; a Controller with a
stale registration is evicted and reloaded first. An applied row is
deleted; a failing row stays for the next pass.

A row whose schema never arrives stays parked. It is never applied, so it
does no damage; there is no GC.

## Schema format — decision

- **Format**: a minimal JSON-Schema subset. `kind` on every node, `items`
  on arrays, `properties` on objects.
- **Kinds**: `string`, `number`, `boolean`, `null`, `array`, `object`,
  `datetime`. `datetime` is any-store's native instant (unix millis,
  memcmp-orderable, index-keyable, `{"$date": …}` in JSON), the shape the
  date operators compute on. `kind` is always explicit, never defaulted from
  `x-format`, and pinned for the life of a property.
- **Recursive validation**: an array with `items` checks every element; an
  object with `properties` checks every field and rejects unknown ones.
  Without those keywords only the kind is checked.
- **Not validated**: `enum`, `required`, `additionalProperties`, `default`.
  A property record may store `required`, but values are not checked
  against it. An absent field (`$unset`) is always valid. Option sets live
  in the consumer-owned `x-format` descriptor.
- **Validator** (`internal/schema`): custom, operating natively on
  `*anyenc.Value` with no conversion to `interface{}` or JSON. It runs on
  every write op, so scalar success allocates nothing. A shape compiles once
  per definition (`schema.CompileShape`).
- **Where schemas apply**:
  - Definition datasets on type objects have a shape hardcoded in Go,
    checked by the built-in handler.
  - Property values on the `objects` row get a kind check against the
    registry: a `$set` value must have the declared kind, `$addToSet` /
    `$pull` need an array property (element kinds are not checked), `$inc` /
    `$incGated` need a number. Sub-shapes (`items`, `properties`) are not
    checked for property values.
  - Fields of schema-declared datasets served by the generic schema
    handler (runtime datasets, registered datasets with a `Schema` and no
    bespoke `Handler`) are validated recursively against their compiled
    shape.
