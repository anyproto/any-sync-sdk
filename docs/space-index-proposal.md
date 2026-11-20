# Per-Space `spaceIndex` Object

Every space holds one derived object, `spaceIndex`, carrying the space's
display metadata. It replicates to every member through the regular DAG,
so all members see the same name, description and icon. The per-account
tech space ([tech-space.md](tech-space.md)) keeps a private copy of
these fields on its `spaces` row, mirrored from this object.

## Data model

| Aspect | Value |
|---|---|
| Object id | Derived from the fixed seed `builtin:spaceIndex` (encrypted derive). Same id on every member's device; `Space.SpaceIndexObjectId()` returns it. |
| Type | Built-in type `spaceIndex` (`any.type = "spaceIndex"` on the object's row). |
| Metadata | Synced string properties under the `spaceIndex` namespace of the object's row in the per-space `objects` dataset: `name`, `description`, `icon` (CID), `spaceType`. |
| Other datasets | `bundles`, the installed-bundles registry ([bundles.md](bundles.md)); `identityKeys`, the one-to-one key exchange ([one-to-one-spaces.md](one-to-one-spaces.md) § Key exchange inside the space). |
| Sync status | Excluded from the space's pending-object count. |

`spaceType` is the app-level tag from `DeriveRequest.SpaceType`, not the
on-wire header type. It is set by the initial write and not exposed for
editing. The handler does not pin it; the initial write wins in practice.

## API

```go
type SetMetadataRequest struct {
    Name        *string
    Description *string
    IconCID     *string
}

sp.SetMetadata(ctx, SetMetadataRequest) error
sp.SpaceIndexObjectId() string
sp.WaitIndexSynced(ctx) error
```

- `SetMetadata` writes the non-nil fields in one multi-field `$set`. A nil
  pointer leaves the field unchanged; a pointer to `""` clears it. At
  least one field is required.
- There is no SDK-side permission check. Peers reject writes from
  members without write permission when they apply the change against
  the ACL.
- Reads go through `Space.Info` / `Service.List` (the tech-space mirror)
  or a `Query.Subscribe` on the object's `objects` row for live UI.
- `WaitIndexSynced` blocks until the local view is trustworthy: either
  the metadata row is already present locally, or a clean head-sync round
  finishes with the sync-status rollup at `Synced`. The second exit covers
  spaces that never seed metadata (one-to-one and nameless derived
  spaces); an absent object after convergence is a valid answer.

## Seeding

**At create.** `Service.Create` derives the object and writes all four
fields, empty strings included, in one change. Writing blanks keeps the
namespace present, so "seeded with blanks" is distinguishable from
"never seeded".

**On load (lazy seed).** Covers spaces whose seed never ran: spaces
predating the object, or an owner that crashed between creating the
space and writing the seed. It runs in the background after a space
loads, and only on an owner's device:

1. If the namespace is present locally, stop.
2. Run a head-sync round and check again. If the sync fails, stop; the
   next load retries.
3. Seed from this device's tech-space row (`spaceType` falls back to the
   header type on legacy rows).

The sync in step 2 is required. Another device of the same account may
already have renamed the space. Seeding from this device's stale row
would produce a newer version on the same derived object and revert the
name on every device.

Non-owners never seed; they wait for the owner's write to sync.

## Mirror to the tech space

A per-space watcher subscribes to the object's `objects` row. On start
and after each apply (local or remote, coalesced), it reads the four
fields and calls `Indexer.OnSpaceMetadataUpdated`, which overwrites the
tech-space row. The mirror is one-way. It skips absent rows (a joiner
before the owner's seed arrives) and rows whose values already match, so
the initial seed does not produce a second tech-space change.

The watcher is best-effort: if its subscription overflows, it exits.
