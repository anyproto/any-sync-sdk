# Bundles registry

A per-space registry of everything installed into the space: marketplace
bundles or hardcoded setups (e.g. `bao/v1`). It is the `bundles` dataset
on the in-space spaceIndex object, so one derived tree carries both the
space's display metadata and its setup state, and one wait covers both.

## Why a registry

Setup that only derives objects converges by construction: ids are
deterministic and no data is written. Once setup writes data it needs a
non-derived root, and two devices installing concurrently mint two
different roots. The registry gives a deterministic winner, keeps the
loser's objects findable, and lets restore learn what is installed
before pulling everything.

## Data model

One record per bundle (`internal/types/spaceindex/bundles.go`):

| Field | Op | Semantics |
|---|---|---|
| record id | — | Stable bundle id (marketplace id, `bao/v1`). Record deletes are rejected: the sticky tombstone would ban the id forever. |
| `name` | `$set` | Display name. |
| `rootId` | `$set` | The winning root, an LWW register; concurrent installs converge on one winner in canonical DAG order. |
| `roots` | `$addToSet` only | Every root ever claimed. A `$set` would stamp `_ver[roots]` and gate out concurrent adds, hiding the losing device's objects. Never shrunk; a loser's removal is recorded by its tree deletion. |

The handler enforces, for every writer: explicit single-field paths
only; a `rootId` write claims the same id in `roots` within the same
change, so `roots ⊇ {rootId}` in every reachable state; `roots` accepts
only `$addToSet` of non-empty strings; no record deletes. Losers are
always `roots − rootId − deleted`.

The dataset is fenced off the public `Modify` / `Delete` surface (like
`payloads`); writes go through the typed API. Reads and subscriptions
use `Space.Query(SpaceIndexObjectId(), "bundles")`. A runtime dataset
named `bundles` is shadowed by the built-in.

## Root strategies

`EnsureBundleRequest` takes exactly one:

- **`NewRoot`** — the caller creates the root (`Objects().Create`) and
  returns its id.
- **Neither, with a declaration** — Ensure creates the root itself and
  stamps it as the definition (§ Bundle-declared definitions). This is
  the only created root the tech space allows.
- **`DerivedRoot`** — the root is derived from the bundle id
  (§ Derived roots).

## Created roots

Everything else in the setup derives from the root with
`DeriveObjectOpts.ParentId = rootId`:

- The single `rootId` names the whole install. Restore derives the
  children ids from it, opens them immediately, and their content syncs
  in the background.
- Deleting a losing root cascade-deletes its children: cleanup is one
  tree delete.
- A `NewRoot` root must not be derived: derived trees cannot be deleted,
  so a losing install would be unresolvable. Ensure rejects a root it can
  prove is derived.
- Ensure stamps `any.name` (`Name`, falling back to the bundle id) on a
  new root. any-sync's head-sync diff skips root-only trees, so a root
  whose data lives entirely in children would never sync, and a losing
  root that never syncs cannot be resolved from another device.

## Derived roots

With `DerivedRoot`, the root is derived from the bundle id
(`spaceindex.BundleRootSeed`). A derived root change carries only
(spaceId, changeType, payload, parentId), with no identity, signature or
timestamp, so **the root id is a pure function of (space, bundle id)**:
every device and every member computes it offline.

A device that installs without having synced the registry therefore
cannot mint a different id. The `rootId` register converges on one
value, `roots` has one element, and `Losers` is empty. Both sides of a
partition get the same working install immediately, including the two
writers of a 1-1 space, where neither side can assume nobody else
installed.

`DerivedRootId(bundleId)` returns the same id without reading the
registry, installing, or touching the network. It answers "where would
this bundle live", not "is it installed".

**A claimed derived root always wins.** If a created and a derived root
are both claimed for one bundle id, every replica reports the derived one
as `rootId` regardless of the LWW register, and the created root becomes
an ordinary resolvable loser. The verdict reads only the add-only claim
set, so every replica reaches it from any prefix containing the claim.
Otherwise the register could leave the derived root as a loser, which
could never be deleted.

**The claim can race.** A device that installs derived without a
converged registry demotes an unseen created install of the same id to a
loser, on every replica, irreversibly. The demoted root keeps its content
and stays deletable, but the app loses its pointer to it; for content
that cannot be merged across objects (chat) that is the same as losing
it. Before installing derived into a space that may already carry a
created install, wait for convergence (§ Restore flow); installing after
the wait expires accepts that demotion.

Permanent costs:

- **No uninstall.** any-sync refuses to delete a derived tree
  (`ErrCantDeleteDerivedObject`), so the dead-winner reinstall (§ Known
  edges) does not apply. Use a derived root for setups that must exist
  on both sides of a partition (a space's chat), never for anything a
  user may remove.
- **Adoption wins.** A bundle already installed on a created root stays
  there even when a later call asks for `DerivedRoot`. `Bundle.Derived`
  reports what the install is.

**Children hang off a derived root by seed, not `ParentId`**: any-sync
rejects a derived parent (`ErrDerivedParent`). Seed them as
`<rootId>/<seed>`; the root id is canonical, so the children are equally
deterministic. `ParentId` only buys cascade deletion, and a derived root
is never deleted.

## Roots Ensure mints

For a derived root and for the SDK-created root of a declaring request,
Ensure writes the root's first content change before registering
anything, so a failed stamp leaves no install to adopt. That change
carries:

- membership: the definition marker when the request declares, otherwise
  `RootType`; plus `RootCollections` and every `RootProperties` owner that
  is neither (a property write to an owner the object lacks is rejected);
- `any.name`;
- the definition metadata;
- the `RootProperties` seeds.

`RootType`, `RootCollections` and `RootProperties` are refused with
`NewRoot`: that root got its state, type included, from the caller.

Later Ensures are idempotent:

- `RootType` is set only when the row has no type.
- A declaring request replaces a `RootType` that an earlier, non-declaring
  request gave an Ensure-minted root with the marker. It never replaces a
  marker of the other kind or the type of a `NewRoot` root (both
  `ErrBundleBadRequest`).
- A collection the row lacks is added (`$addToSet`), so a request that
  gains a root collection reaches an existing install.

On adopt, when the registry row arrived before the derived root's tree,
Ensure materializes the tree and stamps it: the device can mint it, so
there is no reason to return an id that isn't writable yet.
`RootProperties` are not re-seeded; the installer's values sync in. The
`any.name` stamp matters here too: head-sync skips a tree still on its
root change, so an unstamped local copy would never enter the diff or
pull the peer's content.

## Bundle-declared definitions: type roots and collection roots

A request declares a **type** on its root with `XKey`, `Parts`,
`Properties`, `Layout` and `Hidden`, or a **collection** with
`Collection: true`, `XKey` and `Properties`. A declaration is `Parts`,
`Properties` or `XKey` (`DeclaresType` / `DeclaresCollection`).
Metadata without a declaration, and `Parts` or `Layout` with
`Collection`, are `ErrBundleBadRequest`.

The root carries the marker in `any.type` (`__type__` or
`__collection__`) and its id is the definition's id. It has no type of
its own, so `RootType` next to a declaration is refused. It may carry
`RootCollections` (an app root filed under `miniapp`).

A definition object implicitly implements itself (docs/data-structure.md § Type and
collections): the root holds its own `<rootId>.<propId>` values and the
records of the datasets it declares. Three shapes:

- **Records host** — a root that holds its bundle's data (favourites
  entries, an app's setup state). It sets `Hidden`: a listed type is one
  a picker offers for other objects, which would grant them the bundle's
  collections.
- **Definition objects use** — a page whose part shares the editor, a
  person, or a wiki collection whose properties form the tree
  (`parentId` / `pos`). It stays listed. `{"any.type": rootId}` returns
  its objects and `{"any.collections": rootId}` its members, never the
  definition itself.
- **Marker** — an `XKey` alone: a flag an object carries as its type
  ("Template") or is filed under as a collection ("Archived"), with no
  columns or parts.

`Hidden` is always explicit. `XKey` is the definition's handle
(`TypeInfo.XKey` / `CollectionInfo.XKey`, stored as `type.xkey` /
`collection.xkey`): what consumers resolve the definition by and what
`relation.targetTypes` names. The SDK stores it as non-unique metadata;
uniqueness is the consumer's rule.

### Install sequence

A declaring install is the root plus up to three changes:

1. **Stamp** — one `objects` change on the root: membership
   (`any.type`; `any.collections` via `$addToSet` per entry, never a
   whole-array set), `any.name`, the metadata (`type.xkey` / `layout` /
   `hidden`, or `collection.xkey` / `hidden`) and the `RootProperties`
   seeds as their own op, so a seed a peer cannot resolve drops alone.
   The local-write pre-flight grants every namespace the change sets,
   including the root's own, so metadata and seeds validate alongside
   the membership write.
2. **Registry row** on the index object.
3. **Declarations** — one `properties` change and one `datasets` change.

Each dataset lands atomically, but a change covers one dataset
(05a-crdt-spec § 6.3), so a peer may briefly see the definitions before
the parts. Validation runs before any root is minted: an invalid or
duplicate draft, or a seed that cannot be encoded, fails with
`ErrBundleBadRequest`.

### Parts

`Parts` uses the `PartDraft` / `DatasetDraft` vocabulary of
`user-datasets.md`. Nothing is special-cased: the catalog's `__type__`
scan finds the root; the ownership check (the object's type declares the
dataset, or the object is the declaring type) passes on every carrier
and on the root itself; a namespaced dataset lives in `<rootId>_<key>` with the
`<rootId>:<shortId>` gate stamp; a shared one uses the module's canonical
collection; `Types().Parts(rootId)` / `Datasets(rootId)` and
`Space.Datasets()` list the declarations with `Owners = [rootId]`; records
go through `Upsert` / `Modify` / `Query`. A bundle's setup state lives in
records on its root, not in child objects.

Collections cannot collide: a namespaced dataset is `<rootId>_<key>` and
a shared one belongs to its module, so two bundles in one space may use
the same keys.

A part naming a **reserved** module (`handler.Module.Reserved`) is
refused with `ErrModuleReserved` unless the call passes the
`space.SystemInstall()` option, which marks the consumer's own catalog
install. Options are not request fields, so no request body can reach it.
The refusal is draft-time only: a declaration in the DAG stays valid on
apply. The declaring root is the module's only carrier; attaching its
type to another object is refused at local write time
(`handler.ErrValidationReservedCarrier`, docs/user-datasets.md § Model).

### Properties

`Properties` drafts (`PropertyDraft`, validated as `AddProperty`
validates them) get **deterministic ids**: every draft needs an `XKey`
unique within the request, and the id derives from `(rootId, XKey)`. Two
devices installing apart mint one column per handle. The descriptor
model otherwise allows two columns per handle (docs/data-structure.md § Property ids),
which is unacceptable here: a wiki with two `parentId` columns is a
forked tree.

Clients resolve `xKey → propId` through `Types().Properties(rootId)`
(`Collections().Properties(rootId)` answers the same for a collection).
A property added later with `AddProperty(rootId, …)` gets an ordinary
change-derived id. When two devices create the same deterministic id
apart, replicas see one create and one creation-shaped modify; the
property handler projects the shortId row from both, so every schema
state a peer may stamp on data writes is known and those writes don't
park.

On a declaring request, `RootProperties` and `RootCollections` naming the
root's own id are rejected: the root implements itself. Its property ids
are known before the install, so the consumer writes those values with
`Properties().Set(rootId, rootId, …)` afterwards.

### Adopt heals what is absent

Ensure never patches, resurrects or doubles a definition.

- **Parts** are written only on a root with no part declaration at all
  (crash before the declaring write, or a row adopted before the root
  tree synced). A root with any declaration, live or removed through
  `Types().RemovePart` / `RemoveDataset`, is left alone.
- **Properties** are checked one at a time. A definition is present when
  the root carries its deterministic id (live, or removed through
  `Types().RemoveProperty`: the tombstone keeps the id) or a live
  definition with its handle under any id. Only the rest are written.
- **Root metadata**: name, layout and hidden are written only when the
  root has no name. What the row lacks is still filled: the type when it
  has none, a listed collection (`$addToSet`), a handle when the root has
  none. Seeds are never re-written.

Evolution goes through `Types().AddPart` / `AddDataset` /
`AddDatasetField` / `PatchDataset` / `AddProperty` / `PatchProperty` with
`typeId = rootId`.

### Concurrency and crashes

- Declarations are written after the registering change. A failure in
  between leaves a registered row whose root has no declaration, which
  the next Ensure heals (a derived root at each device's own
  materialization; a created winner once its tree is local). The reverse
  order would strand an unreferenced root and wedge the bundle id.
- Two devices declaring the same key write two records;
  `CompileTypeParts` folds them to one per key (earliest `_ver.id`,
  datasets and fields unioned), so every replica converges on one
  definition. A `DefId` read before sync may change after it: look
  definitions up by key when evolving them.
- Concurrent created installs fork into roots that each carry their own
  declarations. Clients merge a loser's records through the winner's
  declaration, then `ResolveLoser` deletes the loser, declarations
  included.

## Tech-space bundles

Account-level product data (favourites, pinned items, personal settings)
lives in bundles on the tech space, reached through
`Spaces().Get(SDK.TechSpaceId())`, the restricted handle described in
`tech-space.md`. Same registry (`bundles` on the tech index object)
and the same `Ensure` / `Get` / `List` / `DerivedRootId` / `ResolveLoser`,
with three rules:

- Roots are minted by `Ensure` only: `NewRoot` is refused because free
  object create is fenced on the tech handle.
- A declaration (`Parts`, `Properties` or `XKey`) is required: a tech
  root exists to host its records as its own definition.
- No `RootType` / `RootCollections` / `RootProperties`.

Both strategies are available. `DerivedRoot` suits bundles that must
never fork or uninstall. The SDK-created root suits ordinary app
installs: `Objects().Delete` works on bundle roots, so deleting the
created winner reads as uninstalled (derived roots refuse at the object
layer), and concurrent offline installs fork and resolve as in any
space. The account is always the tech space's owner, so an app rule
letting the owner install after the convergence wait expires always
applies. Bundle ids are a versioned vocabulary (`favorites/v1`), never a
scratch namespace.

Restore on a second device: `WaitListSynced` →
`Spaces().Get(TechSpaceId())` → `WaitIndexSynced` (delegates to the list
gate) → `Bundles().Get` / `Ensure`. Records on the root sync like any
tree. The tech space is owner-only, so everything in it is synced scope
shared by the account's devices and nobody else.

## API (`space/bundles.go`)

- **`Ensure(req, opts...)`** — adopt or install; returns the row and
  whether this call registered it. Fully local, no network wait: if a
  winner exists it is returned and no root is minted; otherwise the root
  is minted and one change registers it (`$set rootId` + `$addToSet
  roots`). A created winner is provisional until the space syncs; a
  derived one is the same on every device.
- **`DerivedRootId(bundleId)`** — the canonical derived root id; pure
  computation.
- **`Get` / `List`** — registry rows with `Losers` computed and
  `Derived` reported.
- **`ResolveLoser(bundleId, loserRootId)`** — deletes a losing root and
  its children after the caller has merged what mattered. Never invoked
  automatically: two installs can hold real user data for days before
  they meet. Idempotent. `ErrBundleNotLoser` for the winner or an
  unclaimed root; `ErrLoserNotSynced` until the loser's tree is local
  (any-sync's settings-tree delete reads the local head entry).

Errors: `ErrBundleUnknown`, `ErrBundleBadRequest`, `ErrBundleNotLoser`,
`ErrLoserNotSynced`, `ErrBundleRootNotSynced` (the registry references a
root whose tree or row hasn't arrived; retry after sync).

## Wait primitives

- **`Service.WaitListSynced`** — tech-space gate for the create-vs-restore
  decision. After it returns, `List` reflects the responsible node's
  converged view (a clean head-sync round, no parked trees). Retries
  until `ctx` expires.
- **`Space.WaitIndexSynced`** — blocks until the local index view is
  trustworthy: the seeded metadata row is projected locally (immediate,
  works offline), or a clean head-sync round reports Synced. The second
  condition covers 1-1 and nameless derived spaces that never seed
  metadata: an absent index after convergence reads as "nothing set up".
  It covers projection, not just heads.

## Restore flow

1. `WaitListSynced`, then check whether the (derive-ahead) space id is in
   the list.
2. If it exists: open it, run the **convergence gate**
   (`WaitIndexSynced`), then `Bundles().Get` / `Ensure`. Derive children
   from the winner and open them immediately; content syncs in the
   background.
3. Otherwise: derive and create the space, then run setup via `Ensure`.

The convergence gate lets a created install adopt a winner that already
synced instead of forking against it. A derived install needs it only to
avoid demoting an unseen created install (§ Derived roots).

## Known edges

- A deleted winning root reads as **uninstalled** everywhere: `Get` /
  `List` report it absent and `Ensure` reinstalls on a fresh root.
  Otherwise the bundle id would be wedged, since record deletes are
  rejected and `rootId` has no unset.
- A crash between `NewRoot` and the registering write leaves one
  unreferenced object; the retried Ensure installs a fresh root. The
  orphan is unlisted, not a conflict.
- A never-seeded index blocks `WaitIndexSynced` only while offline: a
  converged round returns and the caller reads an empty registry. There
  is no forced re-setup.
- `Losers` counts a root whose deletion status is unknown (tree not
  local) as live. `ResolveLoser` re-validates and deletion is idempotent,
  so over-reporting is safe.
