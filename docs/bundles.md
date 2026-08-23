# Bundles Registry

Per-space registry of everything installed into the space — marketplace
bundles or hardcoded setups (e.g. bao). Lives as the `bundles` dataset
on the in-space spaceIndex object, so one derived tree carries both the
space's display metadata and its setup state, and one wait covers both.

## Problem

Setup used to be derive-only: deterministic ids, no data written, so
concurrent multi-device setup converged by construction. Once a setup
creates a *non-derived* root object (needed the moment setup writes
data), two devices installing concurrently mint two different roots.
Without a registry there is no deterministic winner, no way to find the
loser's objects, and restore has to pull everything before knowing what
is set up.

## Data model

One record per bundle in the `bundles` dataset on the spaceIndex
object (`internal/types/spaceindex/bundles.go`):

| Field | Op | Semantics |
|---|---|---|
| record id | — | Stable bundle identifier (marketplace id, `bao/v1`). Permanent — record deletes are rejected (the sticky tombstone would ban the id forever). |
| `name` | `$set` | Display name. |
| `rootId` | `$set` | The winning root object id — a plain LWW register; concurrent installs converge to one deterministic winner (canonical DAG order). |
| `roots` | `$addToSet` only | Every root ever claimed. Add-only: a `$set` would stamp `_ver[roots]` and gate out concurrent adds, hiding the losing device's objects. Never shrunk — losers stay listed as the audit trail; their death is recorded by tree deletion, not by mutating the array. |

Handler-enforced invariants (deterministic, so they cover every writer):
explicit single-field paths only; `rootId` writes must claim the same id
in `roots` within the same change (`roots ⊇ {rootId}` in every reachable
state); `roots` accepts only `$addToSet` of non-empty strings; no record
deletes. Losers are always computable as `roots − rootId − deleted`.

## Root and children

The registered root is one non-derived object (`Objects().Create`);
everything else in the setup is derived from it with
`DeriveObjectOpts.ParentId = rootId`. Consequences:

- The single `rootId` transitively names the whole install — restore
  derives the children ids from it, opens them immediately, and their
  content syncs in the background.
- Deleting a losing root cascade-deletes its derived children — cleanup
  is one tree delete, no enumeration.
- A root passed through `NewRoot` must NOT be derived (derived trees
  cannot be deleted, so a losing install would be unresolvable). Ensure
  rejects a root it can prove is derived. The opt-in derived root below
  is the one exception, and it earns it by never losing.
- Ensure stamps `any.name` on a freshly created root. Load-bearing:
  any-sync's head-sync diff skips root-only trees, so a root whose data
  lives entirely in derived children would never sync — and a losing
  root that never syncs could never be resolved from another device.

## Derived roots

`EnsureBundleRequest.DerivedRoot` installs the bundle on a root derived
from the bundle id (`spaceindex.BundleRootSeed`) instead of a created
one. A derived root change carries only (spaceId, changeType, payload,
parentId) — no identity, no signature, no timestamp — so **the root id
is a pure function of (space, bundle id)**: every device and every
member computes it offline, with zero communication.

That removes the reason the convergence gate exists. A device that
installs without having synced the registry cannot mint a *different*
id, so there is no fork to prevent: the `rootId` register converges on
an identical value, `roots` converges to one element, `Losers` is empty
by construction. Both sides of a partition — including the two writers
of a 1-1 space, where nobody is the owner and nobody can claim "no one
else could have installed" — get the same working install immediately.

`DerivedRootId(bundleId)` exposes the same computation on its own: it
answers "where would this bundle live" without reading the registry,
installing anything, or touching the network.

**A claimed canonical derived root always wins.** If both a created and
a derived root are ever claimed for one bundle id, every replica reports
the derived one as `rootId` regardless of what the LWW register
converged to, and the created root becomes an ordinary resolvable loser.
The verdict reads only the add-only claim set, so every replica reaches
it from any prefix containing the claim. Without it the LWW register
could strand the derived root as a loser — and a derived tree cannot be
deleted, which is the one conflict the registry could never resolve.

**The verdict is not a race; the claim can be.** A device that installs
derived without a converged registry cannot fork *among derived
installs* — but if the space already carried a **created** install it has
not seen, its claim demotes that install to a loser on every replica,
irreversibly. The demoted root keeps its content and stays deletable, so
nothing is destroyed; what is lost is the app's pointer to it, and for
content that cannot be merged across objects (chat) that is the same
thing. This is the one protection the convergence wait still buys a
derived install, and the deliberate trade of installing anyway: converge
before installing derived into a space that may already carry a created
install of the same id, and treat expiring the wait as accepting that
demotion.

**The costs, both permanent:**

- **No uninstall.** any-sync refuses to delete a derived tree
  (`ErrCantDeleteDerivedObject`), so the dead-winner escape below does
  not apply: the bundle id stays bound to that root forever. Choose a
  derived root for setups that must exist on both sides of a partition
  (a space's chat), not for anything a user may remove.
- **Adoption still wins.** A bundle already installed on a created root
  stays on it, even when a later call asks for `DerivedRoot` — nothing
  migrates behind the app's back. `Bundle.Derived` reports what the
  install actually is.

**Setup objects hang off a derived root by SEED, not by `ParentId`** —
any-sync rejects a derived object as a parent (`ErrDerivedParent`). Seed
them with the root id folded in (`<rootId>/<seed>`): the root id is
canonical, so the children are just as deterministic. Nothing is lost —
the ParentId binding buys cascade deletion, and a derived root is never
deleted.

Ensure derives the root itself, attaches `RootTypes` idempotently and
seeds `RootProperties` before registering anything (a failed seed
therefore leaves no install to adopt); `NewRoot` must be nil. Every type
`RootProperties` writes into is attached along with `RootTypes` — a
property write to a type the object does not implement is rejected, and
the created path attaches the same union through `Objects().Create`.

On the adopt path Ensure materializes the canonical tree — and stamps it
— when the row arrived before the tree did: the same device can mint it,
so there is no reason to hand back an id that is not yet writable, and
the stamp is what puts the fresh local copy back in that device's
head-sync diff. `RootProperties` are not re-seeded there; seeding
belongs to the install, and the installer's values sync in. The `any.name` stamp applies here too and is
load-bearing for a different reason: head-sync skips a tree still
sitting on its root change, so a derived root that a device never
stamped would sit outside that device's diff and never pull the peer's
content.

## Bundle datasets: self-typed roots

`EnsureBundleRequest.Datasets` declares runtime datasets (the
`DatasetDraft` vocabulary of `17-user-datasets.md`) on a derived root.
The root then carries `any.types = ["__type__", rootId]`: it is a type
object implementing itself, `typeId = rootId`. Nothing else is
special-cased — the catalog's `__type__` scan finds it, the membership
check (`any.types ∋ owner`) passes, the gate stamp is the ordinary
`<rootId>:<shortId>`, `Types().Datasets(rootId)` and `Space.Datasets()`
list the declarations with `TypeId = rootId`, and records go through
`Upsert` / `Modify` / `Query` on the root. The bundle's setup state
lives in records, not in child objects.

- Declarations are written after the root's types and before the
  registry row, so a failed declaration leaves no install to adopt;
  the retry declares only what is still missing.
- Declarations are one change. An adopt declares them only on a root
  that carries no declaration yet (crash before the row, a row
  adopted before the root tree synced); a root with any declaration —
  live, or removed through `Types().RemoveDataset` (a tombstone keeps
  no name) — is left alone: `Ensure` never patches, adds or
  resurrects a dataset. Evolution goes through `Types().AddDataset` /
  `AddDatasetField` / `PatchDataset` with `typeId = rootId`. Adopting
  never renames the root either: the name stamp is written only when
  the root carries none.
- Dataset names are unique per space (the runtime catalog resolves
  by name across types). A name another type or bundle already owns,
  or one the store reserves (built-ins, the tech space's system
  datasets), fails `Ensure` with `ErrBundleBadRequest` before the
  permanent root is derived. Two bundles that must coexist in one
  space prefix their dataset names (`favorites_entries`); a bundle
  vocabulary version bump that keeps a name is a new root claiming an
  owned name and is refused — rename the dataset with the version.
- Adopting an install that lives on a created root with `Datasets`
  set fails with `ErrBundleBadRequest` instead of dropping the
  declarations.
- Two devices declaring the same name concurrently write two heads;
  `CompileDatasetDefs` resolves them to one per name (first creation
  `_ver.id`), so every replica converges on one definition. A `DefId`
  read on the losing device before sync changes after it: look
  definitions up by name when evolving them.
- `DerivedRoot` only (a created root's losers would each carry their
  own declarations). Accepted in every space; required on the tech
  space.
- `RootProperties` keyed by the root's own id are rejected: the
  self-type grants a dataset namespace, not property definitions.

## Tech-space bundles

Account-level product data (favourites, pinned items, personal
settings objects) lives in bundles on the tech space, reached through
`Spaces().Get(SDK.TechSpaceId())` — the restricted handle described in
`02-tech-space.md`. Same registry (`bundles` on the tech index object),
same `Ensure` / `Get` / `List` / `DerivedRootId`, with two rules:
derived-only (`NewRoot` is refused, `ResolveLoser` is unsupported) and
`Datasets` required. Bundle ids are a versioned vocabulary
(`favorites/v1`), never a scratch namespace: derived trees cannot be
deleted.

Restore on a second device: `WaitListSynced` → `Spaces().Get(TechSpaceId())`
→ `WaitIndexSynced` (delegates to the list gate) → `Bundles().Get` /
`Ensure`; records on the root sync in like any tree. The tech space is
owner-only, so everything in it is synced scope shared by the
account's devices and nobody else.

## API (`space/bundles.go`)

- `Space.Bundles().Ensure` — adopt-or-install, returning the row and
  whether THIS call registered it. Fully local, no network wait
  (offline-first): a winner exists → return it, no root minted;
  otherwise the root is minted (`NewRoot`, or the canonical derivation
  for `DerivedRoot`) and one change registers it. The returned winner is
  provisional until the space syncs — unless it is derived, which is the
  same on every device by construction. `Datasets` declares runtime
  datasets on a derived root (see § Bundle datasets).
- `DerivedRootId` — the canonical derived root id for a bundle id. Pure
  computation: no registry read, no materialization, no network.
- `Get` / `List` — read the registry with `Losers` computed.
- `ResolveLoser` — deletes a losing root (cascade) after the caller has
  merged whatever content mattered out of it. Never auto-invoked: two
  installs can carry real user data for days before they meet (offline),
  so loser cleanup is always an explicit app decision. Idempotent.
  Deletion needs the loser's tree synced locally (any-sync's
  settings-tree delete reads the local head entry); until then the call
  errors and the app retries after sync.
- Generic reads/subscriptions:
  `Space.Query(SpaceIndexObjectId(), "bundles").Subscribe(...)`.

## Wait primitives

- `Service.WaitListSynced` — tech-space gate for the creation-vs-restore
  split: after it returns, `List` reflects the responsible node's
  converged view (clean head-sync round, no parked trees). Retries until
  ctx expires.
- `Space.WaitIndexSynced` — blocks until the local index view is
  trustworthy: seeded metadata row projected locally (immediate,
  offline-capable), or a clean head-sync round with the Synced rollup —
  the latter covers 1-1 / nameless derived spaces that never seed
  metadata (an absent index after convergence reads as "nothing set
  up", not wait-forever). Covers projection, not just heads.

## Restore flow (app side)

1. `WaitListSynced` → does the (derive-ahead) space id exist in the
   list?
2. Exists → open it → `WaitIndexSynced` → `Bundles().Get/Ensure` →
   derive children from the winner, open immediately, content syncs in
   background.
3. Doesn't exist → derive + create the space, run setup via `Ensure`.

## Known edges

- A deleted winning root reads as **uninstalled** on every surface
  (Get/List report absent, Ensure reinstalls fresh) — without this the
  bundle id would be permanently wedged, since record deletes are
  rejected and `rootId` has no unset.
- The `bundles` dataset is fenced off the public Modify/Delete surface
  (like `payloads`); writes go through the typed API only.
- The dataset name `bundles` is reserved retroactively: a pre-existing
  runtime dataset with that name (legal before this shipped) is
  shadowed by the built-in. Accepted without migration — pre-release
  decision.

- Crash between `NewRoot` and the registering write leaves one
  unreferenced object; the retried Ensure installs a fresh root. The
  orphan is unlisted junk, not a conflict.
- A never-seeded index blocks `WaitIndexSynced` only while offline: a
  converged round returns "nothing set up" and the caller reads an
  empty registry; a hard re-setup mechanism is future work.
- `Losers` counts a root with unknown deletion status (tree not present
  locally) as live; `ResolveLoser` re-validates and deletion is
  idempotent, so over-reporting is safe.
