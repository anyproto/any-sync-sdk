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
- Roots must NOT be derived objects (derived trees cannot be deleted, so
  a losing install would be unresolvable). Ensure rejects a root it can
  prove is derived.
- Ensure stamps `any.name` on a freshly created root. Load-bearing:
  any-sync's head-sync diff skips root-only trees, so a root whose data
  lives entirely in derived children would never sync — and a losing
  root that never syncs could never be resolved from another device.

## API (`space/bundles.go`)

- `Space.Bundles().Ensure` — adopt-or-install. Fully local, no network
  wait (offline-first): a winner exists → return it, `NewRoot` not
  called; otherwise `NewRoot` creates the root and one change registers
  it. The returned winner is provisional until the space syncs.
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
- `Space.WaitIndexSynced` — blocks until the spaceIndex object's seeded
  state is projected locally. Returns immediately (no network) when the
  index is already local; otherwise forces head-sync rounds. The wait
  covers projection, not just heads (materialize + ColdRestore each
  attempt).

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
- A space whose index was never seeded (legacy space, owner crashed
  pre-seed) keeps `WaitIndexSynced` waiting until ctx expires; a hard
  re-setup mechanism is future work.
- `Losers` counts a root with unknown deletion status (tree not present
  locally) as live; `ResolveLoser` re-validates and deletion is
  idempotent, so over-reporting is safe.
