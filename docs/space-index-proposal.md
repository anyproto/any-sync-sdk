# Per-Space `spaceIndex` Object — Proposal

## Problem

Space `Name`, `IconCID`, `Description` currently live only in the per-account
tech-space (the `spaces` dataset, see [02-tech-space.md](02-tech-space.md)).
That gives Alice's own devices a synced view of her spaces, but other members
joining her space see nothing — their tech-space record holds whatever they
typed locally (often empty), not what Alice configured.

We need a per-space, member-visible source-of-truth for these fields.

## Proposal

A derived object inside every space, `any.type = "spaceIndex"`, holding a single row
with name/icon/description as properties. Synced via the regular DAG, no new
infra.

## Data model

| Aspect | Value |
|---|---|
| Tree id | Derived from a fixed seed `"builtin:spaceIndex.v1"`. Same id on every member's device. |
| Tree storage | In the space, sibling to other trees. |
| Dataset | Single dataset, single record, `id = "self"`. |
| Properties | `name` (string), `iconCID` (string), `description` (string). |
| Schema | Bundled built-in type. Versioned via `DataVersion` for forward-compat. |
| ACL | Owner-write, member-read. Handler enforces on every op. |

## API surface

```go
type SpaceMetadata struct {
    Name        string
    IconCID     string
    Description string
}

// Read — local, no network. Reads the synced object's current state.
sp.Metadata(ctx) (SpaceMetadata, present bool, err error)

// Write — owner only. Persists to the spaceIndex tree → DAG sync
// propagates to all members.
sp.UpdateMetadata(ctx, SpaceMetadata) error

// Subscribe — fires when the spaceIndex tree updates, either from a
// local write or a peer push. Mirrors Members.Subscribe.
sp.SubscribeMetadata(cb func(SpaceMetadata)) (cancel func())
```

`CreateRequest.Name` keeps its meaning: `Spaces().Create({Name: "Books"})`
internally derives the spaceIndex tree and writes the initial
`{name: "Books"}` record before returning.

## Wiring

1. **At create time** (`spaceimpl.Service.Create`):
   - After `NewSpace + Init`, derive the spaceIndex tree using the seed
   - `PutTree` (first creation) + write the initial record
   - That single first change is what propagates to other members later

2. **At space load time** (members joining or re-opening):
   - Same derive-id call yields the same tree id
   - `BuildTree` (already-created path)
   - Initial state comes from the synced DAG history — no bootstrap

3. **Mirror to tech-space**:
   - When the in-space object updates (local or remote), mirror
     `name/iconCID/description` to the per-account tech-space `spaces`
     record for that spaceId
   - Keeps existing `SDK.Spaces().List()` working unchanged
   - One-way: in-space → tech-space (see open question 2)

4. **Watcher**:
   - Per-space `spaceIndexWatcher` analogous to `memberWatcher`
   - Polls the controller for changes, fires `SubscribeMetadata`
     callbacks, drives the mirror-to-tech-space step
   - May piggyback on existing controller observer infra if available
     (TBD on inspection)

## Cross-cutting

- **First-write ordering at Create**: the owner is the only member when the
  initial write happens, so no race. Subsequent members see the record via
  normal DAG sync on join.
- **Owner-only enforcement**: handler's `BeforeCreate` / `BeforeModify`
  checks `ChangeCtx.Author == OwnerPubKey`. ACL layer already prevents
  non-owners from publishing — this is defence in depth.
- **Derive seed**: namespaced constant baked into the SDK, paralleling
  `techspace.SpaceIndexDeriveSeed`.
- **Schema bundling**: built-in type, not user-registered. Lives alongside
  other system types.

## Open questions

1. **Beyond owner.** Heart likely allows admins to edit space settings. v1:
   owner-only. Cheap to relax later, expensive to tighten.
2. **Tech-space `name/icon` fields.** Two options:
   - **(a)** Drop them as a writeable surface; the in-space object is the
     only writer. Tech-space becomes a read-only mirror. Cleaner.
   - **(b)** Keep them as local override (e.g. Alice renames `"Books"` →
     `"📚 Books"` locally without affecting members). More work, possibly
     confusing.
   Default to (a); add overrides later if anyone asks.
3. **Empty-state handling.** Same `(present, zero-value)` contract as
   `AccountAPI.Metadata`: `present=false` on a brand-new space whose
   creator never set `Name`.
4. **Migration.** Spaces created before this lands have no spaceIndex
   tree. On first post-upgrade load, owner derives + writes the tree from
   the tech-space record they already have. Members back-fill via the
   subsequent DAG sync.
5. **Heart compatibility.** Heart has a "workspace" object that probably
   carries similar fields. Worth a quick read to see if we can align
   formats and avoid a parallel fork.

## Suggested order of work

1. Define the type + schema + derive seed (one declarative file)
2. Wire derive-and-create in `spaceimpl.Service.Create`
3. `Space.Metadata()` + `Space.UpdateMetadata()` — read/write on top of
   existing object machinery
4. Mirror-to-tech-space hook
5. `Space.SubscribeMetadata()` — patterns from `Members.Subscribe`
6. E2E test: Alice creates "Books", Bob joins, Bob's `sp.Metadata()`
   returns `Name: "Books"` without 60s wait
