# Identities

## Vision

Every space member, 1-1 peer, and join requester is an **account identity**.
Their human-facing profile (name, icon, description) and the question "where
have I seen this person" are cross-cutting — they don't belong to any single
space. `SDK.Identities()` is the account-global directory that answers them.

Two design rules drive everything below:

1. **Sync the key, derive the value.** A contact's profile lives in the
   coordinator's `identityRepo`, encrypted. The only thing a device *must*
   receive to read it is the decryption key; the profile itself is re-derivable
   on every device from that key + identityRepo. So the **key syncs** across the
   account's devices, the **derived profile does not**.
2. **The directory is a cache, not a source of truth.** identityRepo is
   authoritative for profiles; local membership is authoritative for "where seen".
   The directory persists the last resolved values so reads are local and
   offline-tolerant, and is refreshed from the sources.

## Encrypted identities (the metadata symkey)

Each account has a deterministic **metadata symkey**:

```go
space.DeriveAccountMetadataSymKey(accountSignKey) // SLIP-0021, SDK-specific path
```

Deterministic ⇒ no storage or rotation; anyone who has it can read this
account's current and future profile. (It cannot be revoked — same trade-off as
anytype-heart's account symkey.) Every channel below carries the same bytes
for an identity, so the directory's `symKey` needs no ordering rule: a
differing value can only come from a changed derivation, which is a
migration, not a last-writer race.

**Push.** `account.UpdateMetadata` / boot republish encrypt the profile blob
with the symkey and push it to identityRepo, signing the **ciphertext** so the
fetch path verifies before it decrypts (`sdk.go` `pushToIdentityRepo`,
`space.EncryptProfile`). identityRepo `Kind = "anysync-sdk-profile"`.

**Distribute the key** through channels that are already encrypted to the right
audience — the SDK never sends a profile name/icon in the clear, and the ACL no
longer carries inline name/icon (it carries the symkey only):

| Channel | What it carries | Decrypted by |
|---|---|---|
| ACL `RequestMetadata` (join record, owner root) | the joining/owner account's **symkey** | the space metadata key (any member) |
| 1-1 inbox invite body | the initiator's **symkey** | ECIES to the receiver's account key |
| 1-1 `identityKeys` row on the space's spaceIndex object | each participant's **own symkey** | the 1-1 read key (both participants) |

`internal/spaceimpl/acl.go` `encodeSelfSymKeyMetadata` / `decodeSymKeyMetadata`.

**Resolve.** A member/peer name is fetched from identityRepo and decrypted with
the cached symkey (`metadataSymKeyFor` → self-derive or directory cache;
`fetchIdentityProfile`; `decodeIdentityRepoProfile`). No key ⇒ the identity
stays unresolved (identity-only) rather than showing garbage.

## The `identities` collection

One tech-space dataset, keyed by the account identity, **mixing two sync classes
on one row** (`internal/techspace/identities.go`):

| Field | Scope | Why |
|---|---|---|
| `symKey` | **Synced** (DAG / `LocalWrite`) | a device needs it to decrypt; must cross the account's devices |
| `name` / `description` / `iconCID` | **Local** (`LocalSet`, never synced) | derived from symkey + identityRepo; re-resolved per device |
| `spaceIds` | **Local** array | the spaces where we've currently seen this identity |

`spaceIds` is a `KindArray` with an any-store index (`IdentitiesIndexes`) for
by-space queries; mutated with `$addToSet` / `$pull` on the **local** route
(local fields are rejected on the synced route).

Service accessors (`internal/techspace/service.go`): `Get/SetIdentityMetaKey`
(symkey, synced), `SetIdentityProfile` (local), `AddIdentitySpace` /
`RemoveIdentitySpace` / `RemoveSpaceFromIdentities` (sightings, local),
`GetIdentity` / `ListIdentities`.

## Public API

```go
sdk.Identities() space.IdentitiesAPI

type IdentitiesAPI interface {
    List(ctx) ([]IdentityInfo, error)
    Get(ctx, identity string) (IdentityInfo, bool, error)
    Subscribe(cb func(IdentityListEvent)) (cancel func())
}

type IdentityInfo struct {
    Identity    string
    Name        string
    Description string
    IconCID     string
    SpaceIds    []string // spaces where this identity is currently seen
}
```

The symkey is internal — consumers never see decryption keys. `Subscribe`
reuses the tech-space `SubEngine` (same harness as `Spaces().Subscribe`).

## Population & pruning

- **Member watcher** is the authority for space sightings + member profiles. Its
  construction **seed** caches symkeys and records sightings for members present
  at start, and kicks the first profile fetch (the first tick early-returns on an
  unchanged ACL head, so seeded members — e.g. the owner from a fresh joiner's
  view — would otherwise never be processed). The tick handles newcomers and
  members whose symkey just became available, and records sightings for all
  current members on a membership change. `fetchProfilesFor` write-throughs
  resolved profiles to the directory.
- **1-1 resolver** (`resolveOneToOnePeerName`) and the **join-request resolver**
  also write through profiles + sightings.
- **Reads:** `recordToInfo` fills a 1-1 row's name from the directory (the row's
  own name is an out-of-band `displayHint` fallback).
- **Prune on leave/delete:** `OffloadSpace` (the delete/offload chokepoint, not
  routine cache eviction) calls `RemoveSpaceFromIdentities`, so `spaceIds`
  reflects live memberships.

## Cold sync

A fresh device receives the directory's **symkeys** over tech-space sync but
holds no profiles (those are device-local). `Service.ResolveIdentityProfiles`
(kicked from `sdk.Open`) batch-fetches the missing profiles in one round —
**chunked to the coordinator's 350-identities-per-request cap**
(`identityRepoMaxBatch`; the per-space member fetcher chunks too). Best-effort at
boot; symkey rows arriving *after* `Open` on a brand-new device are re-resolved
when a space is next entered (a sync-triggered resolve is a possible follow-up).

## Relationship to members & 1-1

- **Members** (`docs/space.md`) is the per-space, ACL-derived view; member
  names come from the same identityRepo resolution and the directory is the
  shared profile cache.
- **1-1** (`docs/one-to-one-spaces.md`) surfaces the friend's identity as
  `SpaceInfo.Author` and resolves the friend's name through the directory. The
  inbox invite carries the initiator's symkey for the pending row; once the
  space is active on both sides each participant publishes its own symkey
  inside the space (`identityKeys`), so the key crosses in both directions
  without the inbox (§ Key exchange inside the space there).
