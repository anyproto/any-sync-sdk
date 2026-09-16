# Identities

Every space member, 1-1 peer and join requester is an account identity.
A profile (name, icon, description) and the set of spaces where an
identity appears are account-global, so they live in one directory:
`SDK.Identities()`.

Two rules:

1. **Sync the key, derive the value.** Profiles live encrypted in the
   coordinator's `identityRepo`. A device needs only the decryption key
   to read one, so the key syncs across the account's devices and the
   resolved profile stays device-local.
2. **The directory is a cache.** identityRepo is authoritative for
   profiles, local membership for sightings. The directory keeps the
   last resolved values so reads are local and work offline.

## The metadata symkey

Each account has a deterministic metadata symkey:

```go
space.DeriveAccountMetadataSymKey(accountSignKey) // SLIP-0021, SDK-specific path
```

It needs no storage or rotation. Anyone holding it can read the
account's current and future profiles; it cannot be revoked (the same
trade-off as anytype-heart's account symkey). Every channel carries the
same bytes for an identity, so the directory's `symKey` needs no merge
rule.

**Push.** `Account.UpdateMetadata` and the boot republish encrypt the
profile with the symkey (`space.EncryptProfile`) and push it to
identityRepo under `Kind = "anysync-sdk-profile"`, signing the
ciphertext so readers verify before decrypting (`sdk.go`
`pushToIdentityRepo`).

**Distribution.** The symkey travels only over channels already
encrypted to the right audience; names and icons never travel in the
clear.

| Channel | Carries | Decrypted by |
|---|---|---|
| ACL `RequestMetadata` (join record, owner root) | the joining or owner account's symkey | the space metadata key (any member) |
| 1-1 inbox invite body | the initiator's symkey | ECIES to the receiver's account key |
| 1-1 `identityKeys` row on the spaceIndex object | each participant's own symkey | the 1-1 read key (both participants) |

Encoding: `internal/spaceimpl/acl.go` `encodeSelfSymKeyMetadata` /
`decodeSymKeyMetadata`.

**Resolve.** A name is fetched from identityRepo and decrypted with the
cached symkey (`metadataSymKeyFor` → self-derive or directory;
`fetchIdentityProfile`; `decodeIdentityRepoProfile`). Without a key the
identity stays unresolved (identity-only).

## The `identities` collection

One tech-space dataset keyed by account identity, mixing two sync
classes on one row (`internal/techspace/identities.go`):

| Field | Scope | Why |
|---|---|---|
| `symKey` | synced (`LocalWrite`) | every device of the account needs it to decrypt |
| `name` / `description` / `iconCID` | local (`LocalSet`) | derived from symkey + identityRepo per device |
| `spaceIds` | local array | spaces where the identity is currently seen |

`spaceIds` is indexed (`IdentitiesIndexes`) for by-space queries and
mutated with `$addToSet` / `$pull` on the local route; the synced route
rejects local fields.

Accessors (`internal/techspace/service.go`): `GetIdentityMetaKey` /
`SetIdentityMetaKey` (synced), `SetIdentityProfile` (local),
`AddIdentitySpace` / `RemoveIdentitySpace` / `RemoveSpaceFromIdentities`
(sightings), `GetIdentity` / `ListIdentities`.

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

Consumers never see the symkey. `Subscribe` runs on the tech-space
`SubEngine`, like `Spaces().Subscribe`. On the tech-space handle, a
generic `Query` of `identities` returns `ErrUnsupported` for the same
reason.

## Population and pruning

- **Member watcher** owns space sightings and member profiles. On
  construction it caches symkeys, records sightings for current members
  and starts the first profile fetch; its tick skips an unchanged ACL
  head, so members present at start are only processed by this seed. The
  tick handles newcomers and members whose symkey just arrived, and
  records sightings on membership changes. `fetchProfilesFor` writes
  resolved profiles through to the directory.
- **1-1 resolver** (`resolveOneToOnePeerName`) and the **join-request
  resolver** write profiles and sightings the same way.
- **Reads:** `recordToInfo` fills a 1-1 row's name from the directory;
  the row's own name is the out-of-band `displayHint` fallback.
- **Pruning:** `OffloadSpace` (space delete or offload, not cache
  eviction) calls `RemoveSpaceFromIdentities`, so `spaceIds` reflects
  live memberships.

## Cold sync

A fresh device receives symkeys over tech-space sync but no profiles.
`sdk.Open` starts `ResolveIdentityProfiles` in the background: one batch
fetch of every identity with a symkey and no profile, chunked to the
coordinator's limit of 350 identities per request
(`identityRepoMaxBatch`; the per-space member fetcher chunks the same
way). Symkeys that arrive after boot resolve when a space containing the
identity is next opened.

## Members and 1-1

- **Members** (`docs/space.md`) is the per-space, ACL-derived view.
  Member names come from the same identityRepo resolution; the directory
  is the shared profile cache.
- **1-1** (`docs/one-to-one-spaces.md`) surfaces the peer as
  `SpaceInfo.Author` and resolves the name through the directory. The
  inbox invite carries the initiator's symkey for the pending row. Once
  the space is active, each participant publishes its own symkey in
  `identityKeys`, so the key crosses in both directions without the
  inbox (§ Key exchange inside the space there).
