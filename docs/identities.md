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

**Push.** `Account.UpdateMetadata`, `Account.SetIcon` and the boot
republish encrypt the profile with the symkey (`space.EncryptProfile`)
and push it to identityRepo under `Kind = "anysync-sdk-profile"`,
signing the ciphertext so readers verify before decrypting (`sdk.go`
`pushProfile`). The record is `space.Profile`: name, description, the
avatar's content address (`IconCID`) and, when an avatar is set, the
space holding its file and the file key (§ Avatars).

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
| `name` / `description` / `iconCID` / `iconSpaceId` / `iconKey` | local (`LocalSet`) | derived from symkey + identityRepo per device |
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
    OpenIcon(ctx, identity string) (FileReader, error)
}

type IdentityInfo struct {
    Identity    string
    Name        string
    Description string
    IconCID     string
    SpaceIds    []string // spaces where this identity is currently seen
}
```

Consumers never see the symkey or an avatar's file key. `Subscribe`
runs on the tech-space `SubEngine`, like `Spaces().Subscribe`. On the
tech-space handle, a generic `Query` of `identities` returns
`ErrUnsupported` for the same reason.

## Avatars

An avatar is a files-v2 object in the owner's tech space, bound to the
tech index object (`spaceimpl.AttachTechFile`): it rides the owner's
quota and syncs to the owner's devices like any tech-space row. The
restricted tech handle still withholds `Files()`; the account API is
the only writer.

- **Set.** `Account.SetIcon(ctx, r, IconOpts)` stores the bytes (at
  most `space.MaxIconSize`, `ErrIconTooLarge` beyond), always on the
  content-addressed tier — a file under the inline cutoff would ride
  inside the sealed row with no `rootCid` for a non-member to fetch —
  records `{iconCID, iconFileId}` on the `profile` row, deletes the
  replaced file and pushes the profile. `UpdateMetadata` carries name
  and description only; an `IconCID` there fails with
  `ErrProfileIconReadOnly`. `DeleteIcon` clears and republishes.
- **Publish only once durable.** The profile carries the avatar
  reference (`IconSpaceId`, `IconKey`) only after the file's custody
  receipt is on its row: the public read path serves durable objects
  alone, so a reader never holds a reference it cannot fetch. Until
  then the pushed profile names no avatar; a status watch on the tech
  space republishes on the durable flip (and the boot republish re-arms
  it after a restart mid-upload).
- **Read.** `Identities().OpenIcon(ctx, identity)` opens the avatar a
  cached profile names: `fetch.Open(iconSpaceId, iconCID, iconKey)` —
  local cache, LAN peer, then the public read URL
  `{base}/blob/{spaceId}/{rootCid}` — decrypted with the key from the
  profile. The reader never joins or syncs the owner's space and
  nothing on the network checks membership on that path, which is the
  same path every file download already takes. The own avatar opens
  through the tech-space file. `ErrNotFound` when the identity or its
  avatar is unknown; `ErrFileNotAvailable` when the bytes cannot be
  fetched (no public read base on this network).
- **Cache.** A contact's avatar is cached under the ref
  `identity/<identity>`. The safety sweep retains rows of a space it
  cannot resolve, so the directory write-through
  (`spaceimpl.setIdentityProfile`) is the reclaim path: when a profile
  moves to another avatar, or to none, the old ref is released and the
  CAR deleted when nothing else holds it.
- **Key hygiene.** The file key stays the random per-file key the
  byte layer always uses (zero-IV CFB; one key per file). It is sealed
  inside the profile under the metadata symkey, so it reaches exactly
  the contacts who can read the name — never derived from the symkey
  itself, which would reuse one key across avatar versions.
- **What a contact learns.** The owner's tech space id, sealed in the
  profile. It unlocks nothing: sync nodes serve a space to ACL members
  only, and the public object store serves durable blobs by exact
  `rootCid`.

## Population and pruning

- **Member watcher** owns space sightings and member profiles. On
  construction it caches symkeys, records sightings for current members
  and starts the first profile fetch; its tick skips an unchanged ACL
  head, so members present at start are only processed by this seed. The
  tick handles newcomers and members whose symkey just arrived, and
  records sightings on membership changes. `fetchProfilesFor` writes
  resolved profiles through to the directory. Every fetch runs on the
  watcher's profile loop: the tick and `KickProfiles` only queue a
  request (`requestProfiles`), and stopping the watcher cancels the
  round-trip in flight and joins the loop.
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
