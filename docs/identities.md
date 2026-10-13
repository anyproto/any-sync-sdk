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

sdk.Account() adds SetIcon / DeleteIcon / OpenIcon (§ Avatars).

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
  writes `{iconCID, iconFileId}` on the `profile` row, deletes the
  replaced file and pushes the profile. `DeleteIcon` clears and
  republishes; it also clears a cid a client wrote before avatars were
  files. `Account.OpenIcon` reads the own file.
- **The profile row is written per field.** `UpdateMetadata` sets name
  and description, `SetIcon` / `DeleteIcon` set the avatar fields
  (`techspace.SetProfileMetadata` / `SetProfileIcon`), so two devices
  editing different parts of the profile converge per field instead of
  the last full-row write winning. `UpdateMetadata` refuses an
  `IconCID` (`ErrProfileIconReadOnly`) and a NUL byte in a field
  (`ErrProfileFieldInvalid`: NUL is the record separator). A cid with no
  file id behind it is not reported by `Metadata` and never pushed.
- **Publish only once durable.** The pushed profile carries the avatar
  reference (`IconSpaceId`, `IconKey`) only when the file row on this
  device has its key and either carries the custody receipt or the
  row's synced `iconPublishedFileId` names this very file — a device of
  the account already pushed its reference, so the file is durable and
  the receipt just has not synced here yet. The public read path serves
  durable objects alone, so a reader never holds a reference it cannot
  fetch. The device whose push includes the reference writes the mark
  (a local write on its own context, so a cancelled push cannot lose
  it); `SetIcon` and `DeleteIcon` clear it together with the avatar
  fields, and a mark names a file, so one written for an avatar never
  vouches for the next.
- **While an avatar is pending** — uploading, or its row or mark not
  synced here yet — a caller's own write (`SetIcon`, `UpdateMetadata`,
  `DeleteIcon`) goes out without the avatar (contacts see none until
  the flip). A push that mirrors the row (boot or sync-driven) goes
  out without it only when the pending file is this device's own
  upload — its bytes are here, nobody else can publish it first, and
  the watch adds it once durable; otherwise the mirror push is
  skipped, because this device cannot tell whether another device
  already published the reference, and pushing without it would drop
  it for every contact. So an avatar another device never finishes
  uploading (quota, a device lost with the bytes) holds this device's
  mirror pushes until `SetIcon` or `DeleteIcon` replaces it; the
  device's own writes still go out. Every pending observation arms the
  avatar watch (`runIconWatch`): one goroutine that re-reads the row
  on every durable status event for the file (the uploading device's
  queue), every `iconWatchPoll` (the receipt reaches the account's
  other devices through sync of the row, which raises no event), and
  once at start. It republishes under `pushMu` with a bounded,
  close-cancelled context, with the readiness it checked; the watch is
  stopped only after a successful put, so a failed one is retried on
  the next tick. It ends when published, when the profile row is gone
  or names another avatar, or on `SDK.Close`, which cancels and joins
  it (a push queued behind a caller holding `pushMu` waits for that
  caller's own RPC).
- **The row is mirrored.** The profile row is the source of truth, and
  it syncs between the account's devices. After the boot republish the
  SDK subscribes to the row (`startProfileSync`), reconciles once (the
  row may have changed under the boot push), and pushes it again after
  each burst of changes — a name edited elsewhere, avatar fields that
  arrive after this device published without them, the published mark
  — putting the record again even when this device put the same one
  before (another device may have overwritten it meanwhile; a put is
  cheap), retrying a failed push with backoff, and reopening a
  subscription the engine closed. A pending avatar seen here arms the
  watch instead of pushing, unless it is this device's own upload. So
  a device never needs the event it missed: the row it holds is what
  it publishes, as soon as the row is complete.
- **Read.** `Identities().OpenIcon(ctx, identity)` opens the avatar a
  cached profile names: the space id must have the shape of a space id
  (letters and digits around one dot — it reaches a store path and a
  URL), the cid must parse, then `fetch.Open(iconSpaceId, iconCID,
  iconKey)` — local cache, LAN peer, then the public read URL
  `{base}/blob/{spaceId}/{rootCid}` — decrypted with the key from the
  profile. The reader never joins or syncs the owner's space and
  nothing on the network checks membership on that path, which is the
  same path every file download already takes. The remote object is
  capped at the probe (`fetch.OpenLimited`: its total size is known
  from the first range before the index or any block is read) and the
  plaintext size is checked again once the root loads; a refused
  object (`ErrIconTooLarge`) leaves no cached row behind. The own
  avatar opens through the tech-space file (`Account.OpenIcon`, and
  `Identities().OpenIcon` on the own id), where a file row the profile
  names but this device has not synced yet is `ErrFileNotAvailable`,
  not "none", as is any failure past the row. `ErrNotFound` when the identity,
  its avatar or the reference is unknown or malformed;
  `ErrFileNotAvailable` whenever the bytes cannot be fetched now — no
  public read base, an object the store no longer serves (a stale
  reference after the owner replaced or removed the avatar), a
  transport failure, a peer budget — with the underlying cause in the
  debug log only, since it names the owner's space. The caller's own
  context error is returned as is.
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
  itself, which would reuse one key across avatar versions. The
  directory rows that cache it are fenced on the tech handle's
  `Query` and `Aggregate` alike.
- **Wire compatibility.** A profile without an avatar encodes as
  before. A reader on the previous record format decodes the appended
  fields as the tail of `IconCID` (a cid followed by NUL-separated
  space id and key, which it cannot fetch anyway); a device of the same
  account on that format republishes the three-field record on boot,
  so an account's devices update together.
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
