# Files

Files are encrypted on the client, bound to an object, and registered in
the space's CRDT. Content is cached locally and backed up to the
network's fileV2 nodes. There are no standalone file objects.

Surface: `Space.Files()` (`space/files.go`) for members,
`Space.Payloads()` (`space/payloads.go`) for keyless readers, and the
cache controls on `SDK` (`FileCacheSize`, `FreeUpFileCache`,
`SweepFileCache`).

## The `payloads` dataset

Each file is one row in the `payloads` dataset on a **payloads object**:
a derived child of the object the file is bound to, created on the
first `Attach`.

- Signed owner: seed `builtin:payloads` with `ParentId = ownerId`.
  any-sync cascade-deletes it with the owner.
- Derived owner: unparented seed `builtin:payloads/<ownerId>`. any-sync
  rejects a derived object as a parent, so the owner id goes into the
  seed to keep the child id unique per owner.

A payloads object's tree changes are **plaintext** at the any-sync
level, so nodes can read rows for refcount, GC, quota and durability
without a space key. The privacy boundary is the sealed `enc` field.

| Field | Scope | Meaning |
|---|---|---|
| row id | — | `fileId`, derived from the creating change |
| `rootCid` | synced | UnixFS root of the encrypted file; absent for inline files |
| `size` | synced | plaintext size in bytes |
| `networkSign` | synced | verified custody receipt `{fileNetworkId}/{base64(sig)}`; absent until durable, never on inline rows |
| `objectId` | synced | the object the file is bound to |
| `author` | derived | account that attached the file |
| `enc` | synced | `{kid, ct}`: member-only secrets |

`ct` is AES-256-GCM under a key derived from the space read key; `kid`
is the ACL key-record id of that read key, so rows sealed before a key
rotation still open. The sealed payload is
`{key, name, sha256, mime, inline, variant, variantOf}`. Unknown keys
are ignored on read, so metadata can grow without breaking older
readers. The schema is non-dynamic: undeclared cleartext fields are
rejected on both routes.

## Storage tiers

`Attach` picks the tier from the content; callers never choose.

- **Inline** — size < 4096 bytes. The bytes ride inside `enc`. No
  `rootCid`, no upload, durable by construction.
- **Bound** — the per-space dedup index (plaintext SHA-256) finds an
  existing file with the same content. The new row reuses the donor's
  `rootCid`, wrapped key and receipt; no bytes move. A row bound to a
  not-yet-durable donor gets its own backup job.
- **Full** — a random per-file key, AES-256-CFB with a zero IV
  (byte-compatible with anytype-heart), a UnixFS DAG over the
  ciphertext, and one CARv2 per file keyed by its root CID. The zero IV
  is safe because keys are never reused. Integrity comes from the CIDs,
  so only CID-verified bytes reach the decryptor.

Dedup is per space. Per-file random keys mean a `rootCid` never repeats
across spaces.

## Attach and backup

1. Spool the reader, compute SHA-256, choose the tier.
2. Full tier: encrypt, build the DAG, finalize the local CAR.
3. Register the row: one CRDT change. The file now exists for every
   member, whether or not backup succeeds.
4. Enqueue a `durable` job and return. Attach never waits on the
   network: `FileInfo.Durable` is true only for an inline file or one
   bound to a durable donor.
5. The queue worker runs the durable phase: broker `Upload` → presigned
   HTTP PUT of the CAR → `RequestSign` → verify the receipt → write
   `networkSign`. The flip reaches every member as a row update and the
   local status stream as a `durable` transition.

The receipt is signed by the file fleet key (`fileNetworkId` from the
network config) over `{networkId, spaceId, rootCid, size, signedAt}`;
every field is checked before `networkSign` is recorded. A network
config without `fileNetworkId` leaves files registered but never
durable.

Transport failures (offline, node down, object store unreachable) end
the attempt and leave the job to the queue's backoff. Broker outcomes
from lazy space activation, `NotResponsible` routing and the post-PUT
visibility window retry within the attempt, inside the durable-wait
budget. Limit and auth refusals are terminal for the attempt.

Crash safety: an intent marker pins the root before the row write, and
the job is persisted before Attach returns, so a crash never leaves
an unsigned row that GC treats as garbage or the queue forgets.

## Background work

One persistent queue (`internal/files/status`) with two job kinds:

- `durable` — drive an unsigned row to a verified receipt.
- `pin` — fetch a file's full content (`Files().Pin`).

Jobs survive restarts. Failures back off from 30 s, doubling to 10 min;
a storage-limit refusal parks the job on a 10-minute cadence.
`Files().Retry` makes pending work due immediately (e.g. after a quota
raise).

## Open

`Open(ctx, fileId, variant)` returns a seekable reader over verified
plaintext. Content resolves through a source ladder:

1. Local store.
2. A peer holding the CAR, when p2p is enabled (`internal/files/filep2p`).
   Peers serve ciphertext and every block is CID-verified, so a peer
   can neither read nor corrupt content.
3. Public HTTP GET with Range on `{publicReadBaseUrl}/blob/{spaceId}/{rootCid}`,
   for durable files only. The base URL is resolved once from the
   network's fileV2 nodes and cached; `config.Files.PublicReadBaseUrl`
   overrides it for deployments that front the object store themselves.

Every fetched block is verified and persisted into a sparse local CAR,
so streaming, seeking and interrupted reads accrete toward a complete
copy. A file that is neither local, durable, nor held by a reachable
peer fails with `ErrFileNotAvailable`.

## Status

`Status` derives state on read from the row and the queue:

| State | Meaning |
|---|---|
| `durable` | receipt recorded, or inline |
| `inflight` | registered; backup queued, running, or driven by another device |
| `limited` | the network refused backup for storage limit |

`FileStatus` also reports `Cached` (complete local copy), `Attempts` and
`LastErr`. `SubscribeStatus` delivers local transitions only (attach,
backup progress and failure, pin completion, retries); a backup finished
by another device shows up through `Status` / `Get`. `Stats` returns
per-space counts.

## Variants

A variant (e.g. a thumbnail the embedder rendered) is an ordinary sibling
row on the same object, tagged with `variant` / `variantOf` inside `enc`.
It has its own tier, durability and lifecycle; the network sees
independent files. `Attach` requires both fields together and an original
bound to the same object (`ErrFileVariantInvalid`).
`Open(originalId, variant)` resolves the sibling.

## Delete

`Delete` removes the row in one synced change, cancels pending work and
releases the local content ref. Deleting an original also deletes its
variants; a keyless reader cannot see `variantOf` and deletes only the
addressed row. Content shared with a surviving row stays. fileprotov2
has no delete RPC: the broker's row-driven accounting stops counting a
file once the deletion syncs. A second `Delete` returns `ErrNotFound`.

## Local cache

Layout under the store root:

```
<root>/tmp/<rand>.car                    in-progress builds, swept on open
<root>/<spaceId>/<shard>/<rootCid>.car   one CARv2 per file
```

The CAR is byte-identical to the uploaded object. Hot metadata
(download bitmap, refs, dedup index, LRU access time) lives in any-store;
blob bytes never enter the DB. Space deletion is one recursive remove.

Reclamation is embedder-driven. The rule everywhere: bytes are dropped
only when refetchable (a referencing row carries a receipt) or
unreferenced; the only copy of a not-yet-durable file is never dropped.

- `Files().Offload(fileId)` — drop one file's local bytes.
  `ErrFileNotBackedUp` unless durable; inline files are a no-op. Content
  shared through dedup is offloaded for every row that uses it.
- `SDK.FreeUpFileCache(bytes)` — LRU sweep toward a byte target; returns
  what was actually freed.
- `SDK.SweepFileCache` — safety pass: prune refs of deleted rows, delete
  CARs unreferenced for 24 h, offload partials of durable files untouched
  for 7 days. Runs periodically only when `config.Files.GCInterval > 0`.
- `SDK.FileCacheSize` — local bytes across all spaces.

## Listing and queries

- `List(opts)` — typed `FileInfo`s; `ObjectId` is the indexed fast path,
  `Limit` caps the result. An unfiltered listing walks every file in the
  space; large-space consumers page or use `Query` / `Changes`.
- `Query(objectId)` — the generic query surface over one object's rows
  (cleartext fields only). `ErrNotFound` until the object's first file is
  attached.
- `Get(fileId)` — one `FileInfo`. Member-only fields (`Name`, `Mime`,
  `Variant`, `VariantOf`) are empty without the space key.

## Keyless readers

`Space.Payloads()` exposes the cleartext index for an embedder without
the space key, such as the filenode-v2 broker running headless with
selective sync: `ListObjects` returns payloads objects classified by their
signed root change type, `ListRows` returns one object's rows with the
sealed secrets withheld.
