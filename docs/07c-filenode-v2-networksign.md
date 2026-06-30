# Files v7 — filenode v2 (S3-direct broker + networkSign)

Concrete byte/durability layer under `07b`'s derived-payloads composition. **Supersedes `07-files.md`'s whole-blob addressing with IPFS cids.** Validated *"build it"* by adversarial review; the seek/pack model is benchmarked (`internal/spikes/carpack`); the read path is matched to the verified anytype-heart implementation. Envelope crypto / availability≠durability / collective durability from `07-files` are unchanged.

## Decisions locked
- **Addressing = IPFS merkle cids of the *ciphertext*** (1 MiB leaves, balanced UnixFS, fanout 174, CIDv1 / dag-pb / sha2-256). `rootCid` = the UnixFS root = the one global id. Chunk-level dedup is dead (per-file random key → no shared leaves); dedup is **whole-file** via `sha256` + bind.
- **Whole-file AES-256-CFB, *then* chunked** — exactly `any-sync/commonfile/fileservice.AddFile`. Integrity is from the merkle cids (each block verified vs its cid), not cipher tags.
- **One S3 object per file** = a **CARv2** of the encrypted DAG, keyed by `rootCid`. The node holds **no bytes**; bytes go **client ↔ S3 / CloudFront** directly.
- **filenode v2 = a byteless broker**: ACL + quota + presign + `sign` (a pure Oracle — it never writes the CRDT).
- **Fleet:** new nodeconf type **`NodeTypeFileV2`**; spaces sharded by **consistent hash, RF = 2**; the **lower-peerId** of the pair is the GC leader *and* the broker leader.

## any-sync changes
1. **Space header gets `fileprotoVersion`** (signed, immutable). Old filenodes **error** on v2 spaces; new filenodes serve **only** v2. The SDK is greenfield → every space is v2 (no migration); v1/v2 coexistence rides the existing handshake `ProtoVersion` + `NetworkCompatibilityStatus`. Can't be stripped without invalidating the owner signature.
2. **`NodeTypeFileV2`** in the network config — the v2 filenode pool, chash-addressable like the sync/tree pools.
3. **A derived child `payloads` object, partially encrypted** — cleartext (node-readable) `{fileId, rootCid, size, networkSign, author}`; encrypted (member-only) `{key, name, sha256, meta}`. Enforced at the schema level, so no S3-backed cid can hide from the node. Owner removed with its derived payload → all peers (and the node) see orphaned files.

## Payload row
```jsonc
{
  // ---- cleartext (node-readable) ----
  "id":          "{fileId}",          // = creating changeId; content-addressed, NEVER reused
  "rootCid":     "{cid}",             // UnixFS root; the node refcounts / GCs / signs THIS
  "size":        12345,               // hint only — quota uses the node's HEAD-measured size
  "networkSign": "{fileNetworkId}/{sign(rootCid)}",  // node's existence+size receipt; absent until durable
  "author":      "{identity}",        // auto field from the change author
  // ---- encrypted (member-only) ----
  "key":         "{wrappedKey}",      // SEPARATE envelope field (cheap ACL rotation)
  "name":        "image.png",
  "sha256":      "{hash}",            // hash of the UNENCRYPTED file — the dedup key
  "meta":        { "mime": "image/png", "size": [1024, 768] }
  // (no manifest — the UnixFS DAG is reconstructable from rootCid alone)
}
```
Only `rootCid` is node-visible; the whole DAG reconstructs from it, so the row stays pointer-sized and node refcount cardinality is ~1×.

## Durability states (derived from the sign + node knowledge)
Registration is **unconditional** (a row is CRDT data — no node permission, and it's immediately P2P-servable). Only the S3 **backup** is gated. The state of a payload is derived, not a stored authority:

| state | condition | bytes on S3 | counts quota? | readable |
|---|---|---|---|---|
| **in-flight** | no sign, **node authorized** it (reservation / `staging` object exists) | staging | **yes** | P2P now; S3 once durable |
| **durable** | row carries a **valid `networkSign`** | durable | **yes** | P2P + S3/CDN |
| **limited** | no sign, node **refused** (`ErrLimitExceed`) — or not yet requested | none | **no (0)** | **P2P only** |

The presence of a valid sign is the single authoritative durable/not-durable signal. `in-flight` vs `limited` is the node's own knowledge (did *it* issue an allowance); clients may carry a UI hint.

## networkSign — the keystone
- The node **signs the root cid** after verifying the upload; the **client records** it in the row. **No valid sign ⇒ not durable** (and any orphaned bytes are reclaimed).
- **What it attests:** an **existence + size receipt** — "rootCid's bytes are durably stored, size X" (HEAD-confirmed against the cids the node presigned). **Not** content-integrity (that's content-addressing's job) → the node never reads chunk bytes (~0 bandwidth).
- **Oracle property** — the node authors no CRDT change → not a write-member; a key-compromised node can't rewrite space state.
- **It is the promotion trigger** (below) and the **anti-forgery gate** (it's the node's own verifiable signature; a client can't fake durability).
- **BIND reuse is safe** — a new `fileId` for the same content carries the *existing* sign (binds to `rootCid`, not fileId/user). Stripping/withholding it only hurts the attacker.

## Upload flow
1. `sha256(plaintext)` → local-db lookup. **Hit → BIND** (mint a new fileId, reuse the existing `rootCid` + sign + key; done).
2. New random sym key → whole-file CFB → 1 MiB-leaf balanced UnixFS DAG → pack into a CARv2.
3. **Register the payloads row** (rootCid, size, encrypted fields; no sign) — syncs; the file is now P2P-servable.
4. `uploadRequest(spaceId, rootCid, size)` → node checks the space allowance (→ coordinator for the identity total). **If over limit → `ErrLimitExceed`** (row stays `limited`, P2P-only, retried later). Else **reserve** the bytes and return a presigned POST to `blob/{spaceId}/{rootCid}` tagged **`state=staging`**, with a `content-length-range` cap.
5. Client **PUTs the CARv2 directly to S3** (`state=staging`).
6. `requestSign(rootCid)` → node **HEAD-verifies** the staging object → **signs `rootCid`** (returns the sign; the object **stays `staging`**).
7. Client **records the sign** in the row → row syncs.
8. Node **observes the signed row** → **retags `state=durable`** (cheap, no copy) and **finalizes quota** from the HEAD size.

## In-flight & quota accounting
**Storage quota = `Σ durable (signed, HEAD-measured)` + `Σ authorized-in-flight`.** It counts what is in (or going to) S3 — *not* mere row existence — so `limited`/unrequested rows count **0** and never deepen a deficit.

In-flight lives in three layers, no common index:

| layer | where | role | survives restart? |
|---|---|---|---|
| existence / cleanup signal | the **sign-less payload rows** (the per-space derived index) | what exists; what's not yet durable | yes (synced CRDT) |
| in-flight **bytes** | the **`state=staging` S3 objects** | the durable in-flight count (list to recount) | yes (S3-owned) |
| authorize→PUT **race guard** | a small **in-memory reservation map on the leader** (lower peerId) | serialize concurrent `uploadRequest`s | no — rebuilt from staging + rows |

Cleanup is S3-owned: an **S3 Lifecycle rule expires `state=staging` after a TTL** (24–48 h). Promotion to `durable` happens **only** when the node sees the recorded+synced sign (step 8), so anything abandoned at any point — including *"requested the URL but never recorded the sign"* — stays `staging` and **auto-deletes**; the reservation TTL releases the held quota. No durable orphans from the upload path, no node-side pending ledger.

## Over-limit (`limited`) — local-first by construction
Quota limits the **cloud backup**, never creation or sharing:
- The row registers regardless and is **served P2P** (`BlockGet`/`BlocksCheck` over any-sync connections don't touch node quota).
- `uploadRequest` is refused (`ErrLimitExceed`, decided node→coordinator) → `limited`. It consumes **0** node/S3 quota (nothing uploaded), so registering limited files doesn't dig deeper.
- **Stable, not GC'd** (no S3 bytes to remove); **retried** by drive-toward-durable when the user frees space or upgrades (coordinator raises the allowance) → `limited → durable`. Drain by priority (thumbnails/recent first).
- **At-risk:** a `limited` file lives only on peer devices — if every holder goes offline before backup, it's lost. Surface *"Over limit — not backed up. Free space or upgrade."*

## GC / deletion
- **Reachability, server-side.** Owner deleted → derived payload tombstoned (node-visible via `settings ObjectDelete` / `DeletionManager`) → node decrements the `rootCid` ref → an empty `rootCid` past grace → **S3 lifecycle-tombstone** (never a synchronous DELETE; a re-appearing ref inside grace cancels it).
- **Over-count never under-count** — a crashed/late client leaks a blob (delayed GC), never loses data.
- **Async mark-and-sweep backstop** — a `durable` S3 object older than grace with no referencing signed row → delete. Catches the rare promoted-but-unreferenced case.

## Physical S3 mapping & seek (decided, benchmarked)
**One CARv2 per file, keyed by `rootCid`** (or S3 multipart for large uploads). Adversarial review found no in-scope case where packing loses anything (per-file random key already zeroes cross-file leaf sharing).

- **Seek is a real DAG traversal** (stock `boxo` `DagReader`): to reach plaintext offset N, walk root → intermediates → leaf via UnixFS `blocksizes`, **fetching each node by cid**, so the packed object needs a `cid→offset` index over **every** node — exactly CARv2's index. Decrypt at N is CFB-seekable (recover the IV from the 16 ciphertext bytes before N; never decrypt-from-start).
- **Resolution:** node resolves `rootCid` only (presign / HEAD / sign / GC); peers resolve `(rootCid, byte-range)`; the client resolves any block-cid **local → peer (any-sync DRPC) → S3 (Range via the index)**. A leaf cid is an intra-file integrity+ordering coordinate, never a global address.
- **Spike (`internal/spikes/carpack`, tag `carpackspike`):** real boxo DAG + AES-CFB, packed to a CAR with a `cid→offset` index, read back via the stock `DagReader` + a seekable CFB decryptor. 256 MiB / 2 levels: pack+index overhead **~0.01%**, random-seek p50 **~0.53 ms** (matches/beats one-file-per-cid), cold index rebuild ~40 ms (CARv2 persists → 0). **Finding:** the stock `DagReader` read-ahead fetches **~9 MiB to serve a 64 KiB seek** — fine on a local file, but for S3/CDN seek build a **no-preload targeted range reader** (path nodes + covered leaves only).

## Operational model (the `NodeTypeFileV2` fleet)
Byteless brokers, no central index:
- **Sharding:** consistent hash on `spaceId`, **RF = 2** → two responsible filenodes. **RF = broker redundancy over one shared, content-addressed S3 bucket** (bytes stored once; per-file random keys → `rootCid`s never collide across spaces → per-space GC safe).
- **Leader = lower peerId** of the pair: owns GC, the in-memory reservation map, and (route here) `uploadRequest`/`requestSign`. The other node is **warm failover** (has the derived index from synced rows; takes over on leader death, losing only ephemeral reservations — staging-lifecycle + rows reconcile).
- **GC** is best-effort: if the leader is down it waits (over-count + grace → only a brief storage leak, never data loss).
- **No common index; evict inactive spaces.** The per-space index is a derived projection (truth = payload rows + S3), so a node tracks only its **active** responsible spaces and evicts after a few days idle, rebuilding from the rows (+ listing `state=staging`) on reactivation.
- **Limits:** per-space accounting on the node; the **coordinator** owns the per-identity total (issues per-space allowances or answers a reserve-check at `uploadRequest`) — keeps the no-common-filenode-index property.
- **Two transports, one addressing:** durable/WAN bytes over **HTTP S3/CloudFront**; LAN/local over a **separate P2P layer on any-sync connections** (`BlockGet`/`BlocksCheck`, member-gated, served from the holder's local pack via the cid→offset index). Blobs follow object-sync locality (mDNS peers + S3 backstop).

## What v7 resolves (vs the prior reviews)
- **D1 (cids drift)** — node reads cid/sign off the synced object; orphaning is node-derived from the cascade tombstone; no client `$pull`.
- **D2 (node-readability)** — fixed at the schema level by partial encryption.
- **durable-bit / node-as-writer** — durability is a node *signature* the client records; the node never writes the CRDT.
- **dark orphan** — register-row-before-PUT + `state=staging` lifecycle + promote-on-signed-row.
- **quota spoof** — quota from the node's reservation + HEAD, never the cleartext `size`.
- **wrappedKey rotation** — `key` is a separate re-wrappable envelope field.

## Still open / to build
- **OPEN DECISION — RF storage:** one shared content-addressed bucket (RF = broker redundancy — recommended) vs RF physical byte copies (geo-redundancy; each node GCs its own copy, leader computes the orphan set).
- **OPEN DECISION — limits granularity:** keep **per-identity** via the coordinator (allowances / reserve-check — recommended) vs move to **per-space** pricing (simplest for the fleet, but a product change).
- **No-preload targeted range reader** for S3/CDN seek; **range-aware `BlobCheck`/`BlobGet`** for partial LAN holders.
- **Row lease / status hint** so a `limited`/abandoned row is distinguishable in UI and tombstonable past TTL.
- **Cross-owner move** (= bind + delete, new fileId) and **`Get(fileId)` without the owner** (no global fileId→owner index) — unindexed, same as v6.
- **Grace / staging TTLs** — pin the staging-expire (24–48 h) and the GC grace (7–14 d); account for AWS lifecycle tag-age semantics.
- **GATING SPIKE (deferred):** multi-writer + offline-branch + cascade on the real (derived-payload + node-cid-ledger) model — every spike so far is single-writer / linear-DAG.

## Cross-refs
- `07-files.md` — byte layer (envelope crypto, availability≠durability, collective durability). This doc supersedes its whole-blob addressing with IPFS cids.
- `07b-files-derived-payloads.md` — v6 composition (owner binding, the tree-count wall, partial encryption, materialized views).
- `internal/spikes/carpack` — the pack + seek benchmark.
