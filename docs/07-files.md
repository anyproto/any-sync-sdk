# Files

## TL;DR — the SDK2 file model (S3-direct, filenode v2)

1. **Drop IPFS. Filenode becomes a thin token broker; bytes go client↔S3 directly.** The node never proxies a byte. It checks ACL + quota and hands out **presigned upload / CloudFront-signed download URLs**. This is sound because blobs are already client-side **ciphertext** — proxying them through a node adds *zero* confidentiality, only cost and a failure domain. (Expert-reviewed: "FORK.") It *does* need a new server, but a far simpler one (no block store, ~3 RPCs); the team accepts that.
2. **Content-addressing without IPFS: `contentHash = SHA-256(ciphertext)` per chunk.** Files are sliced into **coarse 4–8 MB chunks**, each its own content-addressed blob = its own S3 key. A file/variant is a tiny **manifest** (an encrypted list of chunk hashes). Chunking — not whole-blob — is what makes P2P binary ("have this chunk?"), uploads natively resumable, and S3 request-cost sane.
3. **Payloads are a per-object `payloads` dataset, materialized into a common collection** — synced, owned 1:1, tombstoned with the object; coalesced into one space-level `payloads` collection via the shared-collection override (exactly how properties feed the `objects` collection). One convergent place for the upload/GC/dedup workers to scan. Two tiers by size: **micro-payload (< 32 KB)** = envelope-encrypted bytes inline in the row (no chunks, no S3); **real payload (≥ 32 KB)** = chunked → S3.
4. **Envelope encryption** (keep what anytype does right): random per-file key → stable ciphertext → stable `contentHash` forever. Wrap the key under the space read key; ACL rotation re-wraps only the tiny key. **Reuse the key for identical plaintext** (via `plaintextHash`) → identical chunks → S3 stores them once.
5. **Availability ≠ durability — the core simplification.** A reader resolves a chunk from whatever holder answers: **local file → mDNS space peer (P2P) → CloudFront/S3**. It never waits on an "uploaded" flag — *availability is emergent*. The synced per-variant `status` tracks only **durability** (are the bytes on S3 yet); it drives the backup UI and local-GC retention, never gates a read. This kills the separate filesync queue and the status/byte drift behind most of anytype's file bugs.
6. **Blobs follow the same locality model as objects.** P2P is mDNS/LAN only and rides the **same peer pool, discovery, and secure session as object-tree sync** — not a parallel fabric. **S3 is to blobs what the sync-node is to objects:** the always-online backstop beyond the LAN. WAN "P2P" of bytes is just S3/CDN.
7. **Durability is a space-collective duty, not the author's.** The payload row (manifest + keys) syncs, so any write-member holding the bytes can `RequestUpload` + PUT them under the canonical `fileId`. Alice adds a file → Bob pulls it P2P → Alice's device dies before upload → **Bob makes it durable.**
8. **Thumbnails/variants are host-OS-generated, not Go**, and a tiny **blurhash rides inline in the change** so a preview shows the instant the message arrives — offline, before any byte transfer. One payload row, many variants.
9. **Local bytes live in a plain content-addressed file directory** (`blocks/<spaceId>/<contentHash>`), *not* a database — any-store is dropped for blob storage. A tiny per-chunk **index row stays in the per-space any-store** so GC and `BlobCheck` are indexed, not `stat()`-scanned.

This supersedes the earlier IPFS + filenode-block-proxy direction (kept at the bottom for history).

---

## What's actually below us

### filenode v1 (what we're replacing)
Today's filenode is a zero-knowledge **block store**: clients IPFS-chunk the ciphertext (1 MB, CIDv1+SHA256) and **push every block** to the node, which stores bytes in S3 + index in Redis, refcounts per-CID, enforces ACL + quota. It *proxies all bytes*. The client (`rpcstore`) also does P2P block **read** (verified below). We keep the zero-knowledge + ACL + quota + refcount *ideas* and discard the byte-proxying + IPFS.

### filenode v2 (the broker we need — new server, ~3 RPCs)
- `RequestUpload(spaceId, fileId, []contentHash, sizes)` → check `CanWrite`, **reserve** quota → presigned **S3 POST** per missing chunk (`content-length-range` enforces the per-object cap server-side). Chunks that already exist are skipped (dedup).
- `CommitUpload(spaceId, fileId, []contentHash)` → node `HEAD`-verifies the objects exist in S3, **finalizes** quota, records the `fileId → contentHash` bindings, marks durable. (S3 `ObjectCreated` events are a *background audit* to catch abandoned reservations — never the primary confirm; they lag minutes.)
- `RequestDownload(spaceId, []contentHash)` → check `CanRead`, **SDK-rate-limited** → **CloudFront signed URLs** (never raw S3 — free S3→CF egress, WAF rate-limit, kills the egress-amplifier attack).
- Index only (Redis/DB): `fileId`/space → chunk bindings, refcounts, quota ledger, `deleted_at` tombstones. **No block store; ~0 node bandwidth.**

### Verified client-side P2P pattern (anytype-heart `rpcstore`) — what we align to
any-sync **core** has no peer block server; the P2P block read lives in the **client**, on the regular peer channel. We keep the *pattern*, swap the *payload* (our chunk service for IPFS blocks). Confirmed in `anytype-heart/core/files/filestorage/`:
- **Reads try peers first:** `rpcstore.Get` = local cache → a **discovered space peer** → node fallback (`store.go:205-244`); peers come from `peerStore.LocalPeerIds(spaceId)` (mDNS local discovery + `SpaceExchange` handshake), 1 s timeout, 5-min ban.
- **A client serves what it holds** from its local store (`rpchandler.go:59-91`), auto-caching every fetch (`proxystore.go:58-84`) → a holder is transparently a source.
- This is the **same peer pool / discovery / secure session as object-tree sync** — which is exactly why aligning blob P2P with it is "as aligned as possible."

### What anytype-heart does today (the bar to beat)
- Every file — even a 2 KB icon — is a **full first-class object** + a node upload. Images = **5 separate variant sub-DAGs**.
- Envelope encryption is **already correct** (random per-variant key, wrapped under the space key, synced with the object). The crypto is fine.
- **Sync status is a separate state machine + persistent `filesync/queue`**, updated by async callbacks — the documented root of most file bugs (object state and block state drift).

**Honest conclusion:** the *cryptography* is fine; the pain is **structural** — byte-proxying, five DAGs per image, a full object per icon, a drifting second consistency system. SDK2 keeps the crypto and rebuilds the structure on direct-to-S3 + content-addressed chunks.

---

## Design

### Principle: the node never holds bytes; the CRDT/dataset is the only source of truth
Drift happens because object state and block state are coordinated by a third system (the queue). Instead: the **`payloads` row is the manifest**, S3 + peers are the byte sources, the node is a stateless-ish authorizer. One source of truth.

### Availability vs durability — the split anytype conflates
Anytype folds two independent questions into one "is it uploaded?" flag; that single coupling is the complexity. Separate them:

- **Availability — "can I get these bytes now?"** Emergent, never declared. A reader resolves each `contentHash`: **local file → mDNS space peer → CloudFront/S3**. The payload row's manifest + `wrappedKey` is everything needed to *attempt* it. No "wait until uploaded" gate. A miss at one provider just falls through to the next.
- **Durability — "are the bytes safe long-term?"** What the synced per-variant `status` tracks: have the chunks reached S3. Drives the "backing up…" UI and local-GC retention; never a read precondition.

### Aligned with object sync — same locality, one transport family
The blob exchange is **not** a new network. It reuses the any-sync **peer pool, mDNS discovery, and secure session** that object-tree sync already uses, with one extra DRPC service registered alongside the sync services. The locality model is identical to objects:

| | on the LAN | beyond the LAN |
|---|---|---|
| **objects** | mDNS peers (direct tree sync) | sync-node |
| **blobs** | mDNS peers (`BlobGet`) | **S3 / CloudFront** |

**S3 is to blobs what the sync-node is to objects** — the always-online backstop. P2P is mDNS/single-network only (no DHT, no WAN hole-punching in v1); anything off-LAN goes to S3.

### Where payloads live — per-object dataset, materialized into a common collection
- **Source of truth: a per-object `payloads` dataset.** Written through the object's CRDT → **synced, owned 1:1, tombstoned with the object** (sticky, convergent; `05a-crdt-spec §3.4/§5.7`). Lifecycle safety is free.
- **Scan surface: a common `payloads` collection**, materialized via the Controller's **shared-collection override** — the same mechanism that coalesces property writes into the `objects` collection (`SystemPropertiesHandler`; `06-data-structure`). **Convergent by construction** (same synced changes applied on every peer), not a hand-maintained projection. The upload/GC/dedup workers scan this one collection.

```jsonc
// payloads — per-object dataset, materialized into the common `payloads` collection
{
  "id": "payload_uuid",
  "name": "report.pdf",              // human label (metadata; NOT an identity)
  "plaintextHash": "sha256-…",        // indexed; key/chunk reuse for identical content
  "fileId": "f_uuid",                 // SDK-minted broker binding id (refcount/quota unit)
  "placeholder": "blurhash:LEHV6n…",  // ~30 B, inline, rides the change → instant preview

  // micro-payload (< 32 KB): inline ciphertext, no chunks, no S3
  "inline": { "bytes": "base64…", "wrappedKey": "…", "mime": "image/png" },

  // real payload (>= 32 KB): one row, many variants. `status` is PER VARIANT,
  // tracks DURABILITY only (never a read gate). `manifestHash` -> an encrypted
  // content-addressed blob listing the variant's 4-8 MB chunk hashes.
  "variants": {
    "original":  { "manifestHash": "sha256-…", "wrappedKey": "…", "size": 4000000, "mime": "image/jpeg", "w": 4032, "h": 3024, "status": "local"   },
    "thumbnail": { "manifestHash": "sha256-…", "wrappedKey": "…", "size": 22000,                          "status": "durable" }
  }
}
```

The **manifest is itself an encrypted content-addressed blob** (not an inline list) so the payload row stays tiny regardless of file size, and AWS never sees a file's chunk graph (the manifest is ciphertext too). Single-chunk variants may elide the manifest and reference the one `contentHash` directly.

### Ownership shapes — payloads don't force a "file object", but allow one
- **Incidental payloads.** A page with images, a member avatar — rows in the *content* object's `payloads` dataset. No per-file object minted. (Answers problem #1: no file object per small linked image.)
- **Dedicated file object.** An important standalone file (a library PDF) gets an object owning **exactly one payload** — first-class, referenceable, queryable.

Same mechanism either way; "file object" becomes a modeling choice, and size (inline vs chunked) is an orthogonal axis.

### Why this eliminates the distributed deletion race
A payload row can only be created by syncing the object that owns it, so a peer **cannot observe (and delete) a payload before it has seen the parent**. Parent tombstoned → payload tombstoned with it. The causal DAG gives lifecycle safety for free — **no space-wide reference scan, no grace-period rescue.**

### Content addressing — chunks + manifest + `fileId` binding
- `contentHash = SHA-256(ciphertext)` per **4–8 MB chunk** = the S3 key = the P2P key = the integrity check. A peer-served chunk is **self-verifying**: re-hash it, drop the peer on mismatch, fall through to S3. This is what makes serving from untrusted-ish peers safe.
- A variant = an (encrypted) **manifest** of chunk hashes. Identical plaintext → identical chunks → S3 stores them once; the node refcounts bindings.
- `fileId` is the SDK-minted **broker binding unit** (what quota/refcount attach to), decoupled from content. "Save the same file under three names" = three fileIds binding one chunk set; deleting one leaves the chunks.

### Encryption — envelope, with key reuse for identical content
1. Random symmetric key **per file** → encrypt chunks → **stable ciphertext → stable `contentHash` forever.**
2. **Reuse the key for identical plaintext** (via `plaintextHash`) → identical chunks → `RequestUpload` reports them already-present → skip the PUT, just bind. Key+IV reuse is safe here because it is *only ever across identical plaintext*.
3. Wrap the key under the **current space read key**; store the wrapped key per variant.
4. ACL rotation re-wraps only the tiny `wrappedKey` fields. **The S3 object is never touched.** An evicted member can't unwrap → loses future reads (and can't get new download URLs).

Do **not** derive keys from the read key (rotation would force re-encrypting/re-uploading every blob). Do **not** use convergent encryption (key = hash of plaintext) for cross-*account* dedup — confirmation-of-file leak. Intra-space key reuse gets dedup without that leak.

### Identifiers
| id | role | where |
|---|---|---|
| `fileId` | SDK-minted broker binding unit (refcount/quota); decoupled from content | payload row |
| `plaintextHash` | intra-space key/chunk-reuse index | secondary index |
| `contentHash` | `SHA-256(ciphertext)` of a chunk = S3 key + P2P key + integrity | inside the manifest |
| `manifestHash` | `contentHash` of a variant's encrypted chunk-list blob | inside `variants[*]` |

### Filenode v2 protocol & limits
- **Upload:** `RequestUpload` reserves quota and returns presigned **POST** (with `content-length-range`) only for chunks S3 lacks. Client PUTs ciphertext directly. `CommitUpload` `HEAD`-verifies + finalizes. **Reserve-on-authorize** (not on completion) so concurrent uploads can't overshoot the cap; unconfirmed reservations **expire by TTL** and release. (Watch: a large reservation soft-locks a near-quota user until TTL — keep TTLs tight, release eagerly on `CommitUpload`.)
- **Download:** `RequestDownload` → **CloudFront signed URLs**, SDK-rate-limited per user/space. Never hand out raw S3 GET URLs (egress-amplifier).
- **Confidentiality at the edge:** a leaked URL yields ciphertext only; the envelope key (re-wrapped on rotation) is the real gate. URL TTL is a throttle, not the security boundary.

### Durability is collective — any holder can complete the upload
The payload row (manifest + keys + `status`) syncs, so a holder needs nothing from the author to finish:
- It can serve the chunks P2P **and** `RequestUpload` + PUT them to S3 under the canonical `fileId` (any write-member is ACL-permitted), then `CommitUpload` → `status: durable`.
- **Mechanism is "drive toward durable", not "detect Alice died":** any holder with the bytes + connectivity + a non-durable variant pushes it. Loss window = "time until *any* holder reaches S3", with N candidate uploaders.
- **Herd control:** `RequestUpload` reports chunks already in S3 → other holders skip the PUT and just bind. (Optional peerId-jittered backoff.)
- **Honest limit:** if *every* holder dies before any reaches S3, the content is gone — unavoidable offline-first; collective duty maximizes survival.

### Declarative sync — per-variant durability, never a read gate
`status` is **per variant** and tracks **durability only**; reads always try holders, so it never gates a fetch. The "queue" is just `index WHERE status = local`.
- `local` — bytes exist on ≥1 holder, not yet on S3. Readable P2P now; a holder drives it to `durable`. The valid offline-create state.
- `durable` — confirmed on S3 (`CommitUpload` HEAD-verified). Safe to evict from any local cache.
- `limited` — quota hit; retried when headroom returns.
- `pending_delete` — owner tombstoned; binding teardown owed.

Per-variant enables the chat case: `thumbnail: durable, original: local` (preview safe, full image still backing up), and the worker prioritizes thumbnails across *all* pending payloads. Concurrent `durable` writes are idempotent (LWW). Self-healing: a `durable` variant whose chunks turn up missing on S3 downgrades to `local` and is re-driven. Folds into the general **Sync Status** subsystem (`docs/09`).

### Garbage collection & deletion — refcount + lifecycle-tombstone (no synchronous DELETE)
- **Server:** when a `fileId` unbinds, the node decrements per-`contentHash` refcounts. At zero it sets `deleted_at` (or an S3 `State:Orphaned` object tag) — it **never issues a synchronous S3 DELETE**. An **S3 Lifecycle Policy** (e.g. 7-day) does the actual removal. This **absorbs the delete-while-uploading race** (a concurrent re-create/re-bind inside the window cancels the tombstone) — the structural fix for what was the #1 risk in the proxy model.
- **Local:** bytes are plain files; the per-chunk index (in any-store) drives GC. `unlink` a chunk only if its local binding count is 0 **and** the variant is `durable` (S3 has it). Because the local file dir is also the **P2P serving surface**, a non-durable chunk (incl. one merely *received* from a peer) is **retained** — it may be the last copy. Two retention classes: non-durable = held until `durable`; durable = ordinary LRU cache.

### Local block store — a content-addressed file directory, not a DB
Bytes go to plain files at `blocks/<spaceId>/<contentHash>` — any-store is **not** used for blob storage. Rationale: blobs are opaque, S3 is the durable store, and files give zero B-tree write amplification, `seek`-based range reads (for `BlobGet` ranges), trivial P2P serving, and `rm -rf blocks/<spaceId>/` teardown. A **tiny per-chunk index row** (`contentHash, spaceId, size, last_accessed, durable`, refcount) lives in the per-space any-store metadata so GC/`BlobCheck`/LRU are indexed queries, not filesystem `stat()` scans (mobile inode/handle limits). Metadata in any-store, bytes in files.

### P2P blob exchange — chunk-addressed, member-only, on the object-sync channel
A 2-method DRPC registered on the **same server / peer pool / mDNS discovery as object sync**:
- `BlobGet(spaceId, contentHash, range?)` → ciphertext (range = file `seek`).
- `BlobCheck(spaceId, []contentHash)` → which the peer holds (binary per chunk — coarse chunking makes this trivial; no byte-range bookkeeping).

Rules: **serve only to verified current space members** (ciphertext is safe to leak, but serving non-members invites resource-exhaustion of mobile clients); self-verify every received chunk by hash; auto-cache fetched chunks → become a source. Slots in as the middle rung of the availability ladder (local → peer → S3), mirroring `rpcstore` with our chunk service instead of IPFS blocks.

### Thumbnails / variants — offline-first preview
Only what's **inline in the change** is guaranteed to arrive with the message (peers and S3 are best-effort). Preview ladder:
1. **Blurhash / LQIP (~30–200 B): always inline.** Instant blur-up by any path, including pure offline P2P. The one hard guarantee.
2. **Sharp thumbnail (~10–30 KB): a small variant** fetched from a holder (sender/recipient) P2P the moment the message lands, or from S3 once `durable`. In a live chat the sender is usually a reachable peer → arrives in ~1 s, no upload-wait.
3. **Original:** streamed on demand.

Inlining the *full* thumbnail (sharp preview guaranteed even fully-offline) is a per-collection **option** weighed against permanent DAG weight (~30 KB × 1000 msgs = ~40 MB replicated forever). Default: blurhash inline + thumbnail-as-variant. The Go SDK does **not** decode/resize — a host hook does:
```go
type MediaProcessor interface {
    GenerateVariants(ctx, raw []byte, mime string) (map[VariantKind][]byte, error) // -> blurhash, thumbnail, …
}
```

### Dedup & the metadata-privacy tradeoff
- **Intra-space, client-side, pre-sync:** hash plaintext, look up `plaintextHash`, reuse key/chunks → second copy is bind-only.
- **Cross-space dedup vs metadata privacy — an explicit choice.** Wrapping the *same* inner key for identical plaintext makes the ciphertext (hence `contentHash`) identical across spaces → S3 stores it once (cost win) — **but AWS can group objects by hash and see the dedup graph, plus IP↔hash access patterns.** If strict metadata privacy outweighs storage cost, **salt the ciphertext per space** (breaks global dedup). Default leans cost-saving (global dedup) since blobs are ciphertext; flag for product/security sign-off.
- **Accept offline-concurrent duplication** (two offline devices, different keys → different hashes). Probability ~0; the convergence resolver isn't worth the bug surface.

### Object duplication — re-mint `fileId` (future constraint)
No object-copy primitive exists yet. When designed it MUST be domain-aware: a generic deep-clone copies a payload row *including its `fileId`*, aliasing one binding to two owners → deleting one unbinds chunks the other needs. The sanctioned copy path strips the old `fileId`, mints a fresh one, and re-binds the same `contentHash`es (no re-upload).

### Why not "files as a first-class any-sync type"
Rejected. State-sync optimizes for tiny, causally-linked, fully-replicated mutations; file transport optimizes for large-block streaming, partial/on-demand replication, CDN caching. Direct-to-S3 *is* the file-shaped transport; forcing bytes through the tree protocol chokes metadata sync for no benefit.

---

## Risks (ranked by blast radius)
1. **Quota reservation correctness.** Debit-on-authorize is right, but a large abandoned reservation soft-locks a near-quota user until TTL, and an unconfirmed-but-actually-uploaded chunk leaks quota. Tight TTLs, eager release on `CommitUpload`, S3-event background reconciliation.
2. **Egress / cost abuse.** A leaked download URL or a malicious member can amplify S3 egress. Mitigate with CloudFront signed URLs + WAF rate-limit + SDK per-user/space rate-limit + per-object `content-length-range` cap. Monitor egress.
3. **Received-chunk retention (P2P data loss).** A peer that pulled a chunk can be the last copy until `durable`; evicting before durability strands the swarm. Retain non-durable chunks; evict only at `durable`.
4. **Delete-while-uploading.** Refcount→0 must NOT synchronously DELETE from S3; use `deleted_at`/object-tag + lifecycle grace so a concurrent re-bind cancels it.
5. **Metadata leakage to AWS.** Cross-space ciphertext dedup exposes the dedup graph; IP↔hash exposes collaboration. Decide cost vs per-space salt explicitly; CloudFront hides client IPs from S3 but not from CloudFront.
6. **Cascade-on-delete.** Object delete must tombstone *all* its payload rows (an object can own N).
7. **DMCA / abuse.** Need a broker-level global `contentHash` tombstone (taking down shared ciphertext affects all referencing spaces — document in ToS).
8. **Key-reuse correctness.** Reuse keys only across *identical* plaintext (gate on full `plaintextHash` + size).

---

## Spike plan (prototype before committing v1 vs v1.1)
Against the local any-sync network (`~/projects/local-infra`) + a MinIO/S3 + CloudFront-sim:
1. **Broker round-trip:** `RequestUpload` (reserve) → direct S3 POST → `CommitUpload` (HEAD-verify, finalize). Confirm quota reserve/confirm/expire and the `content-length-range` cap.
2. **Chunking + dedup:** 4–8 MB chunker; two payloads, same plaintext → same chunk hashes → second upload is bind-only. Measure S3 request counts.
3. **`payloads` materialization:** per-object dataset → common collection via the shared-collection override; object-delete cascades tombstones to *all* payload rows.
4. **P2P + collective durability:** two clients on one LAN, S3 unreachable — reader pulls chunks from the holder peer via `BlobGet` on the object-sync channel; then holder A offline before upload, holder B drives to `durable`; `RequestUpload`-reports-present prevents double PUT.
5. **Deletion race:** refcount→0 tombstone + lifecycle grace; concurrent re-create inside the window keeps the chunks.
6. **Download path:** CloudFront signed URL fetch + decrypt + hash-verify; rate-limit behavior.

Decide v1 vs v1.1 from what the spike surfaces (esp. #1, #4, #5).

---

## Sketched SDK API
```go
type Files interface {
    // Attach a payload to an EXISTING owner (the page-with-images case).
    Attach(ctx, ownerObjectId string, reader io.Reader, opts AddOpts) (PayloadRef, error)
    // CreateFileObject mints a dedicated file object owning exactly one payload.
    CreateFileObject(ctx, reader io.Reader, opts AddOpts) (objectId string, ref PayloadRef, err error)
    Open(ctx, fileId string, variant VariantKind) (io.ReadSeekCloser, error) // local -> peer -> CloudFront
    Get(ctx, fileId string) (PayloadRecord, error)
    // Deletion is implicit: tombstone the owner object (or its payload row).
}
type AddOpts struct {
    Name     string
    Mime     string
    Variants map[VariantKind][]byte // from MediaProcessor, optional
}
```
- Micro-payload path is internal: `len(bytes) < InlineThreshold (32 KB)` → inline ciphertext on the row, no chunks, no S3.

## Open questions / decisions to confirm
1. **Chunk size (4 vs 8 MB)** — confirm against the chat+documents asset histogram (P2P resume granularity vs S3 request count vs manifest size).
2. **Inline threshold (32 KB)** — confirm against real asset sizes.
3. **Cross-space dedup vs per-space salt** — product/security sign-off (cost vs metadata privacy).
4. **Quota TTLs** — reservation expiry that balances overshoot vs soft-lock.
5. **CloudFront/WAF policy** — TTLs, rate limits, signed-URL scope.
6. **Manifest storage** — encrypted content-addressed blob (chosen) vs inline list for small files; elision for single-chunk.
7. **Wrapped-key rotation** — who walks payloads re-wrapping keys on read-key rotation.
8. **`MediaProcessor` boundary** for gomobile/wasm; **CORS** if a browser/WASM target ever does direct-to-S3.
9. **Object-copy primitive** — carry the "re-mint fileId + re-bind" constraint forward.
10. **DMCA tombstone** — broker-level global `contentHash` takedown mechanism + ToS.

## Source files (for reference)
- `anytype-heart/core/files/filestorage/rpcstore/store.go:205-244` — the client P2P pattern we align to (local → peer → node); `rpchandler.go:59-91` — client serves blocks, member-gated. We replace IPFS blocks with our chunk service on the same channel.
- `any-sync` localdiscovery / peer pool / `peerStore.LocalPeerIds` — the mDNS discovery + secure session object sync uses; blob P2P rides it.
- `any-sync/commonspace/object/keyvalue/` — KV store (deliberately NOT used for binaries).
- filenode v1 (`any-sync-filenode`) — the refcount/quota/ACL *ideas* we keep; the block-proxy/IPLD path we drop.

## Dependencies
- **Space** — files live in a space; ACL/quota/encryption inherited.
- **Data structure / datasets** — per-object `payloads` dataset materialized into a common collection via the shared-collection override (like properties → `objects`).
- **P2P / mDNS** — blob `BlobGet`/`BlobCheck` ride the same peer pool, discovery, and secure session as object-tree sync (`peerStore.LocalPeerIds`).
- **Sync status (`docs/09`)** — payload `status` is part of the general sync-status surface.
- **filenode v2 (NEW server)** — token broker: ACL + quota + presigned-upload/CloudFront-download. No block store. The one server change this design requires.
- **Infra** — S3 (or compatible) + CloudFront + a lifecycle policy + (optional) S3 event stream for reservation reconciliation.

---

## History (superseded directions)
- **v0 (rejected):** files as a first-class any-sync data type. Expensive cross-team server change, wrong workload fit.
- **v1 (superseded):** space-level `files` collection + epoch mark-and-sweep GC with a 7-day grace. Superseded by per-object-owned `payloads` + causal-DAG lifecycle safety.
- **v2 (superseded):** SDK-only over the **existing filenode block-proxy** — availability/durability split, collective durability via `BlocksBind`, IPFS 1 MB blocks, `rpcstore` P2P. Superseded by v3 once the team accepted a server change: proxying ciphertext bought no security, so byte-proxying + IPFS were removed.
- **v3 (current) — S3-direct, filenode v2:** node becomes a thin token broker (presigned upload / CloudFront download), bytes go client↔S3 directly, content-addressing via SHA-256 of **4–8 MB ciphertext chunks** + encrypted manifests. Availability ladder local → mDNS peer → S3 (blobs mirror object-sync locality; S3 = the sync-node for bytes). Durability collective via `RequestUpload`/`CommitUpload`. Deletion via lifecycle-tombstone (no sync DELETE). Local bytes in a content-addressed file dir (any-store dropped for blobs; tiny index kept). Expert-reviewed FORK; corrections folded in: coarse chunks, `CommitUpload` HEAD-verify, CloudFront + rate-limit, member-only P2P, metadata-privacy + DMCA tradeoffs recorded.
