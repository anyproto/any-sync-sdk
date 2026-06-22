# Files

## TL;DR — the SDK2 file model (S3-direct, whole-blob)

1. **Drop IPFS. Filenode becomes a thin token broker; bytes go client↔S3 directly.** The node never proxies a byte — it checks ACL + quota and hands out **presigned S3 upload / CloudFront-signed download URLs**. Sound because blobs are already client-side **ciphertext**; proxying them adds *zero* confidentiality, only cost and a failure domain. (Expert-reviewed: "FORK.") Needs a new server, but a far simpler one (~3 RPCs, no block store).
2. **One file = one opaque encrypted S3 object**, addressed by `contentHash = SHA-256(ciphertext)`. **No IPFS, no network-level chunks, no manifest, no block protocol** — that was the whole point of the fork. Resume + seek come from HTTP **Range**, not from a chunk graph.
3. **Internally the blob is a Streaming-AEAD file: fixed AES-256-GCM frames (~4 MB) concatenated into one stream.** This is an *encryption-format* detail (like `age` / Tink / AWS Encryption SDK), invisible to the network/storage/dedup/GC layers. It buys per-frame authenticated integrity, verified seek, and cheap resume — without reintroducing a manifest.
4. **Payloads are a per-object `payloads` dataset, materialized into a common collection** — synced, owned 1:1, tombstoned with the object; coalesced into one space-level `payloads` collection via the shared-collection override (like properties → `objects`). Two tiers by size: **micro-payload (< 32 KB)** = encrypted bytes inline in the row (no S3); **real payload (≥ 32 KB)** = one S3 blob.
5. **Envelope encryption**: random per-file key encrypts the frames → stable ciphertext → stable `contentHash` forever. Wrap the key under the space read key; rotation re-wraps only the tiny key. **Reuse the key for identical plaintext** (via `plaintextHash`) → identical blob → S3 stores it once.
6. **Availability ≠ durability.** A reader resolves a blob from whatever holder answers: **local file → mDNS space peer (P2P) → CloudFront/S3**. It never waits on an "uploaded" flag — *availability is emergent*. Synced per-variant `status` tracks only **durability** (is it on S3 yet); never gates a read. Kills the filesync queue and the status/byte drift behind most of anytype's file bugs.
7. **Blobs follow the same locality model as objects.** P2P is mDNS/LAN-only and rides the **same peer pool, discovery, and secure session as object-tree sync**. **S3 is to blobs what the sync-node is to objects** — the always-online backstop beyond the LAN.
8. **Durability is a space-collective duty, not the author's.** The payload row (`contentHash` + key) syncs, so any write-member holding the bytes can `RequestUpload` + PUT them under the canonical `fileId`. Alice adds a file → Bob pulls it P2P → Alice's device dies before upload → **Bob makes it durable.**
9. **Thumbnails are host-OS-generated, not Go**, and a tiny **blurhash rides inline in the change** so a preview shows the instant the message arrives — offline, before any byte transfer. One payload row, many variants (each its own blob).
10. **Local bytes are tiered by size (measured).** Real blobs (≥ ~3.5 KB) live in a content-addressed file directory (`blocks/<spaceId>/<contentHash>`), not a database — in any-store they'd be overflow chains that fragment under churn (12× slower reads, measured). Sub-~3.5 KB blobs pack into any-store instead (one file each = the flatfs inode-waste trap: 33× space, 50× slower). A tiny per-blob **index row always stays in the per-space any-store** so GC and `BlobCheck` are indexed, not `stat()`-scanned. (See "Local block store — tiered by blob size".)

---

## Two approaches considered — A vs B (the fork)

Everything *above* the byte layer is **common to both** — the per-object `payloads` dataset + materialization, ownership shapes, the availability/durability split, per-variant `status`, blurhash preview, the host `MediaProcessor`. The fork is purely the byte/transport/crypto/GC layer.

### A — IPFS + filenode block-proxy (anytype today; our earlier v1–v2)
Client IPFS-chunks the ciphertext into 1 MB UnixFS blocks (CIDv1+SHA256) and **pushes every block to filenode**, which stores bytes in S3 + index in Redis, refcounts per-CID (dual-scoped per-space/per-account), and checks ACL + quota at push time. The client (`rpcstore`) reads blocks **local → peer → filenode**. Encryption is AES-CFB per file/variant. The node proxies all bytes.

### B — S3-direct whole-blob + Streaming AEAD (chosen)
Client encrypts the file as **AES-256-GCM frames concatenated into one blob**, addresses it by `contentHash = SHA-256(ciphertext)`. **filenode v2 is a token broker** (`RequestUpload`/`CommitUpload`/`RequestDownload`): it checks ACL + quota and returns **presigned S3 POST / CloudFront-signed GET** URLs. Bytes go **client↔S3/CloudFront directly**; the node holds none. P2P is `BlobGet(contentHash, range)` on the same mDNS peer channel as object sync (member-only, self-verifying per frame). Dedup is whole-file; refcount/GC per file; deletion via S3 lifecycle-tombstone.

### Comparison

| Dimension | A: IPFS + filenode-proxy | B: S3-direct whole-blob + Streaming AEAD |
|---|---|---|
| **Server role** | Block store: stores bytes, Redis index, per-CID refcount | Token broker: ACL + quota + URL signing; **holds no bytes** |
| **Byte path** | client → **filenode** → S3 (proxied) | client ↔ **S3/CloudFront** directly |
| **Node bandwidth/cost** | Every byte transits the node; bottleneck | **~0** node bandwidth; S3/CDN scale |
| **Server complexity** | High: UnixFS/dag-pb, block protocol, dual-scoped refcount | Low: ~3 RPCs, no block store |
| **New server needed?** | **No** — filenode v1 exists, ships in anytype | **Yes** — filenode v2 (cross-team, but small) |
| **Addressing** | Merkle DAG of 1 MB block CIDs | One `contentHash` (SHA-256 of the whole ciphertext) |
| **Encryption** | AES-CFB (unauthenticated, malleable) | **AES-GCM streaming frames** (authenticated) |
| **Integrity while streaming** | Per-block CID verify (incremental) ✅ | Per-frame GCM tag verify (incremental) ✅ — **tie** |
| **Resume** | Block-level | HTTP **Range** at frame boundary; no prefix re-hash |
| **Seek (video scrub)** | DAG traversal | Range GET aligned to a frame |
| **P2P exchange** | **Free** — reuse `rpcstore` block read | Must **build** `BlobGet` (simpler, same peer channel) |
| **P2P shape** | Block swarm possible | Single-source + Range-resume, S3 fallback (LAN best-effort) |
| **Dedup** | Sub-file (blocks shared across files) | **Whole-file** only (`plaintextHash`) — all we wanted |
| **Refcount / GC** | Per-CID, **dual-scoped** (leak / premature-delete bugs) | **Per-file** + S3 lifecycle-tombstone |
| **Deletion safety** | Refcount race (`ErrCidsNotExist` strand) | Lifecycle grace **structurally absorbs** the race |
| **Local store** | flatfs / `blocks.db` (block-keyed) | Content-addressed **file dir** + tiny any-store index |
| **CDN reads** | No (node-mediated) | **CloudFront** signed URLs — fast global, cacheable |
| **S3 request cost** | ~1000 PUTs/GB (1 MB blocks) | **1 PUT/file** (or multipart parts) |
| **Quota enforcement** | Inline at push (exact) | **Reserve-confirm-expire** (eventual; overshoot/soft-lock window) |
| **Egress-abuse surface** | Low (node-gated) | Higher — needs CloudFront + WAF + SDK rate-limit |
| **Metadata privacy** | S3 sees only filenode IP (clients hidden) | S3/CloudFront see **client IP ↔ hash** (worse) |
| **Maturity** | Battle-tested in anytype | New code + new file format (Tink/age-class, standard) |

### Pros / cons
**A — IPFS + filenode-proxy.** *Pros:* exists today, no new server, free P2P via `rpcstore`, sub-file dedup, mature, clients hidden behind the node, exact inline quota. *Cons:* node proxies every byte (cost/bottleneck), heavy machinery + the dual-scoped-refcount bug class, status drift, no CDN, ~1000× the S3 requests, **AES-CFB unauthenticated**.

**B — S3-direct whole-blob + Streaming AEAD.** *Pros:* node holds no bytes (cheap, scales), tiny server, CDN reads, per-file GC + deletion race gone, cheap S3 requests, **authenticated encryption**, one mental model. *Cons:* needs a new server, no sub-file dedup (unwanted), eventual quota (overshoot/soft-lock), egress-abuse surface, **exposes client IP↔hash to AWS**, must build P2P + the AEAD format.

### Why B
B wins decisively on cost, server simplicity, GC/deletion, CDN, and crypto. A wins on "already exists / no server change," sub-file dedup (which we never wanted), and client-IP privacy. The two real costs of B that need a human call — **metadata privacy** (client IP↔hash to AWS, mitigated not erased by CloudFront) and the **new-server dependency** — are both signed off. We keep A's *lessons*: zero-knowledge ciphertext, ACL at the gate, refcount-for-dedup, incremental integrity (A got it from per-block CIDs; B gets it from per-frame GCM tags).

> **Why not whole-blob with plain SHA-256 (no AEAD)?** Considered and rejected. CFB/CTR are malleable: a peer/MITM flips a ciphertext bit → flips the plaintext bit, and end-of-file SHA-256 catches it only *after* the parser has already eaten the corrupted bytes (mid-stream exploit). It also makes mobile resume a battery killer (re-hash the whole prefix) and corruption non-localizable (one bad byte → discard the whole file). Streaming AEAD frames fix all three while staying one blob / one hash / one refcount.

### C — Consistency-first variant: node-derived reachability GC (no filenode)
A variation on **B's binding/GC tier only** — bytes stay S3-direct + Streaming AEAD, unchanged. It removes the separate broker ledger and **derives blob lifecycle from the CRDT the sync-node already holds**, so object state and byte state cannot drift. **Eliminates filenode entirely.** Documented as an alternative, not the default — see the complexity note.

How it differs from B:
- CID-bindings (`objectId → [contentHash]`) live in a **node-readable per-space `payload-index` tree** (not the encrypted object content). The upload-confirmation is written there as **synced CRDT state** (peers see the placeholder and wait for `durable`), not an out-of-band RPC ack.
- The **sync-node** signs upload (to an S3 `staging/` prefix) + CloudFront-download URLs after an ACL check, and runs **GC by reachability**: an incremental materialized view `(spaceId, contentHash, live_ref_count)` updated as it ingests `payload-index` deltas (attach +1, tombstone −1); `live_ref_count = 0` past a grace window → delete from S3. **No client-issued unbind; no refcount mutated by RPCs.**
- **S3 staging prefix + 24 h auto-expire:** a client that PUTs then crashes before writing the binding leaves an orphan S3 auto-deletes; the node promotes `staging/ → durable/` only on seeing the binding. **Zero dark orphans.**

Why it's attractive (expert verdict: *"the correct end-state / gold standard"*):
- **Single source of truth.** The CRDT is authoritative for *both* metadata and byte lifecycle, so the client-RPC-driven refcount drift that is historically the root of most file bugs (the original problem #2) is **eliminated, not patched.** (RPC refcounting in offline-first systems is a known anti-pattern — dropped packets/crashes guarantee drift.)
- **Feasibility verified in any-sync code:** the settings tree is already node-readable (`ShouldBeEncrypted: false`, `settingsobject.go:242`); the node already computes object liveness/deletion (`DeletionManager`, `HeadStorage.DeletedStatus`); a responsible node holds the **complete** change DAG. A node-readable `payload-index` tree is a new-but-precedented addition (analogous to the settings tree's cleartext `ObjectDelete`).

**Why it's an alternative, not the default — complexity.** It turns the sync-node into a **stateful blob-lifecycle authority**: a node-readable payload-index tree (which grows with payload count), an incremental materialized ref-count view, S3 staging promote/expire, reachability GC, grace windows, and dangling-pointer handling. That is materially more server machinery than B's near-stateless URL-signer + S3 lifecycle. **Decision: keep B as the default; hold C in reserve.** It's a *server-side* change that doesn't alter the client's read/write model, so B can be migrated to C later if status/blob drift proves painful in practice — we don't have to choose now.
- **Must-nail if adopted:** S3 staging + auto-expire (zero orphans); incremental ref-count so GC is `O(changes)` not `O(objects)`; a *dedicated* payload-index tree (don't pollute settings); a 7–14 day GC grace for offline-branch races; graceful "syncing / unavailable" UI for a binding whose S3 object 404s.

---

## What's actually below us

### filenode v2 (the broker we need — new server, ~3 RPCs)
- `RequestUpload(spaceId, fileId, contentHash, size)` → check `CanWrite`, **reserve** quota → presigned **S3 POST** (`content-length-range` enforces the per-object cap) for a single PUT, or multipart part-URLs for blobs > 5 GB. If `contentHash` already exists, return "present" (dedup → skip the PUT).
- `CommitUpload(spaceId, fileId, contentHash)` → node `HEAD`-verifies the object exists in S3, **finalizes** quota, records the `fileId → contentHash` binding, marks durable. (S3 `ObjectCreated` events are a *background audit* for abandoned reservations — never the primary confirm; they lag.)
- `RequestDownload(spaceId, contentHash)` → check `CanRead`, **SDK-rate-limited** → **CloudFront signed URL** (free S3→CF egress, WAF rate-limit; never a raw S3 GET).
- Index only (Redis/DB): `fileId`/space → `contentHash` bindings, refcounts, quota ledger, `deleted_at` tombstones. **No block store; ~0 node bandwidth.**

### Verified client-side P2P pattern (anytype-heart `rpcstore`) — what we align to
any-sync **core** has no peer block server; the P2P read lives in the **client**, on the regular peer channel. We keep the *pattern*, swap the *payload* (a whole-blob `BlobGet` for IPFS block-get). Confirmed in `anytype-heart/core/files/filestorage/`:
- **Reads try peers first:** `rpcstore.Get` = local cache → a discovered space peer → node fallback (`store.go:205-244`); peers from `peerStore.LocalPeerIds(spaceId)` (mDNS discovery + `SpaceExchange`), 1 s timeout, 5-min ban.
- **A client serves what it holds** from its local store (`rpchandler.go:59-91`), auto-caching every fetch (`proxystore.go:58-84`).
- This is the **same peer pool / discovery / secure session as object-tree sync** — which is why aligning blob P2P with it is "as aligned as possible."

### What anytype-heart does today (the bar)
- Every file — even a 2 KB icon — is a full object + a node upload; images = 5 variant sub-DAGs.
- Envelope encryption is **already correct** (random key, wrapped under the space key, synced with the object). The crypto *distribution* is fine; the *mode* (CFB) is what B upgrades.
- **Sync status is a separate state machine + persistent `filesync/queue`** — the documented root of most file bugs.

---

## Design (chosen: B)

### Principle: the node never holds bytes; the CRDT/dataset is the only source of truth
Drift happens when object state and block state are coordinated by a third system (the queue). Instead the **`payloads` row is the source of truth**, S3 + peers are the byte sources, the node is a stateless-ish authorizer.

### Availability vs durability — the split anytype conflates
- **Availability — "can I get these bytes now?"** Emergent, never declared. Resolve the `contentHash`: **local file → mDNS space peer → CloudFront/S3**. The row's `contentHash` + `wrappedKey` is everything needed to *attempt* it; no gate. A miss at one provider falls through.
- **Durability — "are the bytes safe long-term?"** What the synced per-variant `status` tracks: is the blob on S3. Drives the "backing up…" UI and local-GC retention; never a read precondition.

### Aligned with object sync — same locality, one transport family
The blob exchange reuses the any-sync **peer pool, mDNS discovery, and secure session** of object-tree sync, with one extra DRPC service registered alongside it.

| | on the LAN | beyond the LAN |
|---|---|---|
| **objects** | mDNS peers (direct tree sync) | sync-node |
| **blobs** | mDNS peers (`BlobGet`) | **S3 / CloudFront** |

**S3 is to blobs what the sync-node is to objects.** P2P is mDNS/single-network only (no DHT, no WAN hole-punching in v1).

### Where payloads live — per-object dataset, materialized into a common collection
- **Source of truth: a per-object `payloads` dataset**, written through the object's CRDT → **synced, owned 1:1, tombstoned with the object** (sticky, convergent; `05a-crdt-spec §3.4/§5.7`). Lifecycle safety is free.
- **Scan surface: a common `payloads` collection**, materialized via the Controller's **shared-collection override** — the mechanism that coalesces property writes into `objects` (`SystemPropertiesHandler`; `06-data-structure`). **Convergent by construction.** The upload/GC/dedup workers scan this one collection.

```jsonc
// payloads — per-object dataset, materialized into the common `payloads` collection
{
  "id": "payload_uuid",
  "name": "report.pdf",              // human label (metadata; NOT an identity)
  "plaintextHash": "sha256-…",        // indexed; key/blob reuse for identical content
  "fileId": "f_uuid",                 // SDK-minted broker binding id (refcount/quota unit)
  "placeholder": "blurhash:LEHV6n…",  // ~30 B, inline, rides the change → instant preview

  // inline micro-payload: encrypted bytes ride in the row (no S3, no file dir).
  // Ceiling ~3.5 KB = one any-store overflow page (see "Local block store");
  // also weighed against permanent DAG weight — inline rides the change forever.
  "inline": { "bytes": "base64…", "wrappedKey": "…", "mime": "image/png" },

  // real payload: one S3 blob per variant. `status` is PER VARIANT,
  // tracks DURABILITY only (never a read gate). `contentHash` = SHA-256 of the
  // whole encrypted (Streaming-AEAD) blob; `frame` = the AEAD frame size.
  "variants": {
    "original":  { "contentHash": "sha256-…", "wrappedKey": "…", "size": 4000000, "frame": 4194304, "mime": "image/jpeg", "w": 4032, "h": 3024, "status": "local"   },
    "thumbnail": { "contentHash": "sha256-…", "wrappedKey": "…", "size": 22000,   "frame": 4194304,                                              "status": "durable" }
  }
}
```

### Ownership shapes — payloads don't force a "file object", but allow one
- **Incidental payloads.** A page with images, a member avatar — rows in the *content* object's `payloads` dataset. No per-file object. (Answers problem #1.)
- **Dedicated file object.** A library PDF gets an object owning **exactly one payload** — first-class, referenceable, queryable.

Same mechanism either way; size (inline vs S3 blob) is an orthogonal axis.

### Why this eliminates the distributed deletion race
A payload row can only be created by syncing the object that owns it, so a peer **cannot observe (and delete) a payload before it has seen the parent**. Parent tombstoned → payload tombstoned with it. The causal DAG gives lifecycle safety — **no space-wide reference scan, no grace-period rescue.**

### The encrypted file format — whole-blob, Streaming AEAD
- **One blob per variant**, `contentHash = SHA-256(ciphertext)` = the S3 key = the P2P key = the addressing/dedup id + a final end-to-end checksum.
- **Internally, Streaming AEAD:** plaintext sliced into fixed **frames** (default ~4 MB); each encrypted with **AES-256-GCM**, nonce = frame index, AAD binds the frame index (and a final-frame flag to stop truncation). Concatenate `[ct₀‖tag₀][ct₁‖tag₁]…` into the single blob.
- **Verified seek/resume from Range:** to read frame *i*, Range-GET byte offset `i·(frame+16)`, verify its GCM tag, yield authenticated plaintext. Resume continues at a frame boundary — **no prefix re-hash**, corruption is localized to one frame, and a malicious peer's bytes are rejected *before* the parser sees them.
- **CDN alignment:** the SDK aligns Range requests to frame boundaries → CloudFront caches the same ranges across users → high hit rate, low S3 origin egress.
- `fileId` is the SDK-minted **broker binding unit** (what quota/refcount attach to), decoupled from content. "Save the same file under three names" = three fileIds binding one `contentHash`.

### Encryption — envelope, with key reuse for identical content
1. Random AES-256 key **per file** → encrypt the frames → **stable ciphertext → stable `contentHash` forever.**
2. **Reuse the key for identical plaintext** (via `plaintextHash`) → identical blob → `RequestUpload` reports it present → skip the PUT, just bind. Safe because a `(key, frame-nonce)` pair is *only ever* reused across identical plaintext (the goal is identical ciphertext); distinct content always gets a distinct random key, so GCM's deterministic per-frame nonce never collides under one key.
3. Wrap the key under the **current space read key**; store the wrapped key per variant.
4. ACL rotation re-wraps only the tiny `wrappedKey` fields. **The S3 object is never touched.** An evicted member can't unwrap → loses future reads (and can't get new download URLs).

Do **not** derive keys from the read key (rotation would re-encrypt every blob). Do **not** use convergent encryption (key = hash of plaintext) for cross-*account* dedup — confirmation-of-file leak.

### Identifiers
| id | role | where |
|---|---|---|
| `fileId` | SDK-minted broker binding unit (refcount/quota); decoupled from content | payload row |
| `plaintextHash` | intra-space key/blob-reuse index | secondary index |
| `contentHash` | `SHA-256(ciphertext)` of the whole blob = S3 key + P2P key + dedup id | inside `variants[*]` |

### Filenode v2 protocol & limits
- **Upload:** `RequestUpload` reserves quota → presigned **POST** (single PUT < 5 GB) or multipart part-URLs (> 5 GB). Client PUTs ciphertext directly. `CommitUpload` `HEAD`-verifies + finalizes. **Reserve-on-authorize** so concurrent uploads can't overshoot; unconfirmed reservations **expire by TTL**. (Watch: a large reservation soft-locks a near-quota user until TTL — keep TTLs tight, release eagerly on commit.)
- **Download:** `RequestDownload` → **CloudFront signed URL**, SDK-rate-limited per user/space. Never raw S3 GET (egress-amplifier).
- **Confidentiality at the edge:** a leaked URL yields ciphertext only; the envelope key (re-wrapped on rotation) is the real gate. URL TTL is a throttle.

### Durability is collective — any holder can complete the upload
The payload row (`contentHash` + key + `status`) syncs, so a holder needs nothing from the author:
- It can serve the blob P2P **and** `RequestUpload` + PUT it under the canonical `fileId` (any write-member is ACL-permitted), then `CommitUpload` → `status: durable`.
- **Mechanism is "drive toward durable", not "detect Alice died":** any holder with the bytes + connectivity + a non-durable variant pushes it. Loss window = "time until *any* holder reaches S3", with N candidate uploaders.
- **Herd control:** `RequestUpload` reports the blob already present → other holders skip the PUT and just bind.
- **Honest limit:** if *every* holder dies before any reaches S3, the content is gone — unavoidable offline-first; collective duty maximizes survival.

### Declarative sync — per-variant durability, never a read gate
`status` is **per variant** and tracks **durability only**; reads always try holders. The "queue" is `index WHERE status = local`.
- `local` — bytes on ≥1 holder, not yet on S3. Readable P2P now; a holder drives it to `durable`. The valid offline-create state.
- `durable` — confirmed on S3 (`CommitUpload` HEAD-verified). Safe to evict from any local cache.
- `limited` — quota hit; retried when headroom returns.
- `pending_delete` — owner tombstoned; binding teardown owed.

Per-variant enables the chat case: `thumbnail: durable, original: local`, and the worker prioritizes thumbnails across all pending payloads. Concurrent `durable` writes are idempotent (LWW). Self-healing: a `durable` variant whose blob turns up missing on S3 downgrades to `local`. Folds into the general **Sync Status** subsystem (`docs/09`).

### Garbage collection & deletion — per-file refcount + lifecycle-tombstone
- **Server:** a `fileId` unbinds → decrement the `contentHash` refcount. At zero, set `deleted_at` / an S3 `State:Orphaned` object tag — **never a synchronous S3 DELETE**. An **S3 Lifecycle Policy** (e.g. 7-day) does the removal. This **absorbs the delete-while-uploading race** (a concurrent re-bind in the window cancels the tombstone) — the structural fix for what was the #1 risk in the proxy model.
- **Local:** the per-blob index (in any-store) drives GC. For a **tier-3** blob (bytes in a file), `unlink` only if its local binding count is 0 **and** the variant is `durable` (S3 has it); the file dir is also the **P2P serving surface**, so a non-durable blob (incl. one merely *received* from a peer) is **retained** — it may be the last copy. For **tier-1/2** blobs (bytes in the row) GC is just the row delete, same predicate. Two classes: non-durable held until `durable`; durable = ordinary LRU cache.

### Local block store — tiered by blob size (measured)
Where a blob's **bytes** live locally is chosen by size, from a spike benchmarking any-store v2 against `go-ds-flatfs` (the anytype local store) on a real 70 366-block / 5.4 GiB anytype flatfs plus synthetic sweeps (`_spikes/{blobbench,realbench,sweep}`). any-store v2 is a SQLite-derived B-tree with **4 KiB pages** and a **~1 KB inline-cell limit** (`maxLocal`); a value above it spills to a **4 KiB overflow chain** drawn from a freelist that **fragments under churn** — and there is **no vacuum/compact** to recover. The boundaries below are measured, not guessed.

| tier | blob size | local home | why |
|---|---|---|---|
| **1 — tiny** | **< ~900 B** | **packed in any-store** | many docs share one 4 KiB leaf page; no overflow, **churn-immune** (~1.6 µs/doc cold). One file each is catastrophic: flatfs stores 20 749 sub-KiB blocks in **81 MiB for 2.4 MiB (33×)** and reads them ~50× slower. |
| **2 — small** | **~900 B – ~3.5 KB** | **one overflow page in any-store** | a file each would waste an inode + a 4 KiB block (the flatfs anti-pattern). Bounded cost: **exactly one** overflow page ⇒ **one** random read (~17 µs) even if the freelist has fragmented; ~4.6 KiB/blob on disk. ~3.5 KB is the one-page ceiling (a 2nd page is needed at ~4.5 KB). |
| **3 — real blob** | **≥ ~3.5 KB** | **plain file at `blocks/<spaceId>/<contentHash>`** | any-store is **not** used. Files give zero B-tree write amplification, `seek`-based Range reads, trivial P2P serving, `rm -rf blocks/<spaceId>/` teardown. In any-store a blob would be a **multi-page overflow chain that fragments under churn into IOPS-bound random reads — measured 12× slower (1.1 s vs 90 ms) on real 1–100 MiB files**; a contiguous file is immutable and never fragments. |

A **per-blob index row** (`contentHash, spaceId, size, last_accessed, durable`, refcount) **always** lives in the per-space any-store so GC/`BlobCheck`/LRU are indexed queries, not filesystem `stat()` scans (mobile inode/handle limits). For tier-1/2 the bytes ride **in that row**; for tier-3 the row points at the file. **Metadata always in any-store; tier-3 bytes in files.** (Huge files are write-once, read-never archives — for them only storage/write cost matters, so tier-3 is unconditional above the threshold.)

### P2P blob exchange — whole-blob, member-only, on the object-sync channel
A 2-method DRPC on the **same server / peer pool / mDNS discovery as object sync**:
- `BlobGet(spaceId, contentHash, range?)` → ciphertext (range = file `seek`, frame-aligned).
- `BlobCheck(spaceId, []contentHash)` → which the peer holds.

Rules: **serve only to verified current space members** (ciphertext is safe to leak, but serving non-members invites resource-exhaustion of mobile clients); every received frame is GCM-verified before use; auto-cache fetched blobs → become a source. Middle rung of the availability ladder (local → peer → S3); a dropped transfer resumes via Range or falls back to S3.

### Thumbnails / variants — offline-first preview
Only what's **inline in the change** is guaranteed to arrive with the message. Preview ladder:
1. **Blurhash / LQIP (~30–200 B): always inline.** Instant blur-up by any path, including pure offline P2P. The one hard guarantee.
2. **Sharp thumbnail (~10–30 KB): a small variant blob** fetched from a holder P2P the moment the message lands, or from S3 once `durable`.
3. **Original:** streamed on demand via Range.

Inlining the *full* thumbnail (sharp preview guaranteed even fully offline) is a per-collection **option** weighed against permanent DAG weight (~30 KB × 1000 msgs ≈ 40 MB replicated forever). Default: blurhash inline + thumbnail-as-variant. The Go SDK does **not** decode/resize — a host hook does:
```go
type MediaProcessor interface {
    GenerateVariants(ctx, raw []byte, mime string) (map[VariantKind][]byte, error) // -> blurhash, thumbnail, …
}
```

### Dedup & the metadata-privacy tradeoff
- **Intra-space, client-side, pre-sync:** hash plaintext, look up `plaintextHash`, reuse key → second copy is bind-only.
- **Cross-space dedup vs metadata privacy — an explicit choice.** Reusing the same key for identical plaintext makes the ciphertext (hence `contentHash`) identical across spaces → S3 stores it once (cost win) — **but AWS can group objects by hash and see the dedup graph, plus IP↔hash access patterns.** If strict metadata privacy outweighs cost, **salt the ciphertext per space** (breaks global dedup). Default leans cost-saving; flag for product/security sign-off.
- **Accept offline-concurrent duplication** (two offline devices, different keys → different hashes). Probability ~0; the resolver isn't worth the bug surface.

### Object duplication — re-mint `fileId` (future constraint)
No object-copy primitive exists yet. When designed it MUST be domain-aware: a generic deep-clone copies a payload row *including its `fileId`*, aliasing one binding to two owners → deleting one unbinds a blob the other needs. The sanctioned copy path strips the old `fileId`, mints a fresh one, and re-binds the same `contentHash` (no re-upload).

### Why not "files as a first-class any-sync type"
Rejected. State-sync optimizes for tiny, causally-linked, fully-replicated mutations; file transport optimizes for large-object streaming, on-demand replication, CDN caching. Direct-to-S3 *is* the file-shaped transport.

---

## Risks (ranked by blast radius)
1. **Quota reservation correctness.** Debit-on-authorize is right, but a large abandoned reservation soft-locks a near-quota user until TTL, and an unconfirmed-but-uploaded blob leaks quota. Tight TTLs, eager release on `CommitUpload`, S3-event background reconciliation.
2. **Egress / cost abuse.** A leaked download URL can amplify S3 egress. Mitigate with CloudFront signed URLs + WAF rate-limit + SDK per-user/space rate-limit + per-object `content-length-range`. Monitor egress.
3. **Received-blob retention (P2P data loss).** A peer that pulled a blob can be the last copy until `durable`; evicting before durability strands the swarm. Retain non-durable blobs; evict only at `durable`.
4. **Delete-while-uploading.** Refcount→0 must NOT synchronously DELETE from S3; `deleted_at`/tag + lifecycle grace so a concurrent re-bind cancels it.
5. **Metadata leakage to AWS.** Cross-space ciphertext dedup exposes the dedup graph; IP↔hash exposes collaboration. Decide cost vs per-space salt explicitly.
6. **Cascade-on-delete.** Object delete must tombstone *all* its payload rows.
7. **DMCA / abuse.** Need a broker-level global `contentHash` tombstone (taking down shared ciphertext affects all referencing spaces — document in ToS).
8. **AEAD format correctness.** Frame nonce/AAD discipline, final-frame (truncation) protection, deterministic framing for dedup — get the format right (lean on a reviewed design: Tink/age/AWS Encryption SDK).

---

## Spike plan
Against the local any-sync network (`~/projects/local-infra`) + MinIO/S3 + a CloudFront sim:
1. **Broker round-trip:** `RequestUpload` (reserve) → direct S3 POST → `CommitUpload` (HEAD-verify, finalize). Confirm quota reserve/confirm/expire and the `content-length-range` cap.
2. **AEAD format + dedup:** Streaming-AEAD encrypt/seek/resume; two payloads, same plaintext → same `contentHash` → second upload is bind-only.
3. **`payloads` materialization:** per-object dataset → common collection via the shared-collection override; object-delete cascades tombstones to *all* payload rows.
4. **P2P + collective durability:** two clients on one LAN, S3 unreachable — reader pulls the blob via `BlobGet` on the object-sync channel; holder A offline before upload, holder B drives to `durable`; `RequestUpload`-reports-present prevents double PUT.
5. **Deletion race:** refcount→0 tombstone + lifecycle grace; concurrent re-create inside the window keeps the blob.
6. **Download path:** CloudFront signed URL fetch + per-frame verify; Range resume + rate-limit behavior.

Decide v1 vs v1.1 from what the spike surfaces (esp. #1, #4, #5).

---

## Sketched SDK API
```go
type Files interface {
    Attach(ctx, ownerObjectId string, reader io.Reader, opts AddOpts) (PayloadRef, error)
    CreateFileObject(ctx, reader io.Reader, opts AddOpts) (objectId string, ref PayloadRef, err error)
    Open(ctx, fileId string, variant VariantKind) (io.ReadSeekCloser, error) // local -> peer -> CloudFront, Range-seekable
    Get(ctx, fileId string) (PayloadRecord, error)
    // Deletion is implicit: tombstone the owner object (or its payload row).
}
type AddOpts struct {
    Name     string
    Mime     string
    Variants map[VariantKind][]byte // from MediaProcessor, optional
}
```
- Micro-payload path is internal: `len(bytes) < InlineThreshold` → inline ciphertext on the row, no S3. **`InlineThreshold ≈ 3.5 KB`** (one any-store overflow page; see "Local block store — tiered by blob size").

## Open questions / decisions to confirm
1. **AEAD frame size (~4 MB)** — seek/resume granularity vs CDN cache alignment vs tag overhead.
2. **Inline threshold** — measured: any-store packs < ~900 B, stays one overflow page to ~3.5 KB, then multi-page + churn-fragments (`_spikes/sweep`). Storage side says **~3.5 KB**. Open part is the product call: a higher inline ceiling buys instant-offline availability at the cost of permanent DAG weight.
3. **Cross-space dedup vs per-space salt** — product/security sign-off (cost vs metadata privacy).
4. **Quota TTLs** — reservation expiry balancing overshoot vs soft-lock.
5. **CloudFront/WAF policy** — TTLs, rate limits, signed-URL scope.
6. **Wrapped-key rotation** — who walks payloads re-wrapping keys on read-key rotation.
7. **`MediaProcessor` boundary** for gomobile/wasm; **CORS** if a browser/WASM target does direct-to-S3.
8. **Object-copy primitive** — carry the "re-mint fileId + re-bind" constraint forward.
9. **DMCA tombstone** — broker-level global `contentHash` takedown + ToS.
10. **AEAD format choice** — adopt an existing reviewed format (Tink streaming / age / AWS Encryption SDK) vs a minimal in-house one.

## Source files (for reference)
- `anytype-heart/core/files/filestorage/rpcstore/store.go:205-244` — the client P2P pattern we align to (local → peer → node); `rpchandler.go:59-91` — client serves blocks, member-gated. We replace IPFS block-get with a whole-blob `BlobGet` on the same channel.
- `any-sync` localdiscovery / peer pool / `peerStore.LocalPeerIds` — the mDNS discovery + secure session object sync uses; blob P2P rides it.
- filenode v1 (`any-sync-filenode`) — the refcount/quota/ACL *ideas* we keep; the block-proxy/IPLD path we drop.
- `_spikes/{blobbench,realbench,sweep}` — the local-storage spike behind "Local block store": any-store v2 vs flatfs on real anytype data + synthetic sweeps. Pins the size tiers (< ~900 B pack · ~900 B–3.5 KB one overflow page · ≥ 3.5 KB contiguous file) and the churn-fragmentation penalty (12× on real 1–100 MiB files). Each dir has a `FINDINGS.md`.

## Dependencies
- **Space** — files live in a space; ACL/quota/encryption inherited.
- **Data structure / datasets** — per-object `payloads` dataset materialized into a common collection via the shared-collection override.
- **P2P / mDNS** — `BlobGet`/`BlobCheck` ride the same peer pool, discovery, and secure session as object-tree sync (`peerStore.LocalPeerIds`).
- **Sync status (`docs/09`)** — payload `status` is part of the general sync-status surface.
- **filenode v2 (NEW server)** — token broker: ACL + quota + presigned-upload/CloudFront-download. No block store.
- **Infra** — S3 (or compatible) + CloudFront + a lifecycle policy + (optional) S3 event stream for reservation reconciliation.

---

## History (superseded directions)
- **v0 (rejected):** files as a first-class any-sync data type. Expensive cross-team server change, wrong workload fit.
- **v1 (superseded):** space-level `files` collection + epoch mark-and-sweep GC with a 7-day grace. Superseded by per-object-owned `payloads` + causal-DAG lifecycle safety.
- **v2 (superseded):** SDK-only over the **existing filenode block-proxy** (approach A) — availability/durability split, collective durability via `BlocksBind`, IPFS 1 MB blocks, `rpcstore` P2P. Superseded once the team accepted a server change: proxying ciphertext bought no security.
- **v3 (interim):** S3-direct but with **network-level 4–8 MB chunks + manifest** — rejected for rebuilding IPFS by hand (chunk-level refcount/GC/P2P, a manifest synced in the row) with no real gain over A.
- **v4 (current) — S3-direct whole-blob + Streaming AEAD:** one file = one opaque encrypted S3 object (`SHA-256(ciphertext)`); chunking exists **only inside the encryption format** (AES-GCM frames, like Tink/age) for verified seek/resume, not in the network/storage layer. Token-broker filenode v2, client↔S3/CloudFront direct, per-file refcount + lifecycle-tombstone, P2P `BlobGet` on the object-sync channel, content-addressed local file dir. Expert-reviewed: networking chunks conceded unnecessary, whole-blob plain-SHA256 vetoed for malleability → Streaming AEAD is the corrected middle.
- **C (alternative, held in reserve) — consistency-first node-derived reachability GC:** B's bytes, but no filenode; the sync-node derives blob lifecycle from a node-readable `payload-index` tree (incremental ref-count materialized view + S3 staging) so object/byte state can't drift. Expert-rated the "gold standard" and code-verified feasible, but **deferred on complexity grounds** (sync-node becomes a stateful blob-lifecycle authority). Server-side-only, so migratable from B later. See *Two approaches considered → C*.
