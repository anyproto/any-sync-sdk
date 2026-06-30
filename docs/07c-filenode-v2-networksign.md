# Files v7 — filenode v2 + networkSign upload protocol

Concrete protocol for the byte/durability layer. Elaborates `07b` §4 (filenode v2) and **supersedes `07-files.md`'s whole-blob addressing choice with IPFS cids**. Validated *"build it"* by adversarial review (2026-06-30): of every attack on the core, one survived (full-manifest-in-row, addressed below). Sits under `07b`'s derived-payloads composition; envelope crypto / availability≠durability / collective durability from `07-files` are unchanged.

## Decisions locked
- **Addressing = IPFS merkle cids of the *ciphertext*** (per-chunk SHA-256, self-verifying). Chosen over whole-blob+Streaming-AEAD. Rationale: per-chunk integrity + partial-download resume. **Chunk-level dedup is NOT a reason** — per-file random keys mean identical plaintext shares zero ciphertext chunks; dedup is **whole-file** via the plaintext `sha256` + bind.
- **No filenode byte-proxying.** Bytes go client↔S3 direct. Filenode v2 = ACL + quota + presign + **sign** (a pure Oracle).
- **The node never writes the CRDT** — it signs, the client records. Not a write-member.
- **One S3 object per file** (packed leaves, keyed by `rootCid`), not one object per leaf cid — see *Physical S3 mapping*. Adversarial review (5 lenses) found no in-scope case where packing loses anything: the per-file random key already zeroes cross-file leaf sharing.

## any-sync changes
1. **Space header gets `fileprotoVersion`** (in the signed, immutable header). Old filenodes **error** on v2 spaces; new filenodes serve **only** v2. The SDK is greenfield → every space is v2 (no in-place migration); v1/v2 coexistence rides the existing handshake `ProtoVersion` + `NetworkCompatibilityStatus`. The version can't be stripped/downgraded without invalidating the owner signature.
2. **An object may own a derived child `payloads` object, partially encrypted** — cleartext (node-readable) `{fileId, rootCid, size, networkSign, author}`; encrypted (member-only) `{key, name, sha256, meta, manifest}`. Partial encryption is enforced at the schema level, so there is no schema-valid way to register an S3-backed cid the node can't see. Owner removed with its derived payload → **all peers (and the node) see orphaned files.**

## Payload row
```jsonc
{
  // ---- cleartext (node-readable) ----
  "id":          "{fileId}",          // = creating changeId; content-addressed, NEVER reused
  "rootCid":     "{cid}",             // the merkle root; the node refcounts / GCs THIS
  "size":        12345,               // hint only — quota is the node's measured value, not this
  "networkSign": "{fileNetworkId}/{sign(rootCid)}",   // the node's existence receipt (see below)
  "author":      "{identity}",        // auto field from the change author
  // ---- encrypted (member-only) ----
  "key":         "{wrappedKey}",      // SEPARATE envelope field (cheap ACL rotation — do NOT fold into meta)
  "name":        "image.png",
  "sha256":      "{hash}",            // hash of the UNENCRYPTED file — the dedup key
  "meta":        { "mime": "image/png", "size": [1024, 768] },
  "manifest":    ["{cid1}", "{cid2}", "…"]   // the chunk list, member-only (or derive from the root block)
}
```
Only the **root cid** is node-visible; the full chunk manifest stays member-side. (Inlining the whole `cids[]` cleartext was the one confirmed-true review finding — it bloats large-file rows and inflates node refcount cardinality ~1000×. The node only needs the root.)

## networkSign — the keystone
- The node **signs the root cid** after verifying the upload; the **client records** it in the row. **No valid sign ⇒ the file is GC'd after grace.**
- **What it attests: an existence + size *receipt*** — "the node confirms `rootCid`'s bytes are durably stored, total size X," HEAD-confirmed against the cid set the node itself presigned. It is **not** a content-integrity attestation (integrity is delegated to content-addressing + the downloader) — so the node never reads chunk bytes and keeps ~0 bandwidth.
- **Oracle property** — the node authors no CRDT change, so it is not a write-member; a key-compromised node can't rewrite space state. This is what makes durability a synced fact without the node touching the tree.
- **BIND reuse is safe** — a new `fileId` for the same content carries the *existing* sign (the sign binds to `rootCid`, not `fileId`/user). Stripping/withholding it only harms the attacker (their file is non-durable, ignored by peers).
- **Verify-before-sign is mandatory** — lazy signing lets a client mint a receipt for garbage cids and bypass quota. The node verifies via an **S3 `ObjectCreated`-event-fed index**, so step 6 is a DB lookup, not N synchronous `HEAD`s.

## Upload flow
1. `sha256(plaintext)` → local-db lookup. **Hit → BIND** (step 8).
2. Generate a **new sym key**, encrypt, produce IPFS cids (root + chunks).
3. **Register the payloads row** (rootCid, size, encrypted fields) — *before* upload (makes every later crash reachable for GC).
4. `uploadRequest(fileId, rootCid, size)` → node **reserves quota** (reserve-on-authorize + TTL) → a presigned POST (or multipart, frame-aligned parts for big files) for the **one packed object** to a `staging/` prefix.
5. Client **PUTs the packed object** (`[root block][leaves…]`) directly to S3 (`staging/`).
6. `requestSign(rootCid)` → node confirms presence (event-fed index) → **signs the root cid**, promotes `staging/ → durable/`, finalizes the reservation. Sign issuance is **idempotent / re-requestable** from the row's cids.
7. Client **records `networkSign`** in the row.
8. **BIND** (dedup hit): mint a new `fileId`, reuse the existing `rootCid` + sign + key.

## Node-side cid ledger — the single authority
A node-maintained, **persisted, incremental** view: `(spaceId, rootCid) → { live_ref_count, size }`, +1 when a row references `rootCid`, −1 on tombstone. It is the *one* place that:
- **observes the derived-child cascade tombstone** via node-readable deletion records (settings-tree `ObjectDelete` / `DeletionManager` / `HeadStorage.DeletedStatus`) — so it decrements **after** the rows/tree are reclaimed (the node indexes `objId → rootCids` before the tombstone);
- is the **quota authority** — charged from the node's reservation + S3-HEAD-measured size, **never** the cleartext row `size`;
- backs **GC** and the **fast verify-before-sign** lookup.

## GC / orphaning
- Owner delete → derived payloads tombstoned → node decrements `rootCid` refs → an empty `rootCid` past grace → **S3 lifecycle-tombstone** (never a synchronous DELETE). A reappearing ref (offline re-attach inside grace) cancels it.
- **Over-count never under-count:** a crashed/late client leaks a blob (delayed GC), never loses data.
- **Async mark-and-sweep backstop:** an S3 object older than the grace **and** not referenced by a valid sign in the ledger → delete. Reclaims unsigned-but-uploaded and sign-lost orphans without node-side pending state.

## What v7 resolves (vs the prior reviews)
- **D1 (cids drift)** — node reads cid/sign directly off the synced object; orphaning is node-derived from the cascade tombstone; **no client `$pull` anywhere.**
- **D2 (node-readability)** — fixed at the schema level by partial encryption.
- **M3 (durable-bit / node-as-writer)** — durability is a node *signature* the client records; node never writes the CRDT.
- **M4 (dark orphan)** — register-row-before-PUT + no-sign⇒GC + staging prefix.
- **M6/M7 (hot-cid churn / O(rows) cascade)** — no shared `refs[]` array; refcount work lives in the node ledger, decremented by tombstone.

## Physical S3 mapping & seek (decided)
**One S3 object per file, keyed by `rootCid`** — a CAR-style `[root block][leaf₀][leaf₁]…`, or S3 multipart with frame-aligned parts. *Not* one object per leaf cid (that is ≈1000 PUTs/GB + ≈1000 presigned URLs/GB + broker load, for zero benefit). Adversarial review found **no in-scope scenario where packing loses anything**: every "global per-leaf resolution" win (cross-file dedup, swarm leaf-sharing, cross-file CDN reuse, DHT/gateway interop, delta/append) needs two files to share an identically-keyed ciphertext leaf, which the **per-file random key already makes impossible** — the leaf-sharing was zeroed by the crypto, not by the layout.

**Format.** Each leaf is an **independently AES-GCM-encrypted frame** (nonce = frame index) → independently decryptable (so any byte is seekable) and self-verifying (`leaf cid = SHA-256(encrypted frame)`). The **root block** is the `rootCid → [leaf cid, size]…` map; `rootCid = SHA-256(root block)` commits to the whole file. (This makes the IPFS-cids layout and the v4 Streaming-AEAD frame layout the *same* on-disk format.)

**Resolution — global at the file, integrity+seek at the leaf:**
| who | resolves | how |
|---|---|---|
| **filenode (Oracle)** | `rootCid → {refcount, size}` only | one presign / one HEAD / sign rootCid / GC by rootCid; never reads leaves (~0 bandwidth) |
| **peers (P2P)** | `(rootCid, byte-range)` | multi-source = different ranges of one rootCid; each frame cid/GCM-verified; mDNS single-net, single-source + Range-resume + S3 fallback |
| **CDN** | rootCid object + frame-aligned ranges | SDK aligns Range to frame boundaries → ranges shared across users; thumbnails are their own small rootCids (whole-object GET) |
| **client/member** | leaf *i* | fetch+verify the root block once → `offset_i = header + i·(frame+tag)` → Range-GET → `SHA-256`-compare → decrypt (ciphertext-only verify → zero-knowledge audit intact) |

A **leaf cid is an intra-file integrity + ordering coordinate, not a global address** — never individually presigned or resolved across files. The manifest stays member-side (the S3 root block, or inline in the encrypted row for tiny files), so the CRDT row carries only `rootCid` and node refcount cardinality stays ~1×, not ~1000×.

**Build alongside (not reasons to un-pack):** (a) make `BlobCheck`/`BlobGet` **range-aware** (held-range bitmap) so partial LAN holders can serve/repair; (b) **multipart frame-aligned parts** for large uploads → part-level resume; (c) keep the **root block re-derivable/cacheable** by any holder.

**Revisit only if** (both reversible per-blob without touching `rootCid` / `networkSign` / the refcount unit / P2P keys / any CRDT state — packing is a physical mapping, not a one-way door): (1) the crypto moves to a **shared/convergent key + content-defined chunking** (backup / VM-image / generational workloads) where global per-leaf dedup finally pays — which needs a re-encode migration anyway; (2) a **non-SDK reader** (browser/WASM direct-to-S3, public CID gateway) where frame-aligned Range discipline can't be enforced (weak — E2E already forecloses public-gateway value).

## Still open / to build
- **wrappedKey isolation** — keep `key` its own envelope field (above); rotation re-wraps one tiny value per row, not the whole `meta` blob (insert-only DAG → a folded key would rewrite every row forever).
- **Row lease / status** — distinguish a pending upload from a dead/abandoned-unsigned row; define who may tombstone an unsigned row past TTL.
- **Cross-owner move** (= bind + delete; mints a new fileId; references break) and **`Get(fileId)` without the owner** (no global `fileId → owner` index) — unindexed, same as v6.
- **Grace window** — port `07-files`' 7–14 d; account for AWS lifecycle tag-age (expiry counts from object *creation*, so effective grace can collapse below the configured window).
- **GATING SPIKE (deferred):** multi-writer + offline-branch + cascade on the *real* (derived-payload + node-cid-ledger) model — every spike so far is single-writer / linear-DAG, so the convergence + cascade + cross-owner-refcount claims are still unexercised.

## Cross-refs
- `07-files.md` — byte layer (envelope crypto, availability≠durability, collective durability). This doc supersedes its whole-blob addressing with IPFS cids.
- `07b-files-derived-payloads.md` — v6 composition (owner binding, the tree-count wall, partial encryption, materialized views).
