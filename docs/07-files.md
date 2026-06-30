# Files — foundations & rejected alternatives

> **The current design is [07c — filenode v2 + networkSign](07c-filenode-v2-networksign.md) (byte/durability) + [07b — derived payloads](07b-files-derived-payloads.md) (composition). Read those first.**
> This doc holds the **shared foundations** both build on, plus the **alternatives we considered and rejected** (short description + why not).

## Foundations (shared by the current design)

### Envelope encryption + key reuse for dedup
- Random symmetric key **per file**; the client encrypts before upload, so the node/S3 only ever see **ciphertext** (zero-knowledge). The key is wrapped under the **space read key**; ACL rotation re-wraps only the key, never the bytes.
- **Reuse the key for identical plaintext** (found via `plaintextHash`) → identical ciphertext → the store keeps one copy.
- *Not* convergent encryption (key = hash of plaintext → confirmation-of-file leak); *not* derive-from-readKey (rotation would force re-encrypting every blob).

### Availability ≠ durability
- **Availability** is emergent: a reader resolves the bytes from whatever holder answers (local → peer → cloud); it never waits on an "uploaded" flag.
- **Durability** — "are the bytes safe long-term" — is a separate, synced fact, never a read gate. Splitting the two kills the status/byte drift behind most of anytype's file bugs.

### Collective durability
- Durability is a **space-collective** duty, not the author's. Any write-member holding the bytes can make them durable: Alice adds a file → Bob pulls it P2P → Alice's device dies before backup → **Bob backs it up.** No single author is a bottleneck. (Honest limit: if *every* holder dies before backup, it's gone — unavoidable offline-first.)

### Dedup
- **Intra-space, whole-file**, client-side, pre-sync (hash plaintext → reuse key → bind). **No cross-account** dedup (needs convergent encryption → leak). **Accept** offline-concurrent duplication (two offline devices → different keys → two copies; probability ~0, the convergence resolver isn't worth the bug surface).

### Thumbnails / preview
- Variants (thumbnail / original) are **host-OS-generated**, not Go (no Go image pipeline; no hardware access). A tiny **blurhash rides inline in the change** for an instant offline preview; the sharp thumbnail is its own small file fetched P2P/cloud.

## Alternatives considered (rejected)
- **Files as a first-class any-sync type** (bytes sync through the tree protocol). *Why not:* bulk binary chokes metadata sync; expensive cross-team server change for ~0 product gain.
- **Bytes in the CRDT / inline everything** (KV or datasets hold all bytes, one sync system). *Why not:* tree bloat; the KV re-encrypts on every ACL rotation. Kept only for the tiny **inline (<4 KB)** tier.
- **IPFS + filenode block-proxy (approach A — anytype today):** push 1 MB blocks, the node proxies all bytes, dual-scoped per-CID refcount. *Why not:* node bandwidth + the refcount-leak/premature-delete bug class + status/block drift; proxying ciphertext bought no security.
- **S3-direct whole-blob + Streaming-AEAD (approach B):** one opaque encrypted S3 object, internal per-frame AES-GCM. *Why not:* per-leaf/per-frame AEAD changes the ciphertext + trust model and **breaks IPFS/anytype block compatibility**. Reverted to whole-file CFB + IPFS cids (07c).
- **Whole-blob + plain SHA-256 (no AEAD).** *Why not:* CFB/CTR malleability lets a peer/MITM corrupt bytes mid-stream; integrity only verified at end-of-file.
- **Network-level chunks + synced manifest (v3).** *Why not:* rebuilds IPFS by hand (chunk-level refcount/GC/P2P, a synced manifest) with no gain over A.
- **One S3 object per cid.** *Why not:* ~1000 PUTs/GB + presigned-URL + broker-load blow-up for zero benefit (per-file random keys → no cross-file leaf sharing). → one CARv2 per file (07c).
- **Bespoke node-readable payload-index tree (alt C).** *Why not:* unnecessary — the partially-encrypted `payloads` object *is* the node-readable index.
- **Client-driven refcount / unbind RPCs.** *Why not:* RPC refcounting drifts offline-first (dropped packets/crashes); the node derives reachability from the synced rows instead.
- **A common KV/Redis index across filenodes.** *Why not:* a per-space derived index (evicted when idle, rebuilt from rows) avoids the central bottleneck.
- **Space-level `files` collection + epoch mark-and-sweep GC with a 7-day grace (v1).** *Why not:* superseded by per-object-owned payloads + causal-DAG lifecycle safety — the premature-deletion race can't occur, so no grace/rescue needed.

## Source files
- `any-sync/commonfile/fileservice` — UnixFS chunking / CID (the v2 path builds on this).
- `anytype-heart/core/files/filestorage/rpcstore` — the verified client P2P block pattern.
- `internal/spikes/carpack` — the pack + seek benchmark (`carpackspike`).
