# Files

## Current State (Anytype today) — what's wrong with it

**Architecture**:
- **File objects** (normal any-sync objects) store the encryption key + metadata + filenode sync status
- **Payload flow**: the client encrypts the file, splits the encrypted bytes into chunks, builds an IPLD/IPFS merkle tree over those encrypted chunks, and pushes the resulting **binary (encrypted) blocks** to filenode. Filenode never sees plaintext.
- Filenodes **are ACL-aware**: they know `spaceId` and `identity`, validate writers against the space ACL, enforce per-space and per-account quotas, and gate BlockPush by permission
- Reading flow: client fetches blocks by CID, reassembles the encrypted payload, decrypts with the key stored on the file object

**Deduplication model**:
- Filenode deduplicates blocks by CID and maintains **ref counters at three scopes**: global, per-account, and per-space. A single stored block can be referenced by multiple spaces/accounts; the block is only actually removed when all scope counters drop to zero
- This is efficient on storage but makes the ownership/lifetime model complex

**P2P**:
- P2P does apply to files, but differently from object trees. Object trees sync via the regular P2P protocol; file blocks have their own P2P exchange path between peers, separate from the tree sync. It works, but is a second mechanism layered on top of the first

**Problems**:
- **Two parallel sync mechanisms** — object tree sync + file block sync. Both have P2P paths, but the flows, error handling, and status reporting are separate. Filenodes maintain their own index, are slower than the regular node sync path, and the two systems don't share observability
- **Inconsistent deletion** — deleting a file offline has to later propagate to filenode, decrementing the appropriate ref counter. The local object is deleted but the block lives on until the counter reaches zero. The window between local delete and counter update is the root of most file bugs
- **Refcount complexity at three scopes** — global + account + space counters are correct in principle but hard to keep consistent in practice, especially under concurrent writes and partial connectivity. Bugs here cause either leaks (blocks that should be GC'd) or premature deletion
- **Separate consistency story** — file sync status, upload state, quota, and ref counters are all in a different mental model than the CRDT data. Callers end up managing two sync stories and two error surfaces

## Proposed Direction — files as a first-class type in any-sync

**Idea**: store file payloads on **any-sync-node as a new data type, alongside object trees and key-value**, scoped to a space. Files then sync with the **same rules, same CRDT, same ACL, same encryption as other space data**.

```
Current:
  Space {
    ObjectTrees (sync via any-sync)
    KeyValue    (sync via any-sync)
  }
  ─ refers to ─→ Filenode {
    Blocks (sync via separate system, refcount hell)
  }

Proposed:
  Space {
    ObjectTrees (sync via any-sync)
    KeyValue    (sync via any-sync)
    Files       (sync via any-sync, NEW TYPE)
  }
```

### Benefits
- **One sync system** — files sync alongside everything else
- **Clean deletion** — delete a space, files are gone; delete a file object, its blocks are gone. No refcount headaches because blocks are bound to the file object, not globally deduplicated across spaces
- **Natural ACL inheritance** — if you can read the space, you can read its files; if you're removed from the space, you lose access (including re-encryption on key rotation, handled by any-sync)
- **End-to-end encryption by default** — uses the space read key, same as object changes
- **P2P / offline-first works for files too** — same rules as other space data
- **Deduplication within a space** — still possible via content-addressing (chunks share CIDs within one space)
- **No deduplication across spaces** — accepted tradeoff. Uploading the same file to two different spaces stores it twice. This is the cost of making files space-local.

### What any-sync needs
This is a **significant change on the any-sync side**. The node storage layer has to grow a third data type for file blocks, with its own sync protocol (or an extension of the existing one). Current `commonfile/fileservice` is IPLD-based and can be reused for chunking/CID, but block storage and sync flow need to move from filenode to any-sync-node.

**Status**: design idea, not implemented yet. Coordinate with the any-sync team before committing SDK timelines.

## Implications for the SDK

### If files-as-first-class ships in any-sync before SDK v1
The SDK wraps the new file type and presents a clean API:
- Files are just another thing you read/write inside a space
- The SDK reuses space keys for encryption (or any-sync handles it end-to-end and the SDK never sees plaintext on the wire)
- Records reference files by CID; the CID is meaningful only inside the space
- GC is trivial — file blocks live with the space and die with it

### If it doesn't ship in time for SDK v1
Two options:
- **A. Ship SDK v1 without file support.** File APIs come in SDK v1.1 after any-sync lands the new type. Middleware / clients use the current Anytype filenode path directly for files in the interim.
- **B. Ship SDK v1 with a thin filenode wrapper** that mirrors the current Anytype approach. Clean up later when any-sync grows the new type. Downside: we inherit all the consistency problems, and the SDK API may have to break or dual-path when the migration happens.

Strong preference: **Option A**. It avoids building (and later discarding) a wrapper around a system we already know is problematic. Files ship when any-sync has the primitive.

## v1 Decision
**Files are out of SDK v1 scope.** The plan:
1. Flag files-as-first-class as a required any-sync prerequisite
2. Sketch the SDK file API shape now so v1 can be designed without painting us into a corner
3. Ship real file support in SDK v1.1 once any-sync lands the new data type

## Sketched SDK File API (for forward compatibility)

Rough shape so we don't lock v1 into something incompatible:

```go
// Within a space
type Files interface {
    Upload(ctx, reader) (cid string, err error)             // stream in
    Open(ctx, cid) (ReadSeekCloser, err error)              // stream out
    Delete(ctx, cid) error                                  // mark for GC
    Has(ctx, cid) (bool, err error)                         // local presence check
}
```

References in records are plain CID strings. Higher-level helpers (progress, metadata, typed `{cid, size, mime}` wrappers) can layer on later.

## Grooming Questions (open)

### Any-sync prerequisites
1. Timeline — when does the new file type in any-sync land? Who owns it?
2. Does the any-sync team agree with the model (files-as-third-type, space-scoped, no cross-space dedup)?
3. Migration — do existing Anytype file objects need to move to the new system, or do we leave them in filenode and only new files use the new system?

### SDK API shape (v1.1)
4. Encryption — inherited from space read key automatically, or explicit per-upload key option?
5. Upload return — just CID, or metadata struct (`{cid, size, mime, uploadedAt}`)?
6. Records — CID string only, or structured reference?
7. Streaming — `io.Reader` / `io.ReadSeekCloser` style, or chunked callback?
8. Progress reporting — built-in or caller wraps the reader?
9. Offline uploads — queued and flushed on reconnect. CID known immediately (content-addressable). SDK surfaces upload state how?
10. Local cache — same any-store DB or separate file blob store?

### Integration with other sections
11. Do file operations appear in the external API as `space.Files()`, or `sdk.Files(spaceId)`?
12. File sync status — part of the general Sync Status subsystem, or a dedicated File status?
13. Handler for file-referencing datasets — does CRDT section need a convention for file fields, or is a CID just a string?

### v1 placeholder
14. Should the SDK v1 `Space` interface return a `Files()` method that panics / errors until v1.1, or omit it entirely and add it in v1.1?
15. Does middleware need a bridge to current Anytype filenode for v1, or does it bypass the SDK entirely for files?

## Source Files (for reference)
- `any-sync/commonfile/fileservice/` — current IPLD chunking/CID code (can be reused)
- `any-sync/commonfile/fileblockstore/` — block store interfaces
- `any-sync/commonfile/fileproto/` — dRPC protocol (will likely need replacement/extension)
- Anytype filenode repo (external) — what we're moving away from

## Dependencies
- **any-sync** — requires the new file type in the node storage layer (blocking prerequisite)
- **Space** — files live inside a space, ACL/encryption inherited
- **CRDT** — file refs stored as plain string fields in records
- **Sync status** — file upload/download status surfaces here
