# Files v6 — derived payloads children + partial encryption + filenode v2

Latest direction. Main ideas only; rationale/benches live in `internal/spikes/filetree` (build tag `filetreespike`), `e2e/staging_cold_sync_test.go` (build tag `stagingcoldsync`), and the existing `_spikes/{blobbench,realbench,sweep}`. Sits above the byte/transport layer of `07-files.md` (envelope crypto, availability≠durability, collective durability are unchanged).

## 1. Composition — payloads bound to owners

- Files are **bound to an owner object** (a page, a chat). The owner is the file's lifecycle/delete scope.
- A payloads home holds one **row per file**, `fileId = changeId` (content-addressed, globally unique, **never reused** — load-bearing).
- **Default home = a `payloads` dataset in the owner's own tree.** Cascade-reclaims via the owner's own tombstone; no extra tree.
- **Promote to a derived child object** (`Derive(parent)`, cascade-deleted with parent) only when: one owner holds *many* files (gallery/chat), or you want a node-readable index (#3). Verified: parent delete reclaims the child's change-DAG on disk and excludes it from cold sync. *SDK gap:* thread `ParentId` through `DeriveObjectOpts` (one field).
- **Bind one payloads home per delete-scoped owner — never per message/per file.** Benched: cold-sync cost is **super-linear in tree count** (cliff past ~100 trees; 10k tiny trees = 6000× a single fat tree). Tree count, not bytes, is the wall.

## 2. Two materialized views

- **`files`** — keyed `objId/fileId` (B-tree locality for per-owner lists), app-facing; may carry **encrypted** fields (name, key, mime).
- **`cids`** — keyed by `cid` → `{ refs:[fileId], size, durable }`. Minimal, **cleartext, node-readable**. The whole view a filenode needs: dedup + refcount + GC + quota + durability.
- `cids` is a **derived projection of live payloads rows, not a hand-maintained ledger** — so it can't drift, and a row leaving (incl. cascade) empties it for free. (May collapse to `files` + a `cid` secondary index, except `durable` wants a per-cid home and privacy wants a cleartext surface.)

## 3. Partial encryption / privacy boundary

- Split **private** (names/keys/owners → `files`, encrypted) from **node-readable** (`cid`/refcount/size/durable → `cids`, cleartext).
- Cleaner than per-field encryption: the node GCs and accounts quota off `cids` alone, never seeing file metadata.

## 4. filenode v2

- Reads `cids`: a cid with empty `refs` (past grace) → **S3 lifecycle-tombstone** (never synchronous DELETE); writes `durable` back into `cids`.
- **Dedup-aware quota = Σ cids.size** (charged once per distinct blob).
- Bytes go client↔S3 direct (proxy-first iteration ok). Best fit is the file-aware / sync-participant node (#3): reachability is node-derived from the CRDT → no client refcount RPC → no drift.

## 5. Reclaim & GC

- Owner delete → **cascade reclaims the child tree (free, verified)** + an **explicit delete-time pass** drops the orphaned materialized rows and `$pull`s their fileIds from `cids` → emptied cid → blob GC after grace. *Cascade reclaims trees, not rows/cids — the pass is mandatory.*
- **No native history compaction** (verified: storage is insert-only; only whole-tree delete reclaims). History is monotonic → keep rows **pointer-sized**: blurhash-only inline in churny owners (chats); reserve 3.5 KB inline for low-churn owners (pages). Benched: churn is linear and cheap when rows are small, explosive with inline bytes.

## Invariants

1. `fileId ≡ creating changeId`; never reused, never copied (makes `$addToSet`/`$pull` element races impossible).
2. `cids` may over-count (delays GC), never under-count (data loss).
3. No synchronous S3 delete — empty cid → grace → lifecycle delete; a re-appearing ref cancels it.
4. One payloads home per delete-scoped owner; object count = O(owners-with-files), never O(files).
5. Cascade reclaims trees; the explicit pass reclaims rows + cid refs.

## Open / to build

- SDK: `ParentId` on derive; the cascade-cleanup pass (rows + cid `$pull`), incl. late/recovery cascade paths.
- `cids` projection + the `objId/fileId` keying for `files`.
- Per-owner-class inline threshold (not one global value).
- Confirm the O(K²)-ish cold-rebuild mechanism (per-tree open over shared storage) — bounds how many objects a space can hold generally.
