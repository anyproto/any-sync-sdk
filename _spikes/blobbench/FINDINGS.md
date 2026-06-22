# Storing binary blobs in any-store v2 vs flatfs — spike findings

Harness: `main.go` (`go run . -block=<bytes> -perfile=<n> -files=<n> -reads=<n>`).
Candidates, all writing the *block bytes* into the store:

- **flatfs** — anyproto fork of go-ds-flatfs (the anytype-heart local block store). One file per CID, key = uppercase CID string.
- **anystore/cid-key** — one any-store doc per block, `id = CID` (content-addressed, random key), `{id, data:<binary>}`.
- **anystore/cid-key/s2** — same, S2 compression on.
- **anystore/fileid-prefix** — `id = "f<8d>/<6d seq>"` (monotonic, clustered) + a `cid` field for content lookup.

Size column = **actual on-disk allocation** (`st_blocks*512`), so tiny files are charged for block rounding + inodes + dir entries, exactly as the disk sees it. Writes are buffered (default `CommitSync=false`, flatfs `Sync` syncs the dir) — throughput is CPU/cache-bound, not fsync-bound. WAL is checkpointed before measuring (`-wal` was 0 bytes).

## Size (on-disk MiB)

| block size | logical data | flatfs | anystore cid-key | anystore fileid-prefix |
|---|---|---|---|---|
| 200 B   | 3.1 MiB | **64.0** | 6.8 | **5.4** |
| 1500 B  | 23 MiB  | **64.0** | 73.5 | 72.1 |
| 4 KiB   | 64 MiB  | 64.0 | 73.6 | 72.1 |
| 6 KiB   | 46 MiB  | 64.0 | 68.7 | 68.0 |
| 256 KiB | 256 MiB | 256.0 | 260.6 | 260.5 |
| 1 MiB   | 256 MiB | 256.0 | 258.2 | 258.1 |

## Write throughput (MB/s, buffered)

| block size | flatfs | anystore cid-key | anystore fileid-prefix |
|---|---|---|---|
| 200 B   | 21.7 | 56.7 | **145.1** |
| 1500 B  | 169.7 | 185.2 | **325.8** |
| 4 KiB   | 576 | 499 | **863** |
| 256 KiB | **5588** | 1014 | 1257 |
| 1 MiB   | **6541** | 999 | 1167 |

## Read throughput (random, MB/s) — FindId loads + decodes the whole doc

| block size | flatfs | anystore cid-key | anystore fileid-prefix |
|---|---|---|---|
| 200 B   | 73.8 | 99.4 | 96.8 |
| 256 KiB | **7241** | 3298 | 4278 |
| 1 MiB   | **6493** | 3402 | 3970 |

## What the numbers mean

1. **flatfs is pathological for sub-4 KiB blobs.** Every file burns a full filesystem block (~4 KiB) + an inode. 16 384 × 200 B blocks = 3 MiB of data → **64 MiB on disk (21×)**. any-store packs them into shared btree pages → 5.4 MiB. If the IPFS DAG has many small intermediate/metadata nodes, flatfs wastes enormous space; any-store wins ~12×.

2. **any-store's worst band is ~1–4 KiB binary values.** It's a SQLite-derived btree: `pageSize = 4096` (hardcoded, **not** exposed in `Config`), `maxLocal ≈ 1002 B`. A value whose cell exceeds ~1 KB spills to **overflow pages, each a full 4 KiB**. So a 1500 B blob → one 4 KiB overflow page → 73 MiB for 23 MiB of data, *worse* than flatfs. Below ~1 KB it packs; at/above 4 KiB it's page-granular like flatfs.

3. **For large blobs (≥64 KiB) any-store costs ~1.7% over raw** (260 vs 256 MiB) — negligible. The tradeoff is throughput: flatfs is ~4–6× faster on large-blob write/read because it's raw file IO with no anyenc encode + btree insert + WAL. any-store buys: one file (trivial `rm` teardown, no inode pressure — matters on mobile), transactional bytes+index in one commit, per-page XXH3-128 integrity (<1% write cost), optional at-rest DB encryption.

4. **The fileId-prefix hypothesis is confirmed on both axes.** Monotonic keys fill btree pages ~100% and avoid the random-insert page splits/fragmentation that 60-char random CID keys cause:
   - **Writes:** 1.4–2.6× faster than cid-key across every size (e.g. 863 vs 499 MB/s at 4 KiB).
   - **Size:** smaller on disk *even while carrying an extra `cid` field* (5.4 vs 6.8 MiB at 200 B) — the random-CID primary key fragments interior nodes; the monotonic key doesn't.
   - **Cost:** you lose direct content-addressed point lookup. Dedup / BlobCheck-by-hash need a **secondary index on the `cid` field**.

5. **S2 compression is moot for blob bytes** — ciphertext is incompressible; S2 gave 0 size win and was slightly slower. Keep `NoCompression` for the blob payload.

## Caveats

- The `has/op` numbers for any-store are inflated: `has()` calls `FindId`, which loads+decodes the whole binary value. In the real design the **index row is tiny and separate from the bytes**, so existence is a small-doc lookup (~1–2 µs), competitive with flatfs's `stat`. Don't read the has-throughput as the index-probe cost.
- Throughput is buffered (both stores). For durability-sensitive write latency, re-run with `CommitSync=true` / flatfs sync-per-put.

## Price of a content-hash secondary index (+ surrogate "allocated" keys)

Once the primary key is fileId-clustered, looking up by content hash (dedup, BlobCheck, P2P "do I hold CID X") needs a **secondary index on `cid`**. Measured by adding `EnsureIndex{Fields:["cid"]}` and comparing the same layout with/without it. `alloc-key` = a compact monotonic surrogate primary key (`b<10d>`, 11 B) with the CID demoted to an indexed field — i.e. "allocate a prefix/id for each blob."

**200-byte blocks, 16 384 docs (3.1 MiB data) — docs pack, so the index is a visible fraction:**

| layout | write MB/s | byCID/op | disk MiB |
|---|---|---|---|
| fileid-prefix, **no cid idx** | **148** | n/a (no by-hash path) | **5.4** |
| fileid-prefix **+ cid idx** | 55 | 3.52 µs | 7.0 |
| alloc-key + cid idx | 54 | 3.60 µs | **6.5** |
| cid-key (content-addressed primary) | 57 | **1.06 µs** | 6.8 |

**512-byte blocks (same shape):** no-idx 10.8 MiB / 324 MB/s; +cid idx 12.3 MiB / 136 MB/s; alloc 12.2 MiB; cid-key 12.5 MiB, byCID 1.25 µs.

**256 KiB blocks (realistic blob size):** no-idx 260.5 MiB / 1262 MB/s; +cid idx 260.6 MiB / 1184 MB/s (only ~6% write hit, index invisible on disk).

### Answers

- **Disk price of the cid index ≈ ~100 bytes/doc, flat.** +1.6 MiB over 16 384 docs at 200 B (5.4 → 7.0), +1.5 MiB at 512 B. It's the CID string + btree entry + back-reference to the primary key; it scales with **doc count, not payload size**, so against 256 KiB blobs it's invisible (260.5 → 260.6).
- **Write price is size-dependent.** Maintaining a random-keyed (CID) index on every insert roughly **halves small-doc write throughput** (148 → 55 at 200 B; 324 → 136 at 512 B), but costs only **~6 % for 256 KiB blobs** — the index work is a shrinking fraction as the payload grows. For real file blobs it's negligible; for a flood of tiny rows it's the dominant cost.
- **byCID via secondary index is ~3× a direct lookup** (≈3.5 µs vs ≈1.1 µs for cid-key's primary-key hit) — the extra hop index→primaryKey→data. Both are microseconds; fine unless by-hash is the hot path.
- **Allocating a compact surrogate key pays off.** `alloc-key` (11 B numeric) is the **smallest** indexed layout (6.5 vs 6.8 cid-key vs 7.0 fileid-prefix at 200 B): every interior btree node and every index back-reference stores the primary key, so a short key shrinks *all* of them. Keep it monotonic (and you can still encode fileId in the high digits to retain clustering).
- **The irreducible cost:** if you must resolve by content hash, exactly **one** structure has to be keyed by the random hash (the cid index, or a `cid→allocId` map — same thing). Surrogate keys shrink everything *else*; they can't make that one structure monotonic. **The real lever is whether by-hash lookup is needed at all** — if every read enters by fileId, drop the cid index and take the cheapest+fastest `fileid-prefix, no idx` row.
- A composite `(fileId, cid)` index only helps queries that constrain `fileId` *and* `cid` together; it does **not** serve cid-only dedup/BlobCheck (those need `cid` leading). Costs more than the single-field `cid` index. Not worth it here.

## Bearing on the SDK2 file design (docs/07-files.md)

- The doc's split (blob **bytes → content-addressed file dir**, **index row → any-store**) is sound for the **large-blob** path: files are faster and content-addressed for free. any-store-for-bytes only wins if we specifically want transactional bytes+index, single-file teardown, or sub-1 KB packing.
- **Micro-payloads (<32 KB inline, per the doc):** sub-1 KB pack great in any-store; the 1–4 KB band hits the overflow penalty (acceptable, still one DB).
- **Per-blob index rows:** make the doc id **fileId-prefixed and monotonic** (mint fileId as a `lexid`) for the write-throughput + page-fill win; add a **secondary index on contentHash** for dedup / BlobCheck-by-hash.
