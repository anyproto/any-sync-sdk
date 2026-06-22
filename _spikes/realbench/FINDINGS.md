# Real-data disk-I/O: flatfs vs any-store, with the RIGHT workload

Source: a real anytype flatfs (`A6GL…`), 70 366 blocks / 5.4 GiB. `realbench`
reconstructs files from dag-pb links and reads them **cold** (`fsync` +
`posix_fadvise(DONTNEED)`), accounting device reads + read syscalls via
`/proc/self/io`. Staging dbs live on **ext4** (a `/tmp` tmpfs reports
`read_bytes=0` — no block device).

**Workload note (decisive):** the few huge files are **write-once archives,
read-never**. The hot path is **small files**. This dataset is **bimodal** —
~3850 reconstructed files are tiny dag nodes (<4 KiB) and ~61 are >1 MiB
archives, with little in between (74 % of all *blocks* are <1 KiB). So
"small-file reads" here = the tiny-node regime.

## Hot path — cold read of the small-file set

### Tiny nodes (≤4 KiB files), 3000 files / 20 749 blocks / 2.4 MiB logical

| layout | storage MiB | disk read MiB | syscr | wall |
|---|---|---|---|---|
| **flatfs** | **81.1** | **81.0** | **41 501** | **1 409 ms** |
| filedir (one file/logical file) | 11.8 | 11.7 | 6 002 | 92 ms |
| **anystore** (block/doc, clustered) | **3.6** | **3.6** | **935** | **26 ms** |
| anystore — churn-fragmented | 3.9 | 3.9 | 999 | 26 ms |

**any-store vs flatfs on the hot path: 22× less disk space, 22× less disk read, 44× fewer read syscalls, ~54× faster cold.**

Why flatfs is so bad here: every block is its own file, min one 4 KiB filesystem
block. 20 749 sub-KiB blocks → **81 MiB on disk for 2.4 MiB of data (33×)**, and
a cold read = 20 749 scattered file opens (≈2 syscalls each) → 41 k syscalls,
1.4 s. any-store packs many sub-KiB docs per 4 KiB btree page, so a cold read
touches ~935 pages, not 20 749 inodes.

`filedir` (the SDK2 `blocks/<hash>` model, but one file *per logical file*) sits
between: 3000 files instead of 20 749, so 7× fewer opens than flatfs — but still
pays per-file 4 KiB rounding (11.8 MiB) and per-file opens (6 k syscalls).

### Churn does NOT hurt small-file reads

`anystore-churn` (import fillers → delete a scattered half → import the test set
into the holed freelist) is **within 8 % of fresh** (3.9 vs 3.6 MiB, identical
26 ms). The freelist-overflow fragmentation that threatens big values **doesn't
apply to small packed docs** — they live inline in leaf pages, not in 4 KiB
overflow chains. And the only things that *do* use overflow chains (big blobs)
are the read-never archives. **So fragmentation is a non-issue for this workload.**

## Mid files that ARE read (1–100 MiB) — churn BREAKS any-store

59 files / 341 blocks / 247 MiB. These are read (not archives) and big enough
that their blocks use 4 KiB **overflow chains**. Reproduced twice:

| layout | disk read MiB | syscr | wall |
|---|---|---|---|
| flatfs | 248.6 | 685 | 230–243 ms |
| **filedir** (contiguous file/blob) | 247.5 | 120 | **81–85 ms** |
| anystore — **fresh** | 249.5 | 63 876 | **90 ms** |
| anystore — **churn-fragmented** | 249.6 | 63 877 | **1 128 ms (12.5×)** |

Fresh, any-store ties filedir (overflow pages laid down contiguously). After
churn, the **same 63 876 preads become random 4 KiB reads** scattered through the
db by freelist reuse → cold read collapses to **1.1 s**, *worse than flatfs*.
Note disk_MiB is unchanged (~249) — no byte amplification; it's pure seek/IOPS
cost (63 876 random 4 KiB reads ≈ NVMe random-read IOPS ceiling ≈ 1 s). `filedir`
is immutable-contiguous → never fragments → flat 85 ms.

**This is the case that decides the storage threshold:** a file that is *read*
AND whose blocks exceed the inline/overflow size (~1 KiB block) must NOT live in
any-store's overflow chains, because churn turns its reads into IOPS-bound random
I/O. Put its bytes in a contiguous content-addressed file instead.

## Cold path — big files (archives), for reference only

Reading a 695 MiB / 706-block archive cold (read-never in practice): flatfs
697.8 MiB / 1 415 syscr / 0.5–1.2 s; any-store clustered 701 MiB / **179 425
syscr** / 0.23 s. Same bytes; any-store does ~254 `pread`s per 1 MiB blob (one
per 4 KiB overflow page) but, freshly bulk-imported, those pages are contiguous
→ still faster than flatfs's 706 scattered opens. The syscall storm is the
fragility signal (no vacuum to re-compact under churn), but **archives are
write-once / read-never, so it doesn't matter.**

## Verdict for the SDK2 file store — split by the overflow threshold (~1 KiB block)

The crossover is whether a block fits **inline in a btree leaf** (≤ ~1 KiB,
packed) or spills to **4 KiB overflow chains** (> ~1 KiB, fragmentable).

1. **Small / sub-KiB blocks → any-store.** Hot-path read is **~20× smaller on
   disk and ~50× faster cold** than flatfs (packs many docs/page vs one inode +
   4 KiB per block). **Churn-immune** — packed leaf cells don't use overflow
   chains. This is the workload that actually runs; any-store wins it decisively.
2. **Read files with big blocks (1–100 MiB) → contiguous content-addressed
   file** (`blocks/<contentHash>`), NOT any-store. Fresh they tie (~90 ms), but
   under churn any-store's overflow chains fragment → **12× slower (1.1 s),
   IOPS-bound random reads**. The contiguous file is immutable → never fragments.
3. **Huge archives (>100 MiB, read-never) → optimize for storage/write only.**
   Either layout; read latency is irrelevant.
4. **One OS file *per block* (flatfs) is the worst for small files** — per-file
   4 KiB rounding + per-file opens. The win is *packing* (DB), not files. But one
   contiguous file *per whole blob* (filedir) is the best for big read files.

Net: the SDK2 split — **micro-payloads inline in any-store, real blobs as
contiguous content-addressed files** — is the right shape; this data pins the
threshold to the inline/overflow boundary (the doc's 32 KiB cutoff is safely
above it).

Reproduce: `./realbench -flatfs <dir> -small-min B -small-max B -small-n N [-churn]`.
