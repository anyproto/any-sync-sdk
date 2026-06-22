# Exact inline→overflow crossover in any-store v2 (4 KiB pages)

Sweep of synthetic incompressible payloads, 8000 clustered docs/size, doc shape
`{id:"f########/######" (16 B), data:<binary>}`, NoCompression. Per size: on-disk
allocation per doc, read syscalls/doc on a cold read, and cold-read wall time
fresh vs churn-fragmented (import fillers → delete half → import test into the
holed freelist). DB on ext4; cold via fsync + fadvise(DONTNEED); /proc/self/io.

| payload B | disk/doc | syscr/doc | fresh ms | churn ms | churn× |
|---|---|---|---|---|---|
| 512 | 591 | 0.14 | 9 | 9 | 0.9 |
| 700 | 827 | 0.20 | 10 | 9 | 0.9 |
| 800–940 | **1033** | **0.25** | 11 | 12–13 | ≤1.3 |
| **960** | **4614** | **1.13** | 18 | 23 | 1.3 |
| **980** | 4614 | 1.13 | 18 | **145** | **8.0** |
| 1000–4096 | 4614 | 1.13 | 16–19 | 137–142 | ~8 |
| 8192 | 8710 | 2.13 | 22 | 272 | 12 |

## The crossover

- **Inline capacity ≈ 940 B of payload** for this doc shape. Up to 940 B, docs
  pack several per 4 KiB leaf page (disk/doc ≤ 1033, ~0.25 page reads/doc) and are
  **churn-immune** (≤1.3×).
- **At 960 B the value spills to an overflow chain**: disk/doc jumps **1033 → 4614**
  (one full 4 KiB overflow page + local/leaf share) and syscr/doc crosses **0.25 → 1.13**
  (≥1 page read per doc). This is the SQLite-derived `maxLocal ≈ 1002 B` cell limit:
  crossover payload ≈ `1002 − keyLen(16) − framing(~46) ≈ 940 B`.
- **The churn cliff appears at ~980 B**: once a doc owns overflow pages, freelist
  reuse scatters them, so a cold bulk read becomes random 4 KiB I/O — **~8× slower**
  (flat from 980 B to 4 KiB = one overflow page each; 12× at 8 KiB = two pages).
  The penalty scales with *total overflow pages read randomly*, so it compounds for
  bigger blobs (cf. real 1–100 MiB files: 12× / 1.1 s).

## One-page regime: the cost is per-overflow-page, not a size cliff

A second sweep across 0.8–12 KiB shows the churn cost is **exactly one random
read per overflow page** — flat within a page-count band, stepping at each page
boundary:

| payload | disk/doc | overflow pages | churn µs/doc |
|---|---|---|---|
| ≤ 940 B | 1033 | 0 (inline-packed) | 1.6 |
| **1000 B – 4500 B** | 4614 | **1** | **~17.5 (flat)** |
| 4600–5000 B | 4785–5129 | 1→2 | ~18 |
| 6000–8600 B | 8710 | 2 | ~34 |
| 12000 B | 12806 | 3 | ~56 |

**One overflow page covers payloads up to ~4.5 KiB** (local cell keeps ~minLocal,
the 4 KiB overflow page holds the rest). So anything **≤ ~3.5 KiB fits in a single
page** with headroom — under churn it costs **one** random read (~17 µs), bounded;
it does not compound until a value needs a second page (~6 KiB → 2 reads, ~34 µs).

This makes **< ~3.5 KiB a sound inline-tier threshold**: every micro-payload is at
most one overflow page (one random I/O when fragmented), and storing such values
as individual files instead would be *worse* (one inode + one 4 KiB block each —
the flatfs anti-pattern). any-store is the right home up to the one-page ceiling.

## Rule of thumb

A value lives inline (dense, churn-proof) iff `keyLen + valueLen + ~46 B ≲ 1002 B`,
i.e. **value ≲ ~1000 − keyLen bytes** (~940 B with a 16 B key). Above that, every
value pulls ≥1 overflow page that fragments under churn.

## Implication for SDK2 (docs/07-files.md) — three tiers

Two measured boundaries (4 KiB page): **~940 B** (inline-pack → 1 overflow page)
and **~4.5 KiB** (1 → 2 overflow pages). That gives three storage tiers:

1. **< ~900 B → inline, packed.** Many docs per leaf page, churn-immune,
   ~1.6 µs/doc. Index rows, blurhash, tiny metadata.
2. **~900 B – ~3.5 KiB → inline, one overflow page.** Still the best home (files
   would waste an inode + 4 KiB block each). Bounded cost: one random read
   (~17 µs) when fragmented; storage ~4.6 KiB/doc. **Use ~3.5 KiB as the micro-
   payload inline ceiling** — safely one page.
3. **> ~3.5 KiB → contiguous content-addressed file.** Multi-page overflow whose
   churn read cost compounds with size (cf. real 1–100 MiB files: 12× / 1.1 s).
   A contiguous file is immutable → never fragments → flat fast read.

The doc's 32 KiB inline threshold is well into tier 3: a 32 KiB inline payload =
~8 overflow pages = ~8 random reads when fragmented. Lowering the inline ceiling
to **~3.5 KiB** keeps every inline value to a single page, and sends anything
larger to a contiguous file where read-back stays flat.
