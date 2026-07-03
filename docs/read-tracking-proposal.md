# Read Tracking (read/unread changes)

Status: proposal. First consumer: chat (`any` app, `chat_messages`
dataset); the mechanism itself is generic and opt-in per dataset.

## Model

Read state per tracked object is a single **seen-heads frontier** — a
cut of the change DAG. A change is read iff it is an ancestor of (or
member of) the frontier. One common unread set per object; there are
no per-counter-type frontiers (heart's `messages` / `mentions` /
`reactions` split is replaced by tags on unread entries, see
Classification).

- **Forward-only.** `MarkRead(objectId, changeIds)` moves the frontier
  via ancestor closure: the named changes and their entire causal past
  become read. There is no mark-unread operation.
- **Self-authored changes are born read** — compared by identity
  (accountId), not peerId, so all of the account's devices agree.
- Marking accepts arbitrary change ids (what the UI actually
  displayed), not just tree heads — concurrent branches inside the
  viewed range are marked explicitly, so devices converge even though
  their local display orders differ.

## Storage

**Synced (cross-device): tech-space KV.** One key per tracked object,
`read/<objectId>`, value = the frontier (change ids). any-sync's
keyvaluestorage gives per-device rows for free (`key + "-" + peerId`,
LWW per device on writer timestamp, devices never overwrite each
other); the logical frontier is the union of all device rows, merged
by feeding each into the closure. Tech space (not the target space's
own KV) because read positions are private: a target-space KV value is
decryptable by every member. KV, not an account-values carrier,
because read markers are written constantly and KV is LWW with no
history — a carrier tree's DAG would grow forever.

**Local (per space, same DB/tx discipline as apply):**

- `unread` rows: `{objectId, changeId, versionId, addSeq, applySeq,
  recordIds, tags, prevIds}`. A row exists while the change is unread.
  `prevIds` is copied from the any-sync change at apply time so the
  ancestor closure never needs the tree.
- frontier mirror per object: `{objectId, heads, stateSeq}` — the
  local materialization of the KV union.
- counters per `(objectId, tag)` — maintained transactionally
  (increment on unread insert, decrement on closure removal), so
  counter reads are O(1) with no tree or row scan.
- NO transitions log: the feed is dirty-OBJECT, read straight off the
  per-object state rows (stateSeq watermark, `(sp, ss)`-indexed) — it
  never grows and never needs pruning. Consumers re-pull the object's
  unread snapshot and diff against what they hold.
- pending remote heads: KV heads not yet present in the local tree
  (`notFound`), persisted so a crash between KV arrival and tree sync
  loses nothing; resolved when the change applies.

## Classification (the "enrich incoming changes" step)

Runs inside the existing apply pipeline, under the tree lock, in the
same transaction as the change itself — crash-recoverable at any line,
no extra locks (heart interleaved tree lock / subscription lock /
store tx; we add nothing).

A dataset opts in at registration (`handler.Dataset`):

```go
ReadTracking: &handler.ReadTracking{
    // Classify runs per applied record change. Track=false skips the
    // change (edits, typing indicators, plain deletes). Tags label
    // the unread entry: chat returns "message", "mention" (text
    // mentions me), "reaction" (op under reactions.*). Key is an
    // optional supersede key: a new unread entry with the same key
    // REPLACES the previous one, and Track=false with a Key CLEARS
    // it — one rule covers reaction toggles (react → un-react leaves
    // nothing: key "reaction:<emoji>:<account>:<recordId>") and, if a
    // consumer ever tracks edits, edit-replaces-prior-unread-edit.
    Classify func(ctx ChangeCtx, rec RecordChange) Classification
    Seed     SeedMode // SeedReadAtJoin (default) | SeedAllUnread
}

type Classification struct {
    Track bool
    Tags  []string
    Key   string // optional supersede key
}
```

Per applied change on a tracked dataset: self-authored or covered by
the frontier → read (no row); otherwise insert an unread row with the
classifier's tags and bump counters. Untracked datasets pay nothing.

**Record deletion** clears every unread row referencing the deleted
recordIds in the same tx (counters and flags drop, the object surfaces
on the dirty feed) — the delete change itself is typically
not tracked; a deleted unread message must simply stop counting.
Whole-object deletion purges the object's readstate rows and its KV
key alongside the SYN-20 purge.

## Closure engine: any-store rows, not objecttree.DiffManager

any-sync ships `objecttree.DiffManager` (heart's engine). We do not
use it. Its real price is materializing the whole tree in memory;
benchmarked on a synthetic 100k-change DAG (~5% forks/merges,
CID-length ids, Ryzen 9950X, any-store v0.4.7):

| scenario | U=100 | U=1k | U=10k |
|---|---|---|---|
| DiffManager cold build (CPU only, tree already loaded/decrypted) | 27.4 ms / 58 MB | — | — |
| DiffManager cold build + mark all | 53.1 ms / 89 MB | — | — |
| DiffManager steady-state mark (resident in memory) | 12 µs | 128 µs | 1.7 ms |
| **rows: indexed scan of unread rows + in-memory walk** | **32 µs** | **272 µs** | **3.0 ms** |
| rows: FindId-per-node BFS (rejected) | 273 µs | 2.6 ms | 28 ms |
| rows: persist mark (tx: delete U rows + frontier upsert + commit) | 0.97 ms | 10 ms | 93 ms |
| changeId→row point lookup in a 100k-row collection | 3.9 µs | — | — |

U = unread count. The row engine scales with U and is independent of
tree size N; it is within ~2× of a *resident* DiffManager in absolute
microseconds, while never paying the cold build — which at N=100k
costs 27 ms CPU + 58 MB *before* storage I/O and decryption, per
object, and heart keeps that resident per open chat forever. Mark
implementation: one indexed range scan of the object's unread rows →
in-memory BFS over `prevIds` from the marked ids → delete covered
rows, adjust counters, advance frontier and its watermark — one tx.
The persist cost is fsync-bound (~1 ms floor), same budget as any
apply.

Bench source: scratchpad `readbench` module (can be committed under
`internal/readstate/` as a regression bench when implementation
starts).

## Delivery: dirty ping + cursor pull (mirrors ChangeIndexAPI)

Same contract as `space.ChangeIndexAPI`, which the `any` app already
consumes in its indexer (Subscribe for liveness, `ChangedSince` from a
persisted cursor for durability, `Generation` for epoch resets):

The feed is dirty-OBJECT, not per-change: reads delete their state, so
itemizing them would require an append-only log with retention policy
and pruned-cursor edge cases. Since consumers hold their rendered set
anyway (a UI its window, an indexer its flags), "this object changed,
re-pull and diff" carries the same information with zero growth —
exactly how the change-index feed already works. New-unread stays
itemizable for free (live unread rows carry their stateSeq).

```go
type ObjectReadState struct {
    ObjectId string
    StateSeq uint64 // cursor axis — persist the last seen value
}

type UnreadChange struct { // snapshot element
    ObjectId  string
    Dataset   string
    ChangeId  string
    VersionId crdt.VersionId // consumer's local sort/join key (== _ver.id for created records)
    AddSeq    uint64
    ApplySeq  uint64
    RecordIds []string
    Tags      []string
    StateSeq  uint64
}

type ReadStateAPI interface {
    Subscribe(cb func(objectId string, stateSeq uint64)) (cancel func())
    ChangedSince(ctx context.Context, since uint64, limit int) ([]ObjectReadState, error)
    UnreadSnapshot(ctx context.Context, objectId string) ([]UnreadChange, uint64, error)
    UnreadCounts(ctx context.Context, objectId string) (map[string]int, error) // per tag
    MarkRead(ctx context.Context, objectId string, changeIds []string) error
    // MarkReadUpTo marks every unread change with versionId <= upTo
    // ("this and everything before" in local display order). Sugar
    // over MarkRead: resolves the cutoff to the explicit set of
    // unread changeIds (indexed range query on the unread rows), so
    // the synced frontier stays id-based and devices converge even
    // though versionIds are peer-local. upTo == "" means read all.
    MarkReadUpTo(ctx context.Context, objectId string, upTo crdt.VersionId) error
    Generation(ctx context.Context) (string, error)
}

App-facing chat sugar (`any` side) maps 1:1: `ReadAll()` →
`MarkReadUpTo(chatId, "")`; `Read(messageId)` → resolve the message's
`_ver.id` (== its creating change's versionId) and `MarkReadUpTo` with
it. versionId order is safe as the cutoff axis because any-sync's
orderIds respect causality — a late-arriving old branch is slotted
into order, not appended — so the cutoff matches what the UI actually
rendered above the message. Edge case: an unread reaction/edit change
that *targets* a message at-or-below the cutoff but was written after
it has versionId > upTo and stays unread — correct by default (the
user hasn't seen it); a client that renders reactions on visible
messages can clear those explicitly via MarkRead with the row ids from
UnreadSnapshot.
```

`StateSeq` comes off the per-space applySeq allocator: transitions
caused by an apply reuse that apply's ApplySeq; transitions caused by
a KV merge or local MarkRead allocate fresh from the same axis. One
monotonic cursor, one Generation story. Nothing prunes and nothing can
fall off: the feed reads per-object state rows, so a stale cursor just
returns more dirty objects; only a Generation change forces the full
`UnreadSnapshot` resync.

A record-creating change's `versionId` equals the record's `_ver.id`,
so chat joins transitions to messages (ordered by `_ver.id`) with no
extra lookup.

## Materialization (opt-in, tag-driven)

Both materializations are projections of the same unread rows/tags,
written through the existing `LocalSet` route (local-scope fields:
handler-exclusive, device-local, re-derived per device from the synced
frontier, and they flow through the normal query/subscribe engine).

**1. Object counters (chat list UI).** Registration declares
`CounterFields map[string]string` (tag → local-scope property on the
object's `objects` row, e.g. `"message" → "unreadCount"`,
`"mention" → "unreadMentions"`, `"reaction" → "unreadReactions"`).
Counters are durable per `(objectId, tag)`; after a transition batch
commits the service debounce-writes the changed ones. A chat list
sorts/filters on unread with zero extra plumbing. (An app can instead
do this itself: transition ping → `UnreadCounts` → `LocalSet`.)

**2. Per-record flags (message list filtering).** Registration
declares `RecordFlags map[string]string` (tag → local-scope bool field
on the dataset's records, e.g. `"message" → "unread"`,
`"mention" → "unreadMention"`, `"reaction" → "unreadReactions"`).
Invariant: flag is true ⇔ at least one unread row with that tag
references the record. Unread rows already carry `recordIds`; the
closure knows exactly which rows it removed, so it re-checks only the
affected `(recordId, tag)` pairs (indexed query on the unread rows)
and flips flags via `LocalSet` on the tracked object itself — same
tree-lock section, so subscribers see `updated` events with flag flips
in apply order. Whether an edit re-marks a message unread is the
classifier's call (tag the edit change "message" or not) — the flag
rule itself never special-cases. The handler declares any-store
indexes on these fields (chat already declares `idx_ver_id` the same
way), so `Filter(unread == true)` and "unread mentions only" queries
are index-backed.

## Messenger semantics vs Slack / Telegram

|  | Slack | Telegram | here |
|---|---|---|---|
| Read-state shape | per-channel `last_read` timestamp cursor | per-chat max-read message id | seen-heads frontier (a DAG cut) |
| Why it works | server assigns total order; nothing appears behind the cursor | same | no server order exists; the frontier is the cursor generalized to a DAG |
| Late message lands mid-history | near-impossible (server ts) | impossible (server ids) | expected; not an ancestor of the frontier → unread automatically, even below read messages |
| Edit of a read message | stays read, shows "(edited)" | stays read | edit changes untracked → stays read |
| Edit of an unread message | stays unread | stays unread | creating change still uncovered → stays unread |
| Delete of an unread message | counter drops | counter drops | rows cleared on record delete, same tx |
| Reaction then un-reaction, unseen | nothing left | nothing left | supersede key clears the entry |
| Mention badge | separate badge, same read cursor | separate counter | `mention` tag on the same unread set — one frontier, per-tag counts |
| Read receipts (sender sees reader) | none | double-check via public read state | **none — decided**: read state is private (tech space, invisible to other members), Slack semantics. Not a v1 deferral; nothing in this mechanism may ever leak read positions to the space |
| Cross-device read sync | via server | via server | per-device KV rows, union-merged |

Mid-history unreads change one piece of UI vocabulary: Slack's single
"New messages" divider becomes possible-multiple unread regions. The
client needs no new API for it — `unread == true` is an indexed record
filter, so "first unread" is `Filter(unread).Sort(_ver.id).Limit(1)`,
"N unread above/below the viewport" are two indexed counts against the
current window boundary, and the live subscription delivers an
`updated` frame when a mid-history record flips its flag.

What a messenger client actually touches (the transparent-API test):
records with `unread` / `unreadMention` / `unreadReactions` fields
riding the normal query/subscribe flow, `unreadCount` /
`unreadMentions` / `unreadReactions` on the chat's object row for the
chat list, and `Read(msg)` / `ReadAll()`. Counters, flags, frontier,
KV, closure — all invisible. The transitions feed exists only for
indexer-style consumers; a UI never needs it.

## Cross-device flow

Remote device marks read → its per-device KV row updates → head-sync /
broadcast lands it locally → KV `Indexer` hook pings the service →
merge (closure over unread rows), transitions + counters update,
subscribers pinged.

**A merge never loads the object.** Each incoming head either has an
unread row (→ closure over rows), or it doesn't — then it is either
already read or not yet synced, distinguished by a point lookup in
any-sync's changes collection (3.9 µs benched), and stored as a
pending head if absent. Heart's KV callback instead takes the
ObjectTree lock, forcing the chat resident. Serialization: a per-object
mutex in the readstate service orders merges against MarkRead; the
apply-time classification path is already serialized by the tree lock
it runs under, and both routes commit through the same space DB, so
row state stays consistent. The Indexer is best-effort
(errors only logged), so durability comes from a startup reconcile:
compare each tracked key's per-device KV values against a persisted
last-merged stamp and replay the difference (same shape as the SYN-20
deletion reconcile). The reconcile also covers KV rows that arrived
while the object was untracked or unloaded.

## Seeding

On first track of an object with no stored frontier: default
`SeedReadAtJoin` — everything up to the account's ACL join point is
read (a fresh joiner starts clean; heart does the equivalent with a
bespoke tree hook). `SeedAllUnread` for datasets where full history
matters. Both are one-time closure computations at registration.

## Performance model: many big chats

Heart's costs are structural: read state is derived from tree
topology, so nearly every operation forces the tree resident — and it
keeps *three* DiffManagers (messages / mentions / reactions) per open
chat, each built by iterating the tree. Every cost below that scales
with tree size N here scales with unread count U (or is O(1)) in this
design. N = changes in the chat, C = number of chats.

| operation | heart | here |
|---|---|---|
| chat list with unread badges (startup) | per chat: load tree + decrypt, build 3 DiffManagers, KV Get per device, `Count(read==false)` — O(C × N) | the list query the client already runs returns materialized counter properties on the object rows — one indexed query, O(list) |
| idle chat, nothing changed | resident DiffManagers + subscription manager + parent-updater goroutine per open chat | zero: no resident state, no goroutines per object |
| open a big chat | 3 × tree iteration for DiffManagers (27 ms CPU + 58 MB per manager at N=100k, before storage I/O) | nothing read-state-specific; flags are ordinary record fields already in the rows |
| incoming message | DiffManager.Add + flag write + counter update + subscription event | one unread row insert + counter bump inside the apply tx it already pays |
| mark read (U marked) | Remove on resident differ, O(U) — but only if the chat is open | indexed scan + walk: 32 µs (U=100) … 3 ms (U=10k); persist ~1 ms fsync floor |
| KV merge for a *closed* chat | load tree + build DiffManagers under the ObjectTree lock, O(N) | closure over unread rows + 3.9 µs point lookups for pending heads; object stays closed |
| startup reconcile | init per chat regardless of activity | gated on per-device KV stamps — untouched chats cost one stamp compare, no rows read |
| memory per chat (open / closed) | O(N) / 0 (but reopening pays O(N) again) | 0 / 0 |

### Measured (shipped engine)

Scenario benchmarks against the real `internal/readstate` engine
(linear chat-shaped DAGs, per-op transactions with real commits, Ryzen
9950X; bench source is throwaway — rebuild from this table's shapes if
needed):

| scenario | cost |
|---|---|
| inbound tracked message, marginal cost inside the apply tx (batched) | 7.5 µs |
| inbound tracked message when it pays its own commit (worst case) | 51 µs (fsync floor) |
| self-authored message (frontier advance, no row) | 13 µs |
| ReadAll, U = 10 / 100 / 1 000 / 10 000 unread | 121 µs / 764 µs / 8.6 ms / 93 ms |
| partial mark: 500 of 1 000 unread by versionId cutoff | 4.3 ms |
| boot reconcile per chat, nothing new (fast path, one state read) | 2.3 µs → 1 000 idle chats ≈ 2.4 ms |
| per-tag counters read from the engine | 1.6 µs per chat (the chat list itself reads the materialized row properties — no engine call at all) |

The two numbers that define UX: a 1 000-chat account boots its read
state in ~2 ms, and the everyday "open chat, read all" on a hundred
unread costs under a millisecond including the commit. The 10 k-unread
ReadAll (93 ms, one tx) is the chunking case below.

Three guards for pathological sizes, all implemented: `ReadAll` over a
huge unread set commits in bounded chunks of 2048 (forward-only
marking makes any versionId prefix a valid frontier advance, so a
crash mid-way just resumes, and the frontier publishes once at the
end); the frontier itself is capped at 64 members (over the cap, the
oldest by local versionId drop first — locally safe since coverage of
applied changes is the watermark's job, and the published-coverage
trade-off costs another device at most a bounded re-read); and a
first-sight object's initial restore skips tracking entirely (the seed
covers everything present), so a fresh joiner pays no per-change
bookkeeping for pre-join history.

## What is deliberately absent (heart lessons)

- No second source of truth reconciled by hooks. Heart's per-message
  booleans are what its counters are *counted from*, kept in sync with
  seen-heads only via the onRemove hook chain (and distrusted — it
  falls back to full Count reloads). Here unread rows + counters are
  the single local source, derived from the frontier in the same tx;
  record flags and counter properties are opt-in *projections* of that
  source, recomputable from it at any time, never read back as input.
- No mark-unread (heart pays a full DiffManager teardown + tree walk
  for it, and only for one of its three counter types).
- Nothing on the generic `Object`/`source` surface: the service hooks
  the existing apply pipeline and KV Indexer; `internal/object` stays
  read-tracking-free.
- No resident per-object diff structures; memory cost is zero for
  closed objects.

## Open questions

- Whether `subscribe.Event` should also carry the unread flag inline
  for live-query consumers, or the transition feed stays the only
  surface (start: feed-only).

- KV key privacy: keys are visible to nodes (values encrypted).
  objectIds are already visible to nodes via trees; confirm no
  hashing needed (heart hashes with an ACL-salt).
