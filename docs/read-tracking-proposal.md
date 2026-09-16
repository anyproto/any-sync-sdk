# Read Tracking (read/unread changes)

Tracks which changes of an object the account has read. Opt-in per
dataset; the first consumer is chat (the `any` app's `chat_messages`
dataset). Consumers use `Space.ReadState()` (`space.ReadStateAPI`).

## Model

Read state per tracked object is a **seen-heads frontier**: a cut of the
change DAG. A change is read iff it is in the frontier or an ancestor of
it. Each object has one unread set; message, mention and reaction
counts are tags on its entries, not separate frontiers.

- **Forward-only.** `MarkRead(objectId, changeIds)` marks the named
  changes and their entire causal past read. There is no mark-unread.
- **Self-authored changes are born read.** Authorship compares account
  identity, not peerId, so all of the account's devices agree.
- **Marks name arbitrary changes**, not just tree heads: the UI marks
  what it displayed, including concurrent branches inside the viewed
  range, so devices converge even though their local display orders
  differ.
- **Private.** Read state syncs only between the account's own devices
  (through the tech space) and is never visible to other members. There
  are no read receipts.

A frontier replaces the "last read message" cursor that server-ordered
messengers use: without a server-assigned total order, a late change can
land below already-read messages, and it must still count as unread.

## Registration and classification

A dataset opts in through `handler.Dataset.ReadTracking`:

```go
ReadTracking: &handler.ReadTracking{
    Classify:      func(ctx *ChangeCtx, rec *RecordChange) ReadClassification,
    Seed:          handler.ReadSeedAtFirstSight, // or ReadSeedAllUnread
    CounterFields: map[string]string{"message": "unreadCount"},  // optional
    RecordFlags:   map[string]string{"message": "unread"},       // optional
}

type ReadClassification struct {
    Track    bool
    Tags     []string     // e.g. "message", "mention", "reaction"
    Key      string       // optional supersede key
    Audience query.Filter // optional audience restriction
}
```

Classification runs inside the apply pipeline, under the tree lock, in
the same transaction as the change. It runs after the record loop, so
`ChangeCtx.Before` is nil. Local and injected changes are not
classified. Per applied change on a tracked dataset:

- Self-authored or already covered by the frontier: read, no entry. The
  classifier still runs, so a `Key` can clear an entry another device
  tracked.
- Otherwise, if `Track` is true: insert an unread entry with the tags
  and bump the per-tag counters.

**Supersede key.** A new entry with the same key replaces the previous
one; `Track=false` with a key clears it. One rule covers reaction
toggles (react then un-react leaves nothing; key
`reaction:<emoji>:<account>:<recordId>`) and edit-replaces-edit. One key
per change: the first record's key wins.

**Audience.** `Audience` is a typed any-store filter matched against the
target record after the change applies (one in-transaction point read).
The entry tracks only on replicas where the record matches; elsewhere
the change applies untracked, and the key still supersedes or clears. A
missing record tracks for nobody. Classifiers build identity-relative
filters from `ChangeCtx.SelfIdentity` (set only on this path; handler
validation stays replica-independent), e.g. a reaction counts only for
the reacted-to message's author (`creator == SelfIdentity`). The filter
must read only fields immutable after create, or replays diverge.

`Audience` gates the whole entry, every tag. When only part of the
verdict depends on record state (a "mention" tag next to an
unconditional "message" tag), the classifier point-reads the post-apply
record via `ChangeCtx.Get` + `ChangeCtx.RecordId`. Verdicts are
device-local and incremental replays resume from the persisted
watermark, so post-apply state at re-classification matches the
original run.

**Rejections never classify.** The hook narrows each record change to
the ops that landed (`ApplyResult.Rejections`). A rejected delete must
not wipe the victim's unread entries; a rejected edit must not create or
clear keyed entries. Whole-record rejections skip the record; a
partially salvaged multi-field op is dropped from classification (a
rejection can only suppress tracking, never forge it).

**Record deletion** clears every unread entry referencing the deleted
records in the same transaction; counters and flags drop and the object
shows up on the feed.

## Storage

Local, per space, in the space DB:

- `_read_unread`: one row per unread change: object, dataset, changeId,
  versionId, addSeq, applySeq, recordIds, tags, supersede key, prevIds,
  stateSeq. The row is deleted when the change is read. `prevIds` is
  copied from the change at apply time, so the closure walk usually
  needs no tree access.
- `_read_state`: one row per tracked object: frontier heads, pending
  remote heads, per-tag counters, last stateSeq, seeded bit. Counters are
  maintained transactionally, so reading them is O(1).

Pending remote heads are frontier heads from another device whose change
has not arrived locally yet. They persist, so a crash between the KV
update and tree sync loses nothing, and resolve when the change applies.

Synced, cross-device: the tech-space key-value store, one key per
tracked object, `read/<spaceId>/<objectId>`, value `{"h": [heads]}`.
any-sync stores one row per (key, peerId), LWW on the writer timestamp,
so devices never overwrite each other; the logical frontier is the union
of all rows. The tech space keeps positions private: a target-space KV
value is decryptable by every member. KV rather than a carrier tree,
because read marks are frequent and KV keeps no history.

The engine holds no locks and no in-memory state. Callers serialize per
object: the apply path through the tree lock, marks and merges through a
per-object mutex in the sync service.

## Marking

`MarkRead` in one transaction: an indexed scan of the object's unread
rows, a breadth-first walk over `prevIds` from the marked ids, delete
covered rows, adjust counters, advance the frontier and stateSeq. Gaps in
the walk (read, self-authored or untracked changes between unread ones)
resolve through a point lookup in any-sync's change storage, never a
tree load. The walk stops below the object's minimum unread versionId:
an ancestor's versionId is always smaller than its descendant's, so
nothing unread lies below that line.

`MarkReadUpTo(objectId, upTo)` covers every unread change with
`versionId <= upTo` (`""` = all) as a range, with no walk. It resolves to
explicit changes, so the synced frontier stays id-based even though
versionIds are peer-local. It commits in chunks of 2048 entries (any
versionId prefix is a valid forward-only advance, so a crash mid-way
resumes) and publishes the frontier once at the end.

After a local mark commits, the device publishes its frontier to its KV
row.

Bounds:

- The frontier is capped at 64 heads; over the cap, the oldest by local
  versionId drop first. Locally safe: the frontier is only a classify
  shortcut and a walk stop. A dropped head that was not an ancestor of
  the rest costs other devices a bounded re-read, never corruption.
- A first-sight object's initial restore skips tracking entirely (see
  Seeding).

## Delivery

Same consumption contract as `space.ChangeIndexAPI`: `Subscribe` for
liveness, `ChangedSince` from a persisted cursor for durability,
`Generation` for epoch resets.

```go
type ReadStateAPI interface {
    Subscribe(cb func(objectId string, stateSeq uint64)) (cancel func())
    ChangedSince(ctx context.Context, since uint64, limit int) ([]ObjectReadState, error)
    UnreadSnapshot(ctx context.Context, objectId string) ([]UnreadChange, uint64, error)
    UnreadCounts(ctx context.Context, objectId string) (map[string]int, error) // per tag
    MarkRead(ctx context.Context, objectId string, changeIds []string) error
    MarkReadUpTo(ctx context.Context, objectId string, upTo crdt.VersionId) error
    Generation(ctx context.Context) (string, error)
}
```

The feed lists dirty objects, not per-change transitions. Reads delete
state, so itemizing them would need an append-only log with retention.
Consumers already hold their rendered set, so "this object changed,
re-pull `UnreadSnapshot` and diff" carries the same information, and the
feed never grows or needs pruning. A stale cursor just returns more
dirty objects; only a `Generation` change forces a full resync.

`StateSeq` shares the per-space applySeq axis: a transition caused by an
apply reuses that apply's ApplySeq; marks and merges allocate from the
same allocator.

A record-creating change's `versionId` equals the record's `_ver.id`, so
unread entries join to records with no extra lookup.

Errors: `ErrReadTrackingDisabled` when no dataset in the space opted in;
`ErrSpaceNotTracked` from marks when the space is unknown, deleted or
pending on this device.

**Chat sugar** (`any` side): `ReadAll()` → `MarkReadUpTo(chatId, "")`;
`Read(messageId)` → `MarkReadUpTo` with the message's `_ver.id`. any-sync
orderIds respect causality (a late-arriving old branch is slotted into
order, not appended), so the cutoff matches what the UI rendered above
the message. An unread reaction or edit written after the message has a
larger versionId and stays unread; a client that renders reactions on
visible messages clears those via `MarkRead` with ids from
`UnreadSnapshot`.

## Materialization

Two optional projections of the unread entries into local-scope fields,
written through the normal `LocalSet` route, so they ride the regular
query and subscribe engine:

- **Counters** (`CounterFields`, tag → property on the object's row in
  the shared `objects` dataset, e.g. `"message" → "unreadCount"`). A chat
  list sorts and filters on unread with no extra calls.
- **Record flags** (`RecordFlags`, tag → local-scope bool field on the
  dataset's records, e.g. `"mention" → "unreadMention"`). A flag is true
  iff at least one unread entry with that tag references the record. The
  fields must be declared local-scope; the handler declares indexes on
  them, so `Filter(unread == true)` is index-backed.

A materializer worker listens to state pings, debounces 50 ms, coalesces
per object, recomputes desired values from the unread entries and diffs
against stored values. A dropped ping self-heals on the next one. Local
writes don't ping, so there is no feedback loop.

Whether an edit re-marks a message unread is the classifier's call; the
flag rule has no special cases.

## Behavior

| Scenario | Result |
|---|---|
| Late message lands mid-history | Not an ancestor of the frontier → unread, even below read messages |
| Edit of a read message | Untracked (chat) → stays read |
| Edit of an unread message | Creating change still uncovered → stays unread |
| Delete of an unread message | Entries cleared in the same transaction |
| Reaction then un-reaction, unseen | Supersede key clears the entry |
| Mention | `mention` tag on the same unread set; per-tag counts |
| Cross-device | Per-device KV rows, union-merged |

Mid-history unreads mean a chat can have several unread regions instead
of a single "New messages" divider. No extra API is needed: "first
unread" is `Filter(unread).Sort(_ver.id).Limit(1)`, "N unread above/below"
are two indexed counts, and a live subscription delivers an `updated`
frame when a record's flag flips.

A messenger UI touches only the flag fields on records, the counter
properties on the chat row, and `Read(msg)` / `ReadAll()`. The feed is
for indexer-style consumers.

## Cross-device sync

A remote mark updates that device's KV row → head-sync or broadcast
delivers it → the KV hook hands it to a single worker → merge.

**A merge never loads the object.** Each incoming head either has an
unread row (closure over rows), or is already read, or has not synced
yet; a point lookup in any-sync's change storage distinguishes the last
two, and an unsynced head is stored as pending. Merges serialize against
marks through the per-object mutex and commit in their own transaction.

The KV hook is best-effort, so durability comes from a boot reconcile
(`ReconcileAll`): one pass over the tech-space KV store replays every
published frontier through the same idempotent merge (one state read per
already-merged object), then republishes this device's frontier where
its published row diverges from the local one (a publish that failed
after its mark committed). It also covers rows that arrived while an
object was unloaded.

When a space is removed, the sync service publishes a deletion watermark
for the space's `read/<spaceId>/` key prefix, dropping its frontiers on
every replica. Joining again re-seeds.

## Seeding

On an object's first tracked load, the seed checks the account's
published frontiers first (tech-space KV usually syncs before chat
trees):

- **Frontiers published:** the restore tracks history normally and the
  published frontiers merge in, so a fresh device lands on the account's
  real read state. Costs insert-then-cover bookkeeping for the history,
  once per object per fresh device.
- **Nothing published, `ReadSeedAtFirstSight` (default):** the frontier
  becomes the tree heads, everything present starts read, and tracking
  is skipped during the restore. A seeded bit written in the seed's own
  transaction makes a crash on either side of the restore re-seed
  instead of skipping.
- **`ReadSeedAllUnread`:** empty frontier; the whole tracked history is
  unread.

An object loading before its KV key syncs falls back to first-sight and
diverges until the next mark. Booting the tech space first keeps this
window small.

## Performance

The engine scales with unread count U and is independent of tree size
N. The alternative, any-sync's `objecttree.DiffManager`, must
materialize the whole tree: on a synthetic 100k-change DAG a cold build
costs 27 ms CPU and 58 MB per object before storage I/O and decryption,
and it stays resident per open object. Here closed objects cost no
memory, idle objects cost nothing, and chat-list badges are ordinary
fields on rows the list query already reads.

Measured on the engine (linear chat-shaped DAGs, per-op transactions
with real commits, Ryzen 9950X):

| Scenario | Cost |
|---|---|
| Inbound tracked message, marginal cost inside the apply transaction | 7.5 µs |
| Inbound tracked message paying its own commit (worst case) | 51 µs (fsync floor) |
| Self-authored message (frontier advance, no row) | 13 µs |
| ReadAll, U = 10 / 100 / 1 000 / 10 000 | 121 µs / 764 µs / 8.6 ms / 93 ms |
| Partial mark: 500 of 1 000 unread by versionId cutoff | 4.3 ms |
| Boot reconcile per chat, nothing new | 2.3 µs (1 000 idle chats ≈ 2.4 ms) |
| Per-tag counters from the engine | 1.6 µs per chat |

## Non-goals

- Mark-unread.
- Read receipts or any other exposure of read positions to the space.
- Read-state hooks on the generic object surface: the service attaches
  to the apply pipeline and the KV hook; `internal/object` knows nothing
  about read tracking.
- Resident per-object diff structures.

## Open questions

- **Object deletion cleanup.** Deleting an object clears nothing in
  `_read_unread` / `_read_state` and leaves its `read/` KV key; only
  space removal prunes keys.
- **KV key privacy.** Keys are visible to nodes (values are encrypted).
  Object ids are already visible to nodes through trees; confirm no
  hashing is needed.
