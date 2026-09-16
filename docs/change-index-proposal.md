# Change Index

`Space.Changes()` (`space.ChangeIndexAPI`) lets a consumer-side indexer
(full-text, vector search) track which objects changed in a space and
re-index incrementally. The indexer lives entirely on the consumer side:
the SDK has no search logic, no index of its own, and stores no cursor.

## API

```go
type ObjectChange struct {
    ObjectId string
    ApplySeq uint64
    Deleted  bool
}

type ChangeIndexAPI interface {
    MaxApplySeq(ctx context.Context) (uint64, error)
    ChangedSince(ctx context.Context, since uint64, limit int) ([]ObjectChange, error)
    Subscribe(cb func(ObjectChange)) (cancel func())
    Generation(ctx context.Context) (string, error)
}
```

- `ChangedSince` — objects whose applySeq exceeds `since`, ascending,
  capped at `limit` (0 = no cap). Page by passing the last returned
  `ApplySeq` as the next `since`.
- `MaxApplySeq` — the current cursor ceiling; 0 when nothing has applied.
- `Subscribe` — fires once per applied change in the space. Runs
  synchronously on the apply path; cancel is idempotent.
- `Generation` — per-space epoch that changes only when the SDK store is
  rebuilt.

## ApplySeq

The cursor axis is `applySeq`: an SDK-owned, per-space, strictly local,
monotonic counter. It advances on every apply that mutates the space's
records: synced DAG changes, the account mirror's applies, and
device-local writes. any-sync's `AddSeq` covers DAG deliveries only, so
cursoring on it would miss non-DAG mutations.

- Allocated inside the apply `WriteTx`, after the write lock is held, so
  allocation order equals commit order: an ascending cursor never skips
  a seq that commits late. Gaps (rolled-back transactions) are normal.
- Stamped on each written record as `_applySeq`, and persisted as the
  object's watermark on its `_meta` row in the same transaction. The
  allocator re-seeds from the highest persisted watermark, so it needs
  no counter row and never regresses.
- Peer-local: persist and compare it as a cursor on this device; never
  send it to another peer.

Records also carry `_addSeq` (any-sync's delivery counter, kept at the
max seen) for the store's own catch-up against any-sync.

## `_meta` rows

The SDK database is shared across spaces by default, so per-object
`_meta` rows carry `sp` (space id) and are indexed on `(sp, as)`. Every
change-index query filters on `sp`. The per-space `space:<spaceId>` row
has no `sp` and is never returned; it holds `Generation`.

| Key   | Meaning                                    |
| ----- | ------------------------------------------ |
| `sp`  | Space id                                   |
| `as`  | Max applySeq of the object                 |
| `q`   | Max AddSeq of the object                   |
| `hv`  | Handler versions                           |
| `del` | Object purged (deletion entry)             |

Any dataset counts: base content, properties, type definitions. The
per-space `objects` collection only moves on property writes, so it is
not a sufficient dirty signal; `_meta` is.

## Deletions

Deleting an object purges its local projection (the SDK keeps no
`objects` tombstone; any-sync's head storage is the durable delete
record). The purge keeps the object's `_meta` row, sets `del`, and
stamps a fresh applySeq greater than its last content change, in the
same transaction. The object then surfaces once as
`ObjectChange{Deleted: true}` in the same ordered stream as edits, from
both `ChangedSince` and `Subscribe`.

Consuming `Deleted` is required for eviction: a deleted object never
re-appears as a content change, and any-sync guarantees no later content
change follows, so per-object consumer state can be dropped.

A deleted record inside a live object is a normal change to that object.
To read the tombstone, query with `ProjectionOpts.IncludeDeleted` on the
find path (`Iter` / `All` / `One` / `Count`): the row comes back with
content wiped and `id`, `_deletedAt`, `_ver`, `_traces`, `_addSeq` kept. `Snapshot` and
`Subscribe` keep skipping tombstones.

## Consumer contract

1. Persist `(Generation, cursor)`.
2. On start, compare `Generation` with the stored value. If it differs,
   or the stored cursor exceeds `MaxApplySeq` (restore from an older
   backup), reset the cursor to 0 and full-reindex from a live
   `QueryObjects` snapshot; deletions are re-established by absence.
3. Catch up with `ChangedSince(cursor, limit)` pages, advancing the
   cursor after each page is indexed.
4. Use `Subscribe` for liveness only. It is best-effort: a dropped event
   (crash, slow callback) is recovered by re-running `ChangedSince` from
   the persisted cursor. It is not a durable queue.

`Subscribe` is gated on having subscribers, so cold restore and catch-up
pay nothing when no indexer is attached.

## Legacy rows

Per-object `_meta` rows without `sp` are not returned until the object's
next change. Rows without `as` are backfilled once per space with
`as := q` at store load, and the allocator
seeds past the result, so a cursor persisted in AddSeq units stays valid.
