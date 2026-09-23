# Object

An object is an any-sync object tree: a content-addressed DAG of signed
changes, each linking to its parents and to the ACL state it was written
under, carrying encrypted CRDT data. The SDK hides the DAG. Callers see
objects, datasets, records and fields; trees, heads and sync modes stay
internal.

## any-sync object trees

```go
type Change struct {
    Id          string        // CID of the serialized RawTreeChange
    PreviousIds []string      // parent changes (multi-parent DAG)
    AclHeadId   string        // ACL state at change time
    SnapshotId  string        // snapshot this change builds on
    ReadKeyId   string        // read key used to encrypt Data
    Identity    crypto.PubKey // signer
    Data        []byte        // encrypted payload (the SDK's CRDT change)
    Signature   []byte
    DataType    string        // the SDK stores the dataset name here
    IsSnapshot  bool
    Timestamp   int64
    AddSeq      uint64        // per-space monotonic counter for incremental sync
}
```

- The root change is a snapshot and carries the tree's header.
- Changes arrive out of order; any-sync holds unattached changes until their
  parents land, so a receiver never applies a change before its ancestors.
- `AddSeq` lets a peer fetch only changes it hasn't seen
  (`Storage.GetAfterAddSeq`). `AddResult.Mode` (`Append` / `Rebuild` /
  `Nothing`) reports how a tree absorbed new changes.
- Changes are signed by the author's key and encrypted with the space read
  key named by `ReadKeyId` (plaintext-class objects skip encryption). Key
  rotation and re-encryption after a member is removed are handled entirely
  by any-sync.

Sources: `commonspace/object/tree/objecttree` (change, tree, storage, change
builder), `commonspace/object/tree/treechangeproto`,
`commonspace/object/tree/synctree`.

## Model

- **The DAG is hidden.** Callers never get an `ObjectTree` handle; they
  address objects by id at the space level.
- **Objects are created explicitly** with `Objects().Create` or
  `Objects().Derive`. A write to an unknown object fails with
  `ErrObjectNotFound`; it never creates one.
- **One object, many datasets.** Every object has exactly one type
  (`any.type`, required: `ErrTypeRequired`), which contributes the handlers
  (schema and rules) for the datasets it declares, and any number of
  collections (`any.collections`), which contribute properties only. See
  docs/data-structure.md § Type and collections.
- **The per-space `objects` dataset** holds each object's row: membership
  and property values. It is the object index (docs/data-structure.md).
- **CRDT changes exist only inside an object.** One DAG change is one
  write batch on one dataset (docs/crdt.md).

## Object API

```go
space.Objects().Create(ctx, CreateObjectOpts{Type, Collections, InitialProperties}) // → objectId
space.Objects().Derive(ctx, DeriveObjectOpts{Seed, Type, Collections, ParentId})    // → objectId, idempotent
space.Objects().Get(ctx, objectId)    // the objects row; ErrNotFound / ErrObjectDeleted
space.Objects().Delete(ctx, objectId)

// Records: reads and live updates through the query builder.
space.Query(objectId, dataset).Filter(...).Sort(...).Limit(n).Iter|All|One|Count|Snapshot|Subscribe
space.QueryObjects().Filter(...).Sort(...).Limit(n).Iter|All|One|Count|Snapshot|Subscribe

// Writes.
space.Modify(ctx, ModifyBatch)  // → ModifyResult{VersionId, ChangeId, RecordIds, Rejections}
space.Delete(ctx, DeleteBatch)
space.Upsert(ctx, UpsertBatch)
```

`Derive` with a `ParentId` binds the child to its parent: the parent id is
hashed into the child's id, and deleting the parent deletes the child. Creating
the child needs the parent's tree on this device (`ErrObjectNotFound` until it
syncs, `ErrObjectDeleted` after deletion); a child that already synced here
re-derives without it.

## Lifecycle

Callers never open or close objects. The SDK loads trees on demand into a
per-space cache and closes them after one idle minute.

### Deletion

- **Local delete.** `Objects().Delete` records the deletion in the space's
  settings tree (any-sync `DeleteTree`, which syncs to every member), then
  purges local state.
- **Remote delete.** any-sync's deletion manager calls the SDK per tree:
  `DeleteTree` for a tree present locally (mark the tree deleted, purge the
  materialized state, evict the cache) and `MarkTreeDeleted` for one never
  synced here (purge only). A purge failure is returned so any-sync retries
  until the purge commits.
- **No SDK tombstone.** The objects row is removed; any-sync's deleted-tree
  record is the durable fact. `Objects().Get` consults it and returns
  `ErrObjectDeleted` for a deleted object, `ErrNotFound` for an unknown id.
- **Every other per-object operation** opens the object's tree, and a
  deleted or unknown id has none: both return `ErrObjectNotFound`. Match
  that sentinel, not any-sync's `treestorage.ErrUnknownTreeId` /
  `spacestorage.ErrTreeStorageAlreadyDeleted`, which stay in the error
  chain as storage internals. `Subscribe` differs for an unknown object:
  it yields an empty initial snapshot, so a subscription registered before
  the object lands still receives its events.
- **Consumers learn of deletions** through the change index
  (`ObjectChange.Deleted`, docs/change-index-proposal.md).
- **Deletion is final.** The object layer has no restore window.

## Snapshots

The SDK writes no snapshot changes; the root is each tree's only snapshot.
The CRDT doesn't need snapshots, because any-store holds the materialized
state. Snapshots would only shorten any-sync's sync and load paths, and when
to write them is an open question (below). Old changes stay in any-sync
storage, where version history reads them
(docs/version-history-proposal.md).

## Sync and conflicts

- **Catch-up by AddSeq.** The SDK tracks the highest AddSeq applied per
  object and replays only newer changes (docs/crdt.md § AddSeq
  watermark).
- **VersionId is the change's any-sync orderId**, assigned when the change is
  added to the local tree. A local write applies immediately, offline
  included; there is no tentative state to reconcile.
- **Everything auto-merges.** The CRDT never surfaces multiple heads or a
  conflict state to the caller.
- **Rejected writes.** A space the account can't write to fails up front
  with `ErrReadOnlySpace`; a schema violation fails the writer-side
  pre-flight before the change enters the DAG; a handler rejection of
  individual ops lands in `ModifyResult.Rejections` while the rest of the
  change commits.
- **Loading** replays the tree in order and applies each change into
  any-store inside write transactions.
- **Parallelism.** any-sync caps concurrent syncs; the SDK adds no limits of
  its own.

## Open questions

1. **Snapshot policy.** When the SDK should write snapshot changes to bound
   sync and load cost; anytype-heart's chat and store-source heuristics are
   the reference.
