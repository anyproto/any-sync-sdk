# Object

## Vision
An object is a content-addressable DAG (like git). Members of the DAG are "changes" — each contains links to previous changes, a link to the ACL record, and encrypted CRDT data.

**SDK v1 philosophy**: hide the DAG. The caller sees only CRDT-level operations (datasets, records, fields). All DAG complexity is an internal implementation detail.

## Current any-sync Implementation

### Change Structure
```go
type Change struct {
    Id           string        // content-addressable CID
    PreviousIds  []string      // DAG edges to parent changes
    AclHeadId    string        // ACL state at change time
    SnapshotId   string        // reference to snapshot node
    ReadKeyId    string        // encryption key ID
    Identity     crypto.PubKey // creator's public key
    Data         []byte        // encrypted payload
    Signature    []byte        // cryptographic signature
    DataType     string        // e.g., "ObjectDelete"
    IsSnapshot   bool          // marks snapshot nodes
    Timestamp    int64
    AddSeq       uint64        // space-global monotonic sequence
}
```

### DAG Structure (Tree)
```go
type Tree struct {
    root           *Change
    headIds        []string              // current branch heads
    attached       map[string]*Change    // resolved changes
    unAttached     map[string]*Change    // pending (missing deps)
    waitList       map[string][]string   // dependency waitlist
}
```

- Root is a snapshot (`IsSnapshot=true`)
- `PreviousIds` is a slice → supports merge (multi-parent DAG)
- `headIds` tracks current tips
- Changes arrive out of order — `unAttached` + `waitList` handle dependency resolution

### ObjectTree Interface
```go
type ObjectTree interface {
    Id() string
    Root() *Change
    Heads() []string
    Len() int

    // Add new content
    AddContent(ctx, SignableChangeContent) (AddResult, error)
    AddRawChanges(ctx, RawChangesPayload) (AddResult, error)

    // Iterate
    IterateRoot(convert, iterate) error
    IterateFrom(id, convert, iterate) error

    // Sync
    SnapshotPath() ([]string, error)
    ChangesAfterCommonSnapshotLoader(snapshotPath, heads) (LoadIterator, error)
}
```

### Content-Addressable IDs
Each change ID = CID computed from serialized `RawTreeChange` bytes (`cidutil.NewCidFromBytes()`).

### Signing & Encryption
```go
type SignableChangeContent struct {
    Data              []byte
    Key               crypto.PrivKey   // signing key
    IsSnapshot        bool
    ShouldBeEncrypted bool
    Timestamp         int64
    DataType          string
}
```

Changes are signed by the creator's key and optionally encrypted with the space read key (identified by `ReadKeyId`).

### Protocol (protobuf)
- `RootChange` — initial snapshot: changeType, payload, aclHeadId, spaceId, identity, seed
- `TreeChange` — regular: treeHeadIds (prev), snapshotBaseId, changesData, readKeyId, isSnapshot
- `RawTreeChange` — serialized payload + signature
- `RawTreeChangeWithId` — adds CID

### Sync
- `AddSeq` — per-space monotonic counter for incremental sync
- `Storage.GetAfterAddSeq(ctx, lastSeq, iter)` — fetch only new changes
- `AddResult.Mode`: `Append` (fast-forward), `Rebuild` (full rescan), `Nothing`

### Source files
- `any-sync/commonspace/object/tree/objecttree/change.go` — Change struct
- `any-sync/commonspace/object/tree/objecttree/objecttree.go` — ObjectTree interface
- `any-sync/commonspace/object/tree/objecttree/tree.go` — Tree (DAG) struct
- `any-sync/commonspace/object/tree/objecttree/storage.go` — Storage interface
- `any-sync/commonspace/object/tree/objecttree/changebuilder.go` — building & signing changes
- `any-sync/commonspace/object/tree/treechangeproto/` — protobuf definitions
- `any-sync/commonspace/object/tree/synctree/` — sync wrapper

## Key Decisions

### Abstraction Level
- **CRDT-level only** — in v1 the DAG is fully hidden. No `ObjectTree` access for callers; hiding this complexity is a primary SDK goal
- **Explicit creation** — callers create objects explicitly, not implicitly via first write
- **Object ↔ datasets (many)** — one object can hold many datasets. An object may have multiple types; each type contributes handlers (schema + rules) for certain datasets
- **System-level index dataset** — one dataset is implemented at the system level to serve as an object index (discussed in Data Structure section)
- **Permissionless in v1** — no schema/type validation yet. v1 is a permissionless DB for experimenting with schemas and validations. Types/schemas/validation come later

### Object API (v1)
```
object.create()
object.derive()
object.delete()
object.subscribe()
object.query(datasetName, filter, sort)
```

### Lifecycle
- **No open/close in caller API** — SDK internally uses `ocache` to hand any-sync live object instances. Callers never open objects; they read/subscribe via any-store
- **TTL auto-close** — handled internally (any-sync requirement), not caller-visible
- **Deletion**:
  - Remote delete → SDK listens to the settings tree, deletes local data accordingly
  - Local delete → SDK adds entry to settings tree (any-sync side) + deletes local data
- **No restore / time window** — a separate "bin" mechanism will be built later, based on system object properties. Not part of the Object layer

### Snapshots
- **SDK-internal only** — CRDT doesn't need snapshots from the data perspective (any-store already holds materialized state). any-sync needs snapshots to optimize internal sync mechanics
- **Strategy from anytype-heart** — will reuse the approach used in `../anytype-heart` (chats, store source) for when to snapshot
- **Decided by SDK internally** — never caller-triggered
- **Old changes** — any-sync keeps them in its own storage. Version history mechanism comes later

### Sync / Conflicts
- **AddSeq** — internal mechanism: any-sync + SDK use it to apply only unseen changes to the CRDT layer
- **versionId / orderId** — client-side ordering key for consistency between any-store and the event stream
- **Automatic merge** — CRDT auto-merges everything. No multi-head or conflict state ever surfaced to the caller
- **Change flow (writes)** — caller sees `addChange(change) → versionId`. `AddResult.Mode` (Append/Rebuild/Nothing) is internal
- **Linear iteration** — reads look like a linear list (how `any-sync tree.Iterate` works); version history (if any) comes later

### Encryption
- **Key rotation** — supported (different changes can use different `ReadKeyId`s), fully handled inside any-sync
- **Re-encryption on ACL change** (member removed) — any-sync handles it, SDK does nothing

### Performance
- **Loading model** — reads work like `openTx → tree.Iterate(applyChange(N)) → commit`
- **Parallelism limits** — any-sync has its own caps on parallel syncs. No explicit SDK-level limits in v1; priority system is future work
- **unAttached / waitlist** — entirely any-sync's concern

### Dependencies
- CRDT changes can only be produced in an object context — no object, no changes
- v1 is permissionless; types/schemas/validation layer on top later

## Grooming Questions (open)

### Object API
1. Object creation — minimal payload? `space.Object.Create() → objectId` or does it need a "type"/"typeList" argument even in permissionless v1?
2. `derive()` — what are the inputs? Derived from what (keys? parent object? external seed)?
3. `subscribe()` at object level vs dataset level — does the caller subscribe to the whole object (all datasets) or per dataset?
4. `query(datasetName, filter, sort)` on the object level — is this the same API as `space.Query(...)`, just scoped?
5. How does a caller attach multiple "types" to an object if we're going permissionless in v1? Free-form list that can be validated later?

### Deletion & Settings Tree
6. SDK listens to the settings tree for deletions. Does this land in any-store as a deleted marker the caller can observe? Or does the object simply disappear from queries?
7. Local delete path — write to settings tree first, then wipe local data, or parallel? Atomicity considerations.

### Snapshots (internal)
8. Concrete snapshot heuristic from anytype-heart — which rules do we reuse? (N changes? size threshold? time?)
9. When SDK creates a snapshot, it uses the current any-store state. Confirm this is always safe (no mid-write races).

### Write Flow
10. `addChange(change) → versionId` — when is versionId assigned? On local apply, or only after any-sync accepts?
11. Optimistic apply for offline writes — local versionId first, reconciled on sync?
12. Error handling — what happens if a change is rejected (invalid signature, ACL denied)?
