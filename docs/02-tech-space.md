# Tech Space

## Vision
A derived space (deterministic from account key) that stores account-level data. One tech space per account. Hidden from the public SDK API — callers interact through dedicated methods, never with the space directly.

## Key Decisions
- **Derived** — created on first login if not on device; deterministic ID from account key, always re-derivable
- **Derived ACL** — owner-only, network denies any new ACL changes. No shared accounts possible
- **Hidden** — not exposed as a `Space` in SDK API, only through purpose-specific methods
- **Same CRDT** — uses the same version-gated record store CRDT as regular spaces
- **any-store first** — all data lives in any-store, SDK reads DB + listens to event flow, not in-memory state
- **ocache pattern** — any-sync `CommonSpace` managed via ocache (like any-sync-node), init/close by activity. SDK doesn't depend on space being loaded in memory
- **Sync priority** — tech space syncs first on startup, but sync is continuous (decentralized, never "done")
- **Tech space loads before regular spaces** — space list comes from tech space
- **SpaceType (interim)** — the tech space stamps `anytype.techspace` in its header (`techspace.TechSpaceType` constant). The any-sync-coordinator gates inbound spaces against an anytype-specific allow-list (`spacestatus/changeverifier.go`); the library default `spacepayloads.SpaceReserved` (`any-sync.space`) is rejected. See `03-space.md § Space type strings (interim)` for the migration plan.

## What Tech Space Stores

### Space Index (derived object, record set)
Records with fields:
- `id` — space ID
- `type` — space type
- `name` — display name
- `icon` — space icon
- `localStatus` — local state
- `remoteStatus` — remote state
- `createdAt` — added-to-account time (unix seconds). Handler-derived
  (`ScopeDerived`): `SpaceIndexHandler.BeforeCreate` stamps it from the
  creating change's timestamp when the row first lands — Create for the
  author, Join for a joiner — so it's per-account, immutable, and
  identical across the account's devices. Absent on rows created before
  the field existed; readers treat 0 as "unknown".
- etc.

### Account Preferences (derived object, postponed)
Probably a separate derived object with multiple record sets (namespaces) for different purposes. Schema TBD.

### Chat Read Tracking (KV namespace)
Read positions for chats, stored as key-value in tech space.

## Current any-sync Implementation

### Derivation
```go
func (s *spaceService) DeriveSpace(ctx context.Context, payload SpaceDerivePayload) (string, error)
```
Deterministically derived from signing key + master key → same account always produces the same tech space ID.

### SpaceStorage internals
```go
type spaceStorage struct {
    spaceId       string
    headStorage   headstorage.HeadStorage
    stateStorage  statestorage.StateStorage
    aclStorage    list.Storage
    store         anystore.DB              // underlying any-store database
    addSeq        atomic.Uint64
}
```

### Source files
- `any-sync/commonspace/spaceservice.go` — DeriveSpace, DeriveId
- `any-sync/commonspace/spacestorage/spacestorage.go` — SpaceStorage interface & implementation
- `any-sync/commonspace/spacestorage/statestorage/` — StateStorage
- `any-sync/commonspace/spacestorage/headstorage/` — HeadStorage

## Key Decisions (continued)
- **Write access** — space index is written only via SDK methods, never direct writes. Caller can add custom metadata but through SDK API
- **Deletion** — deleted spaces stay in the index with `status=deleted`, never physically removed
- **Space validation on open** — when opening a space from the index, SDK verifies it's still valid (ACL intact, not deleted remotely). Probably at a higher level than tech space itself
- **Chat read tracking** — separate API surface, but internally accesses tech space KV namespace. Chat is not a first-class tech space concept

## Grooming Questions (open)

### Space Index (schema deferred)
1. Record schema will be finalized later. Space loading is complex (statuses like `waitingApprove`, 1-1 spaces with separate logic, etc.) — schema evolves with the space lifecycle design

### Account Preferences (deferred)
2. Postponed — will be groomed separately when we know the SDK API shape

### Data Flow
3. SDK reads any-store + listens events. What exactly triggers re-indexing of the space list? (any-sync push → any-store write → event → SDK notifies caller)
4. How does the caller subscribe to space list changes? Same event flow as regular collections, or dedicated `OnSpaceListChange` callback?

### Dependencies
5. Depends on Auth. Regular spaces depend on tech space for the space index. Opening a space triggers validation at a higher level.

## Account-values carriers

For every target space the account participates in, the tech space
hosts one derived **account-values carrier object** (seed
`builtin:accountValues/<spaceId>`, dataset `account_values`). It is
the transport for account-scoped values — properties (and, later,
dataset fields) declared `scope: account`, which sync across this
account's devices but stay invisible to other space members.

- One carrier record per target `(objectId, dataset, recordId)`,
  record key `<objectId>:<dataset>:<recordId>`; the objects row is the
  degenerate case. Record fields are the account-scoped paths VERBATIM
  (`{typeId}.{propId}`), so the record's own `_ver` — including
  entries retained by $unset — is the version source the per-space
  mirror replays into target rows (injected applies, grouped by
  carrier version).
- Lifecycle is 1:1 with the target space: derived on first use
  (mirror start or first account write), deleted whole on space
  leave/delete. Records of a deleted target object are tombstoned by
  whichever device observes the deletion first (idempotent).
- Full contract: docs/scoped-properties-proposal.md § "Account
  transport"; mirror implementation: internal/spaceimpl/accountmirror.go.
