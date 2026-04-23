# Space

## Vision
A user creates a space or joins someone's space via invite. One space = one ACL + many objects, files, and optionally key-value data.

## Current any-sync Implementation

### Space Interface (core)
```go
type Space interface {
    Id() string
    Acl() syncacl.SyncAcl
    Storage() spacestorage.SpaceStorage
    TreeBuilder() objecttreebuilder.TreeBuilder
    AclClient() aclclient.AclSpaceClient
    KeyValue() kvinterfaces.KeyValueService
    SyncStatus() syncstatus.StatusUpdater
    // + sync handlers, lifecycle
}
```

### Space Creation
`SpaceService.CreateSpace(SpaceCreatePayload)` → space ID

```go
type SpaceCreatePayload struct {
    SigningKey      crypto.PrivKey
    SpaceType       string
    ReplicationKey  uint64          // determines which nodes store this space
    SpacePayload    []byte
    MasterKey       crypto.PrivKey
    ReadKey         crypto.SymKey   // first read key for encryption
    MetadataKey     crypto.PrivKey
    Metadata        []byte          // owner metadata (name, icon)
    Options         *AclSpaceOptions
}
```

Steps: create space header (signed, produces CID-based space ID) → build ACL root → create settings tree → init storage.

### Space Joining

Two mechanisms:

**A. Request-to-Join (approval required)**
```
Invitee has invite key → RequestJoin() → owner receives request → AcceptRequest() / DeclineRequest()
```

**B. AnyoneCanJoin (no approval)**
```
Invitee has invite key → InviteJoin() → immediate access with specified permissions
```

```go
type AclJoiningClient interface {
    RequestJoin(ctx, spaceId string, payload RequestJoinPayload) (aclHeadId string, err error)
    InviteJoin(ctx, spaceId string, payload InviteJoinPayload) (aclHeadId string, err error)
    CancelJoin(ctx, spaceId string) error
    RequestSelfRemove(ctx, spaceId string, aclList AclList) error
}
```

### ACL Permissions
```go
const (
    AclPermissionsNone
    AclPermissionsReader   // read-only
    AclPermissionsGuest    // read-only, can't request removal
    AclPermissionsWriter   // read + write
    AclPermissionsAdmin    // write + manage accounts
    AclPermissionsOwner    // full control
)
```

### ACL Record Types
`AclRoot` → `AclAccountInvite` → `AclAccountRequestJoin` / `AclAccountInviteJoin` → `AclAccountRequestAccept` / `AclAccountRequestDecline` → `AclPermissionChange` → `AclAccountRemove` → `AclOwnershipChange`

### ACL Space Client (owner operations)
```go
type AclSpaceClient interface {
    ReplaceInvite(ctx, permissions) error
    AcceptRequest(ctx, identity) error
    DeclineRequest(ctx, identity) error
    RemoveAccounts(ctx, identities) error
    ChangePermissions(ctx, identity, perms) error
    OwnershipChange(ctx, newOwner) error
    // ...
}
```

### Derived Spaces
- `DeriveSpace()` — deterministic from keys (used for tech space)
- `DeriveOneToOneSpace()` — shared space between two users, same ID regardless of key order

### Source files
- `any-sync/commonspace/space.go` — Space interface
- `any-sync/commonspace/spaceservice.go` — SpaceService
- `any-sync/commonspace/spacepayloads/payloads.go` — create/derive payloads
- `any-sync/commonspace/acl/aclclient/aclspaceclient.go` — owner ACL operations
- `any-sync/commonspace/acl/aclclient/acjoiningclient.go` — join operations
- `any-sync/commonspace/object/acl/list/list.go` — AclList
- `any-sync/commonspace/object/acl/list/models.go` — permissions model
- `any-sync/commonspace/object/acl/aclrecordproto/` — proto definitions

## Key Decisions

### ACL
- **Full ACL feature set** — SDK exposes all ACL operations available in any-sync (invite, accept/decline, remove, change permissions, ownership transfer, etc.). All 6 permission levels.
- **API shape** — mirror `AclClient` one-to-one
- **Invite format** — whatever any-sync requires (inherits format from any-sync, not reinvented)
- **Metadata & member names** — built on any-sync's metadata key in ACL + `identityRepo` for encrypted user data. SDK wraps this, doesn't invent its own
- **identityRepo integration** — handled internally by the SDK (background fetch, writes resolved data to any-store). Only public methods expose are for updating own metadata (probably at account level)
- **Members as a collection** — members are exposed as an any-store system collection with the same query/subscription rules as any other data. Callers read members via `Find()` + event flow
- **Join request notifications** — a new ACL record is added; SDK reacts to ACL changes via event flow and surfaces pending requests to the caller (likely as `status=pending` in the members collection, TBD)

### Space Lifecycle
- **Deletion (v1)** — regular spaces: delete all local data
- **1-1 spaces** — in v1 scope, but with separate logic:
  - Not removable from the network (derived, always re-creatable)
  - Can only be deleted locally
  - Still appear in the space index like regular spaces
- **Offline-first** — everything works offline. Only exception: account recovery still requires p2p peers

### Sync
- **Space loading (v1)** — init all spaces on start. Prioritization/lazy loading is future work on the any-sync side
- **Replication key** — one per account. SDK reads it from the tech space ID (tech space is derived, its replication key is the canonical one for the account)
- **Sync status** — a separate SDK subsystem that tracks per-space, per-object, and peer connection status. Will be designed separately

### Files
- **Deferred** — will be covered in a separate section (07-files.md). Open question: keep the existing filenode system or move files to the any-sync level

### Storage Topology
- **One vs many any-store DBs** — deferred to Data Structure section

### Tech Space Integration
- Space creation/join → SDK writes to tech space index (internal, atomic)
- Space deletion → SDK updates tech space record to `status=deleted`
- All writes go through SDK methods, never direct

## Grooming Questions (open)

### Space Metadata (deferred)
1. For spaces you haven't joined yet: metadata comes from the invite key + identityRepo. Confirm this is the model.
2. For spaces you have access to: maybe a derived object holding space metadata (name, icon, description). TBD.
3. When to groom: after we know more about derived objects and metadata flow.

### Replication Key
4. First login on a new device — do we know the replication key before tech space is loaded? Current any-sync flow needs checking. Probably derive from account key as a fallback.

### 1-1 Spaces
5. API for creating/opening a 1-1 space — `sdk.OneToOne(otherIdentity)`? Returns the derived space if it already exists locally.
6. "Delete only locally" semantics — what does this look like in the space index? `localStatus=deleted, remoteStatus=active`?

### Members Collection
7. Exact record schema — deferred until we start implementing.
8. `status=pending` vs separate join-requests collection — decide during implementation.

### Dependencies
9. Files → separate section (07-files.md)
10. Sync status → separate subsystem, designed later
