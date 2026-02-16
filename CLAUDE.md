# any-sync-sdk

Minimal Go SDK for the [any-sync](https://github.com/anyproto/any-sync) P2P sync protocol. Provides a clean API for spaces, objects (CRDT trees), key-value storage, and real-time event subscriptions without any application-level abstractions.

## Quick start

```go
import (
    syncsdk "github.com/anyproto/any-sync-sdk"
    "github.com/anyproto/any-sync-sdk/client"
    "github.com/anyproto/any-sync-sdk/keys"
)

signingKey, _, _ := keys.GenerateRandomKey()
c, _ := client.New(ctx, syncsdk.Config{
    SigningKey:   signingKey,
    Network:     networkCfg,
    StoragePath: "/tmp/myapp",
})
defer c.Close(ctx)

space, _ := c.CreateSpace(ctx)
obj, _ := space.CreateObject(ctx, syncsdk.WithChangeType("doc"))
obj.AddContent(ctx, []byte(`{"title":"Hello"}`))
```

Use `client.New()` (not `syncsdk.New()`) — the root package stub exists only to avoid import cycles.

## Module

`github.com/anyproto/any-sync-sdk`

Depends on `github.com/anyproto/any-sync` (currently via local replace directive in go.mod).

## Package layout

```
.
├── client.go          # Client interface (incl. JoinSpace)
├── space.go           # Space interface (incl. sharing/ACL methods)
├── object.go          # Object interface, ChangeInfo, AddOption
├── keyvalue.go        # KeyValue interface
├── permission.go      # Permission, MemberStatus, Member, InviteOption types
├── invite.go          # Invite encode/decode/parse helpers
├── event.go           # EventType, Event, Handler
├── config.go          # Config, NetworkConfig, NodeInfo
├── errors.go          # Sentinel errors
├── keys/
│   └── keys.go        # Type aliases for crypto.PrivKey/PubKey/SymKey + generators
├── client/
│   └── client.go      # Public constructor (client.New) — breaks import cycle
├── internal/
│   ├── bootstrap/
│   │   └── bootstrap.go     # Wires 17+ any-sync components into app.App
│   ├── clientimpl/
│   │   └── client.go        # Client implementation (space cache, lifecycle)
│   ├── spaceimpl/
│   │   └── space.go         # Space implementation (lazy init, object cache, KV)
│   ├── objectimpl/
│   │   └── object.go        # Object implementation (CRDT tree, UpdateListener)
│   ├── kvimpl/
│   │   └── keyvalue.go      # KeyValue implementation (wraps keyvaluestorage)
│   └── components/          # Adapter components for any-sync's app.App DI
│       ├── config.go        # ConfigGetter for multiple any-sync subsystems
│       ├── account.go       # accountservice.Service wrapper
│       ├── peermanager.go   # PeerManagerProvider (per-space peer managers)
│       ├── storage.go       # SpaceStorageProvider (any-store DB per space)
│       ├── treemanager.go   # No-op TreeManager (operations are per-space)
│       ├── treesyncer.go    # No-op TreeSyncer (sync handled by individual trees)
│       └── coordsource.go   # nodeconf.Source (returns ErrConfigurationNotChanged)
├── example/
│   └── main.go              # Full usage example
├── staging.yml              # Staging network configuration
├── syncsdk_test.go          # Unit tests (options, keys, config validation)
├── integration_test.go      # Integration tests (full lifecycle, concurrency)
└── e2e_test.go              # E2E tests using staging.yml
```

## Public API

### Client

```go
type Client interface {
    CreateSpace(ctx, ...SpaceCreateOption) (Space, error)
    DeriveSpace(ctx, spaceType string) (Space, error)  // Deterministic from key + type
    OpenSpace(ctx, spaceID string) (Space, error)       // Lazy — no I/O until first use
    JoinSpace(ctx, invite string) (Space, error)        // Join via invite link
    DeleteSpace(ctx, spaceID string) error              // Coordinator RPC + local cleanup
    DeleteAccount(ctx) (deletionTimestamp int64, err error) // Schedule account deletion
    RevertAccountDeletion(ctx) error                    // Cancel pending account deletion
    Subscribe(Handler) (unsubscribe func())
    Close(ctx) error
}
```

### Space

```go
type Space interface {
    ID() string
    GetObject(ctx, objectID string) (Object, error)
    CreateObject(ctx, ...ObjectCreateOption) (Object, error)
    DeriveObject(ctx, ...ObjectDeriveOption) (Object, error)  // Idempotent
    DeleteObject(ctx, objectID string) error
    ListObjectIDs(ctx) ([]string, error)
    KeyValue() KeyValue
    GenerateInvite(ctx, ...InviteOption) (invite string, err error)
    Members(ctx) ([]Member, error)
    AddMember(ctx, identity keys.PublicKey, permissions Permission) error
    RemoveMember(ctx, identity keys.PublicKey) error
    ChangePermissions(ctx, identity keys.PublicKey, permissions Permission) error
    AcceptJoinRequest(ctx, identity keys.PublicKey, permissions Permission) error
    DeclineJoinRequest(ctx, identity keys.PublicKey) error
    RevokeInvite(ctx, inviteRecordID string) error
    Subscribe(Handler) (unsubscribe func())
    Close(ctx) error
}
```

### Permissions & Members

```go
type Permission int
const (
    PermissionOwner  Permission = 1
    PermissionAdmin  Permission = 2
    PermissionWriter Permission = 3
    PermissionReader Permission = 4
)

type MemberStatus int
const (
    MemberStatusActive   MemberStatus = 1
    MemberStatusJoining  MemberStatus = 2
    MemberStatusRemoving MemberStatus = 3
)

type Member struct {
    Identity    keys.PublicKey
    Permissions Permission
    Status      MemberStatus
}
```

### Invites

```go
// Functional options for GenerateInvite
WithInvitePermission(Permission)  // default: PermissionWriter
WithApprovalRequired()            // creates RequestToJoin invite

// Standalone functions (no Space needed)
EncodeInvite(spaceID string, inviteKey crypto.PrivKey, approvalRequired bool) (string, error)
DecodeInvite(invite string) (spaceID string, inviteKey crypto.PrivKey, approvalRequired bool, err error)
ParseInvite(invite string) (spaceID string, err error)
```

### Object

```go
type Object interface {
    ID() string
    SpaceID() string
    Heads() []string
    AddContent(ctx, data []byte, ...AddOption) (ChangeInfo, error)
    Iterate(visitor func(ChangeInfo) bool) error
    Subscribe(Handler) (unsubscribe func())
    Close() error
}
```

Options: `WithChangeType(t)`, `WithEncryption()`, `WithSnapshot()`, `WithDataType(t)`

### KeyValue

```go
type KeyValue interface {
    Set(ctx, key string, value []byte) error
    Get(ctx, key string) ([]byte, error)
    Iterate(ctx, func(key string, value []byte) bool) error
}
```

### Events

```go
type EventType int
const (
    ObjectUpdated       EventType = iota + 1  // Incremental append from remote
    ObjectRebuilt                              // Full DAG rebuild (snapshot)
    SpaceConnected                             // Space sync active
    SpaceDisconnected                          // Lost all peers
    JoinRequestReceived                        // Someone requested to join via approval invite
)

type Event struct {
    Type     EventType
    SpaceID  string
    ObjectID string         // Empty for space-level events
    Heads    []string
    Identity keys.PublicKey  // Populated for ACL events (nil for object events)
}

type Handler func(Event)
```

### Config

```go
type Config struct {
    SigningKey   keys.PrivateKey  // Required. Account identity (signs changes)
    MasterKey   keys.PrivateKey  // Optional. Defaults to SigningKey
    PeerKey     keys.PrivateKey  // Optional. Auto-generated if nil
    Network     NetworkConfig    // Node topology
    StoragePath string           // Required. Local SQLite database directory
}
```

### Errors

`ErrInvalidConfig`, `ErrClientClosed`, `ErrSpaceClosed`, `ErrSpaceNotFound`, `ErrObjectNotFound`, `ErrJoinRequestPending`, `ErrInvalidInvite`

## Architecture

### Lazy space initialization

`OpenSpace()` returns instantly. The underlying `commonspace.Space` is created on the first operation (GetObject, CreateObject, etc.) via `sync.Once`. This avoids connecting to all spaces upfront.

### Bootstrap (internal/bootstrap)

`NewApp()` constructs an `app.App` component graph with 17+ components that `commonspace.SpaceService` requires: config, account, nodeconf, secureservice, yamux, quic, peerservice, pool, coordinatorclient, nodeclient, storage provider, tree manager, syncqueues, and commonspace itself.

### Object trees

Each Object wraps a SyncTree (CRDT-based append-only DAG). The objectImpl implements `updatelistener.UpdateListener` to receive `Update()` and `Rebuild()` callbacks from the sync layer, which it dispatches as events to subscribers.

### Import cycle resolution

The root `syncsdk` package defines interfaces. `internal/clientimpl` imports `syncsdk` for types. To avoid a cycle, the real constructor lives in `client/client.go` which imports `clientimpl` but not the root package's implementation.

## Development

### Build

```bash
go build ./...
```

### Test

```bash
go test ./...              # All tests (72 tests)
go test -race ./...        # With race detector
go test -short ./...       # Skip E2E tests
go test -run TestCreateObject ./...  # Single test
```

### Key patterns to be aware of

- **Object creation needs a random seed.** `ObjectTreeCreatePayload.Seed` must be 32 random bytes to prevent CID collisions when creating multiple objects within the same second.
- **DeriveObject is idempotent.** If the derived tree already exists, it falls back to `GetObject`. Handle `treestorage.ErrTreeExists`.
- **ListObjectIDs uses HeadStorage directly**, not `StoredIds()`. The diff manager excludes empty-root trees from its NewDiff, so recently created objects without content wouldn't appear.
- **HeadUpdater is async.** The DiffManager receives updates via a goroutine queue, not synchronously from HeadStorage writes.
- **Space.Close() closes cached objects** before closing the underlying commonspace to prevent resource leaks.
- **GenerateInvite requires SpaceMakeShareable.** The coordinator must mark the space as shareable before invites can be created. This is called automatically by `GenerateInvite`.
- **ReplaceInvite does NOT send to the network.** After `ReplaceInvite()`, you must call `AddRecord()` with the returned `InviteRec` to actually persist the invite.
- **AclJoiningClient is created manually** (not via bootstrap) because it shares `CName` with `AclSpaceClient`. It's initialized from the parent `app.App` in `clientimpl.New()`.
- **SpaceImpl implements AclUpdater** to receive ACL change callbacks via `SetAclUpdater`. It dispatches `JoinRequestReceived` events asynchronously (via goroutine) because the callback runs while the ACL lock is held.
- **RevokeInvite** delegates to `AclSpaceClient.RevokeInvite(ctx, inviteRecordId)` from any-sync.

### Tests

| File | Count | Coverage |
|------|-------|----------|
| `syncsdk_test.go` | 31 | Config validation, key gen, option resolvers, error values, permission/status constants, invite encode/decode/parse, event types, event Identity field |
| `integration_test.go` | 31 | Full lifecycle: create/derive/open spaces, create/derive/delete objects, add content, iterate, subscribe, concurrent access, persistence, KV, delete space/account after close, members, invite generation, join validation, space dispatch, RevokeInvite |
| `e2e_test.go` | 6 | Staging network (skipped in `-short` mode): create space, derive space, delete space, invite generation, multi-client sync, delete/revert account |

### Adding a new component adapter

1. Create `internal/components/mycomponent.go` implementing the required any-sync interface
2. Register it in `internal/bootstrap/bootstrap.go` in the correct dependency order
3. Any-sync components are initialized in registration order via `app.App.Start()`
