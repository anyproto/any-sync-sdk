# any-sync-sdk

A Go SDK on top of [any-sync](https://github.com/anyproto/any-sync), built around a CRDT data plane: Mongo-style record stores per object, per-field version gating, and an apply pipeline that converges across peers.

## Install

```
go get github.com/anyproto/any-sync-sdk
```

Requires Go 1.26+.

## Public packages

Everything outside these import paths lives under `internal/` and is not importable.

| Path | What it owns |
|---|---|
| `github.com/anyproto/any-sync-sdk` | `Open`, `Close`, the `SDK` handle (`Spaces`, `Account`, `Identities`, `Push`, `PubSub`, `P2PStatus`, `CRDTVersion`) |
| `.../auth` | `Provider` interface, the file-backed wallet (`FileProvider`), mnemonic helpers |
| `.../config` | `Config`: storage layout, network YAML, sync and p2p tunables, registered types, collections and modules |
| `.../space` | the caller surface: `Service`, `Space`, `ObjectService`, `TypesAPI`, `CollectionsAPI`, `PropertiesAPI`, `Query`, `ModifyBatch`, `ACL`, `MembersAPI`, `SyncStatusAPI`, `HistoryAPI`, `Files`, … |
| `.../handler` | declarations for custom datasets, types, collections and modules |
| `.../p2p` | local-network discovery driver injection, power hint, p2p status types |

## Quick start

```go
import (
    anysyncsdk "github.com/anyproto/any-sync-sdk"
    "github.com/anyproto/any-sync-sdk/auth"
    "github.com/anyproto/any-sync-sdk/config"
    "github.com/anyproto/any-sync-sdk/space"
)

provider, _ := auth.NewFileProvider(auth.FileProviderConfig{
    Path: "/path/to/wallet.key",
})

cfg := config.Config{
    Storage: config.Storage{DataDir: "/var/data"},
    Network: config.Network{NodeConfYAML: nodeConfBytes},
}

sdk, err := anysyncsdk.Open(ctx, cfg, provider)
if err != nil { return err }
defer sdk.Close()

sp, _ := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "My Notes"})

typeId, _ := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Movie"})
titleId, _ := sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
    Name: "Title", Kind: space.PropertyKindString,
})

objId, _ := sp.Objects().Create(ctx, space.CreateObjectOpts{
    Type: typeId,
    InitialProperties: map[string]map[string]any{
        typeId: {titleId: "Casablanca"},
    },
})

docs, _ := sp.QueryObjects().
    Filter(map[string]any{"any.type": typeId}).
    All(ctx)
```

[`examples/basic/main.go`](examples/basic/main.go) walks through more: property scopes, subscriptions, writes to a type-owned dataset.

## Storage layout

Two any-store databases under `Config.Storage.DataDir`:

```
<DataDir>/anysync/<spaceId>.db   any-sync per-space tree storage (any-store v1)
<DataDir>/sdk.db                 SDK CRDT collections (any-store v2, shared)
```

any-sync stays on any-store v1 internally; the SDK uses v2 through the `/v2` import path. Separate files keep the two versions from sharing schema or state.

## Custom types and datasets

Built-in definitions: `any` (universal properties: `id`, `author`, `spaceId`, `createdAt`, `modifiedAt`, `modifiedBy`, `name`, `description`, …), `type` (the meta-type that defines other types) and `collection`.

Callers extend the catalog at `Open` by registering `handler.Type` entries. A type declares property definitions, datasets with their handlers, and parts:

```go
import "github.com/anyproto/any-sync-sdk/handler"

cfg.Types = []handler.Type{
    {
        Id:   "movie",
        Name: "Movie",
        Datasets: []handler.Dataset{{
            Name:        "scenes",
            DataVersion: "scenes-v1",
            Handler:     &sceneHandler{},
        }},
    },
}
```

`config.Config.Collections` registers collections and `config.Config.Modules` registers dataset modules (docs/user-datasets.md). Registered types appear in `Space.Types().List()` next to user-created types. The dataset names `objects`, `properties` and `shortIds` are reserved.

## Status

Implemented:
- CRDT apply (`$set`/`$unset`/`$inc`/`$incGated`/`$addToSet`/`$pull`, sticky tombstones, per-field `_ver` gating); per-op rejections surface as `ModifyResult.Rejections`
- Local writes and inbound sync replay through one apply path; cold restore, watermarked replay, parked changes drained once their schema arrives
- Derived fields stamped from the tree: `id`, `author`, `createdAt`, `spaceId` from the immutable header; `modifiedAt` / `modifiedBy` from the object's latest synced change on any dataset
- Types (one per object), collections, property definitions with scopes, runtime datasets and parts, bundles
- Writes: `Modify`, `ModifyMany`, `Delete`, `Upsert`
- Queries: per-object `Space.Query` and per-space `Space.QueryObjects` with `Filter`/`Sort`/`Limit`/`Offset`/`Projection` and terminals `Iter`/`All`/`One`/`Count`/`Snapshot`/`Subscribe`; aggregation pipelines (`Aggregate`, `AggregateObjects`)
- Live windowed subscriptions: `Query.Subscribe(ctx, opts)` returns `*QueryResult{Initial, Total, Sub}`. `Sub.Events()` carries `SubscriptionEvent{VersionId, Added, Updated, Removed}` with the full post-apply doc and projected `$set`/`$unset` ops per record. Overflow closes with `ErrSubscriptionOverflow`; drift past `DriftBudgetPercent` (default 30%) closes with `ErrSubscriptionDrifted`; resubscribe to recover
- Change index for consumer-side indexers: `Space.Changes()`
- Version history: `Space.History()` (`ListChanges`, `ViewAt`, `RecordAt`, `Diff`)
- Read tracking: `Space.ReadState()`
- Files: `Space.Files()` (`Attach`, `Open`, `Status`, `Pin`, `Offload`, …) and `Space.Payloads()`; file cache management on `SDK`
- Sync status: `Space.SyncStatus()` (`Space`, `Object`, `SubscribeObject`); `Service.Status` / `SubscribeStatus`
- Tech space (per-account derived index of all spaces); spaces stay resident and are eager-loaded on boot; `Service.Subscribe` for space-list events
- Space lifecycle: `Create` / `Join` / `JoinGuest` / `Derive` / `OneToOne` / `Get` / `List` / `Track` / `Evict` / `Delete`; direct-add and 1-1 accept/decline
- Collaboration: `ACL` (`CreateInvite` / `RevokeInvite` / `RevokeAllInvites` / `AcceptRequest` / `DeclineRequest` / `ChangePermissions` / `AddAccounts` / `RemoveAccounts` / `OwnershipChange` / `RequestSelfRemove` / `CancelJoinRequest` / `StopSharing` / `CreateGuestKey`); `Members` (`List` / `Get` / `Me` / `JoinRequests` / `Invites` / `Subscribe` / `Query`)
- Account: `Account.Metadata` / `UpdateMetadata`, persisted in the tech space and republished to identityRepo on boot; `SDK.Identities()` directory; devices
- Push notifications (`SDK.Push()`) and pub/sub (`SDK.PubSub()`, `Space.PubSub()`)
- Direct sync with LAN peers (mDNS) and global peers (iroh, account-level discovery)
- CRDT version mark: an SDK refuses an account written by a newer data model

Not implemented:
- `TypesAPI.Delete`, `CollectionsAPI.Delete`

## Specs

See [`docs/`](docs/):

- `common-context.md`: the layer model and design principles
- `auth-module.md`: wallet / provider contract
- `tech-space.md`: derived per-account index
- `space.md`: space lifecycle, ACL surface, sync
- `object.md`: object lifecycle
- `crdt.md`, `crdt-spec.md`: apply algorithm and invariants
- `data-structure.md`: record shape, types, collections, properties
- `files.md`: files and payloads
- `versioning.md`: handler versions, re-indexing, the CRDT version mark
- `sync-status-proposal.md`: per-space and per-object sync status
- `change-index-proposal.md`: change feed for consumer-side indexers
- `one-to-one-spaces.md`: derived 1-1 spaces and inbox discovery
- `identities.md`: account-global identity directory
- `direct-add-invites.md`: adding accounts to a space by identity
- `user-datasets.md`: parts, modules, runtime dataset schemas, batch upsert
- `global-p2p.md`: internet-wide device-to-device sync
- `account-discovery.md`: finding the account's own devices
- `bundles.md`: per-space registry of installed bundles
- `read-tracking-proposal.md`: read/unread state
- `scoped-properties-proposal.md`: property and field scopes
- `space-index-proposal.md`: the per-space `spaceIndex` object
- `types-properties-proposal.md`: schema versioning, DataVersion gating, parked changes
- `version-history-proposal.md`: version history

## Compatibility note

The on-the-wire `header.SpaceType` is constrained to the any-sync-coordinator's allow-list: the `any` product's `any.space` / `any.techspace` / `any.onetoone` (fileproto v2 required) plus anytype's `anytype.space`, `anytype.techspace`, `anytype.chatspace`, `anytype.onetoone`. The SDK mints only the `any.*` family; it can join `anytype.*` spaces but never creates them. See `docs/space.md § Space type strings`.


## License

We plan to release Any under an open-source license. The specific license
is **TBD** and will be added once that decision is made.
