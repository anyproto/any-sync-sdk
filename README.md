# any-sync-sdk

A Go SDK on top of [any-sync](https://github.com/anyproto/any-sync), built around a CRDT data plane: Mongo-style record stores per object, per-field version gating, and an apply pipeline that converges across peers.

Status: **Phase 2** — CRDT data plane, query, subscriptions, collaboration v1 (ACL, members, invites), a change-index surface for consumer-side incremental indexers (`Space.Changes()`), and identityRepo-backed account metadata are wired. Sync-status, change history, files, and a few schema-edit APIs are still stubs; see *Status* below.

## Install

```
go get github.com/anyproto/any-sync-sdk
```

Requires Go 1.26+. Pulls `github.com/anyproto/any-sync v0.12.3` and `github.com/anyproto/any-store/v2 v2.0.0-alpha.3`.

## Public packages

There are exactly three public import paths. Everything else lives under `internal/` and is not importable.

| Path | What it owns |
|---|---|
| `github.com/anyproto/any-sync-sdk` | `Open`, `Close`, the `SDK` handle |
| `.../auth` | `Provider` interface + the file-backed wallet (`FileProvider`) and mnemonic helpers |
| `.../config` | `Config` (storage layout, network YAML, sync tunables, registered types) |
| `.../space` | the whole caller surface — `Service`, `Space`, `ObjectService`, `TypesAPI`, `PropertiesAPI`, `Query`, `Subscription`, `ModifyBatch`, `ACL`, `MembersAPI`, `SyncStatus` |
| `.../handler` | type aliases for declaring custom CRDT dataset handlers + `handler.Type` for the registered-type catalog |

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
    Types: []string{typeId},
    InitialProperties: map[string]map[string]any{
        typeId: {titleId: "Casablanca"},
    },
})

docs, _ := sp.QueryObjects().
    Filter(map[string]any{"any.types": map[string]any{"$in": []any{typeId}}}).
    All(ctx)
```

A more complete walkthrough — type definition, multi-scope properties (base / account / device), subscriptions, custom datasets — lives in [`examples/basic/main.go`](examples/basic/main.go).

## Storage layout

Two any-store databases under `Config.Storage.DataDir`:

```
<DataDir>/anysync/<spaceId>.db   any-sync per-space tree storage (v1)
<DataDir>/sdk.db                 SDK CRDT collections (v2, shared)
```

The split lets the SDK use any-store v2 (compressed objects, better query semantics) while any-sync stays on v1 internally — both modules coexist via the `/v2` import path.

## Custom types and datasets

Built-in catalog: `any` (universal properties: id, author, createdAt, spaceId, name, description, …) and `type` (the meta-type that defines other types).

Callers extend the catalog at SDK init by registering `handler.Type` entries — each binds a typeId to one or more dataset handlers:

```go
import "github.com/anyproto/any-sync-sdk/handler"

cfg.Types = []handler.Type{
    {
        Id:   "movie",
        Name: "Movie",
        Handlers: []handler.Registration{{
            Handler:     &sceneHandler{handler.DefaultHandler{DatasetName: "scenes", HandlerVersion: 1}},
            DataVersion: "scenes-v1",
        }},
    },
}
```

Registered types appear in `Space.Types().List()` alongside user-created types and are recognized at apply time. Reserved dataset names (`objects`, `properties`, `shortIds`) cannot be reused.

## Status

Wired and tested:
- CRDT apply (`$set`/`$unset`/`$inc`/`$incGated`/`$addToSet`/`$pull`, sticky tombstones, per-field `_ver` gating, strict-skip surfaced as `ApplyResult.Rejections`)
- Auto-stamping of `id`, `author`, `createdAt`, `spaceId` at row root, sourced from the tree's immutable header
- Type catalog (built-in + caller-registered) with registered-type query API
- Per-object query (`Space.Query`) and per-space cross-object query (`Space.QueryObjects`) with chained `Filter`/`Sort`/`Limit`/`Offset` and terminals `Iter`/`All`/`One`/`Count`/`Snapshot`/`Subscribe`
- Live windowed subscriptions via `Query.Subscribe(ctx, opts)` — same builder, returns `*QueryResult{Initial, Total, Sub}`. `Sub.Events()` carries `SubscriptionEvent{VersionId, Added, Updated, Removed}` with the full post-apply doc + projected `$set`/`$unset` ops per record. `limit+1` sentinel absorbs single-arrival shifts without re-querying; overflow closes with `ErrSubscriptionOverflow`, drift past `DriftBudgetPercent` (default 30%) closes with `ErrSubscriptionDrifted` — resubscribe to recover
- Local writes + inbound sync replay through one apply primitive
- Cold restore, watermarked replay (`MaxAddSeq`), parked-change drainer for missing schema dependencies
- Tech space (per-account derived index of all spaces); spaces stay resident and are eager-loaded on boot
- Space lifecycle: `Create` / `Get` / `List` / `Delete` / `Derive` / `OneToOne` / `Join`
- Collaboration v1 — `ACL`: `CreateInvite` / `RevokeInvite` / `RevokeAllInvites` / `AcceptRequest` / `DeclineRequest` / `ChangePermissions` / `AddAccounts` / `RemoveAccounts` / `OwnershipChange` / `RequestSelfRemove` / `CancelJoinRequest` / `StopSharing`; `Members`: `List` / `Get` / `Me` / `JoinRequests` / `Invites` (recovers the minting account's invite key from synced custody) / `Subscribe` / `Query`
- Account identity: `Account.Metadata` and `Account.UpdateMetadata`, persisted in the tech space and republished to identityRepo on boot; per-member profile refetch

Not wired yet:
- `Space.SyncStatus` — interface is a stub (`Overall`, `Object`, `Peers`, `Subscribe`)
- `Service.Subscribe` (space-list events)
- `TypesAPI.Delete` (`RemoveProperty` and `PatchProperty` are wired; `Delete` still a stub)
- `PropertiesAPI.SetAccount` / `SetDevice` / `AttachType` / `DetachType`
- Versioning APIs / change history (`internal/versioning` is doc-only)
- Files / blobs (deferred; see `docs/07-files.md`)

## Specs

See [`docs/`](docs/):

- `00-common-context.md` — the layer model and design principles
- `01-auth-module.md` — wallet / provider contract
- `02-tech-space.md` — derived per-account index
- `03-space.md` — space lifecycle, ACL surface
- `04-object.md` — object lifecycle
- `05-crdt.md`, `05a-crdt-spec.md` — apply algorithm and invariants
- `06-data-structure.md` — record shape, datasets, variants
- `07-files.md` — file/blob handling (deferred)
- `08-versioning.md` — schema evolution, versioning hooks
- `17-user-datasets.md` — runtime dataset schemas, generic schema handler, batch upsert

## Compatibility note

The on-the-wire `header.SpaceType` is constrained to the any-sync-coordinator's allow-list: the `any` product's `any.space` / `any.techspace` / `any.onetoone` (fileproto v2 required) plus anytype's `anytype.space`, `anytype.techspace`, `anytype.chatspace`, `anytype.onetoone`. The SDK mints only the any.* family (anytype.* spaces belong to heart clients; the SDK can join them but never creates them) — see `docs/03-space.md § Space type strings`.

