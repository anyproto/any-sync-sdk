# any-sync-sdk — Common Context

any-sync-sdk is a Go library over `any-sync` and `any-store`. It hides
offline-first, end-to-end-encrypted, peer-to-peer sync behind queries
over a local store, writes, and live subscriptions. It runs in-process
inside a middleware layer, not in end-user clients.

## Stack

```
┌─────────────────────────────────────┐
│  Clients  (JS, mobile, desktop)     │
└─────────────────────────────────────┘
              │  gRPC or REST
              ▼
┌─────────────────────────────────────┐
│  Middleware                         │
│  - API (gRPC/REST) endpoints        │
│  - Product-level logic              │
│  - Sessions                         │
│  - Client-local auth (app login)    │
│  - Payments                         │
│  - Transport framing                │
└─────────────────────────────────────┘
              │  Go in-process
              ▼
┌─────────────────────────────────────┐
│  any-sync-sdk                       │
│  - Queries over any-store           │
│  - Writes (CRDT ops + versionId)    │
│  - Live subscriptions               │
└─────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────┐
│  any-sync  +  any-store             │
└─────────────────────────────────────┘
```

The SDK's caller is the middleware:

- The public API is Go interfaces and Go types. The SDK has no gRPC,
  REST or JSON wire format; middleware serializes for its own protocol.
- Sessions, payments, product logic and client-local auth (app login,
  passwords, biometrics) belong to middleware. The auth module only
  consumes private keys ([auth-module.md](auth-module.md)).
- Subscription events carry each record's full post-apply document and
  its changes projected to `$set` / `$unset`
  ([crdt-spec.md](crdt-spec.md) §13), so middleware can forward
  them to clients that have no CRDT engine.
- Errors are Go errors; middleware maps them to its protocol's error
  responses.

## Upstream libraries

- **any-sync** (`github.com/anyproto/any-sync`) — sync engine: spaces,
  ACL, object trees (DAG), key-value store, crypto, sync protocols.
- **any-store** (`github.com/anyproto/any-store`) — embedded document
  database with a MongoDB-style query language, indexes, streaming
  iterators and transactions.

| Concept | Where it lives | Summary |
|---------|----------------|---------|
| Account keys | `any-sync/util/crypto`, `accountservice` | Ed25519 keys derived via BIP-39 / SLIP-10 |
| Space | `any-sync/commonspace` | permission-controlled encrypted container for objects |
| ACL | `any-sync/commonspace/object/acl` | immutable record chain governing membership and permissions |
| Object tree | `any-sync/commonspace/object/tree/objecttree` | content-addressed DAG of signed, encrypted changes |
| Key-value | `any-sync/commonspace/object/keyvalue` | per-space KV with diff-based sync and LWW merge |
| any-store | `any-store` | document collections, filters, modifiers, indexes |
| anyenc | `any-store/anyenc` | binary encoding for JSON-like values, arena-based |

## Doc map

1. [Auth module](auth-module.md) — account and device keys
2. [Tech space](tech-space.md) — per-account index of spaces and settings
3. [Space](space.md) — lifecycle, joining, ACL
4. [Object](object.md) — object trees and changes
5. [CRDT](crdt.md) — record stores and field-level operations; [full spec](crdt-spec.md)
6. [Data structure](data-structure.md) — records, datasets, queries, property scopes
7. [Files](files.md) — file storage
8. [Versioning](versioning.md) — handler and schema versions, re-indexing
9. [Sync status](sync-status-proposal.md) — per-space, per-object and per-peer sync state

Feature designs (one-to-one spaces, identities, invites, datasets, p2p,
read tracking, version history, bundles and others) have their own
documents in this directory.
