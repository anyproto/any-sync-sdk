# any-sync-sdk — Common Context

## Goal
Create a Go library wrapping `any-sync` with a high-level API that hides offline-first / E2E-encrypted / P2P sync complexity. The SDK exposes queries over `any-store` plus an event flow. It is consumed **in-process by a middleware layer**, not directly by end-user clients.

## Full Stack

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
│  any-sync-sdk  (THIS PROJECT)       │
│  - Queries over any-store           │
│  - Writes (CRDT ops + versionId)    │
│  - Event stream                     │
│  - Offline-first, E2E, P2P hidden   │
└─────────────────────────────────────┘
              │
              ▼
┌─────────────────────────────────────┐
│  any-sync  +  any-store             │
└─────────────────────────────────────┘
```

### What this means for the SDK
- **"Caller" = middleware**, not end-user client. The SDK's external API is a Go interface consumed in-process.
- **No transport** in the SDK — no gRPC, no REST, no JSON wire format. Middleware handles all of that.
- **No session management, no payments, no product logic** — middleware owns these concerns.
- **Client-local auth (app login, passwords, biometrics)** lives in middleware. The SDK's auth module only consumes private keys and doesn't know how the caller obtained them.
- **Simplified event view** (Simplified events in CRDT spec §14.2) is a convenience, not a requirement — middleware can always transform raw ops before shipping them to clients over the wire.
- **Error handling** — SDK returns Go errors; middleware translates them into protocol-level error responses.

## Key Source Repos
- **any-sync** (`../any-sync`) — core sync engine: spaces, ACL, object trees (DAG), key-value, crypto, sync protocols.
- **any-store** (`../any-store`) — embedded document DB on SQLite with MongoDB-like query language, CBO query planner, streaming iterators, ACID transactions.

## Architectural Primitives

| Concept | Where it lives | One-liner |
|---------|---------------|-----------|
| Account keys | `any-sync/util/crypto`, `accountservice` | Ed25519 + AES-256-GCM derived via BIP-39 / SLIP-10 / SLIP-21 |
| Space | `any-sync/commonspace` | Permission-controlled encrypted container for objects |
| ACL | `any-sync/commonspace/object/acl` | Immutable record chain governing membership & permissions |
| Object tree | `any-sync/commonspace/object/tree/objecttree` | Content-addressable DAG of signed, encrypted changes |
| Key-value | `any-sync/commonspace/object/keyvalue` | Per-space KV with diff-based sync and LWW conflict resolution |
| any-store | `any-store/` | Document collections, filters, modifiers ($set, $inc, …), indexes |
| anyenc | `any-store/anyenc` | Fast binary encoding for JSON-like values (arena/pool based) |

## SDK Sections (to groom separately)
1. **Auth module** — key derivation, account identity
2. **Tech space** — account-level index of spaces, preferences
3. **Space** — creation, joining, ACL management
4. **Object** — content-addressable DAG, changes, encryption
5. **CRDT** — record sets, field-level operations ([full spec](05a-crdt-spec.md))
6. **Data structure** — queries, subscriptions, system collections, local/account settings layering
7. **Files** — file storage (filenode vs any-sync-native, TBD)
8. **Versioning** — handler/schema versions, re-indexing flow
9. **Sync status** — per-space/object/peer status tracking (separate subsystem, TBD)
