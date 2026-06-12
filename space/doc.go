// Package space is the public caller surface of the SDK. Everything
// middleware reaches for lives here:
//
//   - Space, SpaceService   — lifecycle: Create / Join / Derive / Delete
//   - ACL                   — invite, accept/decline, change perms, ownership
//   - Members               — members system collection
//   - Query, Subscription   — read + event flow over any-store
//   - Agg                   — MongoDB-style aggregation pipelines (snapshot-only)
//   - ModifyBatch           — writes (CRDT ops) returning VersionId
//   - TypesAPI              — type objects and property definitions
//   - PropertiesAPI         — per-scope property writes (base/account/device)
//   - SyncStatus            — per-space/object/peer status accessor
//   - Indexer               — seam the techspace implementation plugs into
//   - VersionId             — re-exported alias of internal/crdt.VersionId
//
// See docs/03-space.md, docs/06-data-structure.md, and docs/05-crdt.md.
package space
