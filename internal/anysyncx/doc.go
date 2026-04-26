// Package anysyncx is the sole importer of github.com/anyproto/any-sync.
// All other packages in this SDK reach any-sync through this adapter,
// so any-sync upgrades have a single blast radius.
//
// Wraps:
//
//   - SpaceService      — Create / Join / Derive spaces
//   - ObjectTree        — content-addressable DAG + AddContent
//   - ocache            — TTL-based live-instance cache for spaces/objects
//   - ACL client        — invite, accept/decline, change permissions,
//     ownership transfer, self-remove
//   - KeyValue service  — per-space KV (used by techspace chat-read tracking)
//   - Identity repo     — encrypted user metadata
//
// Re-exports the any-sync types the rest of the SDK needs so downstream
// packages never import any-sync directly.
package anysyncx
