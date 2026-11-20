// Package types owns the types-and-properties machinery: the per-space
// Registry, the built-in `any` and `type` type objects, property
// definition schemas, schema compilation from property records, and the
// typePropertyHandler (implements crdt.Handler).
//
// The Registry is the single place that:
//
//   - caches compiled schemas per typeId
//   - tracks the known-shortIds set per type (the DataVersion gate)
//   - persists the per-space detached-changes collection
//   - resolves x-key → propId lookups
//   - emits OnShortIdAdded so higher layers can drain detached changes
//
// Not public — space.TypesAPI wraps the registry for caller use. See
// docs/data-structure.md §"Types, Properties & Data Schemas" and
// docs/types-properties-proposal.md.
package types
