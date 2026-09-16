// Package crdt implements the version-gated record store CRDT defined in
// docs/crdt.md and docs/crdt-spec.md.
//
// This is the Phase 1 in-memory implementation: the apply algorithm,
// _ver (per-field order map) handling, and operation semantics, all expressed
// over anyenc values. any-store persistence and any-sync DAG integration
// live in later phases.
package crdt
