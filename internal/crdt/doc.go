// Package crdt implements the version-gated record store CRDT defined in
// docs/crdt.md and docs/crdt-spec.md.
//
// It owns the apply algorithm, _ver (per-field order map) handling and
// operation semantics over anyenc values, persisted to any-store inside
// one WriteTx per change.
package crdt
