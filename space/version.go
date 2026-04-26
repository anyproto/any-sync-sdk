package space

import "github.com/anyproto/any-sync-sdk/internal/crdt"

// VersionId is the lexicographically-sortable local ordering key
// maintained by any-sync's tree storage (= its orderId). It is
// peer-local: scoped to one peer's view of one object tree, NOT a
// globally-consistent identity.
//
// See internal/crdt.VersionId for the full contract. This alias makes
// the type available to middleware without exposing the internal
// package.
type VersionId = crdt.VersionId

// CompareVersion returns -1, 0, or +1 for a < b, a == b, and a > b.
// The empty string is the "no version" sentinel and sorts less than
// any non-empty version.
func CompareVersion(a, b VersionId) int { return crdt.CompareVersion(a, b) }
