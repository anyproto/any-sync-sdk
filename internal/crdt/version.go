package crdt

import (
	"cmp"
	"strings"

	"github.com/anyproto/lexid"
)

// localVersionGen mints monotonic lexids for device-local fields (see
// Change.Local / Object.LocalSet). Same parameters as any-sync's tree
// allocator and internal/object's VersionAllocator so the values share
// one comparable order space with any-sync's OrderIds — a local field's
// version is always taken as NextVersion of that field's own current
// version, so it advances monotonically without colliding with the
// disjoint synced-field namespace.
var localVersionGen = lexid.Must(lexid.CharsAllNoEscape, 4, 100)

// NextVersion returns a VersionId strictly greater than v in the shared
// lexid order. Empty v yields the smallest version. Used by the
// device-local write path to advance a local field past its current
// version (read from the record's _ver), which is correct because no
// synced change ever writes a local-namespace path to compete with it.
func NextVersion(v VersionId) VersionId {
	return VersionId(localVersionGen.Next(string(v)))
}

// LocalFieldPrefix marks a record field as device-local: written only by
// the Change.Local path, never synced, never gated against synced
// versions. Reserved like `id` / `_*` — see reserved_fields docs.
const LocalFieldPrefix = "~"

// IsLocalPath reports whether path targets a device-local field — its
// first segment carries LocalFieldPrefix. The synced apply path rejects
// these; the local path requires them.
func IsLocalPath(path []string) bool {
	return len(path) > 0 && strings.HasPrefix(path[0], LocalFieldPrefix)
}

// VersionId is a lexicographically-sortable local ordering key maintained by
// any-sync's tree storage (= its `orderId`). It is **local to one peer's view
// of one object tree**, not a globally-consistent identity:
//
//   - Scoped to one any-sync object tree — versionIds from different trees
//     are not comparable and must never be mixed.
//   - Peer-local — two peers holding the same logical DAG may encode the
//     "same" logical change with different versionId strings. The literal
//     value in one peer's `_ver` map is meaningless to another peer.
//   - Used in this package for two things: (1) gating in the CRDT apply
//     algorithm, which is always a LOCAL comparison within one peer's
//     Controller, and (2) as the ordered index key for efficient change
//     queries against the local store (Phase 2).
//
// Peer-local convergence: each Controller applies the changes delivered by
// its local any-sync, gating against its own (locally-consistent) versionIds.
// Cross-peer convergence happens because both peers eventually process the
// same DAG changes and reach the same logical record content — they do NOT
// happen to hold the same `_ver` strings, and nothing outside this peer's
// Controller should compare them to another peer's versionIds.
//
// Callers (middleware, and the clients middleware serves) DO see the `_ver`
// tree attached to query results and carried in subscription events —
// clients need per-field version info to reconcile their own optimistic
// in-memory state with the SDK's any-store. The tree shape documented in
// the spec (§3.1, §3.2) is the contract; clients walk it with the same
// lookup rules the SDK uses internally (see GetRecordVersion).
//
// The empty string is the "no version" sentinel and compares less than any
// real version inside the same tree.
//
// The CRDT layer treats VersionId as opaque and never invents versions —
// every value passed through ApplyChange comes from any-sync's local
// orderId for that peer.
type VersionId string

// CompareVersion returns -1, 0, or +1 for a < b, a == b, and a > b
// respectively. Provided as a documented entry point so the comparison
// contract is discoverable; internally just delegates to cmp.Compare.
// Callers inside the package can use raw `<` / `>` / `==` on VersionId
// directly since it's a string type.
//
// The empty string is the "no version" sentinel and sorts less than any
// non-empty version.
func CompareVersion(a, b VersionId) int {
	return cmp.Compare(a, b)
}
