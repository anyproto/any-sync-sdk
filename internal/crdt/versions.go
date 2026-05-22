package crdt

import (
	"bytes"

	"github.com/anyproto/any-store/v2/anyenc"
)

// VersionsKey is the reserved field on every record holding the per-field
// version map (`_ver` in the spec).
const VersionsKey = "_ver"

// defaultKey is the reserved key inside a `_ver` subtree object that holds
// the "default version for any sibling not explicitly enumerated at this
// level".
//
// This is how we encode collapsing without losing precision: a fully
// collapsed subtree is a single string; a partially enumerated subtree is an
// object with `defaultKey` carrying the inherited version and explicit keys
// for the fields whose version has since diverged.
//
// Lookup walks the tree, descending into objects until it either lands on
// the requested key (then returns that subtree's max version) or the key is
// missing (then returns the closest ancestor's defaultKey, or "" if none).
//
// `*` is chosen because:
//   - it's visually distinct from real field names ("this applies to any
//     sibling here"),
//   - it's extraordinarily unlikely as a user field name in practice,
//   - it only appears inside the protocol-owned `_ver` map, never in the
//     user-facing record body, so no field-name reservation is needed,
//   - unlike the empty string, it avoids anyenc's special `emptyKey` byte
//     encoding.
const defaultKey = "*"

// GetRecordVersion returns the version stored in record._ver for the given
// path. Returns "" when no information exists for the path.
func GetRecordVersion(record *anyenc.Value, path ...string) VersionId {
	if record == nil {
		return ""
	}
	o := record.Get(VersionsKey)
	if o == nil {
		return ""
	}
	return getVersion(o, path)
}

// getVersion walks the versions tree to resolve `path`. The returned version
// is the strictest upper bound: when the walk lands inside an explicit
// subtree it returns the maximum leaf version below that subtree; when the
// walk falls off into unenumerated territory it returns the closest
// defaultKey.
func getVersion(node *anyenc.Value, path []string) VersionId {
	cur := node
	for _, key := range path {
		if cur == nil {
			return ""
		}
		switch cur.Type() {
		case anyenc.TypeString:
			// Collapsed ancestor — applies to everything at and below this point.
			return VersionId(cur.GetStringBytes())
		case anyenc.TypeObject:
			next := cur.Get(key)
			if next != nil {
				cur = next
				continue
			}
			// Missing key — fall back to the local default.
			def := cur.Get(defaultKey)
			if def == nil {
				return ""
			}
			// The default is always a string by construction.
			return VersionId(def.GetStringBytes())
		default:
			return ""
		}
	}
	// Path consumed.
	return maxVersion(cur)
}

// maxVersion returns the maximum version present in node (a single string,
// or the max leaf inside an object subtree, including the default key).
func maxVersion(node *anyenc.Value) VersionId {
	if node == nil {
		return ""
	}
	switch node.Type() {
	case anyenc.TypeString:
		return VersionId(node.GetStringBytes())
	case anyenc.TypeObject:
		var best VersionId
		obj, _ := node.Object()
		obj.Visit(func(_ []byte, v *anyenc.Value) {
			if c := maxVersion(v); c > best {
				best = c
			}
		})
		return best
	}
	return ""
}

// SetRecordVersion writes `version` to record._ver at `path` (creating _ver
// if missing) and applies the bottom-up collapse rule. Use after a
// successful gated mutation.
//
// Setting at the leaf overwrites whatever was at that exact path (string or
// subtree). Intermediate string nodes on the path are expanded into objects
// with a defaultKey holding the previous string, preserving inherited
// versions for sibling fields.
func SetRecordVersion(arena *anyenc.Arena, record *anyenc.Value, version VersionId, path ...string) {
	if record == nil {
		return
	}
	o := record.Get(VersionsKey)
	if o == nil || o.Type() != anyenc.TypeObject {
		o = arena.NewObject()
		record.Set(VersionsKey, o)
	}
	setVersion(arena, o, version, path)
}

// setVersion mutates `node` in place to record `version` at `path`. `node`
// must be an object (the caller guarantees this for the root call). For
// recursive descent, when an intermediate value is a string, we expand it
// into an object `{ defaultKey: <oldString> }` and continue.
func setVersion(arena *anyenc.Arena, node *anyenc.Value, version VersionId, path []string) {
	if len(path) == 0 {
		// Caller asked to overwrite at root — should not happen because
		// SetRecordVersion always passes at least one path component (a
		// field name). Guard anyway: collapsing the whole _ver map to a
		// single string would require replacing the parent reference, which
		// we can't do here.
		return
	}
	key := path[0]
	rest := path[1:]
	if len(rest) == 0 {
		// Leaf: overwrite (drop any existing subtree at this key).
		// Bottom-up collapse is deferred to compactVersions, run once
		// per change in the modifier — keeps the per-op write path
		// cheap on cold-restore replay.
		node.Set(key, arena.NewString(string(version)))
		return
	}
	child := node.Get(key)
	switch {
	case child == nil:
		child = arena.NewObject()
		node.Set(key, child)
	case child.Type() == anyenc.TypeString:
		// Expand: { defaultKey: oldVersion }
		old := arena.NewStringBytes(child.GetStringBytes())
		expanded := arena.NewObject()
		expanded.Set(defaultKey, old)
		node.Set(key, expanded)
		child = expanded
	case child.Type() == anyenc.TypeObject:
		// fine, descend
	default:
		// Unexpected — replace with fresh object.
		child = arena.NewObject()
		node.Set(key, child)
	}
	setVersion(arena, child, version, rest)
}

// IsCollapsible reports whether `node` is an object that can be replaced
// with a single string version. Required: every entry is a string, every
// entry holds the same version, and (≥2 entries OR the defaultKey is
// present). The defaultKey requirement makes the collapse lossless (the
// object already claimed authority over unenumerated siblings). The ≥2
// requirement preserves spec §3.2's narrow-path semantic — a single
// $set on "a.b.c" must stay `{a:{b:{c:v}}}`, not collapse all the way
// up to `{a:v}`.
func IsCollapsible(node *anyenc.Value) (VersionId, bool) {
	if node == nil || node.Type() != anyenc.TypeObject {
		return "", false
	}
	obj, _ := node.Object()
	var common VersionId
	count := 0
	hasDefault := false
	ok := true
	obj.Visit(func(k []byte, v *anyenc.Value) {
		if !ok {
			return
		}
		if v.Type() != anyenc.TypeString {
			ok = false
			return
		}
		s := VersionId(v.GetStringBytes())
		if count == 0 {
			common = s
		} else if s != common {
			ok = false
			return
		}
		count++
		if string(k) == defaultKey {
			hasDefault = true
		}
	})
	if !ok || count == 0 {
		return "", false
	}
	if count == 1 && !hasDefault {
		return "", false
	}
	return common, true
}

// compactVersions rewrites a record's `_ver` map to its canonical
// compact form (spec §3.2 — "if every field under a subtree shares the
// same version, only the subtree root is tracked").
//
// Two transformations, applied bottom-up in one pass:
//
//  1. Subtree collapse: any inner object whose entries (post-recursion)
//     are all strings with the same version, and which has either the
//     defaultKey present or ≥2 entries, is replaced in its parent with
//     a single string version.
//
//  2. Root sibling factor: at the `_ver` root, ≥2 non-`id` string
//     siblings sharing a version are removed and replaced with a
//     single `*: version` default. `_ver.id` is never touched (it's
//     the creation marker, spec §3.5). Skipped when `*` already
//     exists at root (delete tombstones already use this shape).
//
// Tombstones are short-circuited — they're already in canonical
// `{id, *}` shape and are sticky.
//
// Invariants preserved:
//   - GetRecordVersion(rec, path) is unchanged for every path that
//     already had an explicit entry. Unenumerated paths may switch
//     from "" to a `*` default (the documented broader-claim shift).
//   - The set of versionIds present in `_ver` is unchanged — every
//     factored versionId still appears via `*`. Trace GC at §3.6
//     therefore agrees with itself before and after compaction.
//   - `_ver.id` and `_ver` root object-ness are preserved.
func compactVersions(arena *anyenc.Arena, rec *anyenc.Value) {
	if rec == nil {
		return
	}
	if rec.Get(DeletedAtField) != nil {
		return
	}
	v := rec.Get(VersionsKey)
	if v == nil || v.Type() != anyenc.TypeObject {
		return
	}
	compactSubtree(arena, v)
	factorRootSiblings(arena, v)
}

// compactSubtree walks `node`'s object children, recurses into each,
// and replaces any child that became safely collapsible with the
// equivalent single-string version. Single-pass: the in-place Set on
// the current key never resizes anyenc's underlying kvs slice, so
// mutation during Visit is safe. Zero allocations when no child is
// an object (the common case — most `_ver` maps are flat).
func compactSubtree(arena *anyenc.Arena, node *anyenc.Value) {
	obj, _ := node.Object()
	obj.Visit(func(k []byte, v *anyenc.Value) {
		if v.Type() != anyenc.TypeObject {
			return
		}
		compactSubtree(arena, v)
		if collapsed, ok := IsCollapsible(v); ok {
			node.Set(string(k), arena.NewString(string(collapsed)))
		}
	})
}

// rootFactorBufSize is the on-stack capacity of the duplicate-detection
// buffer used by factorRootSiblings. Records with more than this many
// top-level `_ver` string siblings fall back to the slow path (rare in
// practice — typical records have ≤ a dozen fields).
const rootFactorBufSize = 16

// factorRootSiblings: at the `_ver` root, factor the most common
// version among non-`id`, non-`*` string siblings into the `*`
// defaultKey. Only fires when (a) `*` is absent and (b) some version
// occurs in ≥2 such siblings. Ties on count are broken
// lexicographically by version (deterministic across peers and runs).
//
// Hot path is allocation-free: a stack-sized buffer detects whether
// any two siblings share a version, and we return early when none do.
// Map allocation happens only when a duplicate is confirmed.
func factorRootSiblings(arena *anyenc.Arena, root *anyenc.Value) {
	if root.Get(defaultKey) != nil {
		return
	}
	obj, _ := root.Object()

	var seenBuf [rootFactorBufSize][]byte
	seen := seenBuf[:0]
	foundDup := false
	overflowed := false
	obj.Visit(func(k []byte, v *anyenc.Value) {
		if foundDup || overflowed {
			return
		}
		if string(k) == IdField || string(k) == defaultKey {
			return
		}
		if v.Type() != anyenc.TypeString {
			return
		}
		s := v.GetStringBytes()
		for _, prev := range seen {
			if bytes.Equal(prev, s) {
				foundDup = true
				return
			}
		}
		if len(seen) < cap(seen) {
			seen = append(seen, s)
		} else {
			overflowed = true
		}
	})
	if !foundDup && !overflowed {
		return
	}
	factorRootSlow(arena, root)
}

// factorRootSlow performs the full count-based selection. Allocates a
// map keyed by versionId — only called when factorRootSiblings has
// already confirmed there is work to do (or the fast path overflowed).
func factorRootSlow(arena *anyenc.Arena, root *anyenc.Value) {
	counts := map[string]int{}
	obj, _ := root.Object()
	obj.Visit(func(k []byte, v *anyenc.Value) {
		if string(k) == IdField || string(k) == defaultKey {
			return
		}
		if v.Type() != anyenc.TypeString {
			return
		}
		counts[string(v.GetStringBytes())]++
	})
	winner := ""
	winnerCount := 0
	for ver, c := range counts {
		if c > winnerCount || (c == winnerCount && (winner == "" || ver < winner)) {
			winner = ver
			winnerCount = c
		}
	}
	if winnerCount < 2 {
		return
	}
	var toRemove []string
	obj.Visit(func(k []byte, v *anyenc.Value) {
		if string(k) == IdField || string(k) == defaultKey {
			return
		}
		if v.Type() == anyenc.TypeString && string(v.GetStringBytes()) == winner {
			toRemove = append(toRemove, string(k))
		}
	})
	for _, k := range toRemove {
		root.Del(k)
	}
	root.Set(defaultKey, arena.NewString(winner))
}
