package crdt

import (
	"github.com/anyproto/any-store/anyenc"
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
		node.Set(key, arena.NewString(string(version)))
		collapseObject(arena, node)
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
	collapseObject(arena, node)
}

// collapseObject collapses an object node to a single string when every
// entry (including defaultKey) holds the same version string. Currently a
// no-op — the only case that matters in v1 is a $set at exactly the field's
// path, which the leaf-overwrite branch above already handles. Aggressive
// bottom-up collapse is a future optimization; correctness does not depend
// on it.
func collapseObject(_ *anyenc.Arena, _ *anyenc.Value) {}

// IsCollapsible reports whether `node` is an object that can be losslessly
// represented as a single string version (every entry, including defaultKey
// if present, is the same string). Used by tests.
func IsCollapsible(node *anyenc.Value) (VersionId, bool) {
	if node == nil || node.Type() != anyenc.TypeObject {
		return "", false
	}
	obj, _ := node.Object()
	var common VersionId
	first := true
	ok := true
	obj.Visit(func(_ []byte, v *anyenc.Value) {
		if !ok {
			return
		}
		if v.Type() != anyenc.TypeString {
			ok = false
			return
		}
		s := VersionId(v.GetStringBytes())
		if first {
			common = s
			first = false
			return
		}
		if s != common {
			ok = false
		}
	})
	if !ok || first {
		return "", false
	}
	return common, true
}
