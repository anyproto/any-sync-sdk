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
// missing (then returns the defaultKey of the node where the walk fell off,
// or "" if that node has none — sufficient because splitting a covered
// entry propagates the inherited version onto created intermediates, see
// setVersion).
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
		// A missing key under a node with a defaultKey is COVERED by that
		// default — the new intermediate must inherit it, or lookups for
		// its unenumerated siblings would drop from the inherited version
		// to "" and gate differently than on peers holding explicit state.
		child = arena.NewObject()
		if def := node.Get(defaultKey); def != nil && def.Type() == anyenc.TypeString {
			child.Set(defaultKey, def)
		}
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

// setVersionNodeAt writes an arbitrary `_ver` node (string or object) at
// `path`, with the same collapsed-ancestor expansion and default
// propagation as setVersion. Used by the broad-write merge path, which
// computes a whole subtree node rather than a single leaf version.
func setVersionNodeAt(arena *anyenc.Arena, record *anyenc.Value, node *anyenc.Value, path []string) {
	if record == nil || len(path) == 0 {
		return
	}
	o := record.Get(VersionsKey)
	if o == nil || o.Type() != anyenc.TypeObject {
		o = arena.NewObject()
		record.Set(VersionsKey, o)
	}
	for len(path) > 1 {
		key := path[0]
		path = path[1:]
		child := o.Get(key)
		switch {
		case child == nil:
			child = arena.NewObject()
			if def := o.Get(defaultKey); def != nil && def.Type() == anyenc.TypeString {
				child.Set(defaultKey, def)
			}
			o.Set(key, child)
		case child.Type() == anyenc.TypeString:
			expanded := arena.NewObject()
			expanded.Set(defaultKey, arena.NewStringBytes(child.GetStringBytes()))
			o.Set(key, expanded)
			child = expanded
		case child.Type() == anyenc.TypeObject:
			// descend
		default:
			child = arena.NewObject()
			o.Set(key, child)
		}
		o = child
	}
	o.Set(path[0], node)
}

// gateVersion resolves the gating context for a $set/$unset at `path`.
// When the path resolves to a single authoritative version — an explicit
// string entry, a collapsed ancestor, a `*` default, or "" when
// untracked — it returns (version, nil). When the `_ver` entry AT the
// path is an object, finer-grained writes exist below it: there is no
// single gate and the caller must merge per-leaf (see mergeReplace), so
// it returns ("", node).
func gateVersion(record *anyenc.Value, path []string) (VersionId, *anyenc.Value) {
	if record == nil {
		return "", nil
	}
	cur := record.Get(VersionsKey)
	for _, key := range path {
		if cur == nil {
			return "", nil
		}
		switch cur.Type() {
		case anyenc.TypeString:
			// Collapsed ancestor — applies to everything at and below.
			return VersionId(cur.GetStringBytes()), nil
		case anyenc.TypeObject:
			next := cur.Get(key)
			if next != nil {
				cur = next
				continue
			}
			if def := cur.Get(defaultKey); def != nil {
				return VersionId(def.GetStringBytes()), nil
			}
			return "", nil
		default:
			return "", nil
		}
	}
	if cur == nil {
		return "", nil
	}
	switch cur.Type() {
	case anyenc.TypeString:
		return VersionId(cur.GetStringBytes()), nil
	case anyenc.TypeObject:
		return "", cur
	}
	return "", nil
}

// IsCollapsible reports whether `node` is an object that can be replaced
// with a single string version without changing any lookup. Required: the
// defaultKey is present and every entry is a string holding the same
// version. Only a node that already claims authority over unenumerated
// siblings (via `*`) may become a collapsed string, which claims the same
// authority — collapsing sibling enumerations WITHOUT a default would
// invent a version for never-written fields and gate out concurrent
// older writes to them, diverging from peers that applied those writes
// before the collapse.
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
	if !ok || count == 0 || !hasDefault {
		return "", false
	}
	return common, true
}

// compactVersions rewrites a record's `_ver` map to its canonical
// compact form. Only lossless transformations are applied: every
// GetRecordVersion lookup — explicit or unenumerated — returns exactly
// the same version before and after compaction. Inventing a `*`
// default for paths no write ever claimed is forbidden: it would gate
// out concurrent older writes to fresh fields and diverge from peers
// that applied those writes before compacting.
//
// Two transformations, applied bottom-up in one pass:
//
//  1. Redundant-entry drop: inside an object that has a defaultKey,
//     explicit string entries equal to the default are removed —
//     lookup falls back to the default with the same result.
//
//  2. Subtree collapse: any inner object whose entries (post-recursion)
//     are all strings equal to its own defaultKey is replaced in its
//     parent with that single string version. The defaultKey already
//     claims authority over every unenumerated sibling, so the
//     collapsed string claims nothing new.
//
// Tombstones are short-circuited — they're already in canonical
// `{id, *}` shape and are sticky.
//
// The set of versionIds present in `_ver` is unchanged (a dropped
// entry's version survives in the default it equaled), so trace GC at
// §3.6 agrees with itself before and after compaction. `_ver.id` and
// `_ver` root object-ness are preserved.
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
	compactSubtree(v, true)
}

// compactSubtree applies the two lossless compaction rules bottom-up:
// recurse into object children, replace any child that became safely
// collapsible with the equivalent single-string version, then drop
// explicit string entries equal to the node's own defaultKey. In-place
// Set during Visit never resizes anyenc's underlying kvs slice, so the
// mutation is safe; deletions are collected first and applied after the
// Visit. Zero allocations when no child is an object and no defaultKey
// is present (the common case — most `_ver` maps are flat).
//
// isRoot guards `_ver.id`: the creation marker stays explicit even when
// it equals a root default (tombstones never reach here, but the
// invariant is cheap to keep).
func compactSubtree(node *anyenc.Value, isRoot bool) {
	obj, _ := node.Object()
	obj.Visit(func(k []byte, v *anyenc.Value) {
		if v.Type() != anyenc.TypeObject {
			return
		}
		compactSubtree(v, false)
		if _, ok := IsCollapsible(v); ok {
			// Reuse the child's own defaultKey string node — same arena,
			// no allocation.
			node.Set(string(k), v.Get(defaultKey))
		}
	})
	def := node.Get(defaultKey)
	if def == nil || def.Type() != anyenc.TypeString {
		return
	}
	defVer := def.GetStringBytes()
	var redundant []string
	obj, _ = node.Object()
	obj.Visit(func(k []byte, v *anyenc.Value) {
		key := string(k)
		if key == defaultKey || (isRoot && key == IdField) {
			return
		}
		if v.Type() == anyenc.TypeString && bytes.Equal(v.GetStringBytes(), defVer) {
			redundant = append(redundant, key)
		}
	})
	for _, k := range redundant {
		node.Del(k)
	}
}
