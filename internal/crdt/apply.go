package crdt

import (
	"strings"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/anyenc/anyencutil"
)

// opFieldHeads returns the top-level field name(s) an op writes: the
// first path segment for a single-path op, or — for a multi-field
// $set/$unset (empty Path, object payload keyed by dotted paths) — the
// head segment of each payload key. Used by the controller's field-class
// enforcement to look each touched field up in the dataset schema.
func opFieldHeads(op Op) []string {
	if len(op.Path) > 0 {
		return []string{op.Path[0]}
	}
	if op.Payload == nil || op.Payload.Type() != anyenc.TypeObject {
		return nil
	}
	var heads []string
	obj, _ := op.Payload.Object()
	obj.Visit(func(k []byte, _ *anyenc.Value) {
		heads = append(heads, strings.SplitN(string(k), ".", 2)[0])
	})
	return heads
}

// applyOp dispatches one op to its arena-parameterized handler. All
// allocation goes through the provided arena (which comes from any-store's
// DocBuffer pool when running inside a modifier, or from the Controller's
// own arena in unit-test mode).
func applyOp(arena *anyenc.Arena, rec *anyenc.Value, ch Change, op Op) {
	switch op.Type {
	case OpSet:
		applySet(arena, rec, ch, op)
	case OpUnset:
		applyUnset(arena, rec, ch, op)
	case OpAddToSet:
		applyAddToSet(arena, rec, ch, op)
	case OpPull:
		applyPull(arena, rec, ch, op)
	case OpInc:
		applyInc(arena, rec, ch, op)
	case OpIncGated:
		applyIncGated(arena, rec, ch, op)
	case OpSetCreate:
		applySetCreate(arena, rec, ch, op)
	case OpDelete:
		// delete is handled at a higher level (buildModifier / applyDelete)
		// because it replaces the whole record rather than mutating it.
	}
}

// ----------------------------------------------------------------------------
// $set / $unset
// ----------------------------------------------------------------------------

func applySet(arena *anyenc.Arena, rec *anyenc.Value, ch Change, op Op) {
	if len(op.Path) == 0 {
		if op.Payload == nil || op.Payload.Type() != anyenc.TypeObject {
			return
		}
		obj, _ := op.Payload.Object()
		obj.Visit(func(k []byte, v *anyenc.Value) {
			path := strings.Split(string(k), ".")
			gatedSet(arena, rec, ch.VersionId, path, v)
		})
		return
	}
	if op.Payload == nil {
		return
	}
	gatedSet(arena, rec, ch.VersionId, op.Path, op.Payload)
}

func gatedSet(arena *anyenc.Arena, rec *anyenc.Value, version VersionId, path []string, value *anyenc.Value) {
	gate, subtree := gateVersion(rec, path)
	if subtree == nil {
		if gate >= version {
			return
		}
		setNested(arena, rec, path, cloneInto(arena, value))
		SetRecordVersion(arena, rec, version, path...)
		return
	}
	// Finer-grained _ver entries exist below the path: a broad write has
	// no single gate. Merge per-leaf — newer leaves survive, older parts
	// are replaced, and a `*` default claims everything unenumerated.
	merged, mergedVer, changed := mergeReplace(arena, rec.Get(path...), subtree, value, version)
	if !changed {
		return
	}
	if merged == nil {
		unsetNested(rec, path)
	} else {
		setNested(arena, rec, path, merged)
	}
	setVersionNodeAt(arena, rec, mergedVer, path)
}

func applyUnset(arena *anyenc.Arena, rec *anyenc.Value, ch Change, op Op) {
	if len(op.Path) == 0 {
		if op.Payload == nil || op.Payload.Type() != anyenc.TypeObject {
			return
		}
		obj, _ := op.Payload.Object()
		obj.Visit(func(k []byte, _ *anyenc.Value) {
			path := strings.Split(string(k), ".")
			gatedUnset(arena, rec, ch.VersionId, path)
		})
		return
	}
	gatedUnset(arena, rec, ch.VersionId, op.Path)
}

func gatedUnset(arena *anyenc.Arena, rec *anyenc.Value, version VersionId, path []string) {
	gate, subtree := gateVersion(rec, path)
	if subtree == nil {
		if gate >= version {
			return
		}
		unsetNested(rec, path)
		SetRecordVersion(arena, rec, version, path...)
		return
	}
	// Broad unset over finer-grained entries: same per-leaf merge as
	// gatedSet, with no incoming value.
	merged, mergedVer, changed := mergeReplace(arena, rec.Get(path...), subtree, nil, version)
	if !changed {
		return
	}
	if merged == nil {
		unsetNested(rec, path)
	} else {
		setNested(arena, rec, path, merged)
	}
	setVersionNodeAt(arena, rec, mergedVer, path)
}

// mergeReplace computes the result of a broad $set (newVal != nil) or
// $unset (newVal == nil) at version v over a position whose `_ver` entry
// is an object — finer-grained writes exist below the target path, so
// the write has no single gate and must merge per-leaf:
//
//   - existing parts whose version >= v survive with their values;
//   - existing parts whose version < v are replaced by the incoming
//     value (or removed when the incoming value doesn't cover them);
//   - the merged `_ver` node carries `*: v`, claiming every path not
//     explicitly enumerated, so a late-arriving older write to a key
//     this peer never saw gates against the broad write's authority;
//   - when nothing survives, the write takes whole-subtree authority:
//     the value is replaced outright and `_ver` collapses to string v.
//
// A non-object incoming value lands only when nothing survives —
// surviving newer leaves keep the position an object, mirroring how a
// finer newer write splits a collapsed older ancestor, so both delivery
// orders agree on the final shape.
//
// Returns the merged value (nil = position removed), the merged `_ver`
// node (string or object), and changed=false when the write was fully
// gated — callers must then leave the record untouched.
func mergeReplace(arena *anyenc.Arena, oldVal, verNode, newVal *anyenc.Value, v VersionId) (*anyenc.Value, *anyenc.Value, bool) {
	if def := verNode.Get(defaultKey); def != nil && def.Type() == anyenc.TypeString {
		if VersionId(def.GetStringBytes()) >= v {
			// The whole position is already claimed at >= v (explicit
			// entries are never older than their level's default).
			return oldVal, verNode, false
		}
	}
	newIsObj := newVal != nil && newVal.Type() == anyenc.TypeObject

	outVal := arena.NewObject()
	outVer := arena.NewObject()
	outVer.Set(defaultKey, arena.NewString(string(v)))
	survivors := 0

	verObj, _ := verNode.Object()
	verObj.Visit(func(kb []byte, entry *anyenc.Value) {
		k := string(kb)
		if k == defaultKey {
			return
		}
		var incoming *anyenc.Value
		if newIsObj {
			incoming = newVal.Get(k)
		}
		switch entry.Type() {
		case anyenc.TypeString:
			if VersionId(entry.GetStringBytes()) >= v {
				// Survivor: keeps its current value and version.
				if oldVal != nil {
					if ov := oldVal.Get(k); ov != nil {
						outVal.Set(k, ov)
					}
				}
				outVer.Set(k, entry)
				survivors++
				return
			}
			// Superseded: the incoming value replaces it when present;
			// the new `*` covers the version either way.
			if incoming != nil {
				outVal.Set(k, cloneInto(arena, incoming))
			}
		case anyenc.TypeObject:
			var childOld *anyenc.Value
			if oldVal != nil {
				childOld = oldVal.Get(k)
			}
			cv, cver, _ := mergeReplace(arena, childOld, entry, incoming, v)
			if cv != nil {
				outVal.Set(k, cv)
			}
			if cver != nil {
				if cver.Type() == anyenc.TypeString && VersionId(cver.GetStringBytes()) == v {
					// Fully replaced child — covered by the new `*`.
					return
				}
				outVer.Set(k, cver)
				survivors++
			}
		}
	})

	// Incoming keys without an explicit `_ver` entry were covered by the
	// old default, which is < v here — they land, covered by the new `*`.
	if newIsObj {
		nobj, _ := newVal.Object()
		nobj.Visit(func(kb []byte, nv *anyenc.Value) {
			k := string(kb)
			if verNode.Get(k) != nil {
				return // handled above
			}
			outVal.Set(k, cloneInto(arena, nv))
		})
	}

	if survivors == 0 {
		// Whole-subtree authority: collapsed version.
		switch {
		case newVal == nil:
			return nil, arena.NewString(string(v)), true
		case newIsObj:
			return outVal, arena.NewString(string(v)), true
		default:
			return cloneInto(arena, newVal), arena.NewString(string(v)), true
		}
	}
	// Survivors keep the position an object; a non-object incoming value
	// is superseded per-leaf and does not land.
	return outVal, outVer, true
}

// ----------------------------------------------------------------------------
// $setCreate (creation stamps, min-version-wins)
// ----------------------------------------------------------------------------

// applySetCreate applies a creation-stamp offer under the min-rule: the
// offer lands when the position is unclaimed OR its current `_ver` entry
// is NEWER than the offering change — i.e. the current value came from a
// later change, and this offer is closer to the record's true creation.
// Min is commutative and associative, so every peer converges to the
// causally-earliest offer in any delivery order — the same argument as
// lowerCreationMarker's `_ver.id` min-rule, applied per field.
//
// Stamps are scalar row-root fields, so a position with finer-grained
// `_ver` entries below it (subtree) never occurs for a well-behaved
// handler; if one exists the offer is skipped rather than merged.
func applySetCreate(arena *anyenc.Arena, rec *anyenc.Value, ch Change, op Op) {
	if len(op.Path) == 0 || op.Payload == nil {
		return
	}
	gate, subtree := gateVersion(rec, op.Path)
	if subtree != nil {
		return
	}
	if gate != "" && gate <= ch.VersionId {
		return
	}
	setNested(arena, rec, op.Path, cloneInto(arena, op.Payload))
	SetRecordVersion(arena, rec, ch.VersionId, op.Path...)
}

// ----------------------------------------------------------------------------
// $addToSet / $pull (commutative, _ver NOT updated)
// ----------------------------------------------------------------------------

func applyAddToSet(arena *anyenc.Arena, rec *anyenc.Value, ch Change, op Op) {
	if len(op.Path) == 0 || op.Payload == nil {
		return
	}
	if GetRecordVersion(rec, op.Path...) >= ch.VersionId {
		return
	}
	if existing := rec.Get(op.Path...); existing != nil && existing.Type() != anyenc.TypeArray {
		return
	}
	addToSet(arena, rec, op.Path, cloneInto(arena, op.Payload))
}

func applyPull(arena *anyenc.Arena, rec *anyenc.Value, ch Change, op Op) {
	if len(op.Path) == 0 || op.Payload == nil {
		return
	}
	if GetRecordVersion(rec, op.Path...) >= ch.VersionId {
		return
	}
	if existing := rec.Get(op.Path...); existing != nil && existing.Type() != anyenc.TypeArray {
		return
	}
	pullFromSet(arena, rec, op.Path, op.Payload)
}

// ----------------------------------------------------------------------------
// $inc / $incGated
// ----------------------------------------------------------------------------

func applyInc(arena *anyenc.Arena, rec *anyenc.Value, ch Change, op Op) {
	if len(op.Path) == 0 || op.Payload == nil || op.Payload.Type() != anyenc.TypeNumber {
		return
	}
	if GetRecordVersion(rec, op.Path...) >= ch.VersionId {
		return
	}
	if existing := rec.Get(op.Path...); existing != nil && existing.Type() != anyenc.TypeNumber {
		return
	}
	delta := op.Payload.GetFloat64()
	cur := getNumber(rec, op.Path)
	setNested(arena, rec, op.Path, arena.NewNumberFloat64(cur+delta))
}

func applyIncGated(arena *anyenc.Arena, rec *anyenc.Value, ch Change, op Op) {
	if len(op.Path) == 0 || op.Payload == nil || op.Payload.Type() != anyenc.TypeNumber {
		return
	}
	if GetRecordVersion(rec, op.Path...) >= ch.VersionId {
		return
	}
	if existing := rec.Get(op.Path...); existing != nil && existing.Type() != anyenc.TypeNumber {
		return
	}
	delta := op.Payload.GetFloat64()
	cur := getNumber(rec, op.Path)
	setNested(arena, rec, op.Path, arena.NewNumberFloat64(cur+delta))
	SetRecordVersion(arena, rec, ch.VersionId, op.Path...)
}

// ----------------------------------------------------------------------------
// Record lifecycle helpers (used by Controller.buildModifier)
// ----------------------------------------------------------------------------

// stampAddSeq records the change's any-sync AddSeq on the record root as
// _addSeq, advancing monotonically. An out-of-order replay (parked-change
// drain, cold-restore re-apply) can carry a lower AddSeq than the record
// already holds; the CRDT applies it idempotently, but the "changed since
// N" watermark must not regress, so we keep the max. A zero AddSeq (unit
// tests, device-local materialisations with no any-sync envelope) is a
// no-op — there's nothing meaningful to stamp.
func stampAddSeq(arena *anyenc.Arena, rec *anyenc.Value, addSeq uint64) {
	if rec == nil || addSeq == 0 {
		return
	}
	if cur := rec.Get(AddSeqField); cur != nil && cur.Type() == anyenc.TypeNumber {
		if uint64(cur.GetInt()) >= addSeq {
			return
		}
	}
	rec.Set(AddSeqField, arena.NewNumberInt(int(addSeq)))
}

// stampApplySeq records the change's apply sequence on the record root
// as _applySeq, advancing monotonically — the record-level twin of the
// per-object maxApplySeq watermark, used by consumer-side chunkers to
// scan "records changed since cursor N". Unlike _addSeq it advances on
// EVERY apply source (DAG, account mirror, local writes). Zero (no
// allocator wired) is a no-op.
func stampApplySeq(arena *anyenc.Arena, rec *anyenc.Value, applySeq uint64) {
	if rec == nil || applySeq == 0 {
		return
	}
	if cur := rec.Get(ApplySeqField); cur != nil && cur.Type() == anyenc.TypeNumber {
		if uint64(cur.GetInt()) >= applySeq {
			return
		}
	}
	rec.Set(ApplySeqField, arena.NewNumberInt(int(applySeq)))
}

// newRecord allocates an empty record with the _ver.id creation marker.
func newRecord(arena *anyenc.Arena, id string, version VersionId) *anyenc.Value {
	rec := arena.NewObject()
	rec.Set(IdField, arena.NewString(id))
	ver := arena.NewObject()
	ver.Set(IdField, arena.NewString(string(version)))
	rec.Set(VersionsKey, ver)
	return rec
}

// newTombstone builds a tombstone record, preserving the creation-version
// marker from an existing record if available.
func newTombstone(arena *anyenc.Arena, id string, ch Change, existing *anyenc.Value) *anyenc.Value {
	var preservedIdVersion VersionId
	if existing != nil {
		if v := existing.Get(VersionsKey); v != nil {
			if idVer := v.Get(IdField); idVer != nil && idVer.Type() == anyenc.TypeString {
				preservedIdVersion = VersionId(idVer.GetStringBytes())
			}
		}
	}
	if preservedIdVersion == "" {
		preservedIdVersion = ch.VersionId
	}
	tomb := arena.NewObject()
	tomb.Set(IdField, arena.NewString(id))
	tomb.Set(DeletedAtField, arena.NewNumberInt(int(ch.Timestamp)))
	o := arena.NewObject()
	o.Set(IdField, arena.NewString(string(preservedIdVersion)))
	o.Set(defaultKey, arena.NewString(string(ch.VersionId)))
	tomb.Set(VersionsKey, o)
	// Preserve _traces across delete. GC in updateTraces prunes entries for
	// versionIds that no longer appear in the shrunken _ver (typically keeps
	// only the creation-marker entry, plus the delete's own entry).
	if existing != nil {
		if t := existing.Get(TracesKey); t != nil && t.Type() == anyenc.TypeObject {
			tomb.Set(TracesKey, cloneInto(arena, t))
		}
	}
	// A delete is itself a change touching the object — carry the
	// AddSeq/ApplySeq onto the tombstone so it surfaces in "changed
	// since N" scans (consumer-side deletion streaming).
	stampAddSeq(arena, tomb, ch.AddSeq)
	stampApplySeq(arena, tomb, ch.ApplySeq)
	return tomb
}

// lowerCreationMarker applies the min-rule to `_ver.id`: if `version` is
// strictly smaller than the current marker (or the marker is absent), write
// it. Returns true if the marker was actually changed (used by the
// any-store modifier to decide whether to persist a tombstone update).
func lowerCreationMarker(arena *anyenc.Arena, rec *anyenc.Value, version VersionId) bool {
	ver := rec.Get(VersionsKey)
	if ver == nil || ver.Type() != anyenc.TypeObject {
		ver = arena.NewObject()
		rec.Set(VersionsKey, ver)
	}
	cur := ver.Get(IdField)
	if cur == nil || cur.Type() != anyenc.TypeString {
		ver.Set(IdField, arena.NewString(string(version)))
		return true
	}
	if version < VersionId(cur.GetStringBytes()) {
		ver.Set(IdField, arena.NewString(string(version)))
		return true
	}
	return false
}

// cloneInto recursively rebuilds v using the given arena.
func cloneInto(arena *anyenc.Arena, v *anyenc.Value) *anyenc.Value {
	if v == nil {
		return nil
	}
	switch v.Type() {
	case anyenc.TypeObject:
		obj := arena.NewObject()
		o, _ := v.Object()
		o.Visit(func(k []byte, vv *anyenc.Value) {
			obj.Set(string(k), cloneInto(arena, vv))
		})
		return obj
	case anyenc.TypeArray:
		arr := arena.NewArray()
		items, _ := v.Array()
		for i, it := range items {
			arr.SetArrayItem(i, cloneInto(arena, it))
		}
		return arr
	case anyenc.TypeString:
		return arena.NewStringBytes(v.GetStringBytes())
	case anyenc.TypeNumber:
		return arena.NewNumberFloat64(v.GetFloat64())
	case anyenc.TypeBinary:
		return arena.NewBinary(v.GetBytes())
	case anyenc.TypeTrue:
		return arena.NewTrue()
	case anyenc.TypeFalse:
		return arena.NewFalse()
	case anyenc.TypeNull:
		return arena.NewNull()
	}
	return nil
}

// hasDelete returns true if any op in the list is OpDelete.
func hasDelete(ops []Op) bool {
	for _, op := range ops {
		if op.Type == OpDelete {
			return true
		}
	}
	return false
}

// ----------------------------------------------------------------------------
// Value helpers (unchanged from Phase 1)
// ----------------------------------------------------------------------------

func setNested(arena *anyenc.Arena, record *anyenc.Value, path []string, value *anyenc.Value) {
	if len(path) == 0 {
		return
	}
	cur := record
	for i, key := range path {
		if i == len(path)-1 {
			cur.Set(key, value)
			return
		}
		next := cur.Get(key)
		if next == nil || next.Type() != anyenc.TypeObject {
			next = arena.NewObject()
			cur.Set(key, next)
		}
		cur = next
	}
}

func unsetNested(record *anyenc.Value, path []string) bool {
	if len(path) == 0 {
		return false
	}
	cur := record
	for i, key := range path {
		if i == len(path)-1 {
			if cur.Get(key) == nil {
				return false
			}
			cur.Del(key)
			return true
		}
		next := cur.Get(key)
		if next == nil || next.Type() != anyenc.TypeObject {
			return false
		}
		cur = next
	}
	return false
}

func addToSet(arena *anyenc.Arena, record *anyenc.Value, path []string, value *anyenc.Value) {
	parent, leaf := walkParent(arena, record, path)
	arr := parent.Get(leaf)
	if arr == nil || arr.Type() != anyenc.TypeArray {
		fresh := arena.NewArray()
		fresh.SetArrayItem(0, value)
		parent.Set(leaf, fresh)
		return
	}
	items, _ := arr.Array()
	for _, it := range items {
		if anyencutil.Equal(it, value) {
			return
		}
	}
	arr.SetArrayItem(len(items), value)
}

func pullFromSet(arena *anyenc.Arena, record *anyenc.Value, path []string, value *anyenc.Value) {
	if len(path) == 0 {
		return
	}
	parent, leaf := walkParentReadOnly(record, path)
	if parent == nil {
		return
	}
	arr := parent.Get(leaf)
	if arr == nil || arr.Type() != anyenc.TypeArray {
		return
	}
	items, _ := arr.Array()
	kept := make([]*anyenc.Value, 0, len(items))
	for _, it := range items {
		if !anyencutil.Equal(it, value) {
			kept = append(kept, it)
		}
	}
	if len(kept) == len(items) {
		return
	}
	fresh := arena.NewArray()
	for i, it := range kept {
		fresh.SetArrayItem(i, it)
	}
	parent.Set(leaf, fresh)
}

func walkParentReadOnly(record *anyenc.Value, path []string) (*anyenc.Value, string) {
	cur := record
	for i := 0; i < len(path)-1; i++ {
		next := cur.Get(path[i])
		if next == nil || next.Type() != anyenc.TypeObject {
			return nil, ""
		}
		cur = next
	}
	return cur, path[len(path)-1]
}

func walkParent(arena *anyenc.Arena, record *anyenc.Value, path []string) (*anyenc.Value, string) {
	cur := record
	for i := 0; i < len(path)-1; i++ {
		key := path[i]
		next := cur.Get(key)
		if next == nil || next.Type() != anyenc.TypeObject {
			next = arena.NewObject()
			cur.Set(key, next)
		}
		cur = next
	}
	return cur, path[len(path)-1]
}

func getNumber(record *anyenc.Value, path []string) float64 {
	v := record.Get(path...)
	if v == nil || v.Type() != anyenc.TypeNumber {
		return 0
	}
	return v.GetFloat64()
}
