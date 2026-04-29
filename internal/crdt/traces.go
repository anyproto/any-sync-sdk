package crdt

import "github.com/anyproto/any-store/v2/anyenc"

// TracesKey is the reserved field that holds per-versionId trace lists on a
// record. Shape: `{ "<versionId>": ["traceA", "traceB"], ... }`. Keyed by
// versionId purely for compaction — multiple fields stamped by the same
// change share one entry.
const TracesKey = "_traces"

// updateTraces reflects ch.TraceIds on the record's `_traces` map, then
// garbage-collects any entry whose versionId is no longer present in `_ver`.
//
// Rules:
//   - If this change's versionId landed somewhere in `_ver` and TraceIds is
//     non-empty → write `_traces[ch.VersionId] = ch.TraceIds`.
//   - If this change's versionId landed and TraceIds is empty → delete
//     `_traces[ch.VersionId]` (explicit clear).
//   - After those, drop every `_traces[v]` whose versionId is not present in
//     `_ver`. This keeps the map bounded by the number of distinct versions
//     currently live on the record.
//   - If `_traces` ends up empty, remove the key entirely.
func updateTraces(arena *anyenc.Arena, rec *anyenc.Value, ch Change) {
	if rec == nil {
		return
	}
	ver := rec.Get(VersionsKey)
	if ver == nil {
		rec.Del(TracesKey)
		return
	}
	inUse := collectVersionIds(ver)
	if len(inUse) == 0 {
		rec.Del(TracesKey)
		return
	}

	if _, landed := inUse[string(ch.VersionId)]; landed {
		if len(ch.TraceIds) == 0 {
			if t := rec.Get(TracesKey); t != nil && t.Type() == anyenc.TypeObject {
				t.Del(string(ch.VersionId))
			}
		} else {
			t := rec.Get(TracesKey)
			if t == nil || t.Type() != anyenc.TypeObject {
				t = arena.NewObject()
				rec.Set(TracesKey, t)
			}
			arr := arena.NewArray()
			for i, s := range ch.TraceIds {
				arr.SetArrayItem(i, arena.NewString(s))
			}
			t.Set(string(ch.VersionId), arr)
		}
	}

	t := rec.Get(TracesKey)
	if t == nil || t.Type() != anyenc.TypeObject {
		return
	}
	obj, _ := t.Object()
	var stale []string
	obj.Visit(func(k []byte, _ *anyenc.Value) {
		if _, ok := inUse[string(k)]; !ok {
			stale = append(stale, string(k))
		}
	})
	for _, k := range stale {
		t.Del(k)
	}
	obj, _ = t.Object()
	empty := true
	obj.Visit(func(_ []byte, _ *anyenc.Value) { empty = false })
	if empty {
		rec.Del(TracesKey)
	}
}

// collectVersionIds walks a `_ver` subtree and returns the set of all string
// version values (both explicit keys and `*` defaults).
func collectVersionIds(verNode *anyenc.Value) map[string]struct{} {
	out := make(map[string]struct{})
	collectVersionIdsInto(verNode, out)
	return out
}

func collectVersionIdsInto(node *anyenc.Value, out map[string]struct{}) {
	if node == nil {
		return
	}
	switch node.Type() {
	case anyenc.TypeString:
		if s := string(node.GetStringBytes()); s != "" {
			out[s] = struct{}{}
		}
	case anyenc.TypeObject:
		obj, _ := node.Object()
		obj.Visit(func(_ []byte, v *anyenc.Value) {
			collectVersionIdsInto(v, out)
		})
	}
}
