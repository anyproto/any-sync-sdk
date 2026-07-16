package history

import (
	"time"
)

// List-time coalescing (proposal §7.1): raw changes are keystroke-
// grained; UIs need human-scale versions. Consecutive changes collapse
// into one entry iff they form a linear chain (each the sole parent of
// the next), share the same author, and fall within a time window.
// Never across merges/branches — concurrent work stays visibly
// separate. The group's handle is its head (newest) ChangeId, which
// composes with causal cuts: viewing the handle includes the whole
// group. Nothing extra is stored; PrevIds ride the index rows.

// DefaultCoalesceWindow is the default author-session window.
const DefaultCoalesceWindow = 5 * time.Minute

// CoalesceOpts tunes grouping. Zero value = defaults.
type CoalesceOpts struct {
	// Window bounds the timestamp spread between ADJACENT group
	// members (author clock, display-only). <=0 = DefaultCoalesceWindow.
	Window time.Duration
}

// Coalesce groups a descending-OrderId ChangeMeta page. The result
// keeps descending order; each group is represented by its head
// (newest) entry with GroupSize set, Touched/TraceIds unioned, and
// PrevIds replaced by the OLDEST member's PrevIds so groups chain.
//
// Branch detection is batch-local: a change referenced as parent by
// two entries IN THIS PAGE never merges into either child's group.
// A branch whose other child is outside the page (filtered out or
// on a later page) is not detected — the group then spans a fork the
// UI can't see; acceptable for a display-level grouping, and the
// causal-cut handle stays correct either way.
func Coalesce(entries []ChangeMeta, opts CoalesceOpts) []ChangeMeta {
	if len(entries) == 0 {
		return entries
	}
	window := opts.Window
	if window <= 0 {
		window = DefaultCoalesceWindow
	}

	// Batch-local child counts: parents referenced by >1 entry are
	// fork points; never group across them.
	parentRefs := make(map[string]int, len(entries))
	for i := range entries {
		for _, p := range entries[i].PrevIds {
			parentRefs[p]++
		}
	}

	var out []ChangeMeta
	i := 0
	for i < len(entries) {
		head := entries[i]
		oldest := i
		for oldest+1 < len(entries) {
			newer := entries[oldest]
			older := entries[oldest+1]
			if !chainable(newer, older, window) || parentRefs[older.Version] > 1 {
				break
			}
			oldest++
		}
		if oldest > i {
			group := head // head entry represents the group
			group.GroupSize = oldest - i + 1
			group.PrevIds = entries[oldest].PrevIds
			// Cursoring resumes AFTER the oldest member, so a trimmed
			// coalesced page never re-lists part of a group.
			group.OrderId = entries[oldest].OrderId
			group.Touched = unionTouched(entries[i : oldest+1])
			group.TraceIds = unionTraces(entries[i : oldest+1])
			out = append(out, group)
		} else {
			out = append(out, head)
		}
		i = oldest + 1
	}
	return out
}

// chainable: older is the SOLE parent of newer, same author, and their
// author-clock timestamps sit within the window. (Descending input:
// newer precedes older.) A merge change (multiple PrevIds) never joins
// a group as the newer member; filtered-out intermediates break the
// PrevIds match naturally, so groups never span invisible changes.
func chainable(newer, older ChangeMeta, window time.Duration) bool {
	if newer.Author != older.Author {
		return false
	}
	if len(newer.PrevIds) != 1 || newer.PrevIds[0] != older.Version {
		return false
	}
	delta := newer.Timestamp - older.Timestamp
	if delta < 0 {
		delta = -delta
	}
	return time.Duration(delta)*time.Second <= window
}

func unionTouched(group []ChangeMeta) []TouchedRecord {
	type key struct{ ds, rec string }
	seen := make(map[key]int)
	var out []TouchedRecord
	for _, e := range group {
		for _, tr := range e.Touched {
			k := key{tr.Dataset, tr.RecordId}
			if idx, ok := seen[k]; ok {
				out[idx].Ops = unionStrings(out[idx].Ops, tr.Ops)
				continue
			}
			seen[k] = len(out)
			out = append(out, TouchedRecord{
				Dataset:  tr.Dataset,
				RecordId: tr.RecordId,
				Ops:      append([]string(nil), tr.Ops...),
			})
		}
	}
	return out
}

func unionTraces(group []ChangeMeta) []string {
	var out []string
	for _, e := range group {
		out = unionStrings(out, e.TraceIds)
	}
	return out
}

func unionStrings(dst, add []string) []string {
	for _, s := range add {
		found := false
		for _, have := range dst {
			if have == s {
				found = true
				break
			}
		}
		if !found {
			dst = append(dst, s)
		}
	}
	return dst
}
