// Package history implements version history over any-sync object trees:
// structural diffs between reconstructed states (this file), on-demand
// causal replay, and the persistent history index. See
// docs/version-history-proposal.md.
package history

import (
	"sort"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/anyenc/anyencutil"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

// Version is the public version handle: a ChangeId (content-hash CID),
// stable across peers and restarts. OrderIds are never exposed as
// identity (docs/version-history-proposal.md §3.1).
type Version = string

// DiffKind classifies what happened to a record between two versions.
type DiffKind int

const (
	// KindAdded — absent (or tombstoned) at base, live at version.
	KindAdded DiffKind = iota + 1
	// KindRemoved — present at base, physically absent at version.
	// CRDT tombstones are sticky, so this only occurs across a history
	// horizon or a scope filter, never from normal applies.
	KindRemoved
	// KindChanged — live at both ends with differing content.
	KindChanged
	// KindDeleted — live (or absent) at base, tombstoned at version.
	KindDeleted
)

func (k DiffKind) String() string {
	switch k {
	case KindAdded:
		return "added"
	case KindRemoved:
		return "removed"
	case KindChanged:
		return "changed"
	case KindDeleted:
		return "deleted"
	default:
		return "unknown"
	}
}

// FieldDiff is one leaf-level difference. Nil Before/After means the
// field is absent on that side.
type FieldDiff struct {
	Path   []string
	Before *anyenc.Value
	After  *anyenc.Value
}

// RecordDiff is the per-record difference between two versions.
type RecordDiff struct {
	Id     string
	Kind   DiffKind
	Fields []FieldDiff
}

// DatasetDiff groups record diffs for one dataset.
type DatasetDiff struct {
	Dataset string
	Records []RecordDiff
}

// DiffResult is the object-level diff between two versions.
type DiffResult struct {
	Base     Version
	Version  Version
	Datasets []DatasetDiff
}

// Peer-local bookkeeping fields excluded from content comparison
// (proposal §5): version maps, trace maps, and apply/add watermarks are
// replay artifacts, not state.
var excludedFields = map[string]struct{}{
	crdt.VersionsKey:   {},
	crdt.TracesKey:     {},
	crdt.ApplySeqField: {},
	crdt.AddSeqField:   {},
}

// DiffRecords deep-compares two record values (either may be nil =
// absent) and classifies the result. A nil return means no difference.
func DiffRecords(id string, before, after *anyenc.Value) *RecordDiff {
	beforeDead := isTombstone(before)
	afterDead := isTombstone(after)

	switch {
	case before == nil && after == nil:
		return nil
	case afterDead:
		if beforeDead {
			return nil // dead at both ends — nothing to show
		}
		return &RecordDiff{Id: id, Kind: KindDeleted}
	case after == nil:
		if beforeDead {
			return nil
		}
		return &RecordDiff{Id: id, Kind: KindRemoved, Fields: diffValues(nil, before, nil)}
	case before == nil || beforeDead:
		return &RecordDiff{Id: id, Kind: KindAdded, Fields: diffValues(nil, nil, after)}
	}

	fields := diffValues(nil, before, after)
	if len(fields) == 0 {
		return nil
	}
	return &RecordDiff{Id: id, Kind: KindChanged, Fields: fields}
}

// diffValues emits leaf-level FieldDiffs between two values at path.
// Objects recurse per-key; arrays and scalars are leaves compared whole
// (LWW ops replace whole field values, so element-wise array diffs would
// suggest precision the model doesn't have). Top-level bookkeeping
// fields are excluded.
func diffValues(path []string, before, after *anyenc.Value) []FieldDiff {
	if before == nil && after == nil {
		return nil
	}
	bothObjects := before != nil && after != nil &&
		before.Type() == anyenc.TypeObject && after.Type() == anyenc.TypeObject
	oneSidedObject := (before == nil && after != nil && after.Type() == anyenc.TypeObject) ||
		(after == nil && before != nil && before.Type() == anyenc.TypeObject)

	if !bothObjects && !oneSidedObject {
		if anyencutil.Equal(before, after) {
			return nil
		}
		return []FieldDiff{{Path: path, Before: before, After: after}}
	}

	keys := unionKeys(before, after)
	var out []FieldDiff
	for _, k := range keys {
		if len(path) == 0 {
			if _, skip := excludedFields[k]; skip {
				continue
			}
		}
		childPath := append(append([]string{}, path...), k)
		out = append(out, diffValues(childPath, get(before, k), get(after, k))...)
	}
	return out
}

// DiffRecordSets merge-joins two record sets (records carry their id in
// the "id" field) and returns the dataset diff. Input order does not
// matter; output is sorted by record id.
func DiffRecordSets(dataset string, base, version []*anyenc.Value) DatasetDiff {
	baseById := byId(base)
	versionById := byId(version)

	ids := make([]string, 0, len(baseById)+len(versionById))
	for id := range baseById {
		ids = append(ids, id)
	}
	for id := range versionById {
		if _, ok := baseById[id]; !ok {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)

	d := DatasetDiff{Dataset: dataset}
	for _, id := range ids {
		if rd := DiffRecords(id, baseById[id], versionById[id]); rd != nil {
			d.Records = append(d.Records, *rd)
		}
	}
	return d
}

// DiffObjects diffs two objects given their per-dataset record sets,
// across the union of datasets. Datasets with no differences are
// omitted; output is sorted by dataset name.
func DiffObjects(base, version Version, baseSets, versionSets map[string][]*anyenc.Value) DiffResult {
	names := make([]string, 0, len(baseSets)+len(versionSets))
	for ds := range baseSets {
		names = append(names, ds)
	}
	for ds := range versionSets {
		if _, ok := baseSets[ds]; !ok {
			names = append(names, ds)
		}
	}
	sort.Strings(names)

	res := DiffResult{Base: base, Version: version}
	for _, ds := range names {
		dd := DiffRecordSets(ds, baseSets[ds], versionSets[ds])
		if len(dd.Records) > 0 {
			res.Datasets = append(res.Datasets, dd)
		}
	}
	return res
}

func isTombstone(r *anyenc.Value) bool {
	return r != nil && r.Get(crdt.DeletedAtField) != nil
}

func get(v *anyenc.Value, key string) *anyenc.Value {
	if v == nil {
		return nil
	}
	return v.Get(key)
}

func unionKeys(before, after *anyenc.Value) []string {
	seen := make(map[string]struct{})
	var keys []string
	collect := func(v *anyenc.Value) {
		if v == nil {
			return
		}
		obj, err := v.Object()
		if err != nil {
			return
		}
		obj.Visit(func(k []byte, _ *anyenc.Value) {
			if _, ok := seen[string(k)]; !ok {
				seen[string(k)] = struct{}{}
				keys = append(keys, string(k))
			}
		})
	}
	collect(before)
	collect(after)
	sort.Strings(keys)
	return keys
}

func byId(records []*anyenc.Value) map[string]*anyenc.Value {
	m := make(map[string]*anyenc.Value, len(records))
	for _, r := range records {
		if r == nil {
			continue
		}
		if id := string(r.GetStringBytes(crdt.IdField)); id != "" {
			m[id] = r
		}
	}
	return m
}
