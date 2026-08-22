package crdt

import (
	"context"
	"fmt"
	"sort"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"

	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// Re-index detection.
//
// Every apply persists dataset→handler version onto the object's _meta
// row (PersistMeta). A handler whose compiled-in logic changes bumps its
// HandlerReg.Version; the next construction sees the stored version
// differ and reports the dataset stale, which is the store's cue to wipe
// the object's materialized rows and replay its tree (see
// spaceobjects/reindex.go).
//
// The wire DataVersion peers gate against is a different counter and must
// NOT be used for this: bumping it parks the dataset's changes on every
// peer still running the old code.

// staleDatasets lists datasets whose persisted handler version differs
// from the registered one, sorted for deterministic logs and tests.
//
// A dataset the object never wrote has no stored version and is never
// stale — there is nothing materialized to rebuild. A stored version for
// a dataset this controller doesn't register is ignored: the handler that
// owns it isn't loaded here (raw mode, a type detached from the space),
// so a replay would not reproduce those rows anyway.
func staleDatasets(stored, registered map[string]int) []string {
	if len(stored) == 0 {
		return nil
	}
	var out []string
	for name, storedVer := range stored {
		if cur, ok := registered[name]; ok && cur != storedVer {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// NormalizedVersion resolves a HandlerReg.Version to the value that is
// actually persisted and compared: an unset version means 1.
func NormalizedVersion(v int) int {
	if v == 0 {
		return 1
	}
	return v
}

// StaleDatasets returns the datasets whose materialized rows were built
// by a different version of their handler. Empty for a fresh object (no
// stored versions) and for the common case where nothing changed.
func (c *Controller) StaleDatasets() []string {
	if c == nil {
		return nil
	}
	return c.staleDatasets
}

// LocalLeaf is one local-scope value captured before a rebuild's wipe.
// Local values never entered the DAG, so no replay reproduces them; the
// re-index path re-applies these after the replay. Value is the leaf's
// anyenc encoding — the document it came from dies with its read.
type LocalLeaf struct {
	Dataset  string
	RecordId string
	Path     []string
	Value    []byte
}

// Leaf keys inside the persisted array: short, the blob rides a row
// that every load of the object reads.
const (
	leafDatasetKey  = "d"
	leafRecordIdKey = "r"
	leafPathKey     = "p"
	leafValueKey    = "v"
)

func encodeLocalLeaves(a *anyenc.Arena, leaves []LocalLeaf) *anyenc.Value {
	arr := a.NewArray()
	n := 0
	for _, leaf := range leaves {
		val, err := anyenc.Parse(leaf.Value)
		if err != nil {
			continue // unreadable capture: nothing to restore anyway
		}
		item := a.NewObject()
		item.Set(leafDatasetKey, a.NewString(leaf.Dataset))
		item.Set(leafRecordIdKey, a.NewString(leaf.RecordId))
		path := a.NewArray()
		for i, seg := range leaf.Path {
			path.SetArrayItem(i, a.NewString(seg))
		}
		item.Set(leafPathKey, path)
		item.Set(leafValueKey, val)
		arr.SetArrayItem(n, item)
		n++
	}
	return arr
}

func decodeLocalLeaves(v *anyenc.Value) []LocalLeaf {
	if v == nil || v.Type() != anyenc.TypeArray {
		return nil
	}
	items := v.GetArray()
	out := make([]LocalLeaf, 0, len(items))
	for _, item := range items {
		val := item.Get(leafValueKey)
		if val == nil {
			continue
		}
		leaf := LocalLeaf{
			Dataset:  item.GetString(leafDatasetKey),
			RecordId: item.GetString(leafRecordIdKey),
			Value:    val.MarshalTo(nil),
		}
		for _, seg := range item.GetArray(leafPathKey) {
			leaf.Path = append(leaf.Path, seg.GetString())
		}
		out = append(out, leaf)
	}
	return out
}

// ReindexPending reports whether a rebuild of this object was started
// and never finished restoring its local-scope leaves — a crash, or the
// sweep cancelled at shutdown, between ResetForReindex and
// PersistVersions. The next load resumes: the replay continues from the
// persisted watermark and the leaves come back from the _meta row.
func (c *Controller) ReindexPending() bool {
	return c != nil && c.reindexPending
}

// ReindexLocalLeaves returns the leaves an in-flight rebuild persisted.
// Empty unless ReindexPending.
func (c *Controller) ReindexLocalLeaves() []LocalLeaf {
	if c == nil || len(c.reindexLocal) == 0 {
		return nil
	}
	v, err := anyenc.Parse(c.reindexLocal)
	if err != nil {
		return nil
	}
	return decodeLocalLeaves(v)
}

// ResetForReindex rewinds the controller to "never applied" so the next
// ColdRestore replays the whole tree, and persists the rewound watermark
// together with the captured local-scope leaves BEFORE the caller wipes
// the materialized rows.
//
// Order matters: a crash between the wipe and the first re-applied change
// must leave a watermark of 0 over the missing rows, so the next boot
// replays them. Persisting after the wipe would leave a window where the
// stored watermark claims changes are applied whose rows are gone. The
// leaves ride the same upsert for the same reason — once the rows are
// gone they exist nowhere else, and a replay interrupted at any point
// (including the sweep's own shutdown cancel) must still find them.
//
// The stored handler versions are deliberately left alone — they keep the
// stale verdict (and therefore the wipe) armed until a replay actually
// re-applies a change, at which point its PersistMeta stamps the current
// versions. The leaves outlive that stamp: only PersistVersions clears
// them.
func (c *Controller) ResetForReindex(ctx context.Context, leaves []LocalLeaf) error {
	c.maxAddSeq = 0
	c.maxApplySeq = 0
	c.staleDatasets = nil
	c.reindexPending = true
	c.reindexLocal = nil
	return persistMeta(ctx, c.metaColl, c.objectId, 0, 0, nil, c.spaceId, func(a *anyenc.Arena, v *anyenc.Value) {
		v.Set(metaReindexLocalKey, encodeLocalLeaves(a, leaves))
	})
}

// PersistVersions stamps the current watermarks and handler versions and
// clears the in-flight mark (the persisted local leaves), ending the
// rebuild. The replay path stamps versions from inside each apply; this
// also covers the object whose replay re-applied nothing (an empty tree,
// or one whose every change belongs to a dataset that is no longer
// registered), which would otherwise be found stale again on every load.
func (c *Controller) PersistVersions(ctx context.Context) error {
	c.reindexPending = false
	c.reindexLocal = nil
	return persistMeta(ctx, c.metaColl, c.objectId, c.maxAddSeq, c.maxApplySeq, c.HandlerVersions(), c.spaceId, func(_ *anyenc.Arena, v *anyenc.Value) {
		v.Del(metaReindexLocalKey)
	})
}

// LocalFields returns the dataset's declared local-scope top-level field
// names. Local values are written by Object.LocalSet and never enter the
// DAG, so a replay cannot reproduce them — the re-index path snapshots
// these fields before the wipe and re-applies them after.
func (c *Controller) LocalFields(dataset string) []string {
	ds, ok := c.schemas[dataset]
	if !ok {
		return nil
	}
	var out []string
	for _, f := range ds.Fields {
		if f.Scope == schema.ScopeLocal {
			out = append(out, f.Id)
		}
	}
	sort.Strings(out)
	return out
}

// RegisteredDatasets returns every dataset name this controller handles,
// sorted. The re-index wipe walks them to find what to clear.
func (c *Controller) RegisteredDatasets() []string {
	out := make([]string, 0, len(c.handlers))
	for name := range c.handlers {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// StaleObjects returns the ids of objects in spaceId whose persisted
// handler versions differ from `registered`, plus those whose rebuild
// was interrupted (captured leaves still on the row) — the sweep's work
// list. Purged objects are skipped: their rows are already gone and a
// rebuild must never resurrect them.
//
// One scan of the space's _meta rows, bounded by its object count. The
// hv map is small (one entry per dataset the object wrote), so the
// per-row compare is cheap.
func StaleObjects(ctx context.Context, coll anystore.Collection, spaceId string, registered map[string]int) ([]string, error) {
	filter := query.Key{Path: []string{metaSpaceIdKey}, Filter: query.NewComp(query.CompOpEq, spaceId)}
	iter, err := coll.Find(filter).Iter(ctx)
	if err != nil {
		return nil, fmt.Errorf("crdt: stale-objects iter: %w", err)
	}
	defer iter.Close()
	var out []string
	stored := make(map[string]int, 8)
	for iter.Next() {
		doc, derr := iter.Doc()
		if derr != nil {
			return nil, derr
		}
		v := doc.Value()
		if v.GetBool(metaDeletedKey) {
			continue
		}
		if v.Get(metaReindexLocalKey) != nil {
			out = append(out, v.GetString(IdField))
			continue
		}
		hv := v.Get(metaHandlerVersionsKey)
		if hv == nil || hv.Type() != anyenc.TypeObject {
			continue
		}
		clear(stored)
		obj, _ := hv.Object()
		obj.Visit(func(k []byte, vv *anyenc.Value) {
			if vv.Type() == anyenc.TypeNumber {
				stored[string(k)] = vv.GetInt()
			}
		})
		if len(staleDatasets(stored, registered)) > 0 {
			out = append(out, v.GetString(IdField))
		}
	}
	return out, iter.Err()
}
