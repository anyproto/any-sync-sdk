package types

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// CompiledDataset is one runtime dataset declaration folded out of a
// type object's `datasets` records.
type CompiledDataset struct {
	// Name is the dataset's collection name (per-object suffix).
	Name string
	// DefId is the head record's id — the definition's stable identity.
	DefId string
	// TypeId is the owning type object.
	TypeId string
	// Schema is the compiled declaration the generic schema handler
	// enforces.
	Schema schema.Dataset
	// SkipHistory mirrors the head's skipHistory flag.
	SkipHistory bool
	// Search mirrors the head's search annotation (also inside Schema).
	Search *schema.SearchFields
	// DisplayName / Description are the head's mutable display fields.
	DisplayName string
	Description string
	// SchemaRev fingerprints the compiled declaration. A controller
	// registered with an older rev (a field added/removed since it was
	// built) is stale and gets lazily evicted; identical declarations
	// on every peer produce the identical rev.
	SchemaRev string
	// Invalid marks a definition whose folded declaration fails
	// validation (e.g. author rules without a creator stamp — a
	// cross-record fact no record-local handler check can see).
	// Invalid definitions never register or accept data, but they stay
	// VISIBLE so the management API can repair (add the missing field)
	// or remove them — otherwise their name would wedge unreachably.
	Invalid       bool
	InvalidReason string
	// FieldDefIds are the field records' ids, aligned with
	// Schema.Fields — the identities RemoveDatasetField targets.
	FieldDefIds []string
}

// schemaRev fingerprints a compiled declaration deterministically:
// json.Marshal sorts object keys, fields ride in the compiler's stable
// order, so equal declarations hash equal everywhere.
func schemaRev(ds schema.Dataset) string {
	raw, err := json.Marshal(ds)
	if err != nil {
		return ""
	}
	h := fnv.New64a()
	_, _ = h.Write(raw)
	return strconv.FormatUint(h.Sum64(), 36)
}

// CompileDatasetDefs folds a type object's `datasets` records into
// CompiledDataset declarations — one storage pass, deterministic on
// converged records:
//
//   - tombstoned records are skipped;
//   - field records whose owning head is absent (orphans) are skipped;
//   - duplicate field keys within a dataset and duplicate dataset names
//     within the type resolve to the record with the smallest creation
//     `_ver.id` — sound within one tree: orderId VALUES are peer-local
//     but their relative order converges, so the comparison picks the
//     same winner on every replica (cross-TREE comparison would not;
//     the catalog layer uses DefId there);
//   - a dataset whose folded declaration fails ValidateDatasetDecl is
//     emitted with Invalid set: visible for repair/removal, never
//     registered.
//
// Cross-TYPE name conflicts are the caller's (catalog layer's) concern.
// Returns (nil, nil) when the type has no datasets collection.
func CompileDatasetDefs(ctx context.Context, db anystore.DB, typeId string) ([]CompiledDataset, error) {
	if db == nil {
		return nil, nil
	}
	coll, err := db.OpenCollection(ctx, typeId+"_datasets")
	if err != nil {
		if errors.Is(err, anystore.ErrCollectionNotFound) {
			return nil, nil
		}
		return nil, err
	}
	iter, err := coll.Find(nil).Iter(ctx)
	if err != nil {
		return nil, fmt.Errorf("types: dataset defs iter: %w", err)
	}
	defer iter.Close()

	type headRec struct {
		id      string
		created string // creation _ver.id, the dedup tiebreak
		name    string
		dynamic bool
		ds      schema.Dataset
		skip    bool
		search  *schema.SearchFields
		display string
		descr   string
	}
	type fieldRec struct {
		id      string
		headId  string
		created string
		field   schema.Field
	}
	var heads []*headRec
	var fields []*fieldRec

	for iter.Next() {
		doc, derr := iter.Doc()
		if derr != nil {
			return nil, derr
		}
		v := doc.Value()
		if v == nil || v.Type() != anyenc.TypeObject {
			continue
		}
		if v.Get("_deletedAt") != nil {
			continue
		}
		created := v.GetString("_ver", "id")
		switch v.GetString("def") {
		case "dataset":
			name := v.GetString("collection")
			if name == "" {
				continue
			}
			h := &headRec{
				id:      v.GetString("id"),
				created: created,
				name:    name,
				display: v.GetString("displayName"),
				descr:   v.GetString("description"),
			}
			h.dynamic = v.GetBool("dynamic")
			h.skip = v.GetBool("skipHistory")
			h.ds.Dynamic = h.dynamic
			if rule, ok := schema.ParseIdRule(v.GetString("idRule")); ok {
				h.ds.IdRule = rule
			}
			h.ds.IdPattern = v.GetString("idPattern")
			if ml := v.GetFloat64("idMaxLen"); ml > 0 {
				h.ds.IdMaxLen = int(ml)
			}
			if pol, ok := schema.ParseDeletePolicy(v.GetString("deleteBy")); ok {
				h.ds.DeleteBy = pol
			}
			if s := v.Get("search"); s != nil && s.Type() == anyenc.TypeObject {
				sf := &schema.SearchFields{
					Title: s.GetString("title"),
					Text:  s.GetString("text"),
				}
				if sf.Title != "" || sf.Text != "" {
					h.search = sf
					h.ds.Search = sf
				}
			}
			heads = append(heads, h)
		case "field":
			headId := v.GetString("dataset")
			key := v.GetString("key")
			if headId == "" || key == "" {
				continue
			}
			shape, serr := schema.CompileShape(v)
			if serr != nil {
				continue // unparsable shape: drop the field, not the dataset
			}
			f := schema.Field{
				Id:     key,
				Name:   v.GetString("name"),
				Schema: shape,
			}
			if sc, ok := schema.ParseScope(v.GetString("scope")); ok {
				f.Scope = sc
			}
			if st, ok := schema.ParseStamp(v.GetString("stamp")); ok {
				f.Stamp = st
			}
			if mb, ok := schema.ParseMutability(v.GetString("mutableBy")); ok {
				f.MutableBy = mb
			}
			f.Required = v.GetBool("required")
			fields = append(fields, &fieldRec{id: v.GetString("id"), headId: headId, created: created, field: f})
		}
	}
	if iter.Err() != nil {
		return nil, iter.Err()
	}
	if len(heads) == 0 {
		return nil, nil
	}

	// Dedup heads by dataset name: smallest creation _ver.id wins.
	headById := make(map[string]*headRec, len(heads))
	winnerByName := make(map[string]*headRec, len(heads))
	for _, h := range heads {
		headById[h.id] = h
		if w, dup := winnerByName[h.name]; !dup || h.created < w.created {
			winnerByName[h.name] = h
		}
	}

	// Attach fields to their heads, dedup keys per head the same way.
	fieldWinners := make(map[string]map[string]*fieldRec, len(heads))
	for _, f := range fields {
		if _, ok := headById[f.headId]; !ok {
			continue // orphan: head unknown (or lost the name dedup — see below)
		}
		byKey := fieldWinners[f.headId]
		if byKey == nil {
			byKey = make(map[string]*fieldRec)
			fieldWinners[f.headId] = byKey
		}
		if w, dup := byKey[f.field.Id]; !dup || f.created < w.created {
			byKey[f.field.Id] = f
		}
	}

	out := make([]CompiledDataset, 0, len(winnerByName))
	for _, h := range winnerByName {
		ds := h.ds
		var fieldIds []string
		if byKey := fieldWinners[h.id]; byKey != nil {
			// Deterministic field order: by creation _ver.id.
			ordered := make([]*fieldRec, 0, len(byKey))
			for _, f := range byKey {
				ordered = append(ordered, f)
			}
			for i := 1; i < len(ordered); i++ {
				for j := i; j > 0 && ordered[j].created < ordered[j-1].created; j-- {
					ordered[j], ordered[j-1] = ordered[j-1], ordered[j]
				}
			}
			for _, f := range ordered {
				ds.Fields = append(ds.Fields, f.field)
			}
			fieldIds = make([]string, 0, len(ordered))
			for _, f := range ordered {
				fieldIds = append(fieldIds, f.id)
			}
		}
		compiled := CompiledDataset{
			Name:        h.name,
			DefId:       h.id,
			TypeId:      typeId,
			SkipHistory: h.skip,
			Search:      h.search,
			DisplayName: h.display,
			Description: h.descr,
			FieldDefIds: fieldIds,
		}
		if err := schema.ValidateDatasetDecl(ds); err != nil {
			compiled.Invalid = true
			compiled.InvalidReason = err.Error()
			compiled.Schema = ds
		} else {
			norm := ds.Normalized()
			compiled.Schema = norm
			compiled.SchemaRev = schemaRev(norm)
		}
		out = append(out, compiled)
	}
	// Deterministic output order: by name.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].Name < out[j-1].Name; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out, nil
}
