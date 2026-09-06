package types

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// CompiledDataset is one dataset declaration folded out of a type
// object's `datasets` records.
type CompiledDataset struct {
	// Name is the dataset's collection name — the module's canonical
	// collection for a shared dataset, `<typeId>_<key>` otherwise.
	Name string
	// Key is the dataset's slug inside its type; for a shared dataset
	// it equals the canonical collection name.
	Key string
	// Module is the module serving the dataset (`records` for the
	// generic schema-enforced kind).
	Module string
	// Shared marks a dataset that participates in the module's
	// canonical collection instead of a namespaced one.
	Shared bool
	// PartId is the owning part record's id.
	PartId string
	// DefId is the head record's id — the definition's stable identity.
	DefId string
	// TypeId is the declaring type object.
	TypeId string
	// Schema is the compiled declaration the generic schema handler
	// enforces. Empty for a module-served dataset — the module owns the
	// schema.
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
	// cross-record fact no record-local handler check can see — or an
	// unknown module). Invalid definitions never register or accept
	// data, but they stay VISIBLE so the management API can repair or
	// remove them — otherwise their key would wedge unreachably.
	Invalid       bool
	InvalidReason string
	// FieldDefIds are the field records' ids, aligned with
	// Schema.Fields — the identities RemoveDatasetField targets.
	FieldDefIds []string
}

// CompiledPart is one part folded out of a type object's records: the
// display slice plus the datasets declared under it.
type CompiledPart struct {
	Id     string // the part record's id
	Key    string
	TypeId string
	Name   string
	Icon   string
	Pos    string
	Hidden bool
	UI     map[string]any
	Uses   []string
	// Datasets are the part's datasets in key order, invalid ones
	// included (flagged).
	Datasets []CompiledDataset
}

// CompiledType is a type's parts and the flat view over their datasets.
type CompiledType struct {
	Parts    []CompiledPart
	Datasets []CompiledDataset
}

// schemaRev fingerprints a compiled declaration deterministically:
// json.Marshal sorts object keys, fields ride in the compiler's stable
// order, so equal declarations hash equal everywhere. The descriptive
// slice (Description, XFormat) is stripped first — it is not enforced,
// so a display edit must not rotate the revision and evict every
// registered controller.
func schemaRev(ds schema.Dataset) string {
	behavioral := ds
	behavioral.Fields = make([]schema.Field, len(ds.Fields))
	for i, f := range ds.Fields {
		f.Description = ""
		f.XFormat = nil
		behavioral.Fields[i] = f
	}
	raw, err := json.Marshal(behavioral)
	if err != nil {
		return ""
	}
	return hashRev(raw)
}

// moduleRev fingerprints a module-served instance: the module owns the
// schema, so the revision is the (module, key) identity — it never
// rotates from a declaration edit.
func moduleRev(module, key string) string {
	return hashRev([]byte("module:" + module + ":" + key))
}

func hashRev(raw []byte) string {
	h := fnv.New64a()
	_, _ = h.Write(raw)
	return strconv.FormatUint(h.Sum64(), 36)
}

// DatasetHeadKey resolves a live head record's dataset key by its
// record id — the winner OR a hidden concurrent duplicate. Empty when
// the id names no live head. Removal by a stale DefId (read before
// convergence) resolves its key here so the whole dataset goes, not
// just the hidden duplicate.
func DatasetHeadKey(ctx context.Context, db anystore.DB, typeId, defId string) (string, error) {
	if db == nil || defId == "" {
		return "", nil
	}
	coll, err := db.OpenCollection(ctx, typeId+"_datasets")
	if err != nil {
		if errors.Is(err, anystore.ErrCollectionNotFound) {
			return "", nil
		}
		return "", err
	}
	doc, err := coll.FindId(ctx, defId)
	if err != nil {
		if errors.Is(err, anystore.ErrDocNotFound) {
			return "", nil
		}
		return "", err
	}
	v := doc.Value()
	if v == nil || v.Type() != anyenc.TypeObject || v.Get("_deletedAt") != nil {
		return "", nil
	}
	if string(v.GetStringBytes("def")) != "dataset" {
		return "", nil
	}
	return string(v.GetStringBytes("key")), nil
}

// DatasetHeadIds lists the live head record ids declaring key on the
// type object — the winner and every concurrent duplicate the compile
// layer hides. Nil when the type has no datasets collection.
func DatasetHeadIds(ctx context.Context, db anystore.DB, typeId, key string) ([]string, error) {
	return liveDefIds(ctx, db, typeId, "dataset", key)
}

// PartIds lists the live part record ids declaring key on the type
// object — the winner and every concurrent duplicate.
func PartIds(ctx context.Context, db anystore.DB, typeId, key string) ([]string, error) {
	return liveDefIds(ctx, db, typeId, "part", key)
}

func liveDefIds(ctx context.Context, db anystore.DB, typeId, def, key string) ([]string, error) {
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
	var out []string
	for iter.Next() {
		doc, derr := iter.Doc()
		if derr != nil {
			return nil, derr
		}
		v := doc.Value()
		if v == nil || v.Type() != anyenc.TypeObject || v.Get("_deletedAt") != nil {
			continue
		}
		if v.GetString("def") == def && v.GetString("key") == key {
			out = append(out, v.GetString("id"))
		}
	}
	if iter.Err() != nil {
		return nil, iter.Err()
	}
	return out, nil
}

// HasDatasetDefs reports whether anything was ever declared on the
// type object — live or tombstoned records alike (a tombstone keeps
// no fields, so a removed part is known only by its presence).
// False when the type has no datasets collection.
func HasDatasetDefs(ctx context.Context, db anystore.DB, typeId string) (bool, error) {
	if db == nil {
		return false, nil
	}
	coll, err := db.OpenCollection(ctx, typeId+"_datasets")
	if err != nil {
		if errors.Is(err, anystore.ErrCollectionNotFound) {
			return false, nil
		}
		return false, err
	}
	n, err := coll.Find(nil).Limit(1).Count(ctx)
	if err != nil {
		return false, fmt.Errorf("types: dataset defs count: %w", err)
	}
	return n > 0, nil
}

// CompileDatasetDefs is the flat dataset view of CompileTypeParts —
// every dataset of every part, in collection order.
func CompileDatasetDefs(ctx context.Context, db anystore.DB, typeId string, modules Modules) ([]CompiledDataset, error) {
	ct, err := CompileTypeParts(ctx, db, typeId, modules)
	if err != nil || ct == nil {
		return nil, err
	}
	return ct.Datasets, nil
}

type headRec struct {
	id      string
	created string // creation _ver.id, the dedup tiebreak
	key     string
	module  string
	shared  bool
	partId  string
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

type partRec struct {
	id      string
	created string
	key     string
	name    string
	icon    string
	pos     string
	hidden  bool
	ui      map[string]any
	uses    []string
}

// pinnedLeaves are the head fields two concurrent declarations of one
// key must agree on; a disagreement marks the definition invalid.
func (h *headRec) pinnedLeaves() string {
	return fmt.Sprintf("%s|%t|%s|%t|%s|%s|%d|%s|%t",
		h.module, h.shared, h.partId, h.ds.Dynamic, h.ds.IdRule, h.ds.IdPattern, h.ds.IdMaxLen, h.ds.DeleteBy, h.skip)
}

// CompileTypeParts folds a type object's `datasets` records into parts
// and dataset declarations — one storage pass, deterministic on
// converged records:
//
//   - tombstoned records are skipped;
//   - duplicate part keys fold into one: display from the smallest
//     creation `_ver.id`, datasets and `uses` unioned;
//   - a head whose part is absent, or a field whose head is absent, is
//     an orphan and is skipped;
//   - duplicate dataset keys within the type name the same collection
//     by construction: the smallest creation `_ver.id` is the
//     definition's identity and display, fields union by key across
//     every head (smallest `_ver.id` per key), and a disagreement on a
//     pinned leaf marks the definition invalid;
//   - `_ver.id` comparison is sound within one tree: orderId VALUES are
//     peer-local but their relative order converges, so every replica
//     picks the same winner;
//   - the collection rule (Modules.Collection) decides the collection;
//     an unknown module or a shared violation marks the definition
//     invalid; a type declaring two shared datasets of one module keeps
//     the smallest `_ver.id` and marks the rest invalid;
//   - a module-served dataset carries no fields (the module owns the
//     schema): field records under it are orphans;
//   - a `records` dataset whose folded declaration fails
//     ValidateDatasetDecl is emitted with Invalid set: visible for
//     repair/removal, never registered.
//
// Returns (nil, nil) when the type has no datasets collection.
func CompileTypeParts(ctx context.Context, db anystore.DB, typeId string, modules Modules) (*CompiledType, error) {
	if db == nil {
		return nil, nil
	}
	if modules == nil {
		modules = NewModules()
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

	var parts []*partRec
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
		case "part":
			key := v.GetString("key")
			if key == "" {
				continue
			}
			p := &partRec{
				id:      v.GetString("id"),
				created: created,
				key:     key,
				name:    v.GetString("name"),
				icon:    v.GetString("icon"),
				pos:     v.GetString("pos"),
				hidden:  v.GetBool("hidden"),
				ui:      DecodeXFormat(v.Get("ui")),
			}
			for _, u := range v.GetArray("uses") {
				if u != nil && u.Type() == anyenc.TypeString {
					p.uses = append(p.uses, string(u.GetStringBytes()))
				}
			}
			parts = append(parts, p)
		case "dataset":
			key := v.GetString("key")
			if key == "" {
				continue
			}
			h := &headRec{
				id:      v.GetString("id"),
				created: created,
				key:     key,
				module:  v.GetString("module"),
				shared:  v.GetBool("shared"),
				partId:  v.GetString("part"),
				display: v.GetString("displayName"),
				descr:   v.GetString("description"),
			}
			if h.module == "" {
				h.module = RecordsModule
			}
			h.ds.Dynamic = v.GetBool("dynamic")
			h.skip = v.GetBool("skipHistory")
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
					Text:  schema.SearchTextFromAnyenc(s.Get("text")),
					Scope: s.GetString("scope"),
				}
				if sf.Title != "" || len(sf.Text) > 0 {
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
				Id:          key,
				Name:        v.GetString("name"),
				Description: v.GetString("description"),
				Schema:      shape,
				XFormat:     DecodeXFormat(v.Get("x-format")),
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
	if len(parts) == 0 {
		return &CompiledType{}, nil
	}

	// Parts: fold duplicates by key, smallest creation _ver.id wins the
	// display slice; every duplicate's id maps onto the winner so heads
	// referencing any of them attach to the same part.
	partWinner := make(map[string]*partRec, len(parts)) // key → winner
	partAlias := make(map[string]*partRec, len(parts))  // any part id → winner
	partUses := make(map[string]map[string]struct{}, len(parts))
	for _, p := range parts {
		if w, dup := partWinner[p.key]; !dup || p.created < w.created {
			partWinner[p.key] = p
		}
	}
	for _, p := range parts {
		w := partWinner[p.key]
		partAlias[p.id] = w
		u := partUses[w.id]
		if u == nil {
			u = map[string]struct{}{}
			partUses[w.id] = u
		}
		for _, use := range p.uses {
			u[use] = struct{}{}
		}
	}

	// Heads: attach to their (winning) part, group by key.
	headById := make(map[string]*headRec, len(heads))
	groups := make(map[string][]*headRec, len(heads)) // key → heads
	for _, h := range heads {
		w, ok := partAlias[h.partId]
		if !ok {
			continue // orphan head: unknown part
		}
		h.partId = w.id
		headById[h.id] = h
		groups[h.key] = append(groups[h.key], h)
	}

	// Fields: attach to any live head of their group, dedup per group key.
	fieldWinners := make(map[string]map[string]*fieldRec, len(groups)) // dataset key → field key → winner
	for _, f := range fields {
		h, ok := headById[f.headId]
		if !ok {
			continue // orphan field
		}
		byKey := fieldWinners[h.key]
		if byKey == nil {
			byKey = make(map[string]*fieldRec)
			fieldWinners[h.key] = byKey
		}
		if w, dup := byKey[f.field.Id]; !dup || f.created < w.created {
			byKey[f.field.Id] = f
		}
	}

	// One shared dataset per module per type: the group with the
	// smallest winner creation keeps it.
	sharedKeep := make(map[string]string) // module → dataset key
	sharedCreated := make(map[string]string)
	keys := make([]string, 0, len(groups))
	for key, hs := range groups {
		keys = append(keys, key)
		sort.Slice(hs, func(i, j int) bool { return hs[i].created < hs[j].created })
		w := hs[0]
		if !w.shared {
			continue
		}
		if prev, taken := sharedKeep[w.module]; !taken || w.created < sharedCreated[w.module] || (w.created == sharedCreated[w.module] && key < prev) {
			sharedKeep[w.module] = key
			sharedCreated[w.module] = w.created
		}
	}
	sort.Strings(keys)

	datasetsByPart := make(map[string][]CompiledDataset, len(partWinner))
	flat := make([]CompiledDataset, 0, len(groups))
	for _, key := range keys {
		hs := groups[key]
		w := hs[0]
		compiled := CompiledDataset{
			Key:         key,
			Module:      w.module,
			Shared:      w.shared,
			PartId:      w.partId,
			DefId:       w.id,
			TypeId:      typeId,
			SkipHistory: w.skip,
			Search:      w.search,
			DisplayName: w.display,
			Description: w.descr,
		}
		invalid := func(reason string) {
			compiled.Invalid = true
			compiled.InvalidReason = reason
		}
		pinned := w.pinnedLeaves()
		for _, h := range hs[1:] {
			if h.pinnedLeaves() != pinned {
				invalid(fmt.Sprintf("concurrent declarations of %q disagree on pinned fields", key))
				break
			}
		}
		name, cerr := modules.Collection(typeId, key, w.module, w.shared)
		if cerr != nil && !compiled.Invalid {
			invalid(cerr.Error())
		}
		if cerr == nil {
			compiled.Name = name
		} else {
			compiled.Name = CollectionName(typeId, key)
		}
		if w.shared && sharedKeep[w.module] != key && !compiled.Invalid {
			invalid(fmt.Sprintf("type already declares a shared %q dataset (%q)", w.module, sharedKeep[w.module]))
		}

		if w.module == RecordsModule {
			ds := w.ds
			var fieldIds []string
			if byKey := fieldWinners[key]; byKey != nil {
				ordered := make([]*fieldRec, 0, len(byKey))
				for _, f := range byKey {
					ordered = append(ordered, f)
				}
				sort.Slice(ordered, func(i, j int) bool { return ordered[i].created < ordered[j].created })
				for _, f := range ordered {
					ds.Fields = append(ds.Fields, f.field)
					fieldIds = append(fieldIds, f.id)
				}
			}
			compiled.FieldDefIds = fieldIds
			if err := schema.ValidateDatasetDecl(ds); err != nil {
				if !compiled.Invalid {
					invalid(err.Error())
				}
				compiled.Schema = ds
			} else {
				norm := ds.Normalized()
				compiled.Schema = norm
				if !compiled.Invalid {
					compiled.SchemaRev = schemaRev(norm)
				}
			}
		} else if !compiled.Invalid {
			compiled.SchemaRev = moduleRev(w.module, key)
		}
		datasetsByPart[w.partId] = append(datasetsByPart[w.partId], compiled)
		flat = append(flat, compiled)
	}

	out := &CompiledType{Datasets: flat}
	partKeys := make([]string, 0, len(partWinner))
	for key := range partWinner {
		partKeys = append(partKeys, key)
	}
	sort.Strings(partKeys)
	for _, key := range partKeys {
		p := partWinner[key]
		cp := CompiledPart{
			Id:       p.id,
			Key:      key,
			TypeId:   typeId,
			Name:     p.name,
			Icon:     p.icon,
			Pos:      p.pos,
			Hidden:   p.hidden,
			UI:       p.ui,
			Datasets: datasetsByPart[p.id],
		}
		for use := range partUses[p.id] {
			if _, known := groups[use]; known {
				cp.Uses = append(cp.Uses, use)
			}
		}
		sort.Strings(cp.Uses)
		out.Parts = append(out.Parts, cp)
	}
	return out, nil
}
