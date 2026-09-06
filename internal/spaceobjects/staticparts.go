package spaceobjects

// Static parts: the parts a registered handler.Type declares at boot,
// compiled into the same view a runtime declaration produces. Module
// datasets among them enter the catalog as ownership (a shared one adds
// the type to the canonical collection's owner set, a namespaced one
// gets its own registration), so the write gate, discovery and the
// module namespace treat them exactly like a user declaration.

import (
	"fmt"
	"sort"

	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/types"
)

// staticModuleDataset is one module dataset a registered type declares
// inside a part, normalized against the module catalog.
type staticModuleDataset struct {
	typeId     string
	partKey    string
	module     string
	shared     bool
	key        string
	collection string
}

// validateStaticParts checks a registered type's part declarations
// for what needs no module catalog: slug keys, unique part keys, each
// dataset entry in exactly one form, static names that exist and are
// named by exactly one part, and every static dataset named once parts
// are declared. `uses` is carried as declared — the compiled view
// keeps it verbatim, like a runtime part's.
func validateStaticParts(t handler.Type) error {
	if len(t.Parts) == 0 {
		return nil
	}
	static := make(map[string]struct{}, len(t.Datasets))
	for _, d := range t.Datasets {
		static[d.Name] = struct{}{}
	}
	partKeys := make(map[string]struct{}, len(t.Parts))
	named := make(map[string]string, len(t.Datasets)) // static name → part key
	for i, p := range t.Parts {
		if err := schema.ValidateSlug("part", p.Key); err != nil {
			return fmt.Errorf("type %q part[%d]: %w", t.Id, i, err)
		}
		if _, dup := partKeys[p.Key]; dup {
			return fmt.Errorf("type %q part[%d]: duplicate part key %q", t.Id, i, p.Key)
		}
		partKeys[p.Key] = struct{}{}
		if len(p.Datasets) == 0 {
			return fmt.Errorf("type %q part %q: at least one dataset is required", t.Id, p.Key)
		}
		for j, d := range p.Datasets {
			switch {
			case d.Name != "" && d.Module != "":
				return fmt.Errorf("type %q part %q dataset[%d]: Name and Module are exclusive", t.Id, p.Key, j)
			case d.Name == "" && d.Module == "":
				return fmt.Errorf("type %q part %q dataset[%d]: Name or Module required", t.Id, p.Key, j)
			case d.Name != "":
				if d.Shared || d.Key != "" {
					return fmt.Errorf("type %q part %q dataset %q: Shared/Key apply to module datasets only", t.Id, p.Key, d.Name)
				}
				if _, ok := static[d.Name]; !ok {
					return fmt.Errorf("type %q part %q: dataset %q is not among the type's Datasets", t.Id, p.Key, d.Name)
				}
				if by, dup := named[d.Name]; dup {
					return fmt.Errorf("type %q part %q: dataset %q is already owned by part %q", t.Id, p.Key, d.Name, by)
				}
				named[d.Name] = p.Key
			default:
				if d.Module == types.RecordsModule {
					return fmt.Errorf("type %q part %q: a static records dataset is a Datasets entry with a Schema, not a module dataset", t.Id, p.Key)
				}
				// The key is settled against the module catalog
				// (staticModuleDatasets); here only the form.
			}
		}
	}
	for _, d := range t.Datasets {
		if _, ok := named[d.Name]; !ok {
			return fmt.Errorf("type %q: dataset %q is named by no part", t.Id, d.Name)
		}
	}
	return nil
}

// staticModuleDatasets resolves the module datasets a registered type
// declares in its parts against the module catalog: the collection
// rule, at most one shared dataset per module per type, unique keys —
// across the static datasets too, since both are keys of one type.
func staticModuleDatasets(t handler.Type, modules types.Modules) ([]staticModuleDataset, error) {
	var out []staticModuleDataset
	keys := make(map[string]struct{}, len(t.Datasets))
	for _, d := range t.Datasets {
		keys[d.Name] = struct{}{}
	}
	shared := make(map[string]struct{})
	for _, p := range t.Parts {
		for _, d := range p.Datasets {
			if d.Module == "" {
				continue
			}
			key := d.Key
			if d.Shared && key == "" {
				if mi, ok := modules[d.Module]; ok {
					key = mi.Canonical
				}
			}
			coll, err := modules.Collection(t.Id, key, d.Module, d.Shared)
			if err != nil {
				return nil, fmt.Errorf("type %q part %q: %w", t.Id, p.Key, err)
			}
			if _, dup := keys[key]; dup {
				return nil, fmt.Errorf("type %q part %q: duplicate dataset key %q", t.Id, p.Key, key)
			}
			keys[key] = struct{}{}
			if d.Shared {
				if _, dup := shared[d.Module]; dup {
					return nil, fmt.Errorf("type %q: two shared %q datasets", t.Id, d.Module)
				}
				shared[d.Module] = struct{}{}
			}
			out = append(out, staticModuleDataset{
				typeId: t.Id, partKey: p.Key, module: d.Module, shared: d.Shared, key: key, collection: coll,
			})
		}
	}
	return out, nil
}

// StaticTypeParts compiles a registered type's parts into the view a
// runtime declaration produces: one CompiledPart per declared part
// (or one implicit part per dataset, keyed by the dataset name, when
// the type declares none), each static dataset carrying its declared
// schema under its own name, each module dataset its collection. Ids
// are the keys — static declarations have no records. Same order and
// folding rules as the runtime compile: parts and the flat dataset
// view by key, `uses` filtered to the type's dataset keys and sorted.
func StaticTypeParts(t handler.Type, modules types.Modules) (*types.CompiledType, error) {
	byName := make(map[string]handler.Dataset, len(t.Datasets))
	for _, d := range t.Datasets {
		byName[d.Name] = d
	}
	staticDataset := func(d handler.Dataset, partKey string) types.CompiledDataset {
		ds := datasetSchema(d)
		cd := types.CompiledDataset{
			Name:        d.Name,
			Key:         d.Name,
			PartId:      partKey,
			DefId:       d.Name,
			TypeId:      t.Id,
			Schema:      ds,
			SkipHistory: d.SkipHistory,
			Search:      ds.Search,
		}
		if d.Handler == nil {
			cd.Module = types.RecordsModule
		}
		for _, f := range ds.Fields {
			cd.FieldDefIds = append(cd.FieldDefIds, f.Id)
		}
		return cd
	}
	out := &types.CompiledType{}
	if len(t.Parts) == 0 {
		for _, d := range t.Datasets {
			cd := staticDataset(d, d.Name)
			out.Parts = append(out.Parts, types.CompiledPart{
				Id: d.Name, Key: d.Name, TypeId: t.Id, Datasets: []types.CompiledDataset{cd},
			})
			out.Datasets = append(out.Datasets, cd)
		}
		return out, nil
	}
	mods, err := staticModuleDatasets(t, modules)
	if err != nil {
		return nil, err
	}
	modByPart := make(map[string][]staticModuleDataset, len(t.Parts))
	known := make(map[string]struct{}, len(t.Datasets)+len(mods))
	for _, d := range t.Datasets {
		known[d.Name] = struct{}{}
	}
	for _, m := range mods {
		modByPart[m.partKey] = append(modByPart[m.partKey], m)
		known[m.key] = struct{}{}
	}
	parts := append([]handler.Part(nil), t.Parts...)
	sort.Slice(parts, func(i, j int) bool { return parts[i].Key < parts[j].Key })
	for _, p := range parts {
		cp := types.CompiledPart{
			Id: p.Key, Key: p.Key, TypeId: t.Id,
			Name: p.Name, Icon: p.Icon, Pos: p.Pos, Hidden: p.Hidden,
			UI: types.CloneXFormat(p.UI),
		}
		for _, u := range p.Uses {
			if _, ok := known[u]; ok {
				cp.Uses = append(cp.Uses, u)
			}
		}
		sort.Strings(cp.Uses)
		for _, d := range p.Datasets {
			if d.Name != "" {
				cp.Datasets = append(cp.Datasets, staticDataset(byName[d.Name], p.Key))
			}
		}
		for _, m := range modByPart[p.Key] {
			cp.Datasets = append(cp.Datasets, types.CompiledDataset{
				Name: m.collection, Key: m.key, Module: m.module, Shared: m.shared,
				PartId: p.Key, DefId: m.key, TypeId: t.Id,
			})
		}
		sort.Slice(cp.Datasets, func(i, j int) bool { return cp.Datasets[i].Key < cp.Datasets[j].Key })
		out.Parts = append(out.Parts, cp)
		out.Datasets = append(out.Datasets, cp.Datasets...)
	}
	sort.Slice(out.Datasets, func(i, j int) bool { return out.Datasets[i].Key < out.Datasets[j].Key })
	return out, nil
}
