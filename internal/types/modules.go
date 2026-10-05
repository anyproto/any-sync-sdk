package types

import (
	"errors"
	"fmt"

	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// RecordsModule is the built-in generic module: a schema-enforced
// dataset with no canonical collection, always namespaced. Every other
// module is caller-registered (config.Config.Modules).
const RecordsModule = "records"

// ModuleInfo is what the compiler needs to know about a module: its
// name, the canonical collection it owns (empty = none), whether it
// admits namespaced instances, and whether runtime declarations may
// name it at all (Reserved — a draft-time refusal; the compile keeps
// an applied declaration valid).
type ModuleInfo struct {
	Name          string
	Canonical     string
	CanonicalOnly bool
	Reserved      bool
}

// Modules is the module catalog keyed by name. Always carries the
// built-in records module.
type Modules map[string]ModuleInfo

// Reserved reports whether module is registered as reserved. Unknown
// modules are not — Collection reports them.
func (m Modules) Reserved(module string) bool {
	mi, ok := m[module]
	return ok && mi.Reserved
}

// NewModules builds a catalog from the caller's modules plus records.
func NewModules(infos ...ModuleInfo) Modules {
	m := Modules{RecordsModule: {Name: RecordsModule}}
	for _, mi := range infos {
		m[mi.Name] = mi
	}
	return m
}

// ErrUnknownModule marks a dataset declaring a module this SDK does not
// carry. The definition compiles Invalid: visible, never registered.
var ErrUnknownModule = errors.New("types: unknown module")

// CollectionName is the namespaced collection of one dataset: the
// declaring type's id and the dataset key joined by `_`. Type ids are
// content-addressed CIDs without `_`, so the first `_` always splits.
func CollectionName(typeId, key string) string { return typeId + "_" + key }

// CanonicalKey is the key a dataset of module takes when a declaration
// leaves it empty: the module's canonical collection name, "" when the
// module has none or is unknown.
func (m Modules) CanonicalKey(module string) string { return m[module].Canonical }

// IsCanonicalKey reports whether key names module's canonical
// collection — the rule a declaration follows: the key decides.
func (m Modules) IsCanonicalKey(module, key string) bool {
	c := m[module].Canonical
	return c != "" && c == key
}

// DraftCollection applies the collection rule to a declaration being
// made: a dataset keyed by its module's canonical name is that
// collection, any other key is namespaced under the declaring type.
// canonical is what the stored head records.
func (m Modules) DraftCollection(typeId, key, module string) (name string, canonical bool, err error) {
	canonical = m.IsCanonicalKey(module, key)
	name, err = m.Collection(typeId, key, module, canonical)
	return name, canonical, err
}

// Collection resolves the collection of one stored declaration.
// canonical is the head's stored marker: set, the dataset is the
// module's canonical collection and its key must be that name; unset,
// it is namespaced under the declaring type, whatever its key. The
// error names the rule the declaration violates.
func (m Modules) Collection(typeId, key, module string, canonical bool) (string, error) {
	mi, ok := m[module]
	if !ok {
		return "", fmt.Errorf("%w %q", ErrUnknownModule, module)
	}
	if canonical {
		if mi.Canonical == "" {
			return "", fmt.Errorf("%w: module %q has no canonical collection", schema.ErrDecl, module)
		}
		if key != mi.Canonical {
			return "", fmt.Errorf("%w: the canonical %q dataset is keyed %q, got %q", schema.ErrDecl, module, mi.Canonical, key)
		}
		return mi.Canonical, nil
	}
	if mi.CanonicalOnly {
		return "", fmt.Errorf("%w: module %q admits only its canonical dataset %q, got key %q", schema.ErrDecl, module, mi.Canonical, key)
	}
	if err := schema.ValidateSlug("dataset", key); err != nil {
		return "", err
	}
	return CollectionName(typeId, key), nil
}
