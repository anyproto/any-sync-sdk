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

// Collection applies the collection rule to one declaration. The key
// decides: a dataset keyed by its module's canonical name is that
// collection, any other key is namespaced under the declaring type.
// The error names the rule the declaration violates.
func (m Modules) Collection(typeId, key, module string) (string, error) {
	mi, ok := m[module]
	if !ok {
		return "", fmt.Errorf("%w %q", ErrUnknownModule, module)
	}
	if mi.Canonical != "" && key == mi.Canonical {
		return mi.Canonical, nil
	}
	if mi.CanonicalOnly {
		return "", fmt.Errorf("%w: a %q dataset is keyed %q, got %q", schema.ErrDecl, module, mi.Canonical, key)
	}
	if err := schema.ValidateSlug("dataset", key); err != nil {
		return "", err
	}
	return CollectionName(typeId, key), nil
}

// IsCanonical reports whether collection is module's canonical one.
func (m Modules) IsCanonical(module, collection string) bool {
	c := m[module].Canonical
	return c != "" && c == collection
}
