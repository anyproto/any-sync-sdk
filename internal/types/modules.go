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
// name, the shared collection it owns (empty = none), whether it
// admits namespaced instances, and whether runtime declarations may
// name it at all (Reserved — a draft-time refusal; the compile keeps
// an applied declaration valid).
type ModuleInfo struct {
	Name       string
	Canonical  string
	SharedOnly bool
	Reserved   bool
}

// ErrModuleReserved marks a draft naming a module the consumer keeps
// for its own installs.
var ErrModuleReserved = errors.New("types: module is reserved")

// CheckReserved refuses a draft naming a reserved module. Unknown
// modules pass here — Collection reports them.
func (m Modules) CheckReserved(module string) error {
	if mi, ok := m[module]; ok && mi.Reserved {
		return fmt.Errorf("%w: %q", ErrModuleReserved, module)
	}
	return nil
}

// Modules is the module catalog keyed by name. Always carries the
// built-in records module.
type Modules map[string]ModuleInfo

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

// Collection applies the collection rule to one declaration: a shared
// dataset is the module's canonical collection, anything else is
// namespaced under the declaring type. The error names the rule the
// declaration violates.
func (m Modules) Collection(typeId, key, module string, shared bool) (string, error) {
	mi, ok := m[module]
	if !ok {
		return "", fmt.Errorf("%w %q", ErrUnknownModule, module)
	}
	if shared {
		if mi.Canonical == "" {
			return "", fmt.Errorf("%w: module %q has no shared collection", schema.ErrDecl, module)
		}
		if key != mi.Canonical {
			return "", fmt.Errorf("%w: a shared %q dataset is keyed %q, got %q", schema.ErrDecl, module, mi.Canonical, key)
		}
		return mi.Canonical, nil
	}
	if mi.SharedOnly {
		return "", fmt.Errorf("%w: module %q admits only shared datasets", schema.ErrDecl, module)
	}
	if err := schema.ValidateSlug("dataset", key); err != nil {
		return "", err
	}
	return CollectionName(typeId, key), nil
}
