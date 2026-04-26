// The system properties handler — the single crdt.Handler every user
// object registers. See doc.go for the package role.

package properties

import "github.com/anyproto/any-sync-sdk/internal/crdt"

// Dataset is the name every object uses for its base-scope property
// writes on its own CRDT. Writes on this dataset project into the
// per-space `properties` system collection keyed by objectId.
//
// The same name appears in two places: the per-object dataset (source
// of writes, handled here) and the per-space system collection
// (projection target). This is intentional — the dataset keeps its
// identity across the hop and queries don't need to learn two names.
const Dataset = "properties"

// HandlerVersion is the DataVersion string stamped on every change
// this handler emits against an object's `properties` dataset. Bump
// the suffix when validation rules change in a way that must reject
// stale writers (see docs/types-properties-proposal.md § "Change-
// level DataVersion" — data-dataset version is a hardcoded handler
// identifier in Phase 1).
const HandlerVersion = "systemPropertyHandler-v1"

// SystemPropertiesHandler validates base-scope property writes on
// every user object. Corresponds to the `baseProperty` handler named
// in docs/06-data-structure.md § "Handlers" — renamed here to
// emphasize its scope (the per-space `properties` system dataset)
// and to distinguish it from the types package's property handler
// (which governs property definitions on type objects).
//
// One instance is registered on every user object's crdt.Controller
// at open time. The concrete validator will enforce, per
// docs/06-data-structure.md § "Object Properties":
//
//   - "an object can only update its own record" — base-scope writes
//     use RecordChange.Id == "" (docs convention: empty record id),
//     and the handler pins the projected record id to the owning
//     object's id;
//   - path namespacing — top-level keys must be a known typeId (or
//     the `any` shortId) that the object implements, and nested
//     keys must be registered propIds under that type;
//   - value kinds — validated against the schema compiled from the
//     governing type object's current property records (see types
//     Registry); kind-mismatched ops drop silently per Phase-1
//     "per-op atomicity".
//
// `any`-typed objects (universal base properties like name,
// description, icon) flow through this same handler — their schema
// comes from internal/types/any.Properties, not a user type record.
//
// The projection into the per-space `properties` collection lives
// outside the crdt.Handler.Validate contract (see crdt/handler.go:
// "Apply is omitted ... Phase 2 will add hooks if/when needed").
// Scaffolded as a no-op until the registry and the apply hook land.
type SystemPropertiesHandler struct{}

func (SystemPropertiesHandler) Dataset() string                              { return Dataset }
func (SystemPropertiesHandler) Version() int                                 { return 1 }
func (SystemPropertiesHandler) Validate(_ crdt.RecordChange, _ crdt.Op) error { return nil }
