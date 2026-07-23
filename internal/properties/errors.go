package properties

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/types"
)

// Validation reasons — the machine-readable discriminant on a
// ValidationError. Stable strings: callers (and agents reading the
// surfaced message) can switch on them.
const (
	ReasonInvalidPath        = "invalid_path"
	ReasonTypeNotImplemented = "type_not_implemented"
	ReasonTypeUnknown        = "type_unknown"
	ReasonUnknownProperty    = "unknown_property"
	ReasonKindMismatch       = "kind_mismatch"
	ReasonScopeMismatch      = "scope_mismatch"
)

// Per-reason sentinels. A ValidationError chains to exactly one of these
// (via Unwrap) so callers can classify the specific rejection with
// errors.Is — no string matching on the message — while still matching
// the umbrella crdt.ErrValidation. The handler package re-exports these
// for external callers.
var (
	ErrInvalidPath        = errors.New("property write rejected: path must be {typeId}.{propId}")
	ErrTypeNotImplemented = errors.New("property write rejected: object does not implement the type")
	ErrTypeUnknown        = errors.New("property write rejected: type schema is not resolvable on this peer")
	ErrUnknownProperty    = errors.New("property write rejected: type has no such property")
	ErrKindMismatch       = errors.New("property write rejected: value kind does not match the declared kind")
	ErrScopeMismatch      = errors.New("property write rejected: write route does not match the property's declared scope")
)

// reasonErr maps a Reason discriminant to its sentinel. Unknown reasons
// (none today) return nil so Unwrap still ties to crdt.ErrValidation.
func reasonErr(reason string) error {
	switch reason {
	case ReasonInvalidPath:
		return ErrInvalidPath
	case ReasonTypeNotImplemented:
		return ErrTypeNotImplemented
	case ReasonTypeUnknown:
		return ErrTypeUnknown
	case ReasonUnknownProperty:
		return ErrUnknownProperty
	case ReasonKindMismatch:
		return ErrKindMismatch
	case ReasonScopeMismatch:
		return ErrScopeMismatch
	}
	return nil
}

// ValidationError describes one rejected property write in terms an
// agent can act on: which type and property, what was expected vs.
// supplied, and the set of valid choices. It wraps crdt.ErrValidation
// so callers can classify with errors.Is and discriminate via Reason.
//
// Produced by the local write-time pre-flight (returned to the caller,
// rejecting the whole write) and by the apply-time per-op validator
// (recorded in ApplyResult.Rejections for dropped inbound ops).
type ValidationError struct {
	Reason string

	TypeId   string
	TypeName string // display name when known
	PropId   string
	PropName string // display name when known

	Expected schema.Kind // declared kind (kind_mismatch)
	Got      schema.Kind // supplied value's kind (kind_mismatch)

	// DeclaredScope / WriteRoute discriminate a scope_mismatch: the
	// property's pinned scope vs the route this write arrived on.
	DeclaredScope schema.Scope
	WriteRoute    schema.Scope

	Known []types.PropInfo // declared properties of the type (unknown_property)
	Types []string         // the object's any.types (type_not_implemented)
	Path  []string         // offending op path (invalid_path)
}

// Error renders an agent-readable, single-line rejection message.
func (e *ValidationError) Error() string {
	switch e.Reason {
	case ReasonInvalidPath:
		return fmt.Sprintf("property write rejected: path must be {typeId}.{propId}; got %q",
			strings.Join(e.Path, "."))
	case ReasonTypeNotImplemented:
		return fmt.Sprintf("property write rejected: object does not implement type %s; attach it to any.types before writing %s.* values. Current types: [%s]",
			e.typeLabel(), e.TypeId, strings.Join(e.Types, ", "))
	case ReasonTypeUnknown:
		return fmt.Sprintf("property write rejected: type %s has no resolvable schema on this peer; define its properties (or wait for the type to sync) before writing %s.* values",
			e.typeLabel(), e.TypeId)
	case ReasonUnknownProperty:
		return fmt.Sprintf("property write rejected: type %s has no property %q. Valid properties: %s — use one of these ids or define it via Types.AddProperty",
			e.typeLabel(), e.PropId, e.knownList())
	case ReasonKindMismatch:
		return fmt.Sprintf("property write rejected: property %s on type %s expects %s, got %s",
			e.propLabel(), e.typeLabel(), e.Expected, e.Got)
	case ReasonScopeMismatch:
		return fmt.Sprintf("property write rejected: property %s on type %s is %s-scoped and cannot be written through the %s route",
			e.propLabel(), e.typeLabel(), e.DeclaredScope, e.WriteRoute)
	}
	return fmt.Sprintf("property write rejected: %s.%s", e.TypeId, e.PropId)
}

// Unwrap ties the error to the crdt.ErrValidation umbrella sentinel (so
// errors.Is(err, crdt.ErrValidation) holds across both validation paths)
// and to the per-reason sentinel (so callers can classify the specific
// rejection with errors.Is). Multi-error form per Go 1.20 semantics.
func (e *ValidationError) Unwrap() []error {
	if r := reasonErr(e.Reason); r != nil {
		return []error{crdt.ErrValidation, r}
	}
	return []error{crdt.ErrValidation}
}

// typeLabel is `"Name" (id)` when a display name is known, else `(id)`.
func (e *ValidationError) typeLabel() string {
	if e.TypeName != "" {
		return fmt.Sprintf("%q (%s)", e.TypeName, e.TypeId)
	}
	return fmt.Sprintf("(%s)", e.TypeId)
}

// propLabel is `"Name" (id)` when a display name is known, else `(id)`.
func (e *ValidationError) propLabel() string {
	if e.PropName != "" {
		return fmt.Sprintf("%q (%s)", e.PropName, e.PropId)
	}
	return fmt.Sprintf("(%s)", e.PropId)
}

// knownList renders the declared properties as
// `name (id, kind), …`, sorted by name then id for stable output.
// "(none)" when the type declares no properties.
func (e *ValidationError) knownList() string {
	if len(e.Known) == 0 {
		return "(none)"
	}
	ps := slices.Clone(e.Known)
	slices.SortFunc(ps, func(a, b types.PropInfo) int {
		return cmp.Or(cmp.Compare(a.Name, b.Name), cmp.Compare(a.Id, b.Id))
	})
	parts := make([]string, 0, len(ps))
	for _, p := range ps {
		if p.Name != "" {
			parts = append(parts, fmt.Sprintf("%s (%s, %s)", p.Name, p.Id, p.Kind))
		} else {
			parts = append(parts, fmt.Sprintf("%s (%s)", p.Id, p.Kind))
		}
	}
	return strings.Join(parts, ", ")
}
