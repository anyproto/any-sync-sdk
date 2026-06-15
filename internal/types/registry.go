package types

import "github.com/anyproto/any-sync-sdk/internal/schema"

// Registry resolves (typeId, propId) to the property's declared schema
// kind. The full Registry will also expose x-key lookups, the known-
// shortIds set, and detached-changes drainage; for v1 only the
// kind-lookup surface is needed by the property handler.
//
// Implementations:
//
//   - real: built from per-space type-object trees on space open;
//     compiled schemas are cached, invalidated by type-object changes.
//   - StubRegistry (registry_stub.go): hand-stuffed map for tests.
//
// A handler depending on the Registry takes a Registry value at
// construction; nil means "no validation" (the handler skips kind
// checks). This lets callers wire the handler before the type system
// is available, and adopts validation later.
type Registry interface {
	// LookupKind returns the declared kind of the property with the
	// given (typeId, propId). Returns (KindUnknown, false) if either
	// the type or the property is unknown to this registry.
	LookupKind(typeId, propId string) (schema.Kind, bool)

	// TypeKnown reports whether a schema for typeId is resolvable —
	// a built-in (any / spaceIndex), a registered external type, or a
	// user type whose property defs have synced. Distinguishes
	// "type not implemented / not here yet" from "type known, property
	// unknown" so the writer-side validator can produce a precise
	// rejection.
	TypeKnown(typeId string) bool

	// PropsOf returns the declared properties of typeId for validation
	// and for building agent-readable error messages (valid-property
	// lists). ok is false when the type is unresolvable.
	PropsOf(typeId string) (props []PropInfo, ok bool)
}

// PropInfo is one declared property: its id, display name, kind, and
// write/sync scope. Returned by PropsOf for validation and error
// formatting. A zero Scope reads as ScopeSynced (definitions written
// before scopes existed carry no scope field).
type PropInfo struct {
	Id    string
	Name  string
	Kind  schema.Kind
	Scope schema.Scope
}

// EffectiveScope normalizes the zero value to ScopeSynced — the scope
// every pre-scope definition implicitly had.
func (p PropInfo) EffectiveScope() schema.Scope {
	if p.Scope == 0 {
		return schema.ScopeSynced
	}
	return p.Scope
}
