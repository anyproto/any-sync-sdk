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
}
