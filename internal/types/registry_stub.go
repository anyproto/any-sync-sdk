package types

import "github.com/anyproto/any-sync-sdk/internal/schema"

// StubRegistry is a hand-stuffed Registry for tests and bring-up of
// downstream packages that need a concrete value before the real
// Registry exists. Not safe for concurrent writes after first use.
type StubRegistry struct {
	// Kinds[typeId][propId] = declared kind.
	Kinds map[string]map[string]schema.Kind
	// Scopes[typeId][propId] = declared scope. Optional — absent
	// entries read as the zero value (ScopeSynced via
	// PropInfo.EffectiveScope), matching pre-scope definitions.
	Scopes map[string]map[string]schema.Scope
}

func (s StubRegistry) LookupKind(typeId, propId string) (schema.Kind, bool) {
	if s.Kinds == nil {
		return schema.KindUnknown, false
	}
	props, ok := s.Kinds[typeId]
	if !ok {
		return schema.KindUnknown, false
	}
	k, ok := props[propId]
	return k, ok
}

// TypeKnown reports whether the stub has any entry for typeId.
func (s StubRegistry) TypeKnown(typeId string) bool {
	if s.Kinds == nil {
		return false
	}
	_, ok := s.Kinds[typeId]
	return ok
}

// PropsOf returns the stub's declared properties for typeId. Names are
// empty (the stub stores kinds and scopes only). ok mirrors TypeKnown.
func (s StubRegistry) PropsOf(typeId string) ([]PropInfo, bool) {
	props, ok := s.Kinds[typeId]
	if !ok {
		return nil, false
	}
	out := make([]PropInfo, 0, len(props))
	for id, k := range props {
		out = append(out, PropInfo{Id: id, Kind: k, Scope: s.Scopes[typeId][id]})
	}
	return out, true
}

// Compile-time check that StubRegistry satisfies Registry.
var _ Registry = StubRegistry{}

// Set adds or overwrites a property entry. Useful for incremental
// test setup: r.Set("any", "name", schema.KindString).
func (s *StubRegistry) Set(typeId, propId string, kind schema.Kind) {
	if s.Kinds == nil {
		s.Kinds = make(map[string]map[string]schema.Kind)
	}
	if _, ok := s.Kinds[typeId]; !ok {
		s.Kinds[typeId] = make(map[string]schema.Kind)
	}
	s.Kinds[typeId][propId] = kind
}

// SetScoped adds or overwrites a property entry with an explicit scope.
func (s *StubRegistry) SetScoped(typeId, propId string, kind schema.Kind, scope schema.Scope) {
	s.Set(typeId, propId, kind)
	if s.Scopes == nil {
		s.Scopes = make(map[string]map[string]schema.Scope)
	}
	if _, ok := s.Scopes[typeId]; !ok {
		s.Scopes[typeId] = make(map[string]schema.Scope)
	}
	s.Scopes[typeId][propId] = scope
}
