package types

import "github.com/anyproto/any-sync-sdk/internal/schema"

// StubRegistry is a hand-stuffed Registry for tests and bring-up of
// downstream packages that need a concrete value before the real
// Registry exists. Not safe for concurrent writes after first use.
type StubRegistry struct {
	// Kinds[typeId][propId] = declared kind.
	Kinds map[string]map[string]schema.Kind
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
