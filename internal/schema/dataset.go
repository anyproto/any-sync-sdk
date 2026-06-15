package schema

import (
	"encoding/json"
	"fmt"
)

// Scope is the single write/sync taxonomy shared by dataset fields AND
// property definitions: how a value is written, which version domain
// stamps its `_ver` entries, and how far it syncs. One vocabulary
// everywhere — dataset schema fields, property defs, and the x-scope
// discovery keyword all use these labels.
//
// Each scope is a disjoint write route. A given field/property lives in
// exactly ONE scope for its whole life (scope is pinned at declaration,
// like a property's kind) — there is no per-value override stack. That
// disjointness is what lets versions from different domains coexist in
// one `_ver` tree: no two routes ever gate on the same path.
type Scope uint8

const (
	// ScopeSynced: user/DAG-written through the object's own tree,
	// change-versioned, normal LWW, synced to everyone with access.
	// (Docs historically called this "base".)
	ScopeSynced Scope = iota + 1
	// ScopeDerived: handler-computed from the change, change-versioned,
	// converges across peers; never writable by an input op. (Was
	// ScopeAuto.) e.g. author(creator)/createdAt/_ver.id.
	ScopeDerived
	// ScopeLocal: materialised on-device via Object.LocalSet,
	// lexid.Next-versioned, never synced. e.g. localStatus.
	// (Docs historically called this "device".)
	ScopeLocal
	// ScopeAccount: synced across the SAME account's devices only, via a
	// carrier record in the private tech space; a per-device watcher
	// mirrors converged values into the target record, stamping the
	// tech tree's versionIds. Invisible to other space members.
	ScopeAccount
)

func (s Scope) String() string {
	switch s {
	case ScopeSynced:
		return "synced"
	case ScopeDerived:
		return "derived"
	case ScopeLocal:
		return "local"
	case ScopeAccount:
		return "account"
	}
	return "unknown"
}

// ParseScope parses a scope label. Returns (0, false) on an unknown
// label — the runtime counterpart of MustScope for wire-read paths.
func ParseScope(label string) (Scope, bool) {
	switch label {
	case "synced":
		return ScopeSynced, true
	case "derived":
		return ScopeDerived, true
	case "local":
		return ScopeLocal, true
	case "account":
		return ScopeAccount, true
	}
	return 0, false
}

// Field is one declared dataset field: a JSON-Schema value shape plus its
// class. Modeled like a type property (Id/Name + recursive Schema) so the
// two share one representation.
type Field struct {
	Id     string
	Name   string
	Schema *Schema // value shape; nil means unconstrained
	Scope  Scope
}

// Dataset is a dataset's required, JSON-Schema-compatible declaration.
// Dynamic datasets (shortIds, the per-type `objects` namespace) carry a
// free-form key space: undeclared fields are allowed and default to
// ScopeSynced; declared fields (e.g. derived auto-fields) are still
// enforced.
type Dataset struct {
	Fields  []Field
	Dynamic bool
}

// ScopeOf returns the declared class of field id and whether it's
// declared. Undeclared fields on a Dynamic dataset are treated as
// ScopeSynced by the apply path; this reports only what's declared.
func (d Dataset) ScopeOf(id string) (Scope, bool) {
	for i := range d.Fields {
		if d.Fields[i].Id == id {
			return d.Fields[i].Scope, true
		}
	}
	return 0, false
}

// MarshalJSON emits a standard JSON Schema object document:
//
//	{"type":"object","properties":{<id>:{<value schema>,"title":..,"x-scope":..}},
//	 "additionalProperties":<Dynamic>}
//
// The per-field class rides as the `x-scope` extension keyword (precedent:
// the docs' x-refType). Cold path — discovery only; allocates freely.
func (d Dataset) MarshalJSON() ([]byte, error) {
	props := make(map[string]any, len(d.Fields))
	for _, f := range d.Fields {
		node := schemaToJSON(f.Schema)
		if f.Name != "" {
			node["title"] = f.Name
		}
		node["x-scope"] = f.Scope.String()
		props[f.Id] = node
	}
	doc := map[string]any{
		"type":                 "object",
		"properties":           props,
		"additionalProperties": d.Dynamic,
	}
	return json.Marshal(doc)
}

// MarshalJSON emits a JSON-Schema node for a value shape: {"type":..},
// with `items` for arrays and `properties` for objects (recursive).
func (s *Schema) MarshalJSON() ([]byte, error) {
	return json.Marshal(schemaToJSON(s))
}

// schemaToJSON builds the JSON-Schema map for a value shape. A nil schema
// is an unconstrained value ({} — any type), matching the validator's
// progressive-disclosure semantics.
func schemaToJSON(s *Schema) map[string]any {
	if s == nil {
		return map[string]any{}
	}
	node := map[string]any{}
	if s.Kind != KindUnknown {
		node["type"] = s.Kind.String()
	}
	switch s.Kind {
	case KindArray:
		if s.Items != nil {
			node["items"] = schemaToJSON(s.Items)
		}
	case KindObject:
		if s.Properties != nil {
			p := make(map[string]any, len(s.Properties))
			for k, sub := range s.Properties {
				p[k] = schemaToJSON(sub)
			}
			node["properties"] = p
		}
	}
	return node
}

// Leaf builds a scalar/leaf value Schema for a Kind (no items/properties).
// Convenience for declaring simple dataset fields.
func Leaf(k Kind) *Schema { return &Schema{Kind: k} }

// MustScope parses a scope label, panicking on an unknown one. For static
// declarations where the value is a compile-time constant.
func MustScope(label string) Scope {
	s, ok := ParseScope(label)
	if !ok {
		panic(fmt.Sprintf("schema: unknown scope %q", label))
	}
	return s
}
