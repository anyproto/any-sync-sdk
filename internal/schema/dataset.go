package schema

import (
	"encoding/json"
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
// label.
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

// Mutability is a declared field's post-create write rule, enforced by
// the generic schema handler. The zero value is write-once: the field
// is writable ONLY in the record's creating change — there is no late
// fill (a presence-based rule would diverge under concurrent fills).
// Declare MutableBy for fields that must stay settable later.
type Mutability uint8

const (
	// MutableNever: writable only in the record's creating change.
	MutableNever Mutability = iota
	// MutableByAuthor: only the record's creator (the StampCreator
	// field) may rewrite; accepted writes bump the modifyTime stamp.
	MutableByAuthor
	// MutableByAnyone: any writer may rewrite; accepted writes bump the
	// modifyTime stamp.
	MutableByAnyone
)

func (m Mutability) String() string {
	switch m {
	case MutableByAuthor:
		return "author"
	case MutableByAnyone:
		return "any"
	}
	return "never"
}

// ParseMutability parses a mutability label ("never"/"author"/"any").
func ParseMutability(label string) (Mutability, bool) {
	switch label {
	case "never", "":
		return MutableNever, true
	case "author":
		return MutableByAuthor, true
	case "any":
		return MutableByAnyone, true
	}
	return 0, false
}

// Stamp marks a field whose value the generic schema handler derives
// from the change at apply time. A stamped field is ScopeDerived, so
// client writes to it are rejected by the controller's scope
// enforcement; the handler is its only writer.
type Stamp uint8

const (
	StampNone Stamp = iota
	// StampCreator: the creating change's signer identity, set once at
	// create. The authorship fact author-gated rules check against.
	StampCreator
	// StampCreateTime: the creating change's timestamp, set once.
	StampCreateTime
	// StampModifyTime: the change timestamp, set at create and bumped
	// on every accepted mutable write.
	StampModifyTime
)

func (s Stamp) String() string {
	switch s {
	case StampCreator:
		return "creator"
	case StampCreateTime:
		return "createTime"
	case StampModifyTime:
		return "modifyTime"
	}
	return "none"
}

// ParseStamp parses a stamp label ("creator"/"createTime"/"modifyTime").
func ParseStamp(label string) (Stamp, bool) {
	switch label {
	case "", "none":
		return StampNone, true
	case "creator":
		return StampCreator, true
	case "createTime":
		return StampCreateTime, true
	case "modifyTime":
		return StampModifyTime, true
	}
	return 0, false
}

// IdRule declares how the dataset's record ids are produced.
type IdRule uint8

const (
	// IdAuto (zero): records are created with an empty id and the id is
	// derived from the change (the existing empty-id upsert sugar);
	// explicit caller ids are rejected at create.
	IdAuto IdRule = iota
	// IdUser: the caller supplies the id, constrained by
	// IdPattern/IdMaxLen. The id doubles as the upsert idempotency
	// key. Contract: one writer per id — concurrent creates of the
	// same id by different members take arrival-order-dependent
	// creation verdicts (required checks, creator stamp) and are
	// outside the convergence guarantee.
	IdUser
)

func (r IdRule) String() string {
	if r == IdUser {
		return "user"
	}
	return "auto"
}

// ParseIdRule parses an id-rule label ("auto"/"user").
func ParseIdRule(label string) (IdRule, bool) {
	switch label {
	case "", "auto":
		return IdAuto, true
	case "user":
		return IdUser, true
	}
	return 0, false
}

// DeletePolicy is the dataset-level record-delete gate.
type DeletePolicy uint8

const (
	// DeleteByAnyone (zero): any writer may delete a record.
	DeleteByAnyone DeletePolicy = iota
	// DeleteByAuthor: only the record's creator (StampCreator field)
	// may delete it.
	DeleteByAuthor
)

func (p DeletePolicy) String() string {
	if p == DeleteByAuthor {
		return "author"
	}
	return "anyone"
}

// ParseDeletePolicy parses a delete-policy label ("anyone"/"author").
func ParseDeletePolicy(label string) (DeletePolicy, bool) {
	switch label {
	case "", "anyone":
		return DeleteByAnyone, true
	case "author":
		return DeleteByAuthor, true
	}
	return 0, false
}

// SearchFields is the dataset's search-extraction annotation: which
// field feeds the document title and which the body text. Opaque to the
// SDK — surfaced through discovery (`x-search`) for external indexers.
type SearchFields struct {
	Title string
	Text  string
}

// Field is one declared dataset field: a JSON-Schema value shape plus its
// class. Modeled like a type property (Id/Name + recursive Schema) so the
// two share one representation.
type Field struct {
	Id     string
	Name   string
	Schema *Schema // value shape; nil means unconstrained
	Scope  Scope

	// Required: the field must be present in the create payload.
	// Enforced by the generic schema handler; mutually exclusive with
	// Stamp.
	Required bool
	// MutableBy: post-create write rule. Zero = write-once.
	MutableBy Mutability
	// Stamp: apply-time derived value. Non-zero forces ScopeDerived.
	Stamp Stamp
}

// Dataset is a dataset's required, JSON-Schema-compatible declaration.
// Dynamic datasets (shortIds, the per-type `objects` namespace) carry a
// free-form key space: undeclared fields are allowed and default to
// ScopeSynced; declared fields (e.g. derived auto-fields) are still
// enforced.
type Dataset struct {
	Fields  []Field
	Dynamic bool

	// DeleteBy: record-delete gate. Zero = anyone.
	DeleteBy DeletePolicy
	// IdRule: how record ids are produced. Zero = auto (derived).
	IdRule IdRule
	// IdPattern is an RE2 pattern user-supplied ids must match in full
	// (IdUser only). Empty = DefaultIdPattern.
	IdPattern string
	// IdMaxLen caps user-supplied id length (IdUser only). 0 =
	// DefaultIdMaxLen.
	IdMaxLen int
	// Search is the optional search-extraction annotation.
	Search *SearchFields
}

// Defaults for IdUser constraints when the declaration leaves them zero.
const (
	DefaultIdPattern = `[A-Za-z0-9._:-]+`
	DefaultIdMaxLen  = 128
)

// Normalized returns a copy with zero-value scopes resolved: stamped
// fields are ScopeDerived (they are handler-written), everything else
// defaults to ScopeSynced. Registration and discovery paths call this
// once so enforcement never sees a zero scope; returns the receiver
// unchanged when nothing needs resolving.
func (d Dataset) Normalized() Dataset {
	needs := false
	for i := range d.Fields {
		f := &d.Fields[i]
		if f.Scope == 0 || (f.Stamp != StampNone && f.Scope != ScopeDerived) {
			needs = true
			break
		}
	}
	if !needs {
		return d
	}
	out := d
	out.Fields = make([]Field, len(d.Fields))
	copy(out.Fields, d.Fields)
	for i := range out.Fields {
		f := &out.Fields[i]
		if f.Stamp != StampNone {
			f.Scope = ScopeDerived
		} else if f.Scope == 0 {
			f.Scope = ScopeSynced
		}
	}
	return out
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
//	 "required":[..],"additionalProperties":<Dynamic>}
//
// The per-field class rides as the `x-scope` extension keyword (precedent:
// the docs' x-refType); behavioral declarations ride as `x-mutable-by`,
// `x-stamp`, and the dataset-level `x-delete-by` / `x-id` /
// `x-id-pattern` / `x-id-max-length` / `x-search` keywords. Defaults are
// omitted. Cold path — discovery only; allocates freely.
func (d Dataset) MarshalJSON() ([]byte, error) {
	props := make(map[string]any, len(d.Fields))
	var required []string
	for _, f := range d.Fields {
		node := schemaToJSON(f.Schema)
		if f.Name != "" {
			node["title"] = f.Name
		}
		node["x-scope"] = f.Scope.String()
		if f.MutableBy != MutableNever {
			node["x-mutable-by"] = f.MutableBy.String()
		}
		if f.Stamp != StampNone {
			node["x-stamp"] = f.Stamp.String()
		}
		if f.Required {
			required = append(required, f.Id)
		}
		props[f.Id] = node
	}
	doc := map[string]any{
		"type":                 "object",
		"properties":           props,
		"additionalProperties": d.Dynamic,
	}
	if len(required) > 0 {
		doc["required"] = required
	}
	if d.DeleteBy != DeleteByAnyone {
		doc["x-delete-by"] = d.DeleteBy.String()
	}
	if d.IdRule != IdAuto {
		doc["x-id"] = d.IdRule.String()
		if d.IdPattern != "" {
			doc["x-id-pattern"] = d.IdPattern
		}
		if d.IdMaxLen > 0 {
			doc["x-id-max-length"] = d.IdMaxLen
		}
	}
	if d.Search != nil {
		s := map[string]any{}
		if d.Search.Title != "" {
			s["title"] = d.Search.Title
		}
		if d.Search.Text != "" {
			s["text"] = d.Search.Text
		}
		doc["x-search"] = s
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
