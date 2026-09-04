package space

import "encoding/json"

// DatasetSchema describes one dataset's fields as a standard JSON Schema
// document, for consumer discovery (Space.Datasets / Service.Datasets).
//
// JSONSchema is a JSON Schema object:
//
//	{"type":"object",
//	 "properties":{"<field>":{"type":"string","title":"…","x-scope":"synced"}},
//	 "additionalProperties":<dynamic>}
//
// The `x-scope` extension keyword carries each field's class:
//   - "synced"  — user/DAG-written, synced across the account's devices;
//   - "derived" — handler-computed, read-only to writers;
//   - "local"   — device-local, never synced.
//
// `additionalProperties:true` marks a dynamic dataset (free-form keys,
// e.g. the per-type object properties), where undeclared fields are
// allowed and treated as synced.
//
// Behavioral declarations ride further x-keywords: per-field
// `x-mutable-by` / `x-stamp`, dataset-level `required`, `x-delete-by`,
// `x-id` (+ `x-id-pattern` / `x-id-max-length`), and `x-search`
// ({title,text} field mapping for external indexers; `text` is a bare
// field key or an array of keys — a single key marshals as the bare
// string).
type DatasetSchema struct {
	Name       string
	JSONSchema json.RawMessage
	// Owners are the types that declare the dataset: exactly one for a
	// registered-type or namespaced dataset, every type declaring a
	// shared dataset of the module for a canonical collection (empty
	// while nothing declares it), none for space-level built-ins.
	// Consumers gate indexing/eviction on it — an object may hold the
	// dataset when it carries one of the owners.
	Owners []string
	// Module is the serving module ("records" for the generic
	// schema-enforced kind, "editor" / "chat" for registered modules);
	// empty for built-ins and registered-type datasets. Shared marks a
	// module's canonical collection.
	Module string
	Shared bool
}
