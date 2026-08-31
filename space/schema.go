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
	// TypeId is the owning type for type-owned datasets (registered via
	// config or defined at runtime on a type object); empty for
	// space-level built-ins. Consumers gate indexing/eviction on it.
	TypeId string
}
