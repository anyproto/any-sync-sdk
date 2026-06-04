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
type DatasetSchema struct {
	Name       string
	JSONSchema json.RawMessage
}
