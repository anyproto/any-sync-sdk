// Package schema implements the minimal JSON-Schema subset used by the
// types/properties layer. It operates natively on *anyenc.Value — no
// conversion to interface{} or JSON on the validation path — and the
// Validate method allocates zero on scalar success paths. This is
// load-bearing: schema checks run on every write op.
//
// Supported in v1:
//   - Leaf kinds: `string`, `number`, `boolean`, `null`, `array`, `object`.
//   - `items` on arrays — recursive sub-schema for array elements.
//   - `properties` on objects — recursive sub-schemas for named fields.
//
// Fields not present on a record are left unconstrained (an object
// without `properties` accepts any shape; an array without `items`
// accepts any element). This is progressive disclosure — simple schemas
// stay simple. Additional keywords (`enum`, `required`, `additionalProperties`)
// land when a concrete need appears. See docs/types-properties-proposal.md.
//
// The Validator's single entry point is Validate(name, value), called
// after an op has been applied to the record. The caller identifies
// which top-level field(s) the op touched and asks "is the current
// state valid?". This keeps the validator API op-type-agnostic — $set,
// $inc, $addToSet, $pull, $unset all reduce to "inspect the top-level
// field's current value".
package schema

import (
	"errors"
	"fmt"

	"github.com/anyproto/any-store/anyenc"
)

// Kind is the declared type of a value.
type Kind uint8

const (
	KindUnknown Kind = iota
	KindString
	KindNumber
	KindBoolean
	KindNull
	KindArray
	KindObject
)

func (k Kind) String() string {
	switch k {
	case KindString:
		return "string"
	case KindNumber:
		return "number"
	case KindBoolean:
		return "boolean"
	case KindNull:
		return "null"
	case KindArray:
		return "array"
	case KindObject:
		return "object"
	}
	return "unknown"
}

func parseKind(s string) (Kind, bool) {
	switch s {
	case "string":
		return KindString, true
	case "number":
		return KindNumber, true
	case "boolean":
		return KindBoolean, true
	case "null":
		return KindNull, true
	case "array":
		return KindArray, true
	case "object":
		return KindObject, true
	}
	return KindUnknown, false
}

// Sentinel errors — specific error messages wrap these via fmt.Errorf on
// the cold failure path. Callers classify with errors.Is. The success
// path never constructs an error.
var (
	ErrCompile = errors.New("schema: compile error")
	ErrUnknown = errors.New("schema: unknown property")
	ErrKind    = errors.New("schema: kind mismatch")
)

// Schema is a recursive schema node. Items is populated only for
// KindArray (nil means "any element"); Properties only for KindObject
// (nil means "any shape").
type Schema struct {
	Kind       Kind
	Items      *Schema
	Properties map[string]*Schema
}

// Validator is a compiled schema. Built once via Compile; used read-only
// on many change-apply paths. Safe for concurrent use after Compile returns.
type Validator struct {
	root *Schema
}

// Compile builds a Validator from top-level property records. Each record
// is an anyenc object with string `key` and a `kind` plus optional
// `items` / `properties` recursive sub-schemas.
//
// Duplicate keys are last-wins — the CRDT layer is responsible for
// surfacing/resolving client-side conflicts.
//
// Allocates freely — this is the cold path that runs once when a schema
// version is compiled.
func Compile(records []*anyenc.Value) (*Validator, error) {
	root := &Schema{Kind: KindObject, Properties: make(map[string]*Schema, len(records))}
	for i, rec := range records {
		if rec == nil || rec.Type() != anyenc.TypeObject {
			return nil, fmt.Errorf("%w: record %d is not an object", ErrCompile, i)
		}
		keyV := rec.Get("key")
		if keyV == nil || keyV.Type() != anyenc.TypeString {
			return nil, fmt.Errorf("%w: record %d: missing or non-string `key`", ErrCompile, i)
		}
		key := string(keyV.GetStringBytes())
		if key == "" {
			return nil, fmt.Errorf("%w: record %d: empty `key`", ErrCompile, i)
		}
		sub, err := compileSchema(rec)
		if err != nil {
			return nil, fmt.Errorf("record %d (key=%q): %w", i, key, err)
		}
		root.Properties[key] = sub
	}
	return &Validator{root: root}, nil
}

// compileSchema reads `kind`, `items`, `properties` from an anyenc
// object. Used for top-level property records (the `key` / `id` fields
// are extracted by the caller) and for recursive sub-schemas.
func compileSchema(v *anyenc.Value) (*Schema, error) {
	if v == nil || v.Type() != anyenc.TypeObject {
		return nil, fmt.Errorf("%w: sub-schema is not an object", ErrCompile)
	}
	kindV := v.Get("kind")
	if kindV == nil || kindV.Type() != anyenc.TypeString {
		return nil, fmt.Errorf("%w: missing or non-string `kind`", ErrCompile)
	}
	kindStr := string(kindV.GetStringBytes())
	k, ok := parseKind(kindStr)
	if !ok {
		return nil, fmt.Errorf("%w: unknown kind %q", ErrCompile, kindStr)
	}
	out := &Schema{Kind: k}
	switch k {
	case KindArray:
		if items := v.Get("items"); items != nil {
			sub, err := compileSchema(items)
			if err != nil {
				return nil, fmt.Errorf("items: %w", err)
			}
			out.Items = sub
		}
	case KindObject:
		if props := v.Get("properties"); props != nil {
			if props.Type() != anyenc.TypeObject {
				return nil, fmt.Errorf("%w: `properties` must be an object", ErrCompile)
			}
			propsObj, _ := props.Object()
			propMap := make(map[string]*Schema, propsObj.Len())
			var firstErr error
			propsObj.Visit(func(k []byte, pv *anyenc.Value) {
				if firstErr != nil {
					return
				}
				sub, err := compileSchema(pv)
				if err != nil {
					firstErr = fmt.Errorf("properties.%s: %w", string(k), err)
					return
				}
				propMap[string(k)] = sub
			})
			if firstErr != nil {
				return nil, firstErr
			}
			out.Properties = propMap
		}
	}
	return out, nil
}

// Kind returns the declared kind of a top-level property and whether
// the property is declared at all.
func (v *Validator) Kind(name string) (Kind, bool) {
	s, ok := v.root.Properties[name]
	if !ok {
		return KindUnknown, false
	}
	return s.Kind, true
}

// Validate checks that the top-level field `name` with its current
// `value` is consistent with the schema.
//
// Call after the op has been applied to the record — pass each top-level
// field the op touched and the field's current value (or nil if the op
// removed it). Absent fields (nil value) are always accepted in v1
// (no `required` keyword yet).
//
// Zero allocations on success when both the schema and value at `name`
// are primitive (kind is string/number/boolean/null and value is a
// scalar).
func (v *Validator) Validate(name string, value *anyenc.Value) error {
	s, ok := v.root.Properties[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknown, name)
	}
	return validateValue(s, value)
}

// kindOf maps an anyenc type to its schema Kind. Returns KindUnknown for
// unsupported types (e.g. binary).
func kindOf(v *anyenc.Value) Kind {
	switch v.Type() {
	case anyenc.TypeString:
		return KindString
	case anyenc.TypeNumber:
		return KindNumber
	case anyenc.TypeTrue, anyenc.TypeFalse:
		return KindBoolean
	case anyenc.TypeNull:
		return KindNull
	case anyenc.TypeArray:
		return KindArray
	case anyenc.TypeObject:
		return KindObject
	}
	return KindUnknown
}

// validateValue checks `value` against `s` recursively.
//
//   - A nil value is always accepted (field absent after $unset).
//   - Leaf kinds are type-checked.
//   - Arrays with an Items schema have every element checked.
//   - Objects with a Properties map have every field checked, unknown
//     fields rejected with ErrUnknown.
//   - Untyped arrays/objects (no Items / no Properties) pass the leaf
//     type check and stop.
func validateValue(s *Schema, value *anyenc.Value) error {
	if value == nil {
		return nil
	}
	actual := kindOf(value)
	if actual == KindUnknown {
		return fmt.Errorf("%w: unsupported value type", ErrKind)
	}
	if actual != s.Kind {
		return fmt.Errorf("%w: got %s, declared %s", ErrKind, actual, s.Kind)
	}
	switch s.Kind {
	case KindArray:
		if s.Items == nil {
			return nil
		}
		items, _ := value.Array()
		for i, it := range items {
			if err := validateValue(s.Items, it); err != nil {
				return fmt.Errorf("[%d]: %w", i, err)
			}
		}
	case KindObject:
		if s.Properties == nil {
			return nil
		}
		obj, _ := value.Object()
		var firstErr error
		obj.Visit(func(k []byte, val *anyenc.Value) {
			if firstErr != nil {
				return
			}
			sub, ok := s.Properties[string(k)]
			if !ok {
				firstErr = fmt.Errorf("%w: %q", ErrUnknown, string(k))
				return
			}
			if err := validateValue(sub, val); err != nil {
				firstErr = fmt.Errorf("%s: %w", string(k), err)
			}
		})
		return firstErr
	}
	return nil
}
