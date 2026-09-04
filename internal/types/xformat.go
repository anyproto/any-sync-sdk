package types

import (
	"github.com/anyproto/any-store/v2/anyenc"
)

// DecodeXFormat reads a stored `x-format` object into plain Go values —
// nested objects as map[string]any, arrays as []any, numbers as
// float64, instants as time.Time, binaries as []byte, object ids as
// their hex string, float vectors as []float64 — for the public
// definition views. Nil for an absent, non-object or empty value. The
// bag is opaque: no key is interpreted here.
func DecodeXFormat(v *anyenc.Value) map[string]any {
	if v == nil || v.Type() != anyenc.TypeObject {
		return nil
	}
	m, _ := anyencToGo(v).(map[string]any)
	if len(m) == 0 {
		return nil
	}
	return m
}

// CloneXFormat deep-copies a descriptor so a public view never aliases
// the caller's (or a registration's) nested maps and slices.
func CloneXFormat(m map[string]any) map[string]any {
	if len(m) == 0 {
		return nil
	}
	out, _ := cloneGo(m).(map[string]any)
	return out
}

func cloneGo(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			out[k] = cloneGo(val)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, el := range x {
			out[i] = cloneGo(el)
		}
		return out
	case []string:
		return append([]string(nil), x...)
	case []float64:
		return append([]float64(nil), x...)
	case []byte:
		return append([]byte(nil), x...)
	}
	return v
}

func anyencToGo(v *anyenc.Value) any {
	if v == nil {
		return nil
	}
	switch v.Type() {
	case anyenc.TypeObject:
		obj, _ := v.Object()
		out := make(map[string]any, obj.Len())
		obj.Visit(func(k []byte, val *anyenc.Value) {
			out[string(k)] = anyencToGo(val)
		})
		return out
	case anyenc.TypeArray:
		arr, _ := v.Array()
		out := make([]any, len(arr))
		for i, el := range arr {
			out[i] = anyencToGo(el)
		}
		return out
	case anyenc.TypeString:
		return string(v.GetStringBytes())
	case anyenc.TypeNumber:
		return v.GetFloat64()
	case anyenc.TypeTrue:
		return true
	case anyenc.TypeFalse:
		return false
	case anyenc.TypeDateTime:
		if ts, err := v.DateTime(); err == nil {
			return ts
		}
		return nil
	case anyenc.TypeBinary:
		return append([]byte(nil), v.GetBytes()...)
	case anyenc.TypeObjectID:
		if id, err := v.ObjectID(); err == nil {
			return id.Hex()
		}
		return nil
	case anyenc.TypeVectorF32:
		vec, err := v.VectorF32()
		if err != nil {
			return nil
		}
		out := make([]float64, len(vec))
		for i, f := range vec {
			out[i] = float64(f)
		}
		return out
	}
	return nil
}
