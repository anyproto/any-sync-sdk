package types

import (
	"github.com/anyproto/any-store/v2/anyenc"
)

// DecodeXFormat reads a stored `x-format` object into plain Go values —
// nested objects as map[string]any, arrays as []any, numbers as
// float64, instants as time.Time — for the public definition views.
// Nil for an absent, non-object or empty value. The bag is opaque: no
// key is interpreted here.
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
	}
	return nil
}
