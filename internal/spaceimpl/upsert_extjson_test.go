package spaceimpl

import (
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/require"
)

// Upsert records arrive as Go maps (encoding/json), Modify ops as
// fastjson: both must yield the same typed value for an Extended-JSON
// wrapper, or a declared datetime field accepts one route and rejects
// the other ("kind mismatch: got object, declared datetime").
func TestGoToAnyencDecodesExtJSONDateWrappers(t *testing.T) {
	a := &anyenc.Arena{}
	for _, in := range []any{
		map[string]any{"$date": "2026-08-25T16:00:00.000Z"},
		map[string]any{"$date": float64(1787673600000)}, // encoding/json numbers
	} {
		v, err := goToAnyenc(a, in)
		require.NoError(t, err)
		require.Equal(t, anyenc.TypeDateTime, v.Type(), "%v", in)
		ms, err := v.DateTimeMillis()
		require.NoError(t, err)
		require.Equal(t, time.Date(2026, 8, 25, 16, 0, 0, 0, time.UTC).UnixMilli(), ms)
	}
	// nested inside a record's object stays decoded too
	v, err := goToAnyenc(a, map[string]any{"meta": map[string]any{"$date": float64(1000)}})
	require.NoError(t, err)
	require.Equal(t, anyenc.TypeDateTime, v.Get("meta").Type())
	// a malformed wrapper and an ordinary object stay objects
	for _, in := range []any{
		map[string]any{"$date": "soon"},
		map[string]any{"$date": "2026-08-25T16:00:00Z", "x": 1},
		map[string]any{"date": "2026-08-25T16:00:00Z"},
	} {
		v, err := goToAnyenc(a, in)
		require.NoError(t, err)
		require.Equal(t, anyenc.TypeObject, v.Type(), "%v", in)
	}
}
