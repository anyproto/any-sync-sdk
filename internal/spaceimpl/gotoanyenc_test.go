package spaceimpl

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"
)

// Go-map and fastjson inputs decode through the same rule, so a
// declared datetime field accepts an Extended-JSON wrapper on every
// write path.
func TestGoToAnyencExtJSONWrappers(t *testing.T) {
	a := &anyenc.Arena{}
	want := time.Date(2026, 8, 25, 16, 0, 0, 0, time.UTC).UnixMilli()
	for _, in := range []any{
		map[string]any{"$date": "2026-08-25T16:00:00.000Z"},
		map[string]any{"$date": float64(want)}, // encoding/json numbers
		map[string]any{"$date": want},
	} {
		v, err := goToAnyenc(a, in)
		require.NoError(t, err)
		require.Equal(t, anyenc.TypeDateTime, v.Type(), "%v", in)
		ms, err := v.DateTimeMillis()
		require.NoError(t, err)
		require.Equal(t, want, ms)
	}

	v, err := goToAnyenc(a, map[string]any{
		"meta": map[string]any{"$date": float64(1000)},
		"list": []any{map[string]any{"$date": "2026-01-01T00:00:00Z"}},
		"blob": map[string]any{"$binary": "aGVsbG8="},
	})
	require.NoError(t, err)
	require.Equal(t, anyenc.TypeDateTime, v.Get("meta").Type())
	require.Equal(t, anyenc.TypeDateTime, v.Get("list", "0").Type())
	require.Equal(t, anyenc.TypeBinary, v.Get("blob").Type())
	require.Equal(t, []byte("hello"), v.Get("blob").GetBytes())

	// a malformed wrapper and an ordinary object stay objects
	for _, in := range []any{
		map[string]any{"$date": "soon"},
		map[string]any{"$date": "2026-08-25T16:00:00Z", "x": 1},
		map[string]any{"date": "2026-08-25T16:00:00Z"},
		map[string]any{"$unknown": 1},
	} {
		v, err := goToAnyenc(a, in)
		require.NoError(t, err)
		require.Equal(t, anyenc.TypeObject, v.Type(), "%v", in)
	}
}

// The Go route and the fastjson route yield byte-identical documents.
func TestGoToAnyencMatchesFastJSONRoute(t *testing.T) {
	doc := map[string]any{
		"at":   map[string]any{"$date": "2026-08-25T16:00:00Z"},
		"n":    float64(3),
		"s":    "x<y&z",
		"tags": []any{"a", "b"},
		"nil":  nil,
	}
	raw, err := json.Marshal(doc)
	require.NoError(t, err)
	jv, err := fastjson.ParseBytes(raw)
	require.NoError(t, err)

	a := &anyenc.Arena{}
	fromGo, err := goToAnyenc(a, doc)
	require.NoError(t, err)
	fromFastJSON, err := goToAnyenc(a, jv)
	require.NoError(t, err)
	require.Equal(t, fromFastJSON.MarshalTo(nil), fromGo.MarshalTo(nil))
}

func TestGoToAnyencGoValues(t *testing.T) {
	a := &anyenc.Arena{}
	type row struct {
		Name  string `json:"name"`
		Count int    `json:"count,omitempty"`
		skip  bool
	}
	cases := []struct {
		in   any
		want anyenc.Type
	}{
		{nil, anyenc.TypeNull},
		{(*fastjson.Value)(nil), anyenc.TypeNull},
		{(*anyenc.Value)(nil), anyenc.TypeNull},
		{true, anyenc.TypeTrue},
		{"s", anyenc.TypeString},
		{int64(7), anyenc.TypeNumber},
		{uint8(7), anyenc.TypeNumber},
		{float32(1.5), anyenc.TypeNumber},
		{json.Number("42"), anyenc.TypeNumber},
		{[]string{"a"}, anyenc.TypeArray},
		{[]int{1, 2}, anyenc.TypeArray},
		{[]map[string]any{{"k": 1}}, anyenc.TypeArray},
		{map[string]string{"k": "v"}, anyenc.TypeObject},
		{row{Name: "r", skip: true}, anyenc.TypeObject},
		{&row{Name: "r"}, anyenc.TypeObject},
		// encoding/json renders these as strings; typed values are
		// wrappers or *anyenc.Value.
		{time.Unix(0, 0).UTC(), anyenc.TypeString},
		{[]byte("raw"), anyenc.TypeString},
	}
	for _, c := range cases {
		v, err := goToAnyenc(a, c.in)
		require.NoError(t, err, "%T", c.in)
		require.Equal(t, c.want, v.Type(), "%T %v", c.in, c.in)
	}

	v, err := goToAnyenc(a, row{Name: "r", Count: 2})
	require.NoError(t, err)
	require.Equal(t, "r", v.GetString("name"))
	require.Equal(t, float64(2), v.GetFloat64("count"))
	require.Nil(t, v.Get("skip"))

	// pass-through keeps the caller's value
	own := a.NewString("own")
	v, err = goToAnyenc(a, own)
	require.NoError(t, err)
	require.Same(t, own, v)

	// encoding/json rejections surface as errors
	_, err = goToAnyenc(a, math.NaN())
	require.Error(t, err)
	_, err = goToAnyenc(a, make(chan int))
	require.Error(t, err)
}
