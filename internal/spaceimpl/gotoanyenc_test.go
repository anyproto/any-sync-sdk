package spaceimpl

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"
)

// Go-map and fastjson inputs decode through the same rule, so a
// declared datetime field accepts an Extended-JSON wrapper on every
// write path.
func TestGoToAnyencExtJSONWrappers(t *testing.T) {
	a := &anyenc.Arena{}
	at := time.Date(2026, 8, 25, 16, 0, 0, 0, time.UTC)
	for _, in := range []any{
		map[string]any{"$date": "2026-08-25T16:00:00.000Z"},
		map[string]any{"$date": float64(at.UnixMilli())}, // encoding/json numbers
		map[string]any{"$date": at.UnixMilli()},
		map[string]any{"$date": at},
		at,
	} {
		v, err := goToAnyenc(a, in)
		require.NoError(t, err)
		require.Equal(t, anyenc.TypeDateTime, v.Type(), "%v", in)
		ms, err := v.DateTimeMillis()
		require.NoError(t, err)
		require.Equal(t, at.UnixMilli(), ms)
	}

	v, err := goToAnyenc(a, map[string]any{
		"meta": map[string]any{"$date": float64(1000)},
		"list": []any{map[string]any{"$date": "2026-01-01T00:00:00Z"}, at},
		"blob": map[string]any{"$binary": "aGVsbG8="},
		"raw":  []byte("hello"),
	})
	require.NoError(t, err)
	require.Equal(t, anyenc.TypeDateTime, v.Get("meta").Type())
	require.Equal(t, anyenc.TypeDateTime, v.Get("list", "0").Type())
	require.Equal(t, anyenc.TypeDateTime, v.Get("list", "1").Type())
	require.Equal(t, anyenc.TypeBinary, v.Get("blob").Type())
	require.Equal(t, []byte("hello"), v.Get("blob").GetBytes())
	require.Equal(t, v.Get("blob").MarshalTo(nil), v.Get("raw").MarshalTo(nil))

	// a malformed wrapper, an ordinary object and an unknown $-key stay objects
	for _, in := range []any{
		map[string]any{"$date": "soon"},
		map[string]any{"$date": "2026-08-25T16:00:00Z", "x": 1},
		map[string]any{"date": "2026-08-25T16:00:00Z"},
		map[string]any{"$unknown": 1},
		map[string]any{"$gte": at},
	} {
		v, err := goToAnyenc(a, in)
		require.NoError(t, err)
		require.Equal(t, anyenc.TypeObject, v.Type(), "%v", in)
	}
	v, err = goToAnyenc(a, map[string]any{"$gte": at})
	require.NoError(t, err)
	require.Equal(t, anyenc.TypeDateTime, v.Get("$gte").Type(), "operator objects are walked, not marshaled")
}

// The Go route and the fastjson route yield byte-identical documents.
func TestGoToAnyencMatchesFastJSONRoute(t *testing.T) {
	doc := map[string]any{
		"at":   map[string]any{"$date": "2026-08-25T16:00:00Z"},
		"n":    float64(3),
		"i":    7,
		"s":    "x<y&z",
		"tags": []any{"a", "b", map[string]any{"$date": float64(5)}},
		"strs": []string{"c"},
		"rows": []map[string]any{{"k": 1}},
		"nil":  nil,
		"one":  map[string]any{"k": []any{}},
		"b":    true,
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
		{time.Unix(0, 0), anyenc.TypeDateTime},
		{[]byte("raw"), anyenc.TypeBinary},
		{[]string{"a"}, anyenc.TypeArray},
		{[]string(nil), anyenc.TypeArray},
		{[]any(nil), anyenc.TypeArray},
		{map[string]any(nil), anyenc.TypeObject},
		{[]int{1, 2}, anyenc.TypeArray},
		{[]map[string]any{{"k": 1}}, anyenc.TypeArray},
		{map[string]string{"k": "v"}, anyenc.TypeObject},
		{row{Name: "r", skip: true}, anyenc.TypeObject},
		{&row{Name: "r"}, anyenc.TypeObject},
		// encoding/json renders these as strings inside a struct
		{struct{ At time.Time }{time.Unix(0, 0)}, anyenc.TypeObject},
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
	v, err = goToAnyenc(a, struct{ At time.Time }{time.Unix(0, 0)})
	require.NoError(t, err)
	require.Equal(t, anyenc.TypeString, v.Get("At").Type())

	// pass-through keeps the caller's value, at any container depth
	own := a.NewString("own")
	v, err = goToAnyenc(a, own)
	require.NoError(t, err)
	require.Same(t, own, v)
	jv, err := fastjson.Parse(`{"$date": 1000}`)
	require.NoError(t, err)
	v, err = goToAnyenc(a, map[string]any{"o": own, "l": []any{own, jv}})
	require.NoError(t, err)
	require.Same(t, own, v.Get("o"))
	require.Same(t, own, v.Get("l", "0"))
	require.Equal(t, anyenc.TypeDateTime, v.Get("l", "1").Type())

	// floats keep their bits; sorted keys make the encoding deterministic
	v, err = goToAnyenc(a, map[string]any{"f": 1.602176634e-19})
	require.NoError(t, err)
	require.Equal(t, math.Float64bits(1.602176634e-19), math.Float64bits(v.GetFloat64("f")))
	first := v.MarshalTo(nil)
	for i := 0; i < 8; i++ {
		v, err = goToAnyenc(a, map[string]any{"z": 1, "a": 2, "m": 3, "f": 1.602176634e-19})
		require.NoError(t, err)
		require.Equal(t, anyenc.TypeObject, v.Type())
		w, err := goToAnyenc(a, map[string]any{"f": 1.602176634e-19})
		require.NoError(t, err)
		require.Equal(t, first, w.MarshalTo(nil))
	}

	// non-finite numbers and encoding/json rejections surface as errors
	for _, in := range []any{
		math.NaN(), math.Inf(1), json.Number("1e999"), json.Number("NaN"),
		make(chan int),
	} {
		_, err = goToAnyenc(a, in)
		require.Error(t, err, "%T %v", in, in)
	}
	_, err = goToAnyenc(a, map[string]any{"bad": []any{math.Inf(1)}})
	require.ErrorContains(t, err, `"bad": [0]:`)
}

// A Go filter literal types a time.Time the way the record holds it,
// so a datetime field matches by either spelling.
func TestParseConditionTypesGoLiterals(t *testing.T) {
	at := time.Date(2026, 8, 25, 16, 0, 0, 0, time.UTC)
	a := &anyenc.Arena{}
	doc := a.NewObject()
	doc.Set("at", a.NewDateTime(at))

	for _, cond := range []any{
		map[string]any{"at": map[string]any{"$gte": at}},
		map[string]any{"at": map[string]any{"$gte": map[string]any{"$date": "2026-08-25T16:00:00Z"}}},
		map[string]any{"$and": []any{map[string]any{"at": at}}},
		`{"at": {"$date": "2026-08-25T16:00:00Z"}}`,
	} {
		f, err := parseCondition(cond)
		require.NoError(t, err, "%v", cond)
		require.True(t, f.Ok(doc, nil), "%v", cond)
	}
	f, err := parseCondition(map[string]any{"at": map[string]any{"$gt": at}})
	require.NoError(t, err)
	require.False(t, f.Ok(doc, nil))

	// prebuilt filters and nil pass straight through
	f, err = parseCondition(query.All{})
	require.NoError(t, err)
	require.Equal(t, query.All{}, f)
	f, err = parseCondition(nil)
	require.NoError(t, err)
	require.Equal(t, query.All{}, f)
}
