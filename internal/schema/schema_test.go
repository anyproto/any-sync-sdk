package schema

import (
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mkProp builds a top-level property record: {id, key, kind}.
func mkProp(a *anyenc.Arena, key, kind string) *anyenc.Value {
	o := a.NewObject()
	o.Set("id", a.NewString(key))
	o.Set("key", a.NewString(key))
	o.Set("kind", a.NewString(kind))
	return o
}

// mkPropWithItems builds an array property record with an items sub-schema.
func mkPropWithItems(a *anyenc.Arena, key, itemKind string) *anyenc.Value {
	o := mkProp(a, key, "array")
	items := a.NewObject()
	items.Set("kind", a.NewString(itemKind))
	o.Set("items", items)
	return o
}

// mkPropWithProperties builds an object property record with a nested
// properties map. subs: map from field name to kind.
func mkPropWithProperties(a *anyenc.Arena, key string, subs map[string]string) *anyenc.Value {
	o := mkProp(a, key, "object")
	props := a.NewObject()
	for k, kind := range subs {
		sub := a.NewObject()
		sub.Set("kind", a.NewString(kind))
		props.Set(k, sub)
	}
	o.Set("properties", props)
	return o
}

func TestCompile_Empty(t *testing.T) {
	v, err := Compile(nil)
	require.NoError(t, err)
	require.NotNil(t, v)
	assert.ErrorIs(t, v.Validate("anything", nil), ErrUnknown)
}

func TestCompile_AllKinds(t *testing.T) {
	a := &anyenc.Arena{}
	recs := []*anyenc.Value{
		mkProp(a, "name", "string"),
		mkProp(a, "year", "number"),
		mkProp(a, "active", "boolean"),
		mkProp(a, "tags", "array"),
		mkProp(a, "meta", "object"),
		mkProp(a, "nullable", "null"),
	}
	v, err := Compile(recs)
	require.NoError(t, err)

	k, ok := v.Kind("name")
	require.True(t, ok)
	assert.Equal(t, KindString, k)

	_, ok = v.Kind("missing")
	assert.False(t, ok)
}

func TestCompile_DuplicateKeyLastWins(t *testing.T) {
	a := &anyenc.Arena{}
	recs := []*anyenc.Value{
		mkProp(a, "count", "string"),
		mkProp(a, "count", "number"),
	}
	v, err := Compile(recs)
	require.NoError(t, err)
	k, _ := v.Kind("count")
	assert.Equal(t, KindNumber, k)
}

func TestCompile_BadRecords(t *testing.T) {
	a := &anyenc.Arena{}

	t.Run("not an object", func(t *testing.T) {
		_, err := Compile([]*anyenc.Value{a.NewString("nope")})
		require.ErrorIs(t, err, ErrCompile)
	})
	t.Run("nil record", func(t *testing.T) {
		_, err := Compile([]*anyenc.Value{nil})
		require.ErrorIs(t, err, ErrCompile)
	})
	t.Run("missing key", func(t *testing.T) {
		o := a.NewObject()
		o.Set("kind", a.NewString("string"))
		_, err := Compile([]*anyenc.Value{o})
		require.ErrorIs(t, err, ErrCompile)
	})
	t.Run("empty key", func(t *testing.T) {
		o := a.NewObject()
		o.Set("key", a.NewString(""))
		o.Set("kind", a.NewString("string"))
		_, err := Compile([]*anyenc.Value{o})
		require.ErrorIs(t, err, ErrCompile)
	})
	t.Run("non-string key", func(t *testing.T) {
		o := a.NewObject()
		o.Set("key", a.NewNumberInt(1))
		o.Set("kind", a.NewString("string"))
		_, err := Compile([]*anyenc.Value{o})
		require.ErrorIs(t, err, ErrCompile)
	})
	t.Run("missing kind", func(t *testing.T) {
		o := a.NewObject()
		o.Set("key", a.NewString("name"))
		_, err := Compile([]*anyenc.Value{o})
		require.ErrorIs(t, err, ErrCompile)
	})
	t.Run("unknown kind", func(t *testing.T) {
		o := a.NewObject()
		o.Set("key", a.NewString("name"))
		o.Set("kind", a.NewString("wibble"))
		_, err := Compile([]*anyenc.Value{o})
		require.ErrorIs(t, err, ErrCompile)
	})
	t.Run("bad items", func(t *testing.T) {
		o := mkProp(a, "tags", "array")
		items := a.NewObject()
		items.Set("kind", a.NewString("wibble"))
		o.Set("items", items)
		_, err := Compile([]*anyenc.Value{o})
		require.ErrorIs(t, err, ErrCompile)
	})
	t.Run("properties not an object", func(t *testing.T) {
		o := mkProp(a, "meta", "object")
		o.Set("properties", a.NewString("nope"))
		_, err := Compile([]*anyenc.Value{o})
		require.ErrorIs(t, err, ErrCompile)
	})
	t.Run("bad nested property", func(t *testing.T) {
		o := mkProp(a, "meta", "object")
		props := a.NewObject()
		bad := a.NewObject()
		bad.Set("kind", a.NewString("wibble"))
		props.Set("inner", bad)
		o.Set("properties", props)
		_, err := Compile([]*anyenc.Value{o})
		require.ErrorIs(t, err, ErrCompile)
	})
}

// Leaf kind checks for all supported kinds.
func TestValidate_LeafKinds(t *testing.T) {
	a := &anyenc.Arena{}
	v, _ := Compile([]*anyenc.Value{
		mkProp(a, "s", "string"),
		mkProp(a, "n", "number"),
		mkProp(a, "b", "boolean"),
		mkProp(a, "arr", "array"),
		mkProp(a, "obj", "object"),
		mkProp(a, "z", "null"),
	})
	assert.NoError(t, v.Validate("s", a.NewString("hi")))
	assert.NoError(t, v.Validate("n", a.NewNumberInt(42)))
	assert.NoError(t, v.Validate("b", a.NewTrue()))
	assert.NoError(t, v.Validate("b", a.NewFalse()))
	assert.NoError(t, v.Validate("arr", a.NewArray()))
	assert.NoError(t, v.Validate("obj", a.NewObject()))
	assert.NoError(t, v.Validate("z", a.NewNull()))
}

func TestValidate_KindMismatch(t *testing.T) {
	a := &anyenc.Arena{}
	v, _ := Compile([]*anyenc.Value{mkProp(a, "name", "string")})
	assert.ErrorIs(t, v.Validate("name", a.NewNumberInt(42)), ErrKind)
}

func TestValidate_UnknownProperty(t *testing.T) {
	a := &anyenc.Arena{}
	v, _ := Compile([]*anyenc.Value{mkProp(a, "name", "string")})
	assert.ErrorIs(t, v.Validate("age", a.NewNumberInt(42)), ErrUnknown)
}

// nil value = field absent ($unset removed it). Always valid in v1.
func TestValidate_NilValueAccepted(t *testing.T) {
	a := &anyenc.Arena{}
	v, _ := Compile([]*anyenc.Value{mkProp(a, "name", "string")})
	assert.NoError(t, v.Validate("name", nil))
}

// Arrays without an items schema pass leaf-only check.
func TestValidate_UntypedArray(t *testing.T) {
	a := &anyenc.Arena{}
	v, _ := Compile([]*anyenc.Value{mkProp(a, "tags", "array")})
	arr := a.NewArray()
	arr.SetArrayItem(0, a.NewString("x"))
	arr.SetArrayItem(1, a.NewNumberInt(5))
	arr.SetArrayItem(2, a.NewTrue())
	assert.NoError(t, v.Validate("tags", arr))
}

// Arrays with items schema: every element checked.
func TestValidate_ArrayItemsMatch(t *testing.T) {
	a := &anyenc.Arena{}
	v, _ := Compile([]*anyenc.Value{mkPropWithItems(a, "tags", "string")})
	arr := a.NewArray()
	arr.SetArrayItem(0, a.NewString("a"))
	arr.SetArrayItem(1, a.NewString("b"))
	assert.NoError(t, v.Validate("tags", arr))
}

func TestValidate_ArrayItemsMismatch(t *testing.T) {
	a := &anyenc.Arena{}
	v, _ := Compile([]*anyenc.Value{mkPropWithItems(a, "tags", "string")})
	arr := a.NewArray()
	arr.SetArrayItem(0, a.NewString("ok"))
	arr.SetArrayItem(1, a.NewNumberInt(5)) // offender
	assert.ErrorIs(t, v.Validate("tags", arr), ErrKind)
}

// Objects without properties pass leaf-only check.
func TestValidate_UntypedObject(t *testing.T) {
	a := &anyenc.Arena{}
	v, _ := Compile([]*anyenc.Value{mkProp(a, "meta", "object")})
	obj := a.NewObject()
	obj.Set("anything", a.NewString("goes"))
	obj.Set("count", a.NewNumberInt(42))
	assert.NoError(t, v.Validate("meta", obj))
}

// Objects with properties: every field checked, unknown fields rejected.
func TestValidate_ObjectPropertiesMatch(t *testing.T) {
	a := &anyenc.Arena{}
	v, _ := Compile([]*anyenc.Value{mkPropWithProperties(a, "meta", map[string]string{
		"title": "string",
		"count": "number",
	})})
	obj := a.NewObject()
	obj.Set("title", a.NewString("hello"))
	obj.Set("count", a.NewNumberInt(1))
	assert.NoError(t, v.Validate("meta", obj))
}

func TestValidate_ObjectPropertyMismatch(t *testing.T) {
	a := &anyenc.Arena{}
	v, _ := Compile([]*anyenc.Value{mkPropWithProperties(a, "meta", map[string]string{
		"title": "string",
	})})
	obj := a.NewObject()
	obj.Set("title", a.NewNumberInt(42)) // wrong kind
	assert.ErrorIs(t, v.Validate("meta", obj), ErrKind)
}

func TestValidate_ObjectUnknownProperty(t *testing.T) {
	a := &anyenc.Arena{}
	v, _ := Compile([]*anyenc.Value{mkPropWithProperties(a, "meta", map[string]string{
		"title": "string",
	})})
	obj := a.NewObject()
	obj.Set("title", a.NewString("ok"))
	obj.Set("extra", a.NewString("rogue")) // not declared
	assert.ErrorIs(t, v.Validate("meta", obj), ErrUnknown)
}

// Deep nesting: array of objects with properties.
func TestValidate_ArrayOfObjects(t *testing.T) {
	a := &anyenc.Arena{}
	// tags is array of {name: string}.
	rec := mkProp(a, "tags", "array")
	items := a.NewObject()
	items.Set("kind", a.NewString("object"))
	itemProps := a.NewObject()
	nameSub := a.NewObject()
	nameSub.Set("kind", a.NewString("string"))
	itemProps.Set("name", nameSub)
	items.Set("properties", itemProps)
	rec.Set("items", items)

	v, err := Compile([]*anyenc.Value{rec})
	require.NoError(t, err)

	good := a.NewArray()
	e := a.NewObject()
	e.Set("name", a.NewString("alpha"))
	good.SetArrayItem(0, e)
	assert.NoError(t, v.Validate("tags", good))

	bad := a.NewArray()
	e2 := a.NewObject()
	e2.Set("name", a.NewNumberInt(1)) // name should be string
	bad.SetArrayItem(0, e2)
	assert.ErrorIs(t, v.Validate("tags", bad), ErrKind)
}

// Zero-alloc guarantee on scalar success paths. If this breaks the hot
// path has regressed.
func TestValidate_ZeroAllocsOnScalarSuccess(t *testing.T) {
	a := &anyenc.Arena{}
	v, err := Compile([]*anyenc.Value{
		mkProp(a, "name", "string"),
		mkProp(a, "count", "number"),
		mkProp(a, "active", "boolean"),
		mkProp(a, "nullable", "null"),
	})
	require.NoError(t, err)

	strVal := a.NewString("hi")
	numVal := a.NewNumberInt(1)
	boolVal := a.NewTrue()
	nullVal := a.NewNull()

	cases := []struct {
		name string
		fn   func()
	}{
		{"string", func() { _ = v.Validate("name", strVal) }},
		{"number", func() { _ = v.Validate("count", numVal) }},
		{"boolean", func() { _ = v.Validate("active", boolVal) }},
		{"null", func() { _ = v.Validate("nullable", nullVal) }},
		{"nil (absent)", func() { _ = v.Validate("name", nil) }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			allocs := testing.AllocsPerRun(100, c.fn)
			assert.Equal(t, 0.0, allocs, "expected zero allocs on scalar success")
		})
	}
}
