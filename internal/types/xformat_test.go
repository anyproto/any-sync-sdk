package types

import (
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every anyenc type the write path can produce decodes to a plain Go
// value; nothing is dropped silently.
func TestDecodeXFormat_AllTypes(t *testing.T) {
	arena := &anyenc.Arena{}
	ts := time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC)
	var oid anyenc.ObjectID
	copy(oid[:], []byte{0x50, 0x7f, 0x1f, 0x77, 0xbc, 0xf8, 0x6c, 0xd7, 0x99, 0x43, 0x90, 0x11})

	obj := arena.NewObject()
	obj.Set("s", arena.NewString("text"))
	obj.Set("n", arena.NewNumberFloat64(2.5))
	obj.Set("t", arena.NewTrue())
	obj.Set("f", arena.NewFalse())
	obj.Set("z", arena.NewNull())
	obj.Set("d", arena.NewDateTime(ts))
	obj.Set("b", arena.NewBinary([]byte{1, 2}))
	obj.Set("o", arena.NewObjectID(oid))
	obj.Set("v", arena.NewVectorF32([]float32{0.5, 1}))
	arr := arena.NewArray()
	arr.SetArrayItem(0, arena.NewString("a"))
	nested := arena.NewObject()
	nested.Set("deep", arena.NewNumberInt(1))
	arr.SetArrayItem(1, nested)
	obj.Set("arr", arr)

	got := DecodeXFormat(obj)
	require.NotNil(t, got)
	assert.Equal(t, "text", got["s"])
	assert.Equal(t, 2.5, got["n"])
	assert.Equal(t, true, got["t"])
	assert.Equal(t, false, got["f"])
	_, hasNull := got["z"]
	assert.True(t, hasNull, "a null member is present as nil")
	assert.Nil(t, got["z"])
	assert.Equal(t, ts, got["d"])
	assert.Equal(t, []byte{1, 2}, got["b"])
	assert.Equal(t, oid.Hex(), got["o"])
	assert.Equal(t, []float64{0.5, 1}, got["v"])
	assert.Equal(t, []any{"a", map[string]any{"deep": float64(1)}}, got["arr"])

	assert.Nil(t, DecodeXFormat(nil))
	assert.Nil(t, DecodeXFormat(arena.NewString("email")), "a non-object reads back absent")
	assert.Nil(t, DecodeXFormat(arena.NewObject()), "an empty object reads back absent")
}

// A clone shares nothing with its source below the top level.
func TestCloneXFormat_Deep(t *testing.T) {
	src := map[string]any{
		"config":  map[string]any{"multiple": true},
		"targets": []any{"a", map[string]any{"k": "v"}},
		"keys":    []string{"x"},
	}
	got := CloneXFormat(src)
	got["config"].(map[string]any)["multiple"] = false
	got["targets"].([]any)[1].(map[string]any)["k"] = "changed"
	got["keys"].([]string)[0] = "changed"
	assert.Equal(t, true, src["config"].(map[string]any)["multiple"])
	assert.Equal(t, "v", src["targets"].([]any)[1].(map[string]any)["k"])
	assert.Equal(t, "x", src["keys"].([]string)[0])
	assert.Nil(t, CloneXFormat(nil))
}
