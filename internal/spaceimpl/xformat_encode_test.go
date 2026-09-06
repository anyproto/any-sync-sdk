package spaceimpl

import (
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The descriptor rides the record as one object; a bag whose only key
// is an extended-JSON wrapper would convert to a scalar and hit the
// handler's shape rule with a raw error, so it is refused up front.
func TestEncodeXFormat(t *testing.T) {
	arena := &anyenc.Arena{}
	v, err := encodeXFormat(arena, map[string]any{"type": "choice", "options": map[string]any{"a": map[string]any{"name": "A"}}})
	require.NoError(t, err)
	assert.Equal(t, anyenc.TypeObject, v.Type())
	assert.Equal(t, "A", v.GetString("options", "a", "name"))

	_, err = encodeXFormat(arena, map[string]any{"$date": "2026-01-01T00:00:00Z"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be a JSON object")

	// A wrapper nested under a real key is a value like any other.
	v, err = encodeXFormat(arena, map[string]any{"acme": map[string]any{"$date": "2026-01-01T00:00:00Z"}})
	require.NoError(t, err)
	assert.Equal(t, anyenc.TypeDateTime, v.Get("acme").Type())
}
