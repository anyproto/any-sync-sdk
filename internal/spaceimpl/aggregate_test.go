package spaceimpl

import (
	"context"
	"errors"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-sync-sdk/space"
)

const tombstoneSkipJSON = `{"$match":{"_deletedAt":{"$exists":false}}}`

// combinedJSON builds the executable pipeline and renders it as JSON
// (via the FastJson bridge — anyenc's own MarshalTo is binary).
func combinedJSON(t *testing.T, a *aggImpl) string {
	t.Helper()
	require.NoError(t, a.parseErr)
	fa := &fastjson.Arena{}
	return string(a.combined().FastJson(fa).MarshalTo(nil))
}

func TestAggNormalizePipeline_InputForms(t *testing.T) {
	userJSON := `[{"$match":{"a":1}},{"$count":"n"}]`
	want := `[` + tombstoneSkipJSON + `,{"$match":{"a":1}},{"$count":"n"}]`

	t.Run("json string", func(t *testing.T) {
		a := newAgg(nil, "obj", "ds", userJSON)
		assert.Equal(t, want, combinedJSON(t, a))
	})

	t.Run("fastjson value", func(t *testing.T) {
		fv, err := fastjson.Parse(userJSON)
		require.NoError(t, err)
		a := newAgg(nil, "obj", "ds", fv)
		assert.Equal(t, want, combinedJSON(t, a))
	})

	t.Run("anyenc value cross-arena graft", func(t *testing.T) {
		// The user value lives on its own parser buffers; combined()
		// grafts its stages into an array built on the aggImpl arena.
		// The render must round-trip intact.
		av, err := anyenc.ParseJson(userJSON)
		require.NoError(t, err)
		a := newAgg(nil, "obj", "ds", av)
		assert.Equal(t, want, combinedJSON(t, a))
	})

	t.Run("marshaled anyenc bytes", func(t *testing.T) {
		raw := anyenc.MustParseJson(userJSON).MarshalTo(nil)
		a := newAgg(nil, "obj", "ds", raw)
		assert.Equal(t, want, combinedJSON(t, a))
	})

	t.Run("go value", func(t *testing.T) {
		a := newAgg(nil, "obj", "ds", []map[string]any{{"$limit": 2}})
		assert.Equal(t, `[`+tombstoneSkipJSON+`,{"$limit":2}]`, combinedJSON(t, a))
	})

	t.Run("nil pipeline is just the skip stage", func(t *testing.T) {
		a := newAgg(nil, "obj", "ds", nil)
		assert.Equal(t, `[`+tombstoneSkipJSON+`]`, combinedJSON(t, a))
	})
}

func TestAggNormalizePipeline_Errors(t *testing.T) {
	t.Run("malformed json", func(t *testing.T) {
		a := newAgg(nil, "obj", "ds", `[{"$match":`)
		require.Error(t, a.parseErr)
		assert.ErrorIs(t, a.parseErr, space.ErrBadPipeline)
	})

	t.Run("non-array", func(t *testing.T) {
		a := newAgg(nil, "obj", "ds", `{"$match":{"a":1}}`)
		require.Error(t, a.parseErr)
		assert.ErrorIs(t, a.parseErr, space.ErrBadPipeline)
	})

	t.Run("unmarshalable go value", func(t *testing.T) {
		a := newAgg(nil, "obj", "ds", map[string]any{"ch": make(chan int)})
		require.Error(t, a.parseErr)
		assert.ErrorIs(t, a.parseErr, space.ErrBadPipeline)
	})

	t.Run("parse error surfaces on terminals before store access", func(t *testing.T) {
		// store is nil — a terminal must short-circuit on parseErr
		// without touching it.
		a := newAgg(nil, "obj", "ds", `not json`)
		_, err := a.All(context.Background())
		assert.ErrorIs(t, err, space.ErrBadPipeline)
		_, err = a.Count(context.Background())
		assert.ErrorIs(t, err, space.ErrBadPipeline)
		_, err = a.Explain(context.Background())
		assert.ErrorIs(t, err, space.ErrBadPipeline)
	})
}

func TestClassifyAggErr(t *testing.T) {
	assert.NoError(t, classifyAggErr(nil))
	assert.ErrorIs(t, classifyAggErr(context.Canceled), context.Canceled)
	assert.NotErrorIs(t, classifyAggErr(context.Canceled), space.ErrBadPipeline)
	assert.ErrorIs(t, classifyAggErr(space.ErrAggGroupLimitExceeded), space.ErrAggGroupLimitExceeded)
	assert.NotErrorIs(t, classifyAggErr(space.ErrAggGroupLimitExceeded), space.ErrBadPipeline)
	assert.ErrorIs(t, classifyAggErr(errors.New("any-store: aggregate: stage 0: unknown stage")), space.ErrBadPipeline)
	// Already-classified errors are not double-wrapped.
	wrapped := classifyAggErr(errors.New("boom"))
	assert.Equal(t, wrapped, classifyAggErr(wrapped))
}
