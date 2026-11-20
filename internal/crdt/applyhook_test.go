package crdt

import (
	"context"
	"errors"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplyHook_RunsWithResolvedIds(t *testing.T) {
	st := newClassController(t)
	arena := &anyenc.Arena{}

	var gotDataset string
	var gotIds []string
	st.SetApplyHook(func(_ context.Context, ch *Change, recordIds []string, _ *ApplyResult) error {
		gotDataset = ch.Dataset
		gotIds = append([]string(nil), recordIds...)
		return nil
	})

	ch := makeUpsert("v1", "r1", Op{Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("Alpha")})
	ch.ChangeId = "ch-1"
	require.NoError(t, st.ApplyChange(ctx, ch))

	assert.Equal(t, testDS, gotDataset)
	assert.Equal(t, []string{"r1"}, gotIds)
}

func TestApplyHook_ErrorRollsBackChange(t *testing.T) {
	st := newClassController(t)
	arena := &anyenc.Arena{}

	boom := errors.New("boom")
	st.SetApplyHook(func(context.Context, *Change, []string, *ApplyResult) error { return boom })

	err := st.ApplyChange(ctx, makeUpsert("v1", "r1",
		Op{Type: OpSet, Path: []string{"name"}, Payload: arena.NewString("Alpha")}))
	require.ErrorIs(t, err, boom)

	// The record write rolled back with the hook error.
	assert.Nil(t, st.Get(ctx, testDS, "r1"))
}
