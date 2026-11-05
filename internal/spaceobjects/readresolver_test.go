package spaceobjects

import (
	"context"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/internal/anysyncx"
	"github.com/anyproto/any-sync-sdk/internal/readstate"
)

// A store with a tracked dataset wires its engine to the resolver and
// the loader. The resolver refuses a cold space instead of loading it
// from inside the write tx, and the engine then loads through the
// loader: with no space cache behind the app, the merge ends with the
// loader's error, not the refusal, and commits nothing.
func TestReadResolver_ColdSpaceRefusesThenLoads(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, t.TempDir()+"/test.db", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	tracked := []handler.Type{{Id: "t", Name: "T", Datasets: []handler.Dataset{{
		Name: "msgs", Handler: handler.DefaultHandler{}, Schema: handler.Schema{Dynamic: true},
		ReadTracking: &handler.ReadTracking{
			Classify: func(*handler.ChangeCtx, *handler.RecordChange) handler.ReadClassification {
				return handler.ReadClassification{Track: true}
			},
		},
	}}}}
	s := NewStore(&anysyncx.App{}, db, nil, "spaceA", nil, tracked, nil, nil)
	eng := s.ReadState()
	require.NotNil(t, eng)

	_, _, _, err = s.readResolver()(ctx, "obj", "c1")
	require.ErrorIs(t, err, readstate.ErrSpaceNotResident)

	err = eng.WriteTx(ctx, func(txCtx context.Context) error {
		_, err := eng.MergeHeads(txCtx, "obj", []string{"c1"})
		return err
	})
	require.Error(t, err)
	require.NotErrorIs(t, err, readstate.ErrSpaceNotResident, "the loader ran and its error surfaced")
	heads, _, err := eng.Frontier(ctx, "obj")
	require.NoError(t, err)
	require.Empty(t, heads, "nothing committed")
}
