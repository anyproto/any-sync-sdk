package crdt

import (
	"context"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ListRecords reports a read the context ended; Records reads it as
// nil, and a dataset without a collection is empty for both.
func TestListRecords_EndedContextIsAnError(t *testing.T) {
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "test.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	ctrl, err := NewController(ctx, "obj-lr", db,
		HandlerReg{Name: "notes", Handler: DefaultHandler{}, Schema: dynSchema},
		HandlerReg{Name: "empty", Handler: DefaultHandler{}, Schema: dynSchema})
	require.NoError(t, err)
	a := &anyenc.Arena{}
	_, err = ctrl.ApplyChangeWithResult(ctx, Change{
		Dataset:     "notes",
		DataVersion: "v1",
		VersionId:   "a000000001",
		AddSeq:      1,
		Records:     []RecordChange{{Id: "r1", Upsert: true, Ops: []Op{setOp(a, "v", "x")}}},
	})
	require.NoError(t, err)

	rows, err := ctrl.ListRecords(ctx, "notes")
	require.NoError(t, err)
	assert.Len(t, rows, 1)
	rows, err = ctrl.ListRecords(ctx, "empty")
	require.NoError(t, err)
	assert.Nil(t, rows)

	ended, cancel := context.WithCancel(ctx)
	cancel()
	_, err = ctrl.ListRecords(ended, "notes")
	require.Error(t, err)
	assert.Nil(t, ctrl.Records(ended, "notes"))
}
