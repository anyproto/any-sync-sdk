package crdt

import (
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDroppedOnly(t *testing.T) {
	rollback := errors.New("rollback failed")
	assert.True(t, droppedOnly(errRecordDropped))
	assert.True(t, droppedOnly(errors.Join(errRecordDropped)))
	assert.True(t, droppedOnly(fmt.Errorf("upsert: %w", errors.Join(errRecordDropped))))
	assert.False(t, droppedOnly(errors.Join(errRecordDropped, rollback)))
	assert.False(t, droppedOnly(rollback))
}

// readRejectHandler reads a record of another dataset, then rejects
// the create.
type readRejectHandler struct {
	DefaultHandler
	readDataset string
}

func (h *readRejectHandler) BeforeCreate(ctx *ChangeCtx, rec *RecordChange, sink *Sink) error {
	ctx.Get(h.readDataset, "target1")
	return errors.New("rejected")
}

// A hook's read can be the first open of a dataset, which reconciles
// its indexes inside the record's call. A rejection rolls the call
// back; the cached handle keeps its indexes all the same.
func TestDrop_KeepsIndexesAHookReconciled(t *testing.T) {
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "test.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	before, err := NewController(ctx, "obj1", db,
		HandlerReg{Name: testDS, Handler: DefaultHandler{}, Schema: dynSchema},
	)
	require.NoError(t, err)
	arena := &anyenc.Arena{}
	require.NoError(t, before.ApplyChange(ctx, makeUpsert("v1", "target1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "hello"}),
	})))

	// The registration now asks for an index the collection lacks.
	st, err := NewController(ctx, "obj1", db,
		HandlerReg{Name: testDS, Handler: DefaultHandler{}, Schema: dynSchema,
			Indexes: []anystore.IndexInfo{{Name: "idx_name", Fields: []string{"name"}}}},
		HandlerReg{Name: refDS, Handler: &readRejectHandler{readDataset: testDS}, Schema: dynSchema},
	)
	require.NoError(t, err)
	require.NoError(t, st.ApplyChange(ctx, makeRefUpsert("v2", "r1", map[string]any{"ref": "target1"})))
	assert.Nil(t, st.Get(ctx, refDS, "r1"), "the rejected create wrote nothing")

	coll := st.collectionForRead(ctx, testDS)
	require.NotNil(t, coll)
	var names []string
	for _, idx := range coll.GetIndexes() {
		names = append(names, idx.Info().Name)
	}
	assert.True(t, slices.Contains(names, "idx_name"), "indexes: %v", names)
}
