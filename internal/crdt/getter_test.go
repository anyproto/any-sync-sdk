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

// refHandler derives "refName" on create from the record named by the
// create payload's "ref" field, read cross-dataset via ctx.Get — the
// reply-fold-in shape (chat reads the replied-to message's creator).
// It also records what Get returned so tests can assert on the
// tombstone / absent cases.
type refHandler struct {
	DefaultHandler
	refDataset string
	saw        []*anyenc.Value
}

func (h *refHandler) BeforeCreate(ctx *ChangeCtx, rec *RecordChange, sink *Sink) error {
	if len(rec.Ops) == 0 || rec.Ops[0].Payload == nil {
		return nil
	}
	ref := string(rec.Ops[0].Payload.GetStringBytes("ref"))
	if ref == "" {
		return nil
	}
	got := ctx.Get(h.refDataset, ref)
	h.saw = append(h.saw, got)
	if got == nil || got.Get("_deletedAt") != nil {
		return nil
	}
	if name := string(got.GetStringBytes("name")); name != "" {
		a := &anyenc.Arena{}
		sink.Derive(Op{Type: OpSet, Path: []string{"refName"}, Payload: a.NewString(name)})
	}
	return nil
}

const refDS = "refs"

func newGetterController(t *testing.T) (*Controller, *refHandler) {
	t.Helper()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "test.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	h := &refHandler{refDataset: testDS}
	st, err := NewController(ctx, "obj1", db,
		HandlerReg{Name: testDS, Handler: DefaultHandler{}, Schema: dynSchema},
		HandlerReg{Name: refDS, Handler: h, Schema: dynSchema},
	)
	require.NoError(t, err)
	return st, h
}

func makeRefUpsert(version VersionId, recordId string, fields map[string]any) Change {
	arena := &anyenc.Arena{}
	ch := makeUpsert(version, recordId, Op{
		Type:    OpSet,
		Payload: recordPayload(arena, fields),
	})
	ch.Dataset = refDS
	return ch
}

// A BeforeCreate hook reads a record of ANOTHER dataset via ctx.Get,
// inside the apply WriteTx — visibility plus no deadlock (pins
// any-store's tx-reuse read path from within a Modify callback).
func TestChangeCtxGet_CrossDatasetRead(t *testing.T) {
	st, h := newGetterController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "target1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "hello"}),
	})))
	require.NoError(t, st.ApplyChange(ctx, makeRefUpsert("v2", "ref1", map[string]any{"ref": "target1"})))

	rec := st.Get(ctx, refDS, "ref1")
	require.NotNil(t, rec)
	assert.Equal(t, "hello", rec.GetString("refName"), "derived from the referenced record")
	require.Len(t, h.saw, 1)
	require.NotNil(t, h.saw[0])
}

// Same-dataset read: a hook reading its own dataset's earlier record —
// the chat reply case reads chat_messages while applying a
// chat_messages create.
func TestChangeCtxGet_SameDatasetRead(t *testing.T) {
	st, _ := newGetterController(t)

	require.NoError(t, st.ApplyChange(ctx, makeRefUpsert("v1", "first1", map[string]any{"name": "alpha"})))
	// Point the second record's handler at its OWN dataset.
	ch := makeRefUpsert("v2", "second1", map[string]any{"ref": "first1"})
	st.handlers[refDS].(*refHandler).refDataset = refDS
	require.NoError(t, st.ApplyChange(ctx, ch))

	rec := st.Get(ctx, refDS, "second1")
	require.NotNil(t, rec)
	assert.Equal(t, "alpha", rec.GetString("refName"))
}

// A deleted target comes back as its tombstone (content wiped,
// _deletedAt set) — the handler must check, and derives nothing.
func TestChangeCtxGet_TombstoneReturnedAsIs(t *testing.T) {
	st, h := newGetterController(t)
	arena := &anyenc.Arena{}

	require.NoError(t, st.ApplyChange(ctx, makeUpsert("v1", "target1", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"name": "hello"}),
	})))
	require.NoError(t, st.ApplyChange(ctx, makeChange("v2", "target1", Op{Type: OpDelete})))
	require.NoError(t, st.ApplyChange(ctx, makeRefUpsert("v3", "ref1", map[string]any{"ref": "target1"})))

	rec := st.Get(ctx, refDS, "ref1")
	require.NotNil(t, rec)
	assert.Equal(t, "", rec.GetString("refName"), "tombstoned target derives nothing")
	require.Len(t, h.saw, 1)
	require.NotNil(t, h.saw[0], "tombstone is returned, not nil")
	assert.NotNil(t, h.saw[0].Get("_deletedAt"))
}

// Absent target and empty id both resolve to nil, not an error.
func TestChangeCtxGet_AbsentAndEmptyId(t *testing.T) {
	st, h := newGetterController(t)

	require.NoError(t, st.ApplyChange(ctx, makeRefUpsert("v1", "ref1", map[string]any{"ref": "nosuchrecord"})))
	require.Len(t, h.saw, 1)
	assert.Nil(t, h.saw[0])

	rec := st.Get(ctx, refDS, "ref1")
	require.NotNil(t, rec, "record still lands; the getter miss is not a rejection")
	assert.Equal(t, "", rec.GetString("refName"))
}

// stampingHandler derives "creator" from the change envelope on create
// — the chat stampCreate shape.
type stampingHandler struct{ DefaultHandler }

func (stampingHandler) BeforeCreate(ctx *ChangeCtx, _ *RecordChange, sink *Sink) error {
	a := &anyenc.Arena{}
	sink.Derive(Op{Type: OpSet, Path: []string{"creator"}, Payload: a.NewString(ctx.Change.Creator)})
	return nil
}

// The apply hook (the read-tracking classify seam) runs after the
// record loop in the same tx: Controller.Get from inside it sees the
// record INCLUDING handler-derived fields, keyed by the resolved
// record id even when the create auto-derived it. This is what lets a
// classifier tag verdicts off a derived field (chat mentions).
func TestApplyHook_GetSeesHandlerDerivedFields(t *testing.T) {
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "test.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	st, err := NewController(ctx, "obj1", db,
		HandlerReg{Name: testDS, Handler: stampingHandler{}, Schema: dynSchema})
	require.NoError(t, err)

	var hookSawCreator string
	var hookRecordId string
	st.SetApplyHook(func(txCtx context.Context, ch *Change, recordIds []string, _ *ApplyResult) error {
		require.Len(t, recordIds, 1)
		hookRecordId = recordIds[0]
		if rec := st.Get(txCtx, ch.Dataset, recordIds[0]); rec != nil {
			hookSawCreator = rec.GetString("creator")
		}
		return nil
	})

	arena := &anyenc.Arena{}
	ch := makeUpsert("v1", "", Op{
		Type:    OpSet,
		Payload: recordPayload(arena, map[string]any{"text": "hello"}),
	})
	ch.ChangeId = "ch-1"
	ch.Creator = "identityA"
	require.NoError(t, st.ApplyChange(ctx, ch))

	assert.Equal(t, DeriveRecordId("ch-1"), hookRecordId, "auto-derived id resolved for the hook")
	assert.Equal(t, "identityA", hookSawCreator, "in-tx Get sees the handler-derived field")
}
