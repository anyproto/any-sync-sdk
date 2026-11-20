package spaceobjects

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/properties"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/subscribe"
	"github.com/anyproto/any-sync-sdk/space"
)

// tsStamper is a shared-dataset handler whose only behavior is the
// object stamp pair: modifiedAt = the change Timestamp, modifiedBy =
// the change Creator.
type tsStamper struct{ crdt.DefaultHandler }

func (tsStamper) StampObject(ctx *crdt.ChangeCtx, sink *crdt.Sink) {
	a := &anyenc.Arena{}
	sink.DeriveOnce(crdt.Op{Type: crdt.OpSet, Path: []string{"modifiedAt"}, Payload: a.NewNumberFloat64(float64(ctx.Change.Timestamp))})
	sink.DeriveOnce(crdt.Op{Type: crdt.OpSet, Path: []string{"modifiedBy"}, Payload: a.NewString(ctx.Change.Creator)})
}

type stampFixture struct {
	ctx   context.Context
	store *Store
	ctrl  *crdt.Controller
	sub   *subscribe.Sub
}

const stampObj = "o1"

func newStampFixture(t *testing.T) *stampFixture {
	t.Helper()
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "stamp.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	shared, err := db.Collection(ctx, "spaceA_objects")
	require.NoError(t, err)
	ctrl, err := crdt.NewControllerWithShared(ctx, stampObj, db, crdt.SharedCollections{properties.Dataset: shared},
		crdt.HandlerReg{Name: properties.Dataset, Handler: tsStamper{}, Schema: schema.Dataset{Dynamic: true}},
		crdt.HandlerReg{Name: "notes", Handler: crdt.DefaultHandler{}, Schema: schema.Dataset{Dynamic: true}},
	)
	require.NoError(t, err)
	s := &Store{db: db, spaceId: "spaceA", engine: subscribe.New("spaceA")}
	t.Cleanup(func() { _ = s.engine.Close() })

	a := &anyenc.Arena{}
	require.NoError(t, ctrl.ApplyChange(ctx, crdt.Change{
		ObjectId: stampObj, Dataset: properties.Dataset, ChangeId: "c0", VersionId: "v0", Timestamp: 100, DataVersion: "objects-v1",
		Records: []crdt.RecordChange{{Id: stampObj, Upsert: true, Ops: []crdt.Op{{Type: crdt.OpSet, Path: []string{"name"}, Payload: a.NewString("n")}}}},
	}))
	sub, err := s.engine.Subscribe(subscribe.SubConfig{Scope: subscribe.Scope{Shared: true}}, func(yield func(id string, doc *anyenc.Value)) error {
		yield(stampObj, ctrl.Get(ctx, properties.Dataset, stampObj))
		return nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Close() })
	return &stampFixture{ctx: ctx, store: s, ctrl: ctrl, sub: sub}
}

// noteWrite applies a notes change signed by "acct-<ver>" and hands
// its stamps to the dispatcher the way afterApplyFor does, with the
// given replay mode.
func (f *stampFixture) noteWrite(t *testing.T, ver crdt.VersionId, ts int64, replaying bool) {
	t.Helper()
	a := &anyenc.Arena{}
	ch := crdt.Change{
		ObjectId: stampObj, Dataset: "notes", ChangeId: "c-" + string(ver), VersionId: ver, Timestamp: ts, Creator: "acct-" + string(ver), DataVersion: "notes-v1",
		Records: []crdt.RecordChange{{Id: "r1", Upsert: true, Ops: []crdt.Op{{Type: crdt.OpSet, Path: []string{"text"}, Payload: a.NewString(string(ver))}}}},
	}
	res, err := f.ctrl.ApplyChangeWithResult(f.ctx, ch)
	require.NoError(t, err)
	require.Len(t, res.ObjectStamps, 1)
	f.store.dispatchObjectStamps(f.ctx, f.ctrl, replaying, nil, &ch, res.ObjectStamps)
}

func (f *stampFixture) noEvent(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 50*time.Millisecond)
	defer cancel()
	_, err := f.sub.Events().WaitOne(ctx)
	require.True(t, errors.Is(err, context.DeadlineExceeded), "expected no event, got err=%v", err)
}

func (f *stampFixture) oneUpdate(t *testing.T) space.SubRecord {
	t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, time.Second)
	defer cancel()
	ev, err := f.sub.Events().WaitOne(ctx)
	require.NoError(t, err)
	require.Len(t, ev.Updated, 1)
	require.Empty(t, ev.Added)
	require.Empty(t, ev.Removed)
	return ev.Updated[0]
}

// stampOf reads the stamp pair off an update event: the Doc values,
// after checking the event carries exactly one op per stamped field.
func stampOf(t *testing.T, rec space.SubRecord) (float64, string) {
	t.Helper()
	require.NotNil(t, rec.Doc)
	v := rec.Doc.Get("modifiedAt")
	require.NotNil(t, v)
	stamps := map[string]int{}
	for _, op := range rec.Ops {
		if len(op.Path) == 1 {
			stamps[op.Path[0]]++
		}
	}
	require.Equal(t, map[string]int{"modifiedAt": 1, "modifiedBy": 1}, stamps, "exactly one op per stamped field")
	return v.GetFloat64(), rec.Doc.GetString("modifiedBy")
}

// A single write dispatches its stamp at once.
func TestObjectStamps_SingleWriteEmitsImmediately(t *testing.T) {
	f := newStampFixture(t)
	f.noteWrite(t, "v1", 200, false)
	rec := f.oneUpdate(t)
	assert.Equal(t, stampObj, rec.Id)
	at, by := stampOf(t, rec)
	assert.EqualValues(t, 200, at)
	assert.Equal(t, "acct-v1", by)
	f.noEvent(t)
}

// Inside a replay batch the stamps are held back and flushed as one
// event carrying the batch's final value.
func TestObjectStamps_ReplayBatchCoalesces(t *testing.T) {
	f := newStampFixture(t)
	f.noteWrite(t, "v1", 200, true)
	f.noteWrite(t, "v2", 300, true)
	f.noteWrite(t, "v3", 400, true)
	f.noEvent(t)

	f.store.flushObjectStamps(f.ctx, f.ctrl, nil, stampObj)
	rec := f.oneUpdate(t)
	assert.Equal(t, stampObj, rec.Id)
	at, by := stampOf(t, rec)
	assert.EqualValues(t, 400, at)
	assert.Equal(t, "acct-v3", by, "the flush carries the batch's final values for every stamped field")
	f.noEvent(t)

	// Nothing pending: a second flush is a no-op.
	f.store.flushObjectStamps(f.ctx, f.ctrl, nil, stampObj)
	f.noEvent(t)
}

// A subscription that appears after the batch was recorded still gets
// the flush; a batch recorded with no listener is dropped at flush.
func TestObjectStamps_FlushWithoutListenerDrops(t *testing.T) {
	f := newStampFixture(t)
	f.noteWrite(t, "v1", 200, true)
	require.NoError(t, f.sub.Close())
	f.store.flushObjectStamps(f.ctx, f.ctrl, nil, stampObj)
	_, pending := f.store.stampPending.Load(stampObj)
	assert.False(t, pending, "flush clears the entry even without listeners")
}

// An object stamp whose row cannot be read reaches a per-object window
// as an unreadable record carrying the apply's object as its source, so
// the window refills through it; a window with no refill closes.
func TestObjectStamps_UnreadableStampCarriesSource(t *testing.T) {
	f := newStampFixture(t)
	var sources []any
	rows := func(yield func(id string, doc *anyenc.Value)) error {
		yield(stampObj, f.ctrl.Get(f.ctx, properties.Dataset, stampObj))
		return nil
	}
	sub, err := f.store.engine.Subscribe(subscribe.SubConfig{
		Scope: subscribe.Scope{ObjectId: stampObj, Dataset: properties.Dataset},
		Refill: func(source any) (subscribe.RefillRead, error) {
			sources = append(sources, source)
			return subscribe.RefillRead{Rows: rows, Lookup: func(string) (*anyenc.Value, error) { return nil, nil }, Close: func() {}}, nil
		},
	}, rows)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Close() })

	src := &struct{ id string }{stampObj}
	stamp := crdt.ObjectStamp{Dataset: properties.Dataset, RowId: stampObj, Ops: []crdt.Op{{Type: crdt.OpSet, Path: []string{"modifiedAt"}}}}
	f.store.emitObjectStampWith(f.ctx, src, "spaceA", stampObj, "v9", stamp,
		func(int) (*anyenc.Value, error) { return nil, errors.New("read failed") })
	require.Len(t, sources, 1)
	assert.Same(t, src, sources[0])
	assert.NoError(t, sub.Err(), "the refill settled the row")
	assert.ErrorIs(t, f.sub.Err(), space.ErrSubscriptionDrifted, "no refill behind the shared window")
}

// An object marked as rebuilding its rows is not a loaded instance a
// refill may read through.
func TestStore_RebuildingHidesTheObject(t *testing.T) {
	s := &Store{}
	assert.False(t, s.Rebuilding("o1"))
	s.rebuilding.Store("o1", struct{}{})
	assert.True(t, s.Rebuilding("o1"))
	assert.Nil(t, s.PeekLoaded("o1"))
	s.rebuilding.Delete("o1")
	assert.False(t, s.Rebuilding("o1"))
}
