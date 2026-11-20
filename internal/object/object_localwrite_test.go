package object

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// fakeLocalWriteTree is a minimal stand-in for objecttree.ObjectTree —
// just enough to walk LocalWrite end-to-end without spinning a full
// any-sync stack. Records the timestamp AddContent was called with so
// the test can assert the apply-time and on-the-wire values agree.
// Every AddContent is kept in stored with its cleartext payload, so
// IterateAfterAddSeq replays it the way the real tree does.
type fakeLocalWriteTree struct {
	objecttree.ObjectTree
	mu       sync.Mutex
	id       string
	root     *objecttree.Change
	changes  map[string]*objecttree.Change
	captured int64
	nextId   string
	nextOrd  string
	nextSeq  uint64
	calls    int
	stored   []*objecttree.Change
	// addErr fails AddContent before anything is stored; storeThenErr
	// stores the change and fails after it, like a failed head update.
	addErr       error
	storeThenErr error
	// cursorOpen is true while IterateAfterAddSeq runs; cursorErr ends
	// it after the stored changes.
	cursorOpen bool
	cursorErr  error
}

// Heads is the last stored change, the root when none is.
func (f *fakeLocalWriteTree) Heads() []string {
	if len(f.stored) == 0 {
		return []string{f.root.Id}
	}
	return []string{f.stored[len(f.stored)-1].Id}
}

func (f *fakeLocalWriteTree) Lock()         { f.mu.Lock() }
func (f *fakeLocalWriteTree) Unlock()       { f.mu.Unlock() }
func (f *fakeLocalWriteTree) TryLock() bool { return f.mu.TryLock() }
func (f *fakeLocalWriteTree) Id() string    { return f.id }
func (f *fakeLocalWriteTree) Root() *objecttree.Change {
	return f.root
}
func (f *fakeLocalWriteTree) GetChange(id string) (*objecttree.Change, error) {
	if c, ok := f.changes[id]; ok {
		return c, nil
	}
	for _, c := range f.stored {
		if c.Id == id {
			return c, nil
		}
	}
	return nil, errors.New("not found")
}

// AddContent hands out nextId/nextOrd/nextSeq on the first call and
// "ch-N" / "a00000000N" / nextSeq+N-1 after it.
func (f *fakeLocalWriteTree) AddContent(_ context.Context, content objecttree.SignableChangeContent) (objecttree.AddResult, error) {
	f.captured = content.Timestamp
	if f.addErr != nil {
		return objecttree.AddResult{}, f.addErr
	}
	f.calls++
	id, ord, seq := f.nextId, f.nextOrd, f.nextSeq
	if f.calls > 1 {
		id = fmt.Sprintf("ch-%d", f.calls)
		ord = fmt.Sprintf("a%09d", f.calls)
		seq = f.nextSeq + uint64(f.calls-1)
	}
	f.stored = append(f.stored, &objecttree.Change{
		Id:        id,
		OrderId:   ord,
		AddSeq:    seq,
		Data:      append([]byte(nil), content.Data...),
		Timestamp: content.Timestamp,
		Identity:  f.root.Identity,
	})
	if f.storeThenErr != nil {
		return objecttree.AddResult{}, f.storeThenErr
	}
	return objecttree.AddResult{
		Added: []objecttree.StorageChange{{
			Id:      id,
			OrderId: ord,
			AddSeq:  seq,
		}},
	}, nil
}

// IterateAfterAddSeq replays stored changes above addSeq: convert runs
// on the cleartext payload in a buffer reused across changes, its
// result lands on Model, iterate runs until it returns false.
func (f *fakeLocalWriteTree) IterateAfterAddSeq(_ context.Context, addSeq uint64, convert objecttree.ChangeConvertFunc, iterate objecttree.ChangeIterateFunc) error {
	f.cursorOpen = true
	defer func() { f.cursorOpen = false }()
	var buf []byte
	for _, c := range f.stored {
		if c.AddSeq <= addSeq {
			continue
		}
		buf = append(buf[:0], c.Data...)
		model, err := convert(c, buf)
		if err != nil {
			return err
		}
		c.Model = model
		if !iterate(c) {
			return nil
		}
	}
	return f.cursorErr
}

// timestampHandler records the ctx.Change.Timestamp it observes on
// every BeforeCreate. We assert this matches the on-the-wire value
// AddContent was called with.
type timestampHandler struct {
	crdt.DefaultHandler
	observed []int64
}

func (h *timestampHandler) BeforeCreate(ctx *crdt.ChangeCtx, _ *crdt.RecordChange, _ *crdt.Sink) error {
	h.observed = append(h.observed, ctx.Change.Timestamp)
	return nil
}

func newLocalWriteFixture(t *testing.T, dataset string, handler crdt.Handler) (*Object, *fakeLocalWriteTree) {
	t.Helper()
	ctx := context.Background()

	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "lw.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctrl, err := crdt.NewController(ctx, "obj-lw", db, crdt.HandlerReg{Name: dataset, Handler: handler, Schema: schema.Dataset{Dynamic: true}})
	require.NoError(t, err)

	priv, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	_, rootPub, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)

	tree := &fakeLocalWriteTree{
		id: "obj-lw",
		root: &objecttree.Change{
			Id:        "root-id",
			Identity:  rootPub,
			Timestamp: 1700000000,
		},
		changes: map[string]*objecttree.Change{
			"ch-1": {Id: "ch-1", Identity: rootPub},
		},
		nextId:  "ch-1",
		nextOrd: "a000000001",
		nextSeq: 1,
	}

	o := &Object{
		signKey: priv,
		codec:   NewCodec(),
		ctrl:    ctrl,
		spaceId: "space-lw",
		tree:    tree,
	}
	return o, tree
}

// recordOp helps build a single $set record op for the test.
func recordOp(t *testing.T, key string, val any) crdt.Op {
	t.Helper()
	a := &anyenc.Arena{}
	var payload *anyenc.Value
	switch v := val.(type) {
	case string:
		payload = a.NewString(v)
	case int:
		payload = a.NewNumberInt(v)
	default:
		t.Fatalf("recordOp: unsupported type %T", val)
	}
	return crdt.Op{Type: crdt.OpSet, Path: []string{key}, Payload: payload}
}

// TestLocalWrite_BackfillsTimestamp — ch.Timestamp is zero on input;
// LocalWrite must back-fill it via ts() before AddContent so handler
// callbacks see the same value any-sync stamps on the wire. The
// captured AddContent timestamp and the handler-observed timestamp
// must agree.
func TestLocalWrite_BackfillsTimestamp(t *testing.T) {
	h := &timestampHandler{}
	o, tree := newLocalWriteFixture(t, "blocks", h)

	before := time.Now().Unix()
	_, err := o.LocalWrite(context.Background(), crdt.Change{
		Dataset:     "blocks",
		DataVersion: "test-v1",
		Records: []crdt.RecordChange{{
			Id:     "rec-1",
			Upsert: true,
			Ops:    []crdt.Op{recordOp(t, "name", "hello")},
		}},
	})
	require.NoError(t, err)
	after := time.Now().Unix()

	require.Len(t, h.observed, 1, "BeforeCreate should fire exactly once")
	observed := h.observed[0]

	assert.Greater(t, observed, int64(0), "handler must see a non-zero Timestamp on local writes")
	assert.GreaterOrEqual(t, observed, before, "Timestamp not below the pre-call wall-clock floor")
	assert.LessOrEqual(t, observed, after, "Timestamp not above the post-call wall-clock ceiling")
	assert.Equal(t, tree.captured, observed,
		"apply-time Timestamp must equal the value AddContent was called with (LocalWrite/replay symmetry)")
}

// TestLocalWrite_PreservesCallerTimestamp — caller-supplied positive
// Timestamp must NOT be overwritten by the time.Now() fallback. The
// existing ts() helper handles the branch; this is a regression guard
// against accidentally always-overwriting.
func TestLocalWrite_PreservesCallerTimestamp(t *testing.T) {
	h := &timestampHandler{}
	o, tree := newLocalWriteFixture(t, "blocks", h)

	const wanted int64 = 1234567890
	_, err := o.LocalWrite(context.Background(), crdt.Change{
		Dataset:     "blocks",
		DataVersion: "test-v1",
		Timestamp:   wanted,
		Records: []crdt.RecordChange{{
			Id:     "rec-1",
			Upsert: true,
			Ops:    []crdt.Op{recordOp(t, "name", "hi")},
		}},
	})
	require.NoError(t, err)

	require.Len(t, h.observed, 1)
	assert.Equal(t, wanted, h.observed[0], "caller-supplied Timestamp must reach the handler unchanged")
	assert.Equal(t, wanted, tree.captured, "caller-supplied Timestamp must reach AddContent unchanged")
}
