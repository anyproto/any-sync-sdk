package history

import (
	"context"
	"errors"
	"fmt"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// fakeHistoryTree feeds replayIntoScratch a prepared change sequence
// through the ReadableObjectTree iteration contract — the same
// convert(decrypted bytes) → iterate(change with Model) protocol
// any-sync drives. Cut selection (Heads → causal past) is any-sync's
// job, covered by objecttree's own tests; here we test everything the
// history engine does on top.
type fakeHistoryTree struct {
	objecttree.HistoryTree
	id      string
	root    *objecttree.Change
	entries []fakeEntry
	walked  bool
}

type fakeEntry struct {
	ch      *objecttree.Change
	payload []byte
}

func (f *fakeHistoryTree) Id() string               { return f.id }
func (f *fakeHistoryTree) Root() *objecttree.Change { return f.root }

func (f *fakeHistoryTree) GetChange(id string) (*objecttree.Change, error) {
	if id == f.id {
		return f.root, nil
	}
	for _, e := range f.entries {
		if e.ch.Id == id {
			return e.ch, nil
		}
	}
	return nil, fmt.Errorf("fake tree: change %s not found", id)
}

// IterateRoot enforces the real objecttree contract the engines must
// live with: IterateFrom caches each change's decoded Model and drops
// its raw Data, so a second convert-walk cannot re-decode — it would
// replay cached models whose Op.Payload values alias the first walk's
// codec arena (memory that walk's later decodes already overwrote).
// The real tree corrupts silently; the fake fails loudly. Engines are
// single-pass per tree; tests needing another walk clone via freshTree
// (production builds a new history tree per engine call).
func (f *fakeHistoryTree) IterateRoot(convert objecttree.ChangeConvertFunc, iterate objecttree.ChangeIterateFunc) error {
	if convert != nil {
		if f.walked {
			return errors.New("fake tree: second convert-walk over one history tree — stale arena-aliased payloads; use freshTree per engine call")
		}
		f.walked = true
	}
	for _, e := range f.entries {
		if e.ch.Model == nil && convert != nil {
			m, err := convert(e.ch, e.payload)
			if err != nil {
				return err
			}
			e.ch.Model = m
		}
		if !iterate(e.ch) {
			return nil
		}
	}
	return nil
}

// freshTree returns an independently walkable tree over the first n
// entries (n <= 0: all) — fresh Change structs, no cached models.
func (f *fakeHistoryTree) freshTree(n int) *fakeHistoryTree {
	if n <= 0 || n > len(f.entries) {
		n = len(f.entries)
	}
	out := &fakeHistoryTree{id: f.id, root: f.root, entries: make([]fakeEntry, n)}
	for i, e := range f.entries[:n] {
		chCopy := *e.ch
		chCopy.Model = nil
		out.entries[i] = fakeEntry{ch: &chCopy, payload: e.payload}
	}
	return out
}

const testObjectId = "obj-hist"

// treeBuilder accumulates encoded changes with ascending OrderIds.
type treeBuilder struct {
	t     *testing.T
	codec *object.Codec
	tree  *fakeHistoryTree
	seq   int
}

func newTreeBuilder(t *testing.T) *treeBuilder {
	return &treeBuilder{
		t:     t,
		codec: object.NewCodec(),
		tree: &fakeHistoryTree{
			id:   testObjectId,
			root: &objecttree.Change{Id: testObjectId, Timestamp: 1700000000},
		},
	}
}

func (b *treeBuilder) add(dataset string, rec crdt.RecordChange) string {
	b.t.Helper()
	b.seq++
	ch := crdt.Change{
		Dataset:     dataset,
		DataVersion: "v1",
		Records:     []crdt.RecordChange{rec},
	}
	payload, err := b.codec.Encode(&ch)
	require.NoError(b.t, err)
	changeId := fmt.Sprintf("cid-%03d", b.seq)
	// Linear chain: each change's sole parent is its predecessor (the
	// root for the first) — what the ancestor walk and coalescing see.
	prev := b.tree.id
	if b.seq > 1 {
		prev = fmt.Sprintf("cid-%03d", b.seq-1)
	}
	b.tree.entries = append(b.tree.entries, fakeEntry{
		ch: &objecttree.Change{
			Id:          changeId,
			OrderId:     fmt.Sprintf("o%03d", b.seq),
			AddSeq:      uint64(b.seq),
			Timestamp:   1700000000 + int64(b.seq),
			PreviousIds: []string{prev},
		},
		payload: payload,
	})
	return changeId
}

func setOp(t *testing.T, fields string) crdt.Op {
	t.Helper()
	v, err := anyenc.ParseJson(fields)
	require.NoError(t, err)
	return crdt.Op{Type: crdt.OpSet, Payload: v}
}

func upsert(t *testing.T, id, fields string) crdt.RecordChange {
	return crdt.RecordChange{Id: id, Upsert: true, Ops: []crdt.Op{setOp(t, fields)}}
}

func testRegs() []crdt.HandlerReg {
	return []crdt.HandlerReg{
		{Name: "notes", Handler: crdt.DefaultHandler{}, Schema: schema.Dataset{Dynamic: true}},
		{Name: "tasks", Handler: crdt.DefaultHandler{}, Schema: schema.Dataset{Dynamic: true}},
	}
}

func buildScratch(t *testing.T, tree *fakeHistoryTree, p ViewParams) (*View, error) {
	t.Helper()
	ctx := context.Background()
	db, err := anystore.Open(ctx, ":memory:", &anystore.Config{InMemory: true})
	require.NoError(t, err)
	v, err := replayIntoScratch(ctx, db, tree, p)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	t.Cleanup(func() { _ = v.Close() })
	return v, nil
}

func TestReplayBuildsProjection(t *testing.T) {
	ctx := context.Background()
	b := newTreeBuilder(t)
	b.add("notes", upsert(t, "n1", `{"title":"first","count":1}`))
	b.add("notes", crdt.RecordChange{Id: "n1", Ops: []crdt.Op{{
		Type: crdt.OpSet, Path: []string{"title"}, Payload: mustVal(t, `"renamed"`),
	}}})
	b.add("notes", upsert(t, "n2", `{"title":"second"}`))

	v, err := buildScratch(t, b.tree, ViewParams{
		ObjectId: testObjectId, Heads: []string{"cid-003"}, Regs: testRegs(),
	})
	require.NoError(t, err)

	n1 := v.Record(ctx, "notes", "n1")
	require.NotNil(t, n1)
	assert.Equal(t, "renamed", string(n1.GetStringBytes("title")))
	assert.Equal(t, 1, n1.GetInt("count"))
	// _ver stamped from live OrderIds; _applySeq never stamped (no allocator)
	assert.NotNil(t, n1.Get(crdt.VersionsKey))
	assert.Nil(t, n1.Get(crdt.ApplySeqField))

	assert.Len(t, v.Records(ctx, "notes"), 2)
	assert.Equal(t, []string{"notes", "tasks"}, v.Datasets())
	assert.Equal(t, "cid-003", v.Version)
}

func TestReplayDatasetFilter(t *testing.T) {
	ctx := context.Background()
	b := newTreeBuilder(t)
	b.add("notes", upsert(t, "n1", `{"title":"a"}`))
	b.add("tasks", upsert(t, "t1", `{"done":false}`))

	v, err := buildScratch(t, b.tree, ViewParams{
		ObjectId: testObjectId, Heads: []string{"cid-002"}, Dataset: "tasks", Regs: testRegs(),
	})
	require.NoError(t, err)

	assert.Nil(t, v.Record(ctx, "notes", "n1"))
	require.NotNil(t, v.Record(ctx, "tasks", "t1"))
	assert.Equal(t, []string{"tasks"}, v.Datasets())
}

func TestReplaySkipsUnregisteredDataset(t *testing.T) {
	ctx := context.Background()
	b := newTreeBuilder(t)
	b.add("notes", upsert(t, "n1", `{"title":"a"}`))
	b.add("ghost", upsert(t, "g1", `{"x":1}`)) // no handler registered

	v, err := buildScratch(t, b.tree, ViewParams{
		ObjectId: testObjectId, Heads: []string{"cid-002"}, Regs: testRegs(),
	})
	require.NoError(t, err)
	require.NotNil(t, v.Record(ctx, "notes", "n1"))
}

func TestReplayGuardrail(t *testing.T) {
	b := newTreeBuilder(t)
	b.add("notes", upsert(t, "n1", `{"title":"a"}`))
	b.add("notes", upsert(t, "n2", `{"title":"b"}`))

	_, err := buildScratch(t, b.tree, ViewParams{
		ObjectId: testObjectId, Heads: []string{"cid-002"}, Regs: testRegs(), MaxRecords: 1,
	})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrViewTooLarge))
}

func TestReplayTombstones(t *testing.T) {
	ctx := context.Background()
	b := newTreeBuilder(t)
	b.add("notes", upsert(t, "n1", `{"title":"a"}`))
	b.add("notes", upsert(t, "n2", `{"title":"b"}`))
	b.add("notes", crdt.RecordChange{Id: "n2", Ops: []crdt.Op{{Type: crdt.OpDelete}}})

	v, err := buildScratch(t, b.tree, ViewParams{
		ObjectId: testObjectId, Heads: []string{"cid-003"}, Regs: testRegs(),
	})
	require.NoError(t, err)

	assert.Len(t, v.Records(ctx, "notes"), 1) // live only
	all, err := v.AllRecords(ctx, "notes")
	require.NoError(t, err)
	assert.Len(t, all, 2) // tombstone included for the diff engine
}

// TestDiffViewsAcrossCuts is the Engine A acceptance test: two cuts of
// the same history, diffed. The "base" view replays a prefix; the
// "version" view replays the full sequence.
func TestDiffViewsAcrossCuts(t *testing.T) {
	ctx := context.Background()
	b := newTreeBuilder(t)
	b.add("notes", upsert(t, "n1", `{"title":"first","count":1}`))
	cutBase := b.add("notes", upsert(t, "n2", `{"title":"second"}`))
	prefixLen := len(b.tree.entries)

	b.add("notes", crdt.RecordChange{Id: "n1", Ops: []crdt.Op{{
		Type: crdt.OpSet, Path: []string{"title"}, Payload: mustVal(t, `"renamed"`),
	}}})
	b.add("notes", crdt.RecordChange{Id: "n2", Ops: []crdt.Op{{Type: crdt.OpDelete}}})
	cutVersion := b.add("notes", upsert(t, "n3", `{"title":"third"}`))
	b.add("tasks", upsert(t, "t1", `{"done":true}`))

	baseTree := &fakeHistoryTree{id: b.tree.id, root: b.tree.root, entries: b.tree.entries[:prefixLen]}
	base, err := buildScratch(t, baseTree, ViewParams{
		ObjectId: testObjectId, Heads: []string{cutBase}, Regs: testRegs(),
	})
	require.NoError(t, err)
	version, err := buildScratch(t, b.tree, ViewParams{
		ObjectId: testObjectId, Heads: []string{cutVersion}, Regs: testRegs(),
	})
	require.NoError(t, err)

	res, err := DiffViews(ctx, base, version, DiffFilter{})
	require.NoError(t, err)
	assert.Equal(t, cutBase, res.Base)
	assert.Equal(t, cutVersion, res.Version)
	require.Len(t, res.Datasets, 2)

	notes := res.Datasets[0]
	require.Equal(t, "notes", notes.Dataset)
	kinds := map[string]DiffKind{}
	for _, r := range notes.Records {
		kinds[r.Id] = r.Kind
	}
	assert.Equal(t, KindChanged, kinds["n1"])
	assert.Equal(t, KindDeleted, kinds["n2"])
	assert.Equal(t, KindAdded, kinds["n3"])

	tasks := res.Datasets[1]
	require.Equal(t, "tasks", tasks.Dataset)
	require.Len(t, tasks.Records, 1)
	assert.Equal(t, KindAdded, tasks.Records[0].Kind)

	// dataset-scoped
	res, err = DiffViews(ctx, base, version, DiffFilter{Dataset: "notes"})
	require.NoError(t, err)
	require.Len(t, res.Datasets, 1)
	assert.Equal(t, "notes", res.Datasets[0].Dataset)

	// record-scoped
	res, err = DiffViews(ctx, base, version, DiffFilter{Dataset: "notes", RecordIds: []string{"n1"}})
	require.NoError(t, err)
	require.Len(t, res.Datasets, 1)
	require.Len(t, res.Datasets[0].Records, 1)
	assert.Equal(t, "n1", res.Datasets[0].Records[0].Id)
	assert.Equal(t, KindChanged, res.Datasets[0].Records[0].Kind)

	// record ids without dataset is a caller error
	_, err = DiffViews(ctx, base, version, DiffFilter{RecordIds: []string{"n1"}})
	require.Error(t, err)
}

func mustVal(t *testing.T, json string) *anyenc.Value {
	t.Helper()
	v, err := anyenc.ParseJson(json)
	require.NoError(t, err)
	return v
}
