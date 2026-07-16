package history

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

// diffRangeFixture builds: create n1, create n2, edit n1, delete n2,
// create n3 (+ a tasks-dataset write) — returning the cut handles.
type diffRangeFixture struct {
	tree                *fakeHistoryTree
	prefixLen           int
	cutBase, cutVersion string
	editN1, deleteN2    string
}

func newDiffRangeFixture(t *testing.T) diffRangeFixture {
	b := newTreeBuilder(t)
	fx := diffRangeFixture{}
	b.add("notes", upsert(t, "n1", `{"title":"first","count":1}`))
	fx.cutBase = b.add("notes", upsert(t, "n2", `{"title":"second"}`))
	fx.prefixLen = len(b.tree.entries)
	fx.editN1 = b.add("notes", crdt.RecordChange{Id: "n1", Ops: []crdt.Op{{
		Type: crdt.OpSet, Path: []string{"title"}, Payload: mustVal(t, `"renamed"`),
	}}})
	fx.deleteN2 = b.add("notes", crdt.RecordChange{Id: "n2", Ops: []crdt.Op{{Type: crdt.OpDelete}}})
	b.add("tasks", upsert(t, "t1", `{"done":true}`))
	fx.cutVersion = b.add("notes", upsert(t, "n3", `{"title":"third"}`))
	fx.tree = b.tree
	return fx
}

func rangeParams(tree *fakeHistoryTree) ViewParams {
	return ViewParams{ObjectId: testObjectId, Tree: tree, Regs: testRegs()}
}

func kindsOf(dd DatasetDiff) map[string]DiffKind {
	out := map[string]DiffKind{}
	for _, r := range dd.Records {
		out[r.Id] = r.Kind
	}
	return out
}

// DiffRange over an ancestor base must produce exactly what the
// two-view DiffViews produces for the same cuts.
func TestDiffRangeMatchesDiffViews(t *testing.T) {
	ctx := context.Background()
	fx := newDiffRangeFixture(t)
	versionTree := fx.tree // fake serves the full sequence = cut at cutVersion

	got, err := DiffRange(ctx, rangeParams(versionTree), []string{fx.cutBase}, fx.cutVersion, DiffFilter{})
	require.NoError(t, err)
	assert.Equal(t, fx.cutBase, got.Base)
	assert.Equal(t, fx.cutVersion, got.Version)

	// Reference: two independent views, each over a fresh tree (a
	// walked tree cannot be walked again — see fakeHistoryTree).
	baseView, err := buildScratch(t, fx.tree.freshTree(fx.prefixLen), ViewParams{
		ObjectId: testObjectId, Heads: []string{fx.cutBase}, Regs: testRegs(),
	})
	require.NoError(t, err)
	versionView, err := buildScratch(t, fx.tree.freshTree(0), ViewParams{
		ObjectId: testObjectId, Heads: []string{fx.cutVersion}, Regs: testRegs(),
	})
	require.NoError(t, err)
	want, err := DiffViews(ctx, baseView, versionView, DiffFilter{})
	require.NoError(t, err)

	require.Len(t, got.Datasets, len(want.Datasets))
	for i := range want.Datasets {
		assert.Equal(t, want.Datasets[i].Dataset, got.Datasets[i].Dataset)
		wantKinds := kindsOf(want.Datasets[i])
		gotKinds := kindsOf(got.Datasets[i])
		assert.Equal(t, wantKinds, gotKinds, "dataset %s", want.Datasets[i].Dataset)
	}

	// Spot-check content: n1 changed with the title transition.
	notes := got.Datasets[0]
	require.Equal(t, "notes", notes.Dataset)
	kinds := kindsOf(notes)
	assert.Equal(t, KindChanged, kinds["n1"])
	assert.Equal(t, KindDeleted, kinds["n2"])
	assert.Equal(t, KindAdded, kinds["n3"])
}

// Effect diff: empty-base resolution is the caller's job; here base =
// the edit's parents, version = the edit — only n1 may appear.
func TestDiffRangeEffectDiff(t *testing.T) {
	ctx := context.Background()
	fx := newDiffRangeFixture(t)

	// Parents of editN1 = [cutBase] (linear chain). The tree passed to
	// DiffRange is always built AT the version cut (the fake serves
	// whatever it holds), so truncate to editN1's prefix.
	editTree := fx.tree.freshTree(fx.prefixLen + 1)
	res, err := DiffRange(ctx, rangeParams(editTree), []string{fx.cutBase}, fx.editN1, DiffFilter{})
	require.NoError(t, err)
	require.Len(t, res.Datasets, 1)
	require.Len(t, res.Datasets[0].Records, 1)
	rd := res.Datasets[0].Records[0]
	assert.Equal(t, "n1", rd.Id)
	assert.Equal(t, KindChanged, rd.Kind)
	require.Len(t, rd.Fields, 1)
	assert.Equal(t, "title", rd.Fields[0].Path[0])
	assert.Equal(t, `"first"`, rd.Fields[0].Before.String())
	assert.Equal(t, `"renamed"`, rd.Fields[0].After.String())
}

func TestDiffRangeEmptyBase(t *testing.T) {
	ctx := context.Background()
	fx := newDiffRangeFixture(t)

	res, err := DiffRange(ctx, rangeParams(fx.tree), nil, fx.cutVersion, DiffFilter{})
	require.NoError(t, err)
	// Everything relative to the empty projection: n1/n3 added,
	// n2 dead at both ends (created AND deleted inside the range) is
	// omitted, tasks/t1 added.
	require.Len(t, res.Datasets, 2)
	notes := kindsOf(res.Datasets[0])
	assert.Equal(t, KindAdded, notes["n1"])
	assert.Equal(t, KindDeleted, notes["n2"])
	assert.Equal(t, KindAdded, notes["n3"])
	assert.Equal(t, KindAdded, kindsOf(res.Datasets[1])["t1"])
}

func TestDiffRangeFilters(t *testing.T) {
	ctx := context.Background()
	fx := newDiffRangeFixture(t)

	res, err := DiffRange(ctx, rangeParams(fx.tree), []string{fx.cutBase}, fx.cutVersion,
		DiffFilter{Dataset: "notes", RecordIds: []string{"n1"}})
	require.NoError(t, err)
	require.Len(t, res.Datasets, 1)
	require.Len(t, res.Datasets[0].Records, 1)
	assert.Equal(t, "n1", res.Datasets[0].Records[0].Id)

	_, err = DiffRange(ctx, rangeParams(fx.tree.freshTree(0)), []string{fx.cutBase}, fx.cutVersion,
		DiffFilter{RecordIds: []string{"n1"}})
	require.Error(t, err, "record ids without dataset")
}

func TestDiffRangeNotAncestor(t *testing.T) {
	ctx := context.Background()
	fx := newDiffRangeFixture(t)

	_, err := DiffRange(ctx, rangeParams(fx.tree), []string{"cid-unknown"}, fx.cutVersion, DiffFilter{})
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotAncestor))
}
