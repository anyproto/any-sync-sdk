package object

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// sequencedTree is fakeLocalWriteTree minting a fresh change per
// AddContent, so successive writes apply as distinct changes.
type sequencedTree struct {
	*fakeLocalWriteTree
	adds int
}

func (s *sequencedTree) AddContent(ctx context.Context, content objecttree.SignableChangeContent) (objecttree.AddResult, error) {
	s.adds++
	id := fmt.Sprintf("ch-%d", s.adds)
	s.changes[id] = &objecttree.Change{Id: id, Identity: s.root.Identity}
	s.nextId, s.nextOrd, s.nextSeq = id, fmt.Sprintf("a%09d", s.adds), uint64(s.adds)
	return s.fakeLocalWriteTree.AddContent(ctx, content)
}

func newLocalWriteIfFixture(t *testing.T) (*Object, *sequencedTree) {
	t.Helper()
	ctx := context.Background()

	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "lwif.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctrl, err := crdt.NewController(ctx, "obj-lwif", db,
		crdt.HandlerReg{Name: "blocks", Handler: crdt.DefaultHandler{}, Schema: schema.Dataset{Dynamic: true}},
		crdt.HandlerReg{Name: "other", Handler: crdt.DefaultHandler{}, Schema: schema.Dataset{Dynamic: true}},
	)
	require.NoError(t, err)
	ctrl.SetApplySeqAllocator(crdt.NewApplySeqAllocator(func(context.Context) (uint64, error) { return 0, nil }))

	priv, _, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	_, rootPub, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)

	tree := &sequencedTree{fakeLocalWriteTree: &fakeLocalWriteTree{
		id:      "obj-lwif",
		root:    &objecttree.Change{Id: "root-id", Identity: rootPub, Timestamp: 1700000000},
		changes: map[string]*objecttree.Change{},
	}}
	return &Object{signKey: priv, codec: NewCodec(), ctrl: ctrl, spaceId: "space-lwif", tree: tree}, tree
}

func nameChange(t *testing.T, dataset, id, val string) crdt.Change {
	return crdt.Change{
		Dataset:     dataset,
		DataVersion: "test-v1",
		Records:     []crdt.RecordChange{{Id: id, Upsert: true, Ops: []crdt.Op{recordOp(t, "name", val)}}},
	}
}

// TestLocalWriteIf pins the conditional write: it lands while the
// dataset is unchanged since the given apply sequence — whatever
// happened in the object's other datasets — and is refused, with
// nothing written, once any record of the dataset moved past it,
// a tombstone included.
func TestLocalWriteIf(t *testing.T) {
	ctx := context.Background()
	o, tree := newLocalWriteIfFixture(t)

	first, err := o.LocalWrite(ctx, nameChange(t, "blocks", "a", "v1"))
	require.NoError(t, err)
	require.NotZero(t, first.ApplySeq)

	second, err := o.LocalWriteIf(ctx, nameChange(t, "blocks", "a", "v2"), first.ApplySeq)
	require.NoError(t, err, "unchanged since the read: the write lands")
	assert.Greater(t, second.ApplySeq, first.ApplySeq)

	adds := tree.adds
	_, err = o.LocalWriteIf(ctx, nameChange(t, "blocks", "a", "stale"), first.ApplySeq)
	require.ErrorIs(t, err, crdt.ErrPreconditionFailed)
	assert.Equal(t, adds, tree.adds, "a refused write adds no change")
	assert.Equal(t, "v2", o.ctrl.Get(ctx, "blocks", "a").GetString("name"))

	// Another dataset of the object moves the watermark; this one is
	// still as read, so the write lands.
	_, err = o.LocalWrite(ctx, nameChange(t, "other", "x", "elsewhere"))
	require.NoError(t, err)
	third, err := o.LocalWriteIf(ctx, nameChange(t, "blocks", "b", "v1"), second.ApplySeq)
	require.NoError(t, err, "a write to another dataset does not fail the precondition")

	// A delete is a change to the dataset too.
	_, err = o.LocalWrite(ctx, crdt.Change{
		Dataset:     "blocks",
		DataVersion: "test-v1",
		Records:     []crdt.RecordChange{{Id: "a", Ops: []crdt.Op{{Type: crdt.OpDelete}}}},
	})
	require.NoError(t, err)
	_, err = o.LocalWriteIf(ctx, nameChange(t, "blocks", "b", "v2"), third.ApplySeq)
	require.ErrorIs(t, err, crdt.ErrPreconditionFailed, "a tombstone stamped after the read fails the precondition")
}

// TestLocalWriteIf_UpdateAndDeleteInOneChange pins that one conditional
// change can create, update and delete records together.
func TestLocalWriteIf_UpdateAndDeleteInOneChange(t *testing.T) {
	ctx := context.Background()
	o, _ := newLocalWriteIfFixture(t)

	seed, err := o.LocalWrite(ctx, crdt.Change{
		Dataset:     "blocks",
		DataVersion: "test-v1",
		Records: []crdt.RecordChange{
			{Id: "keep", Upsert: true, Ops: []crdt.Op{recordOp(t, "name", "old")}},
			{Id: "drop", Upsert: true, Ops: []crdt.Op{recordOp(t, "name", "gone soon")}},
		},
	})
	require.NoError(t, err)

	res, err := o.LocalWriteIf(ctx, crdt.Change{
		Dataset:     "blocks",
		DataVersion: "test-v1",
		Records: []crdt.RecordChange{
			{Id: "keep", Ops: []crdt.Op{recordOp(t, "name", "new")}},
			{Id: "fresh", Upsert: true, Ops: []crdt.Op{recordOp(t, "name", "added")}},
			{Id: "drop", Ops: []crdt.Op{{Type: crdt.OpDelete}}},
		},
	}, seed.ApplySeq)
	require.NoError(t, err)
	assert.Empty(t, res.Rejections)

	assert.Equal(t, "new", o.ctrl.Get(ctx, "blocks", "keep").GetString("name"))
	assert.Equal(t, "added", o.ctrl.Get(ctx, "blocks", "fresh").GetString("name"))
	assert.NotNil(t, o.ctrl.Get(ctx, "blocks", "drop").Get(crdt.DeletedAtField), "the deleted record is a tombstone")
}
