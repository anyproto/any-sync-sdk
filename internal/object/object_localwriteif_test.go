package object

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
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
	return newLocalWriteIfFixtureWith(t,
		crdt.HandlerReg{Name: "blocks", Handler: crdt.DefaultHandler{}, Schema: schema.Dataset{Dynamic: true}},
		crdt.HandlerReg{Name: "other", Handler: crdt.DefaultHandler{}, Schema: schema.Dataset{Dynamic: true}},
	)
}

func newLocalWriteIfFixtureWith(t *testing.T, regs ...crdt.HandlerReg) (*Object, *sequencedTree) {
	t.Helper()
	ctx := context.Background()

	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "lwif.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctrl, err := crdt.NewController(ctx, "obj-lwif", db, regs...)
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

	second, err := o.LocalWriteIf(ctx, nameChange(t, "blocks", "a", "v2"), &first.ApplySeq)
	require.NoError(t, err, "unchanged since the read: the write lands")
	assert.Greater(t, second.ApplySeq, first.ApplySeq)

	adds := tree.adds
	_, err = o.LocalWriteIf(ctx, nameChange(t, "blocks", "a", "stale"), &first.ApplySeq)
	require.ErrorIs(t, err, crdt.ErrPreconditionFailed)
	assert.Equal(t, adds, tree.adds, "a refused write adds no change")
	assert.Equal(t, "v2", o.ctrl.Get(ctx, "blocks", "a").GetString("name"))

	// Another dataset of the object moves the watermark; this one is
	// still as read, so the write lands.
	_, err = o.LocalWrite(ctx, nameChange(t, "other", "x", "elsewhere"))
	require.NoError(t, err)
	third, err := o.LocalWriteIf(ctx, nameChange(t, "blocks", "b", "v1"), &second.ApplySeq)
	require.NoError(t, err, "a write to another dataset does not fail the precondition")

	// A delete is a change to the dataset too.
	_, err = o.LocalWrite(ctx, crdt.Change{
		Dataset:     "blocks",
		DataVersion: "test-v1",
		Records:     []crdt.RecordChange{{Id: "a", Ops: []crdt.Op{{Type: crdt.OpDelete}}}},
	})
	require.NoError(t, err)
	_, err = o.LocalWriteIf(ctx, nameChange(t, "blocks", "b", "v2"), &third.ApplySeq)
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
	}, &seed.ApplySeq)
	require.NoError(t, err)
	assert.Empty(t, res.Rejections)

	assert.Equal(t, "new", o.ctrl.Get(ctx, "blocks", "keep").GetString("name"))
	assert.Equal(t, "added", o.ctrl.Get(ctx, "blocks", "fresh").GetString("name"))
	assert.NotNil(t, o.ctrl.Get(ctx, "blocks", "drop").Get(crdt.DeletedAtField), "the deleted record is a tombstone")
}

// TestLocalWriteIf_StaleWriteFailsThePrecondition pins the order of the
// checks: a write built on a read that predates a change is a
// precondition failure (re-read and retry), not a validation error
// against state the caller never saw — here a write-once field another
// write already set.
func TestLocalWriteIf_StaleWriteFailsThePrecondition(t *testing.T) {
	ctx := context.Background()
	ds := schema.Dataset{
		Fields: []schema.Field{{Id: "name", Schema: schema.Leaf(schema.KindString)}},
		IdRule: schema.IdUser,
	}
	h, err := crdt.NewSchemaHandler(ds)
	require.NoError(t, err)
	o, _ := newLocalWriteIfFixtureWith(t, crdt.HandlerReg{Name: "docs", Handler: h, Schema: ds})

	before := uint64(0) // the caller read the dataset while it was empty
	_, err = o.LocalWrite(ctx, nameChange(t, "docs", "a", "first"))
	require.NoError(t, err)

	_, err = o.LocalWriteIf(ctx, nameChange(t, "docs", "a", "stale"), &before)
	require.ErrorIs(t, err, crdt.ErrPreconditionFailed)
	assert.NotErrorIs(t, err, crdt.ErrValidation)
}

// sharedWriteStamper models the objects row's production ObjectStamper:
// a synced body change also advances the owning object's row stamp.
type sharedWriteStamper struct{ crdt.DefaultHandler }

func (sharedWriteStamper) StampObject(ctx *crdt.ChangeCtx, sink *crdt.Sink) {
	a := &anyenc.Arena{}
	sink.DeriveOnce(crdt.Op{Type: crdt.OpSet, Path: []string{"modifiedAt"}, Payload: a.NewNumberInt(int(ctx.Change.Timestamp))})
}

type sharedConditionalTree struct {
	*sequencedTree
	inbound []*objecttree.Change
}

func (s *sharedConditionalTree) IterateAfterAddSeq(_ context.Context, seq uint64, convert objecttree.ChangeConvertFunc, iterate objecttree.ChangeIterateFunc) error {
	for _, ch := range s.inbound {
		if ch.AddSeq <= seq {
			continue
		}
		model, err := convert(ch, ch.Data)
		if err != nil {
			return err
		}
		ch.Model = model
		if !iterate(ch) {
			break
		}
	}
	return nil
}

func newSharedConditionalObjects(t *testing.T) (first, other *Object, tree *sharedConditionalTree) {
	t.Helper()
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "shared-conditional.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	shared, err := db.Collection(ctx, "objects")
	require.NoError(t, err)
	alloc := crdt.NewApplySeqAllocator(nil)
	makeObject := func(id string) (*Object, *sharedConditionalTree) {
		ctrl, err := crdt.NewControllerWithShared(ctx, id, db, crdt.SharedCollections{"objects": shared},
			crdt.HandlerReg{Name: "objects", Handler: sharedWriteStamper{}, Schema: schema.Dataset{Dynamic: true}},
			crdt.HandlerReg{Name: "blocks", Handler: crdt.DefaultHandler{}, Schema: schema.Dataset{Dynamic: true}},
		)
		require.NoError(t, err)
		ctrl.SetApplySeqAllocator(alloc)
		priv, pub, err := crypto.GenerateRandomEd25519KeyPair()
		require.NoError(t, err)
		tree := &sharedConditionalTree{sequencedTree: &sequencedTree{fakeLocalWriteTree: &fakeLocalWriteTree{
			id: id, root: &objecttree.Change{Id: id, Identity: pub, Timestamp: 1700000000}, changes: map[string]*objecttree.Change{},
		}}}
		return &Object{signKey: priv, codec: NewCodec(), ctrl: ctrl, spaceId: "shared-conditional-space", tree: tree}, tree
	}
	first, tree = makeObject("first")
	other, _ = makeObject("other")
	return
}

func TestLocalWriteIf_SharedObjects(t *testing.T) {
	ctx := context.Background()
	for _, mutation := range []string{"own property", "own body", "peer property", "peer body"} {
		t.Run(mutation, func(t *testing.T) {
			o, _, tree := newSharedConditionalObjects(t)
			seed, err := o.LocalWrite(ctx, nameChange(t, "objects", "first", "original"))
			require.NoError(t, err)
			current, err := o.LocalWriteIf(ctx, nameChange(t, "objects", "first", "current"), &seed.ApplySeq)
			require.NoError(t, err, "a current objects-row token admits a write")
			require.Greater(t, current.ApplySeq, seed.ApplySeq)

			dataset, id := "objects", "first"
			if mutation == "own body" || mutation == "peer body" {
				dataset, id = "blocks", "paragraph"
			}
			edit := nameChange(t, dataset, id, "concurrent edit")
			if mutation == "peer property" || mutation == "peer body" {
				// Exercise the inbound listener and codec, under the same lock
				// that real synctree Update callbacks hold.
				payload, err := NewCodec().Encode(&edit)
				require.NoError(t, err)
				peer := &objecttree.Change{Id: "peer-change", Identity: tree.root.Identity, Timestamp: 1700000001, AddSeq: 3, OrderId: "b000000001", Data: payload}
				tree.Lock()
				tree.inbound = []*objecttree.Change{peer}
				tree.changes[peer.Id] = peer
				err = o.Update(tree)
				tree.Unlock()
				require.NoError(t, err)
			} else {
				_, err = o.LocalWrite(ctx, edit)
				require.NoError(t, err)
			}
			require.Equal(t, "concurrent edit", o.ctrl.Get(ctx, dataset, id).GetString("name"))
			before := tree.adds
			_, err = o.LocalWriteIf(ctx, nameChange(t, "objects", "first", "migration overwrite"), &current.ApplySeq)
			require.ErrorIs(t, err, crdt.ErrPreconditionFailed)
			assert.Equal(t, before, tree.adds, "a stale conditional write never enters the DAG")
			assert.Equal(t, "concurrent edit", o.ctrl.Get(ctx, dataset, id).GetString("name"), "the edit is preserved")
			assert.NotEqual(t, "migration overwrite", o.ctrl.Get(ctx, "objects", "first").GetString("name"))
		})
	}
}

func TestLocalWriteIf_SharedObjectsIgnoresOtherObject(t *testing.T) {
	ctx := context.Background()
	o, other, _ := newSharedConditionalObjects(t)
	seed, err := o.LocalWrite(ctx, nameChange(t, "objects", "first", "original"))
	require.NoError(t, err)
	_, err = other.LocalWrite(ctx, nameChange(t, "objects", "other", "unrelated edit"))
	require.NoError(t, err)
	res, err := o.LocalWriteIf(ctx, nameChange(t, "objects", "first", "migrated"), &seed.ApplySeq)
	require.NoError(t, err)
	assert.Empty(t, res.Rejections)
	assert.Equal(t, "migrated", o.ctrl.Get(ctx, "objects", "first").GetString("name"))
	assert.Equal(t, "unrelated edit", other.ctrl.Get(ctx, "objects", "other").GetString("name"))
}

// The source dataset and its owning object's metadata are read separately by
// installers. Both snapshots must still hold when a recovery write commits.
func TestLocalWriteGuarded_MetadataAndDatasetPreconditions(t *testing.T) {
	ctx := context.Background()
	for _, mutation := range []string{"metadata", "source"} {
		t.Run(mutation, func(t *testing.T) {
			o, _, tree := newSharedConditionalObjects(t)
			_, err := o.LocalWrite(ctx, nameChange(t, "objects", "first", "installer owned"))
			require.NoError(t, err)
			source, err := o.LocalWrite(ctx, nameChange(t, "blocks", "main", "bundled source"))
			require.NoError(t, err)
			metadataSeq := uint64(o.ctrl.Get(ctx, "objects", "first").GetInt(crdt.ApplySeqField))
			current, err := o.LocalWriteGuarded(ctx, nameChange(t, "blocks", "main", "next bundled source"), &source.ApplySeq, &metadataSeq)
			require.NoError(t, err, "unchanged metadata and source admit the guarded source write")
			require.Empty(t, current.Rejections)
			metadataSeq = uint64(o.ctrl.Get(ctx, "objects", "first").GetInt(crdt.ApplySeqField))
			if mutation == "metadata" {
				_, err = o.LocalWrite(ctx, nameChange(t, "objects", "first", "user changed ownership"))
				require.NoError(t, err)
				changed, err := o.ctrl.ChangedSince(ctx, "blocks", current.ApplySeq)
				require.NoError(t, err)
				require.False(t, changed, "the ordinary dataset precondition cannot detect this edit")
			} else {
				_, err = o.LocalWrite(ctx, nameChange(t, "blocks", "main", "user source"))
				require.NoError(t, err)
				// Refresh ONLY metadata to isolate the source condition. A caller
				// holding this newer metadata snapshot must still honor old source.
				metadataSeq = uint64(o.ctrl.Get(ctx, "objects", "first").GetInt(crdt.ApplySeqField))
			}
			before := tree.adds
			_, err = o.LocalWriteGuarded(ctx, nameChange(t, "blocks", "main", "unsafe recovery"), &current.ApplySeq, &metadataSeq)
			require.ErrorIs(t, err, crdt.ErrPreconditionFailed)
			assert.Equal(t, before, tree.adds, "a failed condition prevents DAG append")
			assert.NotEqual(t, "unsafe recovery", o.ctrl.Get(ctx, "blocks", "main").GetString("name"))
			if mutation == "metadata" {
				assert.Equal(t, "user changed ownership", o.ctrl.Get(ctx, "objects", "first").GetString("name"))
				assert.Equal(t, "next bundled source", o.ctrl.Get(ctx, "blocks", "main").GetString("name"))
			} else {
				assert.Equal(t, "user source", o.ctrl.Get(ctx, "blocks", "main").GetString("name"))
			}
		})
	}
}
