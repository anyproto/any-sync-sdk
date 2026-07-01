package object

import (
	"context"
	"path/filepath"
	"sync"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// fakePlaintextTree extends the LocalWrite fake with ChangeInfo (the
// plaintext-class anchor) and a scripted IterateAfterAddSeq for the
// inbound replay tests. Captures every AddContent call.
type fakePlaintextTree struct {
	objecttree.ObjectTree
	mu         sync.Mutex
	id         string
	changeType string
	root       *objecttree.Change
	captured   []objecttree.SignableChangeContent
	// stored simulates inbound changes for IterateAfterAddSeq —
	// each carries Id/AddSeq/OrderId/Data.
	stored []*objecttree.Change
}

func (f *fakePlaintextTree) Lock()         { f.mu.Lock() }
func (f *fakePlaintextTree) Unlock()       { f.mu.Unlock() }
func (f *fakePlaintextTree) TryLock() bool { return f.mu.TryLock() }
func (f *fakePlaintextTree) Id() string    { return f.id }
func (f *fakePlaintextTree) Root() *objecttree.Change {
	return f.root
}
func (f *fakePlaintextTree) ChangeInfo() *treechangeproto.TreeChangeInfo {
	return &treechangeproto.TreeChangeInfo{ChangeType: f.changeType}
}
func (f *fakePlaintextTree) GetChange(id string) (*objecttree.Change, error) {
	for _, c := range f.stored {
		if c.Id == id {
			return c, nil
		}
	}
	return f.root, nil
}
func (f *fakePlaintextTree) AddContent(_ context.Context, content objecttree.SignableChangeContent) (objecttree.AddResult, error) {
	f.captured = append(f.captured, content)
	return objecttree.AddResult{
		Added: []objecttree.StorageChange{{
			Id:      "ch-local",
			OrderId: "a00000000" + string(rune('1'+len(f.captured))),
			AddSeq:  uint64(len(f.captured)),
		}},
	}, nil
}

// IterateAfterAddSeq mimics the real contract: convert runs per
// change with its raw Data (cleartext passthrough — these fakes model
// ReadKeyId=="" changes), the result lands on ch.Model, then iterate
// runs until it returns false.
func (f *fakePlaintextTree) IterateAfterAddSeq(_ context.Context, addSeq uint64, convert objecttree.ChangeConvertFunc, iterate objecttree.ChangeIterateFunc) error {
	for _, c := range f.stored {
		if c.AddSeq <= addSeq {
			continue
		}
		model, err := convert(c, c.Data)
		if err != nil {
			return err
		}
		c.Model = model
		if !iterate(c) {
			return nil
		}
	}
	return nil
}

const (
	plaintextType    = "ptype"
	allowedDataset   = "pt-data"
	forbiddenDataset = "other"
)

func newPlaintextFixture(t *testing.T, changeType string) (*Object, *fakePlaintextTree) {
	t.Helper()
	ctx := context.Background()

	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "pt.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctrl, err := crdt.NewController(ctx, "obj-pt", db,
		crdt.HandlerReg{Name: allowedDataset, Handler: crdt.DefaultHandler{}, Schema: schema.Dataset{Dynamic: true}},
		crdt.HandlerReg{Name: forbiddenDataset, Handler: crdt.DefaultHandler{}, Schema: schema.Dataset{Dynamic: true}},
	)
	require.NoError(t, err)

	priv, pub, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)

	tree := &fakePlaintextTree{
		id:         "obj-pt",
		changeType: changeType,
		root: &objecttree.Change{
			Id:        "obj-pt",
			Identity:  pub,
			Timestamp: 1700000000,
		},
	}
	o := &Object{
		signKey: priv,
		codec:   NewCodec(),
		ctrl:    ctrl,
		spaceId: "space-pt",
		tree:    tree,
		plaintextSpecs: map[string]PlaintextSpec{
			plaintextType: {Datasets: map[string]struct{}{allowedDataset: {}}},
		},
	}
	return o, tree
}

func setChange(t *testing.T, dataset, recId, key, val string) crdt.Change {
	t.Helper()
	a := &anyenc.Arena{}
	return crdt.Change{
		Dataset:     dataset,
		DataVersion: "test-v1",
		Records: []crdt.RecordChange{{
			Id:     recId,
			Upsert: true,
			Ops:    []crdt.Op{{Type: crdt.OpSet, Path: []string{key}, Payload: a.NewString(val)}},
		}},
	}
}

// TestLocalWrite_PlaintextShipsUnencrypted: a plaintext-class object
// writes its allowlisted dataset with ShouldBeEncrypted=false, and the
// on-wire bytes are the cleartext SDK payload (decodable with no key).
func TestLocalWrite_PlaintextShipsUnencrypted(t *testing.T) {
	o, tree := newPlaintextFixture(t, plaintextType)

	_, err := o.LocalWrite(context.Background(), setChange(t, allowedDataset, "r1", "name", "hello"))
	require.NoError(t, err)
	require.Len(t, tree.captured, 1)
	content := tree.captured[0]
	assert.False(t, content.ShouldBeEncrypted, "plaintext-class change must ship unencrypted")
	assert.Equal(t, allowedDataset, content.DataType)

	// The wire bytes must decode with no key — the broker view.
	decoded, err := NewCodec().Decode(content.Data)
	require.NoError(t, err)
	assert.Equal(t, allowedDataset, decoded.Dataset)
	require.Len(t, decoded.Records, 1)
	assert.Equal(t, "r1", decoded.Records[0].Id)
}

// TestLocalWrite_PlaintextRejectsForeignDataset: any non-allowlisted
// dataset hard-errors BEFORE AddContent — nothing leaks into the DAG.
func TestLocalWrite_PlaintextRejectsForeignDataset(t *testing.T) {
	o, tree := newPlaintextFixture(t, plaintextType)

	_, err := o.LocalWrite(context.Background(), setChange(t, forbiddenDataset, "r1", "name", "leak"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "plaintext")
	assert.Empty(t, tree.captured, "the rejected change must never reach AddContent")
}

// TestLocalWrite_RegularObjectStaysEncrypted: an object whose root
// ChangeType is not a plaintext class keeps ShouldBeEncrypted=true
// even with specs registered.
func TestLocalWrite_RegularObjectStaysEncrypted(t *testing.T) {
	o, tree := newPlaintextFixture(t, "object")

	_, err := o.LocalWrite(context.Background(), setChange(t, forbiddenDataset, "r1", "name", "hello"))
	require.NoError(t, err)
	require.Len(t, tree.captured, 1)
	assert.True(t, tree.captured[0].ShouldBeEncrypted)
}

// TestReplay_PlaintextSkipsForeignDataset: the inbound counterpart —
// a peer-authored change on a non-allowlisted dataset is skipped
// tolerantly, the allowlisted one applies.
func TestReplay_PlaintextSkipsForeignDataset(t *testing.T) {
	ctx := context.Background()
	o, tree := newPlaintextFixture(t, plaintextType)
	codec := NewCodec()

	good := setChange(t, allowedDataset, "r-good", "name", "kept")
	bad := setChange(t, forbiddenDataset, "r-bad", "name", "smuggled")
	goodBytes, err := codec.Encode(&good)
	require.NoError(t, err)
	goodBytes = append([]byte(nil), goodBytes...)
	badBytes, err := codec.Encode(&bad)
	require.NoError(t, err)

	tree.stored = []*objecttree.Change{
		{Id: "ch-good", AddSeq: 1, OrderId: "a1", Data: goodBytes},
		{Id: "ch-bad", AddSeq: 2, OrderId: "a2", Data: badBytes},
	}
	require.NoError(t, o.ColdRestore(ctx))

	assert.NotNil(t, o.ctrl.Get(ctx, allowedDataset, "r-good"), "allowlisted dataset must materialize")
	assert.Nil(t, o.ctrl.Get(ctx, forbiddenDataset, "r-bad"), "foreign dataset must be skipped on a plaintext object")
}

// TestApplyDecoded_PlaintextGuard: the drain path refuses to
// materialize a non-allowlisted dataset on a plaintext object.
func TestApplyDecoded_PlaintextGuard(t *testing.T) {
	o, _ := newPlaintextFixture(t, plaintextType)
	ch := setChange(t, forbiddenDataset, "r1", "name", "leak")
	ch.VersionId = "v1"
	err := o.ApplyDecoded(context.Background(), ch)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "plaintext")
}
