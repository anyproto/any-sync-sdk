package object

import (
	"errors"
	"testing"

	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

// fakeTree satisfies objecttree.ObjectTree by embedding the interface
// (nil at runtime); the methods we need are overridden, anything else
// will panic — which is what we want for a unit test that only
// exercises stampObjectMeta.
type fakeTree struct {
	objecttree.ObjectTree
	id      string
	root    *objecttree.Change
	changes map[string]*objecttree.Change
}

func (f *fakeTree) Id() string             { return f.id }
func (f *fakeTree) Root() *objecttree.Change { return f.root }
func (f *fakeTree) GetChange(id string) (*objecttree.Change, error) {
	if c, ok := f.changes[id]; ok {
		return c, nil
	}
	return nil, errors.New("not found")
}

func mustGenKey(t *testing.T) crypto.PubKey {
	t.Helper()
	_, pub, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	return pub
}

// TestStampObjectMeta_PeerChange — root signed by A, the change being
// applied signed by B (e.g. an inbound peer write). Creator must be B,
// ObjectAuthor must remain A. This is the load-bearing case for shared
// objects: per-message author enforcement reads Creator, not
// ObjectAuthor.
func TestStampObjectMeta_PeerChange(t *testing.T) {
	authorKey := mustGenKey(t)
	peerKey := mustGenKey(t)

	root := &objecttree.Change{
		Id:        "root-id",
		Identity:  authorKey,
		Timestamp: 1700000000,
	}
	peerChange := &objecttree.Change{
		Id:       "ch-peer",
		Identity: peerKey,
	}
	tree := &fakeTree{
		id:   "obj-1",
		root: root,
		changes: map[string]*objecttree.Change{
			"ch-peer": peerChange,
		},
	}
	o := &Object{tree: tree}
	ch := crdt.Change{ChangeId: "ch-peer"}

	o.stampObjectMeta(&ch)

	assert.Equal(t, authorKey.Account(), ch.ObjectAuthor, "ObjectAuthor must be the root signer")
	assert.Equal(t, peerKey.Account(), ch.Creator, "Creator must be the per-change signer")
	assert.NotEqual(t, ch.ObjectAuthor, ch.Creator, "peer write: Creator and ObjectAuthor diverge")
	assert.Equal(t, int64(1700000000), ch.ObjectCreatedAt, "ObjectCreatedAt comes from the root timestamp")
}

// TestStampObjectMeta_RootChange — for the root change, Creator and
// ObjectAuthor coincide. Regression guard against stamping logic that
// accidentally treats the root as an exception.
func TestStampObjectMeta_RootChange(t *testing.T) {
	authorKey := mustGenKey(t)
	root := &objecttree.Change{
		Id:        "root-id",
		Identity:  authorKey,
		Timestamp: 1700000000,
	}
	tree := &fakeTree{
		id:   "obj-1",
		root: root,
		changes: map[string]*objecttree.Change{
			"root-id": root,
		},
	}
	o := &Object{tree: tree}
	ch := crdt.Change{ChangeId: "root-id"}

	o.stampObjectMeta(&ch)

	assert.Equal(t, authorKey.Account(), ch.ObjectAuthor)
	assert.Equal(t, authorKey.Account(), ch.Creator)
	assert.Equal(t, ch.ObjectAuthor, ch.Creator)
}

// TestStampObjectMeta_LocalWrite — the typical local-author scenario
// where the local signer wrote both the root and the current change.
// Creator equals the local account (and equals ObjectAuthor here
// because there's no other peer in play).
func TestStampObjectMeta_LocalWrite(t *testing.T) {
	localKey := mustGenKey(t)
	root := &objecttree.Change{
		Id:        "root-id",
		Identity:  localKey,
		Timestamp: 1700000000,
	}
	localChange := &objecttree.Change{
		Id:       "ch-local",
		Identity: localKey,
	}
	tree := &fakeTree{
		id:   "obj-1",
		root: root,
		changes: map[string]*objecttree.Change{
			"ch-local": localChange,
		},
	}
	o := &Object{tree: tree}
	ch := crdt.Change{ChangeId: "ch-local"}

	o.stampObjectMeta(&ch)

	assert.Equal(t, localKey.Account(), ch.Creator)
	assert.Equal(t, localKey.Account(), ch.ObjectAuthor)
}

// TestStampObjectMeta_NoTree — tests / pre-bind drains can have no
// tree wired. Stamping must be a clean no-op.
func TestStampObjectMeta_NoTree(t *testing.T) {
	o := &Object{}
	ch := crdt.Change{ChangeId: "ch-x"}

	o.stampObjectMeta(&ch)

	assert.Empty(t, ch.Creator)
	assert.Empty(t, ch.ObjectAuthor)
	assert.Zero(t, ch.ObjectCreatedAt)
}

// TestStampObjectMeta_ChangeNotInTree — the change isn't attached to
// the tree (e.g. hand-built test change). Creator stays empty without
// erroring; ObjectAuthor / ObjectCreatedAt still come from the root.
func TestStampObjectMeta_ChangeNotInTree(t *testing.T) {
	authorKey := mustGenKey(t)
	root := &objecttree.Change{
		Id:        "root-id",
		Identity:  authorKey,
		Timestamp: 1700000000,
	}
	tree := &fakeTree{
		id:      "obj-1",
		root:    root,
		changes: map[string]*objecttree.Change{},
	}
	o := &Object{tree: tree}
	ch := crdt.Change{ChangeId: "ch-missing"}

	o.stampObjectMeta(&ch)

	assert.Empty(t, ch.Creator)
	assert.Equal(t, authorKey.Account(), ch.ObjectAuthor)
	assert.Equal(t, int64(1700000000), ch.ObjectCreatedAt)
}

// TestStampObjectMeta_PreservesPrestamped — when the caller
// pre-populated Creator (or ObjectAuthor / ObjectCreatedAt), the
// stamper must leave those values alone. Mirrors the existing
// "if empty" guard on ObjectAuthor.
func TestStampObjectMeta_PreservesPrestamped(t *testing.T) {
	authorKey := mustGenKey(t)
	peerKey := mustGenKey(t)
	root := &objecttree.Change{
		Id:        "root-id",
		Identity:  authorKey,
		Timestamp: 1700000000,
	}
	peerChange := &objecttree.Change{
		Id:       "ch-peer",
		Identity: peerKey,
	}
	tree := &fakeTree{
		id:   "obj-1",
		root: root,
		changes: map[string]*objecttree.Change{
			"ch-peer": peerChange,
		},
	}
	o := &Object{tree: tree}
	ch := crdt.Change{
		ChangeId:        "ch-peer",
		Creator:         "preset-creator",
		ObjectAuthor:    "preset-author",
		ObjectCreatedAt: 999,
	}

	o.stampObjectMeta(&ch)

	assert.Equal(t, "preset-creator", ch.Creator)
	assert.Equal(t, "preset-author", ch.ObjectAuthor)
	assert.Equal(t, int64(999), ch.ObjectCreatedAt)
}
