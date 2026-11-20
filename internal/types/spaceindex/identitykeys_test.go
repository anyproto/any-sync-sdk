package spaceindex_test

import (
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/lexid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/types/spaceindex"
)

const (
	alice = "AAlice"
	bob   = "ABob"
)

func newIdentityKeysController(t *testing.T) *crdt.Controller {
	t.Helper()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "test.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	ctrl, err := crdt.NewController(ctx, "spaceIndexObj", db, crdt.HandlerReg{
		Name:    spaceindex.IdentityKeysDataset,
		Handler: spaceindex.IdentityKeysHandler{},
		Schema:  spaceindex.IdentityKeysSchema(),
	})
	require.NoError(t, err)
	return ctrl
}

// keyChange is one participant's publish write: $set symKey on the row
// keyed by rowId, signed by creator — the shape publishOneToOneKey emits.
func keyChange(version crdt.VersionId, creator, rowId, symKey string) crdt.Change {
	arena := &anyenc.Arena{}
	return crdt.Change{
		ObjectId:    "spaceIndexObj",
		Dataset:     spaceindex.IdentityKeysDataset,
		VersionId:   version,
		DataVersion: spaceindex.IdentityKeysHandlerVersion,
		Creator:     creator,
		Records: []crdt.RecordChange{{
			Id:     rowId,
			Upsert: true,
			Ops:    []crdt.Op{{Type: crdt.OpSet, Path: []string{spaceindex.FieldIdentityKeySymKey}, Payload: arena.NewString(symKey)}},
		}},
	}
}

func identityKeyVersions(t *testing.T) func() crdt.VersionId {
	t.Helper()
	lx := lexid.Must(lexid.CharsBase64, 4, 100)
	var last string
	return func() crdt.VersionId {
		last = lx.Next(last)
		return crdt.VersionId(last)
	}
}

func TestIdentityKeys_OwnRowAccepted(t *testing.T) {
	ctrl := newIdentityKeysController(t)
	next := identityKeyVersions(t)

	res, err := ctrl.ApplyChangeWithResult(ctx, keyChange(next(), alice, alice, "k-alice"))
	require.NoError(t, err)
	require.Empty(t, res.Rejections)
	res, err = ctrl.ApplyChangeWithResult(ctx, keyChange(next(), bob, bob, "k-bob"))
	require.NoError(t, err)
	require.Empty(t, res.Rejections)

	assert.Equal(t, "k-alice", spaceindex.IdentityKeyOf(ctrl.Get(ctx, spaceindex.IdentityKeysDataset, alice)))
	assert.Equal(t, "k-bob", spaceindex.IdentityKeyOf(ctrl.Get(ctx, spaceindex.IdentityKeysDataset, bob)))

	// Re-publishing the same key is a valid no-op write.
	res, err = ctrl.ApplyChangeWithResult(ctx, keyChange(next(), alice, alice, "k-alice"))
	require.NoError(t, err)
	require.Empty(t, res.Rejections)
}

func TestIdentityKeys_ForeignRowRejected(t *testing.T) {
	ctrl := newIdentityKeysController(t)
	next := identityKeyVersions(t)

	// Bob cannot create Alice's row …
	res, err := ctrl.ApplyChangeWithResult(ctx, keyChange(next(), bob, alice, "forged"))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)
	assert.Nil(t, ctrl.Get(ctx, spaceindex.IdentityKeysDataset, alice))

	// … nor overwrite it once it exists.
	res, err = ctrl.ApplyChangeWithResult(ctx, keyChange(next(), alice, alice, "k-alice"))
	require.NoError(t, err)
	require.Empty(t, res.Rejections)
	res, err = ctrl.ApplyChangeWithResult(ctx, keyChange(next(), bob, alice, "forged"))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)
	assert.Equal(t, "k-alice", spaceindex.IdentityKeyOf(ctrl.Get(ctx, spaceindex.IdentityKeysDataset, alice)))
}

func TestIdentityKeys_NoCreatorRejected(t *testing.T) {
	ctrl := newIdentityKeysController(t)
	next := identityKeyVersions(t)
	res, err := ctrl.ApplyChangeWithResult(ctx, keyChange(next(), "", alice, "k-alice"))
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)
	assert.Nil(t, ctrl.Get(ctx, spaceindex.IdentityKeysDataset, alice))
}

func TestIdentityKeys_ShapeRules(t *testing.T) {
	ctrl := newIdentityKeysController(t)
	next := identityKeyVersions(t)
	arena := &anyenc.Arena{}
	bad := func(ops ...crdt.Op) crdt.Change {
		return crdt.Change{
			ObjectId: "spaceIndexObj", Dataset: spaceindex.IdentityKeysDataset,
			VersionId: next(), DataVersion: spaceindex.IdentityKeysHandlerVersion, Creator: alice,
			Records: []crdt.RecordChange{{Id: alice, Upsert: true, Ops: ops}},
		}
	}
	for name, ch := range map[string]crdt.Change{
		"empty key":     bad(crdt.Op{Type: crdt.OpSet, Path: []string{spaceindex.FieldIdentityKeySymKey}, Payload: arena.NewString("")}),
		"unknown field": bad(crdt.Op{Type: crdt.OpSet, Path: []string{"name"}, Payload: arena.NewString("Alice")}),
		"unset":         bad(crdt.Op{Type: crdt.OpUnset, Path: []string{spaceindex.FieldIdentityKeySymKey}}),
	} {
		res, err := ctrl.ApplyChangeWithResult(ctx, ch)
		require.NoError(t, err, name)
		assert.Len(t, res.Rejections, 1, name)
	}
	assert.Empty(t, spaceindex.IdentityKeyOf(ctrl.Get(ctx, spaceindex.IdentityKeysDataset, alice)))
}

func TestIdentityKeys_DeleteRejected(t *testing.T) {
	ctrl := newIdentityKeysController(t)
	next := identityKeyVersions(t)
	res, err := ctrl.ApplyChangeWithResult(ctx, keyChange(next(), alice, alice, "k-alice"))
	require.NoError(t, err)
	require.Empty(t, res.Rejections)

	res, err = ctrl.ApplyChangeWithResult(ctx, crdt.Change{
		ObjectId: "spaceIndexObj", Dataset: spaceindex.IdentityKeysDataset,
		VersionId: next(), DataVersion: spaceindex.IdentityKeysHandlerVersion, Creator: alice,
		Records: []crdt.RecordChange{{Id: alice, Ops: []crdt.Op{{Type: crdt.OpDelete}}}},
	})
	require.NoError(t, err)
	require.Len(t, res.Rejections, 1)
	assert.Equal(t, "k-alice", spaceindex.IdentityKeyOf(ctrl.Get(ctx, spaceindex.IdentityKeysDataset, alice)))
}
