package spaceindex_test

import (
	"crypto/rand"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/types/spaceindex"
)

func TestInviteKeys_CustodyCannotBeOverwrittenOrDeleted(t *testing.T) {
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "keys.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	ctrl, err := crdt.NewController(ctx, "index", db, crdt.HandlerReg{
		Name: spaceindex.InviteKeysDataset, Handler: spaceindex.InviteKeysHandler{}, Schema: spaceindex.InviteKeysSchema(),
	})
	require.NoError(t, err)
	key, pub, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	encoded, err := crypto.EncodeKeyToString(key)
	require.NoError(t, err)
	other, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)
	wrongKey, err := crypto.EncodeKeyToString(other)
	require.NoError(t, err)
	forged, err := key.Raw()
	require.NoError(t, err)
	forged[0] ^= 1 // keep the cached public key, corrupt its seed
	forgedKey := crypto.EncodeBytesToString(forged)
	arena := &anyenc.Arena{}
	next := identityKeyVersions(t)
	apply := func(rec crdt.RecordChange) crdt.ApplyResult {
		t.Helper()
		res, err := ctrl.ApplyChangeWithResult(ctx, crdt.Change{
			ObjectId: "index", Dataset: spaceindex.InviteKeysDataset, DataVersion: spaceindex.InviteKeysHandlerVersion,
			VersionId: next(), Creator: alice, Records: []crdt.RecordChange{rec},
		})
		require.NoError(t, err)
		return res
	}
	set := func(id, value string) crdt.RecordChange {
		return crdt.RecordChange{Id: id, Upsert: true, Ops: []crdt.Op{{
			Type: crdt.OpSet, Path: []string{spaceindex.FieldInviteKey}, Payload: arena.NewString(value),
		}}}
	}
	require.NotEmpty(t, apply(set(pub.Account(), wrongKey)).Rejections, "invalid initial custody")
	require.Empty(t, apply(set(pub.Account(), encoded)).Rejections)
	require.Empty(t, apply(set(pub.Account(), encoded)).Rejections, "republishing is harmless")
	for _, rec := range []crdt.RecordChange{
		set(pub.Account(), wrongKey),
		set(pub.Account(), "malformed"),
		set(pub.Account(), forgedKey),
		{Id: pub.Account(), Ops: []crdt.Op{{Type: crdt.OpDelete}}},
		{Id: pub.Account(), Ops: []crdt.Op{{Type: crdt.OpUnset, Path: []string{spaceindex.FieldInviteKey}}}},
	} {
		require.NotEmpty(t, apply(rec).Rejections)
		require.Equal(t, encoded, ctrl.Get(ctx, spaceindex.InviteKeysDataset, pub.Account()).GetString(spaceindex.FieldInviteKey))
	}
}
