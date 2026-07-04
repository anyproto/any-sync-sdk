package filep2p

import (
	"bytes"
	"context"
	"crypto/rand"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-sync/commonfile/fileproto/filep2p"
	"github.com/anyproto/any-sync/commonfile/fileproto/filep2p/filep2perr"
	"github.com/anyproto/any-sync/commonfile/fileservice"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/files/crypt"
	"github.com/anyproto/any-sync-sdk/internal/files/store"
)

const testSpace = "space-test"

func newStore(t *testing.T) *store.Store {
	t.Helper()
	dir := t.TempDir()
	db, err := anystore.Open(context.Background(), filepath.Join(dir, "meta.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	s, err := store.New(context.Background(), filepath.Join(dir, "files"), db)
	require.NoError(t, err)
	return s
}

// addFile runs the real upload pipeline (ciphertext → UnixFS DAG → CAR),
// returning the complete file's root and object size.
func addFile(t *testing.T, s *store.Store, plain, key []byte) (cid.Cid, int64) {
	t.Helper()
	ctx := context.Background()
	b, err := s.NewBuild(ctx, testSpace)
	require.NoError(t, err)
	ct, err := crypt.NewEncryptReader(key, bytes.NewReader(plain))
	require.NoError(t, err)
	root, err := fileservice.NewFileHandler(b.Blockstore()).AddFile(ctx, ct)
	require.NoError(t, err)
	info, err := b.Finalize(ctx, root.Cid(), "ref1")
	require.NoError(t, err)
	return info.Root, info.Size
}

func allow(string, string) bool { return true }
func deny(string, string) bool  { return false }

func ctxPeer(id string) context.Context {
	return peer.CtxWithPeerId(context.Background(), id)
}

func TestServerAuthz(t *testing.T) {
	s := newStore(t)
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	root, _ := addFile(t, s, bytes.Repeat([]byte("x"), 300_000), key)

	srv := NewServer(deny)
	srv.SetStore(s)
	ctx := ctxPeer("peer-1")

	_, err := srv.FileCheck(ctx, &filep2p.FileCheckRequest{SpaceId: testSpace, RootCids: [][]byte{root.Bytes()}})
	require.ErrorIs(t, err, filep2perr.ErrForbidden)

	_, err = srv.ObjectRead(ctx, &filep2p.ObjectReadRequest{SpaceId: testSpace, RootCid: root.Bytes(), Offset: 0, Length: 16})
	require.ErrorIs(t, err, filep2perr.ErrForbidden)

	// Missing peer identity is also forbidden.
	_, err = srv.FileCheck(context.Background(), &filep2p.FileCheckRequest{SpaceId: testSpace})
	require.ErrorIs(t, err, filep2perr.ErrForbidden)

	// Empty space is invalid.
	_, err = srv.FileCheck(ctxPeer("p"), &filep2p.FileCheckRequest{SpaceId: ""})
	require.ErrorIs(t, err, filep2perr.ErrInvalidRequest)
}

func TestServerFileCheck(t *testing.T) {
	s := newStore(t)
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	root, _ := addFile(t, s, bytes.Repeat([]byte("y"), 300_000), key)

	srv := NewServer(allow)
	srv.SetStore(s)
	other, _ := cid.Decode("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")

	resp, err := srv.FileCheck(ctxPeer("p"), &filep2p.FileCheckRequest{
		SpaceId:  testSpace,
		RootCids: [][]byte{root.Bytes(), other.Bytes()},
	})
	require.NoError(t, err)
	require.Len(t, resp.Files, 2)
	require.Equal(t, filep2p.Availability_Full, resp.Files[0].Have, "held file is Full")
	require.Equal(t, filep2p.Availability_None, resp.Files[1].Have, "unknown file is None")
}

func TestServerObjectRead(t *testing.T) {
	s := newStore(t)
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	plain := make([]byte, 300_000)
	_, _ = rand.Read(plain)
	root, size := addFile(t, s, plain, key)

	srv := NewServer(allow)
	srv.SetStore(s)
	ctx := ctxPeer("p")

	// Unknown file → FileNotFound.
	unknown, _ := cid.Decode("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
	_, err := srv.ObjectRead(ctx, &filep2p.ObjectReadRequest{SpaceId: testSpace, RootCid: unknown.Bytes(), Offset: 0, Length: 16})
	require.ErrorIs(t, err, filep2perr.ErrFileNotFound)

	// Read the whole object in chunks and reassemble; it must equal the
	// bytes a direct handle read yields (the exact CAR object).
	h, err := s.Open(ctx, testSpace, root)
	require.NoError(t, err)
	defer h.Close()
	want := make([]byte, size)
	_, err = h.ReadAt(want, 0)
	require.NoError(t, err)

	var got []byte
	const chunk = 64 << 10
	for off := int64(0); off < size; off += chunk {
		resp, rErr := srv.ObjectRead(ctx, &filep2p.ObjectReadRequest{
			SpaceId: testSpace, RootCid: root.Bytes(), Offset: uint64(off), Length: chunk,
		})
		require.NoError(t, rErr)
		require.EqualValues(t, size, resp.TotalSize)
		got = append(got, resp.Data...)
	}
	require.True(t, bytes.Equal(want, got), "reassembled object must equal the CAR bytes")

	// Zero / oversized length is invalid.
	_, err = srv.ObjectRead(ctx, &filep2p.ObjectReadRequest{SpaceId: testSpace, RootCid: root.Bytes(), Offset: 0, Length: 0})
	require.ErrorIs(t, err, filep2perr.ErrInvalidRequest)
}
