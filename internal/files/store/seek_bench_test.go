package store

import (
	"bytes"
	"context"
	"io"
	mrand "math/rand"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-sync/commonfile/fileservice"
	ufsio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/files/crypt"
)

// BenchmarkRandomSeek is the carpack-spike measurement over the
// production stack (CARv2 + embedded index + verified reads + seekable
// CFB): random 64 KiB reads at random offsets of a 32 MiB file. The
// spike baseline was ~0.5 ms p50 per seek on the packed layout — a
// regression here means the index/read path grew a hidden cost.
func BenchmarkRandomSeek(b *testing.B) {
	ctx := context.Background()
	dir := b.TempDir()
	db, err := anystore.Open(ctx, filepath.Join(dir, "meta.db"), nil)
	require.NoError(b, err)
	defer db.Close()
	s, err := New(ctx, filepath.Join(dir, "files"), db)
	require.NoError(b, err)

	rnd := mrand.New(mrand.NewSource(1))
	plain := make([]byte, 32<<20)
	rnd.Read(plain)
	key := make([]byte, crypt.KeySize)
	rnd.Read(key)

	build, err := s.NewBuild(ctx, spaceId)
	require.NoError(b, err)
	ct, err := crypt.NewEncryptReader(key, bytes.NewReader(plain))
	require.NoError(b, err)
	root, err := fileservice.NewFileHandler(build.Blockstore()).AddFile(ctx, ct)
	require.NoError(b, err)
	info, err := build.Finalize(ctx, root.Cid())
	require.NoError(b, err)

	h, err := s.Open(ctx, spaceId, info.Root)
	require.NoError(b, err)
	defer h.Close()
	getter := h.NodeGetter(nil)
	rootNode, err := getter.Get(ctx, h.Root())
	require.NoError(b, err)
	dr, err := ufsio.NewDagReader(ctx, rootNode, getter)
	require.NoError(b, err)
	r, err := crypt.NewReader(key, dr)
	require.NoError(b, err)

	buf := make([]byte, 64<<10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		off := int64(rnd.Intn(len(plain) - len(buf)))
		if _, err = r.Seek(off, io.SeekStart); err != nil {
			b.Fatal(err)
		}
		if _, err = io.ReadFull(r, buf); err != nil {
			b.Fatal(err)
		}
	}
}
