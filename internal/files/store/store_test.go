package store

import (
	"bytes"
	"context"
	"io"
	mrand "math/rand"
	"os"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-sync/commonfile/fileservice"
	ufsio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/files/crypt"
)

const spaceId = "space.test"

func newStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	db, err := anystore.Open(context.Background(), filepath.Join(dir, "meta.db"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	s, err := New(context.Background(), filepath.Join(dir, "files"), db)
	require.NoError(t, err)
	return s
}

// addFile runs the real upload-side pipeline: plaintext → CFB
// ciphertext → fileservice UnixFS DAG → CAR build → finalize.
func addFile(t *testing.T, s *Store, plain, key []byte, refs ...string) Info {
	t.Helper()
	ctx := context.Background()
	b, err := s.NewBuild(ctx, spaceId)
	require.NoError(t, err)
	ct, err := crypt.NewEncryptReader(key, bytes.NewReader(plain))
	require.NoError(t, err)
	fh := fileservice.NewFileHandler(b.Blockstore())
	root, err := fh.AddFile(ctx, ct)
	require.NoError(t, err)
	info, err := b.Finalize(ctx, root.Cid(), refs...)
	require.NoError(t, err)
	return info
}

// openPlain builds the read pipeline over a handle: DagReader over the
// NodeGetter, CFB decryptor on top.
func openPlain(t *testing.T, h *Handle, key []byte, fetch func(ctx context.Context, c cid.Cid) ([]byte, error)) io.ReadSeeker {
	t.Helper()
	ctx := context.Background()
	getter := h.NodeGetter(fetch)
	rootNode, err := getter.Get(ctx, h.Root())
	require.NoError(t, err)
	dr, err := ufsio.NewDagReader(ctx, rootNode, getter)
	require.NoError(t, err)
	r, err := crypt.NewReader(key, dr)
	require.NoError(t, err)
	return r
}

// headAndIndex reads the two ranges a downloader fetches first — the
// object head and the index tail — off a source handle.
func headAndIndex(t *testing.T, src *Handle) (head, idx []byte) {
	t.Helper()
	head = make([]byte, 4096)
	_, err := src.ReadAt(head, 0)
	require.NoError(t, err)
	size, err := src.Size()
	require.NoError(t, err)
	idxOff := int64(src.car.Header().IndexOffset)
	idx = make([]byte, size-idxOff)
	_, err = src.ReadAt(idx, idxOff)
	require.NoError(t, err)
	return head, idx
}

func testPayload(t *testing.T, size int) (plain, key []byte) {
	t.Helper()
	rnd := mrand.New(mrand.NewSource(int64(size)))
	plain = make([]byte, size)
	rnd.Read(plain)
	key = make([]byte, crypt.KeySize)
	rnd.Read(key)
	return
}

func TestAddOpenReadSeek(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	// >1 leaf: 3.5 MiB → 4 leaves + root.
	plain, key := testPayload(t, 7<<19)
	info := addFile(t, s, plain, key, "file1")

	require.Equal(t, StateComplete, info.State)
	require.Equal(t, info.Sections, info.Present)
	require.Equal(t, []string{"file1"}, info.Refs)
	require.Greater(t, info.Sections, 1)

	h, err := s.Open(ctx, spaceId, info.Root)
	require.NoError(t, err)
	defer h.Close()
	require.True(t, h.Complete())
	require.Empty(t, h.MissingBlocks())

	r := openPlain(t, h, key, nil)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.True(t, bytes.Equal(plain, got))

	// Random seeks through the verified read path.
	rnd := mrand.New(mrand.NewSource(9))
	buf := make([]byte, 64<<10)
	for i := 0; i < 20; i++ {
		off := int64(rnd.Intn(len(plain) - len(buf)))
		_, err = r.Seek(off, io.SeekStart)
		require.NoError(t, err)
		_, err = io.ReadFull(r, buf)
		require.NoError(t, err)
		assert.True(t, bytes.Equal(plain[off:off+int64(len(buf))], buf), "off=%d", off)
	}
}

func TestBuildDiscardAndSweep(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	b, err := s.NewBuild(ctx, spaceId)
	require.NoError(t, err)
	b.Discard()
	ents, err := os.ReadDir(filepath.Join(s.root, "tmp"))
	require.NoError(t, err)
	require.Empty(t, ents)

	// A crash-abandoned temp is swept by New.
	require.NoError(t, os.WriteFile(filepath.Join(s.root, "tmp", "dead.car"), []byte("x"), 0o644))
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "meta2.db"), nil)
	require.NoError(t, err)
	defer db.Close()
	_, err = New(ctx, s.root, db)
	require.NoError(t, err)
	ents, err = os.ReadDir(filepath.Join(s.root, "tmp"))
	require.NoError(t, err)
	require.Empty(t, ents)
}

// TestSparseDownload simulates SYN-28: a second store reconstructs the
// file from (head range, index range, per-block fetches) and ends
// byte-identical and complete — reads work mid-fill via the fetch
// callback.
func TestSparseDownload(t *testing.T) {
	ctx := context.Background()
	src := newStore(t)
	plain, key := testPayload(t, 2<<20+333)
	info := addFile(t, src, plain, key, "f")

	srcH, err := src.Open(ctx, spaceId, info.Root)
	require.NoError(t, err)
	defer srcH.Close()

	head, idx := headAndIndex(t, srcH)

	dst := newStore(t)
	dinfo, err := dst.CreateSparse(ctx, spaceId, head, idx, "f")
	require.NoError(t, err)
	require.Equal(t, info.Root, dinfo.Root)
	require.Equal(t, StatePartial, dinfo.State)
	require.Equal(t, info.Sections, dinfo.Sections)

	dh, err := dst.Open(ctx, spaceId, info.Root)
	require.NoError(t, err)
	defer dh.Close()
	require.False(t, dh.Complete())
	require.NotEmpty(t, dh.MissingBlocks())

	fetches := 0
	fetch := func(ctx context.Context, c cid.Cid) ([]byte, error) {
		fetches++
		return srcH.ReadBlock(ctx, c)
	}
	r := openPlain(t, dh, key, fetch)
	got, err := io.ReadAll(r)
	require.NoError(t, err)
	require.True(t, bytes.Equal(plain, got))
	require.Greater(t, fetches, 0)

	// Whole-file read pulled every block → complete, byte-identical.
	require.True(t, dh.Complete())
	dinfo, err = dst.Info(ctx, spaceId, info.Root)
	require.NoError(t, err)
	require.Equal(t, StateComplete, dinfo.State)

	srcBytes, err := os.ReadFile(src.carPath(spaceId, info.Root))
	require.NoError(t, err)
	dstBytes, err := os.ReadFile(dst.carPath(spaceId, info.Root))
	require.NoError(t, err)
	require.True(t, bytes.Equal(srcBytes, dstBytes), "reconstructed CAR must be byte-identical")

	// A reopened complete file serves without any fetcher.
	dh2, err := dst.Open(ctx, spaceId, info.Root)
	require.NoError(t, err)
	defer dh2.Close()
	got2, err := io.ReadAll(openPlain(t, dh2, key, nil))
	require.NoError(t, err)
	require.True(t, bytes.Equal(plain, got2))
}

// TestSparseResume interrupts a fill and resumes it from the persisted
// bitmap through a fresh handle (the pause/resume path).
func TestSparseResume(t *testing.T) {
	ctx := context.Background()
	src := newStore(t)
	plain, key := testPayload(t, 3<<20)
	info := addFile(t, src, plain, key)
	srcH, err := src.Open(ctx, spaceId, info.Root)
	require.NoError(t, err)
	defer srcH.Close()

	head, idx := headAndIndex(t, srcH)

	dst := newStore(t)
	_, err = dst.CreateSparse(ctx, spaceId, head, idx)
	require.NoError(t, err)

	dh, err := dst.Open(ctx, spaceId, info.Root)
	require.NoError(t, err)
	missing := dh.MissingBlocks()
	require.Greater(t, len(missing), 2)

	// First session: fetch only one block, then "crash".
	data, err := srcH.ReadBlock(ctx, missing[0])
	require.NoError(t, err)
	require.NoError(t, dh.WriteBlock(ctx, missing[0], data))
	require.NoError(t, dh.Close())

	// Resume: the persisted bitmap knows what's left.
	dh2, err := dst.Open(ctx, spaceId, info.Root)
	require.NoError(t, err)
	defer dh2.Close()
	left := dh2.MissingBlocks()
	require.Len(t, left, len(missing)-1)
	require.True(t, dh2.HasBlock(missing[0]))
	for _, c := range left {
		data, err := srcH.ReadBlock(ctx, c)
		require.NoError(t, err)
		require.NoError(t, dh2.WriteBlock(ctx, c, data))
	}
	require.True(t, dh2.Complete())
}

func TestOffloadAndRefill(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	plain, key := testPayload(t, 1<<20+17)
	info := addFile(t, s, plain, key, "f1")

	require.NoError(t, s.Offload(ctx, spaceId, info.Root))
	oinfo, err := s.Info(ctx, spaceId, info.Root)
	require.NoError(t, err)
	require.Equal(t, StateOffload, oinfo.State)
	require.Equal(t, []string{"f1"}, oinfo.Refs, "offload keeps the row")
	_, err = os.Stat(s.carPath(spaceId, info.Root))
	require.ErrorIs(t, err, os.ErrNotExist)

	_, err = s.Open(ctx, spaceId, info.Root)
	require.ErrorIs(t, err, ErrNoBytes)

	// Refill via CreateSparse from a peer copy (the same store's twin).
	src := newStore(t)
	addFile(t, src, plain, key)
	srcH, err := src.Open(ctx, spaceId, info.Root)
	require.NoError(t, err)
	defer srcH.Close()
	head, idx := headAndIndex(t, srcH)

	rinfo, err := s.CreateSparse(ctx, spaceId, head, idx)
	require.NoError(t, err)
	require.Equal(t, StatePartial, rinfo.State)
	require.Equal(t, []string{"f1"}, rinfo.Refs, "refill keeps existing refs")

	h, err := s.Open(ctx, spaceId, info.Root)
	require.NoError(t, err)
	defer h.Close()
	got, err := io.ReadAll(openPlain(t, h, key, func(ctx context.Context, c cid.Cid) ([]byte, error) {
		return srcH.ReadBlock(ctx, c)
	}))
	require.NoError(t, err)
	require.True(t, bytes.Equal(plain, got))
}

func TestRefsAndDelete(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	plain, key := testPayload(t, 100_000)
	info := addFile(t, s, plain, key, "a")

	require.NoError(t, s.AddRefs(ctx, spaceId, info.Root, "b", "a"))
	i2, err := s.Info(ctx, spaceId, info.Root)
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, i2.Refs)

	n, err := s.RemoveRef(ctx, spaceId, info.Root, "a")
	require.NoError(t, err)
	require.Equal(t, 1, n)
	n, err = s.RemoveRef(ctx, spaceId, info.Root, "b")
	require.NoError(t, err)
	require.Equal(t, 0, n)

	require.NoError(t, s.Delete(ctx, spaceId, info.Root))
	_, err = s.Info(ctx, spaceId, info.Root)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = os.Stat(s.carPath(spaceId, info.Root))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestDedupIndex(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	plain, key := testPayload(t, 50_000)
	info := addFile(t, s, plain, key)
	sha := bytes.Repeat([]byte{7}, 32)

	_, _, ok, err := s.LookupContent(ctx, spaceId, sha)
	require.NoError(t, err)
	require.False(t, ok)

	require.NoError(t, s.RecordContent(ctx, spaceId, sha, info.Root, "file9"))
	root, fileId, ok, err := s.LookupContent(ctx, spaceId, sha)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, info.Root, root)
	require.Equal(t, "file9", fileId)

	// Per-space scope: another space sees nothing.
	_, _, ok, err = s.LookupContent(ctx, "other.space", sha)
	require.NoError(t, err)
	require.False(t, ok)
}

func TestDeleteSpace(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	plain, key := testPayload(t, 80_000)
	info := addFile(t, s, plain, key)
	require.NoError(t, s.RecordContent(ctx, spaceId, bytes.Repeat([]byte{1}, 32), info.Root, "f"))

	require.NoError(t, s.DeleteSpace(ctx, spaceId))
	_, err := s.Info(ctx, spaceId, info.Root)
	require.ErrorIs(t, err, ErrNotFound)
	_, _, ok, err := s.LookupContent(ctx, spaceId, bytes.Repeat([]byte{1}, 32))
	require.NoError(t, err)
	require.False(t, ok)
	_, err = os.Stat(filepath.Join(s.root, spaceId))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestListSpace(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	p1, k1 := testPayload(t, 10_000)
	p2, k2 := testPayload(t, 20_000)
	addFile(t, s, p1, k1, "f1")
	addFile(t, s, p2, k2, "f2")

	infos, err := s.ListSpace(ctx, spaceId)
	require.NoError(t, err)
	require.Len(t, infos, 2)
	infos, err = s.ListSpace(ctx, "other.space")
	require.NoError(t, err)
	require.Empty(t, infos)
}

// TestConcurrentHandlesShareProgress: two handles on the same partial
// file must see one bitmap — progress written through one is visible
// through the other and completion is reached exactly once.
func TestConcurrentHandlesShareProgress(t *testing.T) {
	ctx := context.Background()
	src := newStore(t)
	plain, key := testPayload(t, 3<<20)
	info := addFile(t, src, plain, key)
	srcH, err := src.Open(ctx, spaceId, info.Root)
	require.NoError(t, err)
	defer srcH.Close()
	head, idx := headAndIndex(t, srcH)

	dst := newStore(t)
	_, err = dst.CreateSparse(ctx, spaceId, head, idx)
	require.NoError(t, err)

	hA, err := dst.Open(ctx, spaceId, info.Root)
	require.NoError(t, err)
	defer hA.Close()
	hB, err := dst.Open(ctx, spaceId, info.Root)
	require.NoError(t, err)
	defer hB.Close()

	missing := hA.MissingBlocks()
	require.Greater(t, len(missing), 1)
	// Alternate blocks between the two handles.
	for i, c := range missing {
		data, err := srcH.ReadBlock(ctx, c)
		require.NoError(t, err)
		h := hA
		if i%2 == 1 {
			h = hB
		}
		require.NoError(t, h.WriteBlock(ctx, c, data))
		// Progress through one handle is immediately visible via the other.
		require.True(t, hA.HasBlock(c))
		require.True(t, hB.HasBlock(c))
	}
	require.True(t, hA.Complete())
	require.True(t, hB.Complete())
	dinfo, err := dst.Info(ctx, spaceId, info.Root)
	require.NoError(t, err)
	require.Equal(t, StateComplete, dinfo.State)
	require.Equal(t, dinfo.Sections, dinfo.Present)
}

// TestOffloadWithOpenHandle: an open handle must observe the offload
// and stop mutating — its writes never resurrect the row.
func TestOffloadWithOpenHandle(t *testing.T) {
	ctx := context.Background()
	src := newStore(t)
	plain, key := testPayload(t, 2<<20)
	info := addFile(t, src, plain, key)
	srcH, err := src.Open(ctx, spaceId, info.Root)
	require.NoError(t, err)
	defer srcH.Close()
	head, idx := headAndIndex(t, srcH)

	dst := newStore(t)
	_, err = dst.CreateSparse(ctx, spaceId, head, idx)
	require.NoError(t, err)
	dh, err := dst.Open(ctx, spaceId, info.Root)
	require.NoError(t, err)
	defer dh.Close()
	missing := dh.MissingBlocks()
	require.NotEmpty(t, missing)

	require.NoError(t, dst.Offload(ctx, spaceId, info.Root))

	// The handle sees the flip: writes are silent no-ops, no row mutation.
	data, err := srcH.ReadBlock(ctx, missing[0])
	require.NoError(t, err)
	require.NoError(t, dh.WriteBlock(ctx, missing[0], data))
	oinfo, err := dst.Info(ctx, spaceId, info.Root)
	require.NoError(t, err)
	require.Equal(t, StateOffload, oinfo.State)
	require.False(t, dh.Complete())
	_, err = dh.ReadAt(make([]byte, 1), 0)
	require.Error(t, err)
}

// TestDeleteNoZombie: mutations racing a Delete must not recreate the
// row (updateExisting never upserts).
func TestDeleteNoZombie(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	plain, key := testPayload(t, 100_000)
	info := addFile(t, s, plain, key, "f")

	require.NoError(t, s.Delete(ctx, spaceId, info.Root))
	require.ErrorIs(t, s.Touch(ctx, spaceId, info.Root), ErrNotFound)
	require.ErrorIs(t, s.AddRefs(ctx, spaceId, info.Root, "g"), ErrNotFound)
	_, err := s.Info(ctx, spaceId, info.Root)
	require.ErrorIs(t, err, ErrNotFound)
}

// TestCorruptBlockHeals: a complete file that lost a block to disk
// corruption reads as ErrBlockMissing, flips to partial, and heals
// through a refetch back to complete.
func TestCorruptBlockHeals(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	plain, key := testPayload(t, 2<<20)
	info := addFile(t, s, plain, key)

	h, err := s.Open(ctx, spaceId, info.Root)
	require.NoError(t, err)
	defer h.Close()
	// Corrupt the last byte of some block's data region on disk.
	victim := h.car.Section(1)
	f, err := os.OpenFile(s.carPath(spaceId, info.Root), os.O_RDWR, 0)
	require.NoError(t, err)
	orig := make([]byte, 1)
	_, err = f.ReadAt(orig, victim.Offset+victim.Size-1)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{orig[0] ^ 0xff}, victim.Offset+victim.Size-1)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = h.ReadBlock(ctx, victim.Cid)
	require.ErrorIs(t, err, ErrBlockMissing)
	require.False(t, h.Complete())
	cinfo, err := s.Info(ctx, spaceId, info.Root)
	require.NoError(t, err)
	require.Equal(t, StatePartial, cinfo.State)
	require.Equal(t, cinfo.Sections-1, cinfo.Present)

	// Heal: rewrite the true bytes (as the fetch path would).
	src := newStore(t)
	addFile(t, src, plain, key)
	srcH, err := src.Open(ctx, spaceId, info.Root)
	require.NoError(t, err)
	defer srcH.Close()
	data, err := srcH.ReadBlock(ctx, victim.Cid)
	require.NoError(t, err)
	require.NoError(t, h.WriteBlock(ctx, victim.Cid, data))
	require.True(t, h.Complete())
	got, err := io.ReadAll(openPlain(t, h, key, nil))
	require.NoError(t, err)
	require.True(t, bytes.Equal(plain, got))
}

// TestReadAtRefusesPartial: the raw byte stream (the upload source)
// must refuse a file with holes.
func TestReadAtRefusesPartial(t *testing.T) {
	ctx := context.Background()
	src := newStore(t)
	plain, key := testPayload(t, 2<<20)
	info := addFile(t, src, plain, key)
	srcH, err := src.Open(ctx, spaceId, info.Root)
	require.NoError(t, err)
	defer srcH.Close()
	head, idx := headAndIndex(t, srcH)

	dst := newStore(t)
	_, err = dst.CreateSparse(ctx, spaceId, head, idx)
	require.NoError(t, err)
	dh, err := dst.Open(ctx, spaceId, info.Root)
	require.NoError(t, err)
	defer dh.Close()
	_, err = dh.ReadAt(make([]byte, 16), 0)
	require.ErrorIs(t, err, ErrNoBytes)
}

// TestCreateSparseRebuildsOrphan: a CAR file with no metadata row (the
// debris of a crashed Delete) must not wedge refetching.
func TestCreateSparseRebuildsOrphan(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	plain, key := testPayload(t, 1<<20)
	info := addFile(t, s, plain, key)
	srcH, err := s.Open(ctx, spaceId, info.Root)
	require.NoError(t, err)
	head, idx := headAndIndex(t, srcH)
	require.NoError(t, srcH.Close())

	// Simulate a crash between row delete and file unlink.
	require.NoError(t, s.files.DeleteId(ctx, rowId(spaceId, info.Root)))

	rinfo, err := s.CreateSparse(ctx, spaceId, head, idx, "f")
	require.NoError(t, err)
	require.Equal(t, info.Root, rinfo.Root)
	require.Equal(t, []string{"f"}, rinfo.Refs)
	_, err = s.Open(ctx, spaceId, info.Root)
	require.NoError(t, err)
}
