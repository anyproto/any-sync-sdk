package carfile

import (
	"bytes"
	"context"
	mrand "math/rand"
	"os"
	"path/filepath"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

// makeBlock wraps data in a CIDv1/dag-pb/sha2-256 block, the only cid
// shape our files contain.
func makeBlock(t *testing.T, data []byte) blocks.Block {
	t.Helper()
	sum, err := mh.Sum(data, mh.SHA2_256, -1)
	require.NoError(t, err)
	b, err := blocks.NewBlockWithCid(data, cid.NewCidV1(cid.DagProtobuf, sum))
	require.NoError(t, err)
	return b
}

// buildCar writes n random blocks (~sizes around 1 KiB) into a CARv2
// and returns the path, the blocks, and the root (last block, like a
// real DAG build where the root is emitted last).
func buildCar(t *testing.T, n int) (string, []blocks.Block) {
	t.Helper()
	rnd := mrand.New(mrand.NewSource(int64(n)))
	path := filepath.Join(t.TempDir(), "file.car")
	b, err := NewBuilder(path)
	require.NoError(t, err)
	blks := make([]blocks.Block, n)
	for i := range blks {
		data := make([]byte, 512+rnd.Intn(1024))
		rnd.Read(data)
		blks[i] = makeBlock(t, data)
	}
	require.NoError(t, b.Put(context.Background(), blks))
	require.NoError(t, b.Finalize(context.Background(), blks[n-1].Cid()))
	return path, blks
}

func TestBuildAndOpen(t *testing.T) {
	path, blks := buildCar(t, 20)

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	cf, err := Open(f)
	require.NoError(t, err)

	require.Equal(t, blks[len(blks)-1].Cid(), cf.Root())
	require.Equal(t, len(blks), cf.NumSections())
	for _, blk := range blks {
		i, ok := cf.Lookup(blk.Cid())
		require.True(t, ok)
		data, err := cf.ReadBlock(i)
		require.NoError(t, err)
		require.True(t, bytes.Equal(blk.RawData(), data))
	}
	_, ok := cf.Lookup(makeBlock(t, []byte("absent")).Cid())
	require.False(t, ok)
}

func TestFinalizeRootMustExist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file.car")
	b, err := NewBuilder(path)
	require.NoError(t, err)
	require.NoError(t, b.Put(context.Background(), []blocks.Block{makeBlock(t, []byte("x"))}))
	err = b.Finalize(context.Background(), makeBlock(t, []byte("other")).Cid())
	require.Error(t, err)
}

func TestReadBlockDetectsCorruption(t *testing.T) {
	path, blks := buildCar(t, 5)

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f.Close()
	cf, err := Open(f)
	require.NoError(t, err)

	// Flip one byte inside block 2's data region.
	i, ok := cf.Lookup(blks[2].Cid())
	require.True(t, ok)
	sec := cf.Section(i)
	_, err = f.WriteAt([]byte{0xff}, sec.Offset+sec.Size-1)
	require.NoError(t, err)

	_, err = cf.ReadBlock(i)
	require.ErrorIs(t, err, ErrCorrupt)
	// Other blocks still verify.
	j, _ := cf.Lookup(blks[3].Cid())
	_, err = cf.ReadBlock(j)
	require.NoError(t, err)
}

// TestSparseReconstruction is the byte-identity invariant: rebuild the
// object from (head range, index range, out-of-order verified blocks)
// and end with the exact original bytes.
func TestSparseReconstruction(t *testing.T) {
	path, blks := buildCar(t, 30)
	orig, err := os.ReadFile(path)
	require.NoError(t, err)

	f, err := os.Open(path)
	require.NoError(t, err)
	cf, err := Open(f)
	require.NoError(t, err)
	hdr := cf.Header()
	require.NoError(t, f.Close())

	// The two ranges a downloader fetches first.
	headLen := int64(4096)
	if headLen > int64(hdr.IndexOffset) {
		headLen = int64(hdr.IndexOffset)
	}
	head := orig[:headLen]
	idx := orig[hdr.IndexOffset:]

	sparsePath := filepath.Join(t.TempDir(), "sparse.car")
	root, present, err := CreateSparse(sparsePath, head, idx)
	require.NoError(t, err)
	require.Equal(t, blks[len(blks)-1].Cid(), root)
	// Head-covered sections came back verified; blocks are ~0.5-1.5 KiB
	// so a 4 KiB head always delivers at least the first one.
	require.NotEmpty(t, present)

	sf, err := os.OpenFile(sparsePath, os.O_RDWR, 0)
	require.NoError(t, err)
	defer sf.Close()
	sp, err := OpenSparse(sf)
	require.NoError(t, err)
	require.Equal(t, len(blks), sp.NumSections())

	// Blocks not yet written read as corrupt/absent... unless the head
	// range already covered them (a small file's first sections ride
	// in with the 4 KiB head fetch).
	rnd := mrand.New(mrand.NewSource(1))
	perm := rnd.Perm(len(blks))
	last := perm[len(perm)-1]
	li, ok := sp.Lookup(blks[last].Cid())
	require.True(t, ok)
	if sec := sp.Section(li); sec.Offset >= headLen {
		_, err = sp.ReadBlock(li)
		require.ErrorIs(t, err, ErrCorrupt)
	}

	// Fill in random order; every block readable right after its write.
	for _, bi := range perm {
		i, err := sp.WriteBlock(blks[bi].Cid(), blks[bi].RawData())
		require.NoError(t, err)
		data, err := sp.ReadBlock(i)
		require.NoError(t, err)
		require.True(t, bytes.Equal(blks[bi].RawData(), data))
	}

	got, err := os.ReadFile(sparsePath)
	require.NoError(t, err)
	require.True(t, bytes.Equal(orig, got), "reconstructed file must be byte-identical to the source object")
}

func TestSparseWriteRejects(t *testing.T) {
	path, blks := buildCar(t, 3)
	orig, err := os.ReadFile(path)
	require.NoError(t, err)
	f, err := os.Open(path)
	require.NoError(t, err)
	cf, err := Open(f)
	require.NoError(t, err)
	hdr := cf.Header()
	require.NoError(t, f.Close())

	sparsePath := filepath.Join(t.TempDir(), "sparse.car")
	_, _, err = CreateSparse(sparsePath, orig[:hdr.IndexOffset], orig[hdr.IndexOffset:])
	require.NoError(t, err)
	sf, err := os.OpenFile(sparsePath, os.O_RDWR, 0)
	require.NoError(t, err)
	defer sf.Close()
	sp, err := OpenSparse(sf)
	require.NoError(t, err)

	// Unknown cid.
	_, err = sp.WriteBlock(makeBlock(t, []byte("stranger")).Cid(), []byte("stranger"))
	require.ErrorIs(t, err, ErrNotIndexed)
	// Data that doesn't hash to the cid (verify-before-persist).
	_, err = sp.WriteBlock(blks[0].Cid(), []byte("tampered"))
	require.ErrorIs(t, err, ErrCorrupt)
	// Right hash, wrong section length is impossible (hash pins the
	// bytes), but a truncated write of the RIGHT data to the WRONG cid
	// is covered by the two cases above.
}

func TestCreateSparseValidates(t *testing.T) {
	dir := t.TempDir()
	_, _, err := CreateSparse(filepath.Join(dir, "a.car"), []byte("garbage"), nil)
	require.Error(t, err)

	// Valid head, garbage index.
	path, _ := buildCar(t, 3)
	orig, err := os.ReadFile(path)
	require.NoError(t, err)
	f, err := os.Open(path)
	require.NoError(t, err)
	cf, err := Open(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	_, _, err = CreateSparse(filepath.Join(dir, "b.car"), orig[:cf.Header().IndexOffset], []byte("garbage"))
	require.Error(t, err)
}
