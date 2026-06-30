//go:build carpackspike

// Spike: pack a file's whole IPFS/UnixFS DAG into ONE packed object (CAR-style,
// with a cid->offset index over EVERY node incl. intermediates) and measure
// random-seek reads through the *real* anytype-heart read path:
//
//	plaintext --AES-256-CFB(whole file, zero IV)--> ciphertext
//	         --1 MiB SizeSplitter + balanced UnixFS (fanout 174, CIDv1/dag-pb)--> DAG
//	         --pack all blocks into one file + cid->offset index-->
//	read: CAR-backed NodeGetter -> boxo ufsio.DagReader (tree traversal, fetches
//	      intermediates by cid) -> seekable CFB decryptor (recovers IV from the
//	      16 ciphertext bytes before the offset). Identical layout/crypto to
//	      any-sync/commonfile/fileservice.AddFile + anytype-heart's reader.
//
// It answers: does packing the whole DAG into one object preserve random-seek
// performance vs one-file-per-cid, what's the pack/index/size overhead, and is
// a multi-level traversal cheap. Real boxo importer + real AES, so the numbers
// are true.
//
// Run:
//
//	FILE_MB=8   go test -tags carpackspike ./internal/spikes/carpack -run TestCarPackSeek -v   # smoke
//	FILE_MB=256 go test -tags carpackspike ./internal/spikes/carpack -run TestCarPackSeek -v -timeout 20m  # multi-level
package carpack

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"fmt"
	"io"
	mrand "math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	chunker "github.com/ipfs/boxo/chunker"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	"github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	ufsio "github.com/ipfs/boxo/ipld/unixfs/io"
	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	mh "github.com/multiformats/go-multihash"
)

const chunkSize = 1 << 20 // 1 MiB leaves, matching any-sync fileservice.ChunkSize

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
}

// ---- in-memory DAGService used only to capture the importer's output ----

type memDAG struct{ store map[string][]byte }

func newMemDAG() *memDAG { return &memDAG{store: map[string][]byte{}} }

func decode(c cid.Cid, data []byte) (ipld.Node, error) {
	b, err := blocks.NewBlockWithCid(data, c) // verifies data hashes to c
	if err != nil {
		return nil, err
	}
	return merkledag.DecodeProtobufBlock(b)
}

func (m *memDAG) Get(_ context.Context, c cid.Cid) (ipld.Node, error) {
	data, ok := m.store[c.KeyString()]
	if !ok {
		return nil, ipld.ErrNotFound{Cid: c}
	}
	return decode(c, data)
}
func (m *memDAG) GetMany(ctx context.Context, cs []cid.Cid) <-chan *ipld.NodeOption {
	ch := make(chan *ipld.NodeOption, len(cs))
	go func() {
		defer close(ch)
		for _, c := range cs {
			n, err := m.Get(ctx, c)
			ch <- &ipld.NodeOption{Node: n, Err: err}
		}
	}()
	return ch
}
func (m *memDAG) Add(_ context.Context, n ipld.Node) error {
	m.store[n.Cid().KeyString()] = n.RawData()
	return nil
}
func (m *memDAG) AddMany(ctx context.Context, ns []ipld.Node) error {
	for _, n := range ns {
		if err := m.Add(ctx, n); err != nil {
			return err
		}
	}
	return nil
}
func (m *memDAG) Remove(_ context.Context, c cid.Cid) error { delete(m.store, c.KeyString()); return nil }
func (m *memDAG) RemoveMany(_ context.Context, cs []cid.Cid) error {
	for _, c := range cs {
		delete(m.store, c.KeyString())
	}
	return nil
}

// ---- packed file (CAR-style records: uvarint(len) || cid || blockData) ----

type extent struct{ off, len int64 }

func writeCAR(path string, store map[string][]byte) (idx map[string]extent, size int64, err error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	idx = make(map[string]extent, len(store))
	var pos int64
	var hdr [binary.MaxVarintLen64]byte
	for ks, data := range store {
		c, err := cid.Cast([]byte(ks))
		if err != nil {
			return nil, 0, err
		}
		cb := c.Bytes()
		section := int64(len(cb) + len(data))
		n := binary.PutUvarint(hdr[:], uint64(section))
		if _, err = f.Write(hdr[:n]); err != nil {
			return nil, 0, err
		}
		if _, err = f.Write(cb); err != nil {
			return nil, 0, err
		}
		dataOff := pos + int64(n) + int64(len(cb))
		if _, err = f.Write(data); err != nil {
			return nil, 0, err
		}
		idx[ks] = extent{off: dataOff, len: int64(len(data))}
		pos += int64(n) + section
	}
	return idx, pos, nil
}

// scanCAR rebuilds the cid->offset index from a packed file (cold-open cost).
func scanCAR(f *os.File) (map[string]extent, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	end := fi.Size()
	idx := map[string]extent{}
	buf := make([]byte, binary.MaxVarintLen64)
	var pos int64
	for pos < end {
		nr, _ := f.ReadAt(buf, pos)
		section, vn := binary.Uvarint(buf[:nr])
		if vn <= 0 {
			return nil, fmt.Errorf("bad varint at %d", pos)
		}
		sec := make([]byte, section)
		if _, err := f.ReadAt(sec, pos+int64(vn)); err != nil {
			return nil, err
		}
		cn, c, err := cid.CidFromBytes(sec)
		if err != nil {
			return nil, err
		}
		idx[c.KeyString()] = extent{off: pos + int64(vn) + int64(cn), len: int64(int(section) - cn)}
		pos += int64(vn) + int64(section)
	}
	return idx, nil
}

// carGetter resolves any cid (leaf or intermediate) from the packed file by index.
type carGetter struct {
	f       *os.File
	idx     map[string]extent
	reads   int64 // block fetches (= DAG nodes touched)
	readsB  int64 // bytes read from the packed object
}

func (g *carGetter) raw(c cid.Cid) ([]byte, error) {
	e, ok := g.idx[c.KeyString()]
	if !ok {
		return nil, ipld.ErrNotFound{Cid: c}
	}
	b := make([]byte, e.len)
	if _, err := g.f.ReadAt(b, e.off); err != nil {
		return nil, err
	}
	g.reads++
	g.readsB += e.len
	return b, nil
}
func (g *carGetter) Get(_ context.Context, c cid.Cid) (ipld.Node, error) {
	data, err := g.raw(c)
	if err != nil {
		return nil, err
	}
	return decode(c, data) // NewBlockWithCid re-verifies the block against its cid
}
func (g *carGetter) GetMany(ctx context.Context, cs []cid.Cid) <-chan *ipld.NodeOption {
	ch := make(chan *ipld.NodeOption, len(cs))
	go func() {
		defer close(ch)
		for _, c := range cs {
			n, err := g.Get(ctx, c)
			ch <- &ipld.NodeOption{Node: n, Err: err}
		}
	}()
	return ch
}

// ---- seekable AES-256-CFB decryptor (ports anytype-heart cfb.CFBDecryptor) ----

type cfbReader struct {
	block cipher.Block
	ct    io.ReadSeeker
	sr    *cipher.StreamReader
	pos   int64
}

func newCFBReader(key []byte, ct io.ReadSeeker) (*cfbReader, error) {
	b, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	r := &cfbReader{block: b, ct: ct}
	return r, r.seekTo(0)
}
func (r *cfbReader) seekTo(off int64) error {
	corrected := (off / aes.BlockSize) * aes.BlockSize
	iv := make([]byte, aes.BlockSize) // zero IV at start, matching cfb.New(key, {})
	if corrected == 0 {
		if _, err := r.ct.Seek(0, io.SeekStart); err != nil {
			return err
		}
	} else {
		if _, err := r.ct.Seek(corrected-aes.BlockSize, io.SeekStart); err != nil {
			return err
		}
		if _, err := io.ReadFull(r.ct, iv); err != nil { // previous ciphertext block = the new IV
			return err
		}
	}
	r.sr = &cipher.StreamReader{S: cipher.NewCFBDecrypter(r.block, iv), R: r.ct}
	if rem := off - corrected; rem > 0 {
		if _, err := io.CopyN(io.Discard, r.sr, rem); err != nil {
			return err
		}
	}
	r.pos = off
	return nil
}
func (r *cfbReader) Read(p []byte) (int, error) {
	n, err := r.sr.Read(p)
	r.pos += int64(n)
	return n, err
}
func (r *cfbReader) Seek(off int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = off
	case io.SeekCurrent:
		abs = r.pos + off
	case io.SeekEnd:
		end, err := r.ct.Seek(0, io.SeekEnd)
		if err != nil {
			return 0, err
		}
		abs = end + off
	}
	return abs, r.seekTo(abs)
}

// ---- the spike ----

func TestCarPackSeek(t *testing.T) {
	ctx := context.Background()
	fileMB := envInt("FILE_MB", 64)
	seeks := envInt("SEEKS", 3000)
	readLen := envInt("READ_KB", 256) * 1024
	size := fileMB << 20
	r := mrand.New(mrand.NewSource(42))

	// plaintext (kept for verification)
	plain := make([]byte, size)
	r.Read(plain)
	key := make([]byte, 32)
	r.Read(key)

	// ---- build: CFB(whole) -> ciphertext -> balanced UnixFS DAG (1 MiB) ----
	blockAES, _ := aes.NewCipher(key)
	ctReader := cipher.StreamReader{
		S: cipher.NewCFBEncrypter(blockAES, make([]byte, aes.BlockSize)),
		R: bytes.NewReader(plain),
	}
	dag := newMemDAG()
	prefix := cid.Prefix{Version: 1, Codec: cid.DagProtobuf, MhType: mh.SHA2_256, MhLength: -1}
	dbp := helpers.DagBuilderParams{
		Maxlinks:   helpers.DefaultLinksPerBlock, // 174
		CidBuilder: prefix,
		Dagserv:    dag,
	}
	tb := time.Now()
	db, err := dbp.New(chunker.NewSizeSplitter(ctReader, chunkSize))
	must(t, err)
	root, err := balanced.Layout(db)
	must(t, err)
	rootCid := root.Cid()
	buildDur := time.Since(tb)

	var blockBytes int64
	for _, d := range dag.store {
		blockBytes += int64(len(d))
	}
	nLeaves := (size + chunkSize - 1) / chunkSize

	// ---- pack into one file + index ----
	dir := t.TempDir()
	carPath := filepath.Join(dir, "file.car")
	tp := time.Now()
	_, carSize, err := writeCAR(carPath, dag.store)
	must(t, err)
	packDur := time.Since(tp)

	// ---- cold open: reopen + rebuild index by scanning ----
	f, err := os.Open(carPath)
	must(t, err)
	defer f.Close()
	ts := time.Now()
	idx, err := scanCAR(f)
	must(t, err)
	scanDur := time.Since(ts)

	getter := &carGetter{f: f, idx: idx}
	open := func() (*cfbReader, error) {
		rn, err := getter.Get(ctx, rootCid)
		if err != nil {
			return nil, err
		}
		dr, err := ufsio.NewDagReader(ctx, rn, getter)
		if err != nil {
			return nil, err
		}
		return newCFBReader(key, dr)
	}

	// ---- correctness: full sequential read == plaintext ----
	rd, err := open()
	must(t, err)
	got, err := io.ReadAll(rd)
	must(t, err)
	if !bytes.Equal(got, plain) {
		t.Fatalf("sequential read mismatch: got %d bytes", len(got))
	}

	// ---- sequential throughput ----
	getter.reads, getter.readsB = 0, 0
	rd, _ = open()
	tseq := time.Now()
	_, err = io.Copy(io.Discard, rd)
	must(t, err)
	seqDur := time.Since(tseq)
	seqReads, seqReadsB := getter.reads, getter.readsB

	// ---- random seeks (packed) ----
	if readLen > size {
		readLen = size
	}
	rd, _ = open()
	getter.reads, getter.readsB = 0, 0
	buf := make([]byte, readLen)
	lat := make([]time.Duration, seeks)
	for i := 0; i < seeks; i++ {
		off := int64(r.Intn(size - readLen + 1))
		t0 := time.Now()
		if _, err := rd.Seek(off, io.SeekStart); err != nil {
			t.Fatalf("seek: %v", err)
		}
		if _, err := io.ReadFull(rd, buf); err != nil {
			t.Fatalf("read: %v", err)
		}
		lat[i] = time.Since(t0)
		if !bytes.Equal(buf, plain[off:off+int64(readLen)]) {
			t.Fatalf("seek read mismatch at off=%d", off)
		}
	}
	packReads, packReadsB := getter.reads, getter.readsB

	// ---- comparison: one file per cid ----
	pbDir := filepath.Join(dir, "blocks")
	must(t, os.Mkdir(pbDir, 0o755))
	for ks, d := range dag.store {
		c, _ := cid.Cast([]byte(ks))
		must(t, os.WriteFile(filepath.Join(pbDir, c.String()), d, 0o644))
	}
	pb := &fileGetter{dir: pbDir}
	pbOpen := func() (*cfbReader, error) {
		rn, err := pb.Get(ctx, rootCid)
		if err != nil {
			return nil, err
		}
		dr, err := ufsio.NewDagReader(ctx, rn, pb)
		if err != nil {
			return nil, err
		}
		return newCFBReader(key, dr)
	}
	rd2, err := pbOpen()
	must(t, err)
	r2 := mrand.New(mrand.NewSource(42)) // same offsets
	lat2 := make([]time.Duration, seeks)
	for i := 0; i < seeks; i++ {
		off := int64(r2.Intn(size - readLen + 1))
		t0 := time.Now()
		_, err := rd2.Seek(off, io.SeekStart)
		must(t, err)
		_, err = io.ReadFull(rd2, buf)
		must(t, err)
		lat2[i] = time.Since(t0)
	}

	// ---- report ----
	p := func(ds []time.Duration, q float64) time.Duration {
		s := append([]time.Duration(nil), ds...)
		sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
		return s[int(float64(len(s))*q)]
	}
	mb := func(b int64) float64 { return float64(b) / (1 << 20) }
	t.Logf("================ CAR-PACK SEEK (file=%d MiB, %d-byte chunks, %d seeks x %d KiB) ================",
		fileMB, chunkSize, seeks, readLen/1024)
	t.Logf("rootCid             : %s", rootCid)
	t.Logf("DAG                 : %d leaves, %d total nodes (intermediates incl.), fanout %d", nLeaves, len(dag.store), helpers.DefaultLinksPerBlock)
	t.Logf("build (CFB+import)  : %s", buildDur.Round(time.Millisecond))
	t.Logf("pack (write CAR)    : %s   -> %.1f MiB on disk (plaintext %.1f MiB, dag/car overhead %.2f%%)",
		packDur.Round(time.Millisecond), mb(carSize), mb(int64(size)), 100*(float64(carSize)/float64(size)-1))
	t.Logf("cold index scan     : %s   (%d cids; CARv2 would persist this -> 0)", scanDur.Round(time.Millisecond), len(idx))
	t.Logf("sequential read     : %s  = %.0f MiB/s   (%d node-fetches, %.1f MiB read)",
		seqDur.Round(time.Millisecond), mb(int64(size))/seqDur.Seconds(), seqReads, mb(seqReadsB))
	t.Logf("PACKED random seek  : p50 %s  p99 %s   (avg %.1f node-fetches/seek, %.2f MiB read/seek over %d DAG levels)",
		p(lat, 0.5), p(lat, 0.99), float64(packReads)/float64(seeks), mb(packReadsB)/float64(seeks), levels(nLeaves))
	t.Logf("PER-CID-FILE seek   : p50 %s  p99 %s   (same offsets; %d separate files)",
		p(lat2, 0.5), p(lat2, 0.99), len(dag.store))
	floorMiB := 1.0 + float64(readLen)/float64(chunkSize) // whole leaves a read touches
	t.Logf("READ AMPLIFICATION  : ~%.1f MiB fetched/seek to serve %d KiB; floor is ~%.2f MiB (whole leaves) -> stock DagReader read-ahead adds ~%.0fx. For S3/CDN seek, build a no-preload TARGETED range reader (path nodes + covered leaves only).",
		mb(packReadsB)/float64(seeks), readLen/1024, floorMiB, (mb(packReadsB)/float64(seeks))/floorMiB)
}

func levels(leaves int) int {
	d := 1
	n := leaves
	for n > helpers.DefaultLinksPerBlock {
		n = (n + helpers.DefaultLinksPerBlock - 1) / helpers.DefaultLinksPerBlock
		d++
	}
	return d
}

// fileGetter: one file per cid (named by cid.String()).
type fileGetter struct{ dir string }

func (g *fileGetter) Get(_ context.Context, c cid.Cid) (ipld.Node, error) {
	data, err := os.ReadFile(filepath.Join(g.dir, c.String()))
	if err != nil {
		return nil, err
	}
	return decode(c, data)
}
func (g *fileGetter) GetMany(ctx context.Context, cs []cid.Cid) <-chan *ipld.NodeOption {
	ch := make(chan *ipld.NodeOption, len(cs))
	go func() {
		defer close(ch)
		for _, c := range cs {
			n, err := g.Get(ctx, c)
			ch <- &ipld.NodeOption{Node: n, Err: err}
		}
	}()
	return ch
}
