// Command blobbench compares strategies for storing binary IPFS-style block
// payloads, measuring write throughput, random-read throughput, and on-disk
// size. Baseline is go-ds-flatfs (the anytype-heart local block store);
// candidates are any-store/v2 collections under different key layouts and
// compression settings.
//
// Run:
//
//	go run . -block=262144 -perfile=16 -files=64 -reads=1024
package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"

	anystorev0 "github.com/anyproto/any-store"
	anyencv0 "github.com/anyproto/any-store/anyenc"
	queryv0 "github.com/anyproto/any-store/query"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	flatfs "github.com/ipfs/go-ds-flatfs"
	"github.com/multiformats/go-multihash"
)

type block struct {
	cid    cid.Cid
	fileID int
	seq    int
	data   []byte
}

// genBlocks creates `files`*`perfile` incompressible blocks of `size` bytes,
// each addressed by a real CIDv1/sha2-256 over its bytes (random-distributed
// keys, exactly like real content addressing).
func genBlocks(size, perfile, files int) []block {
	out := make([]block, 0, perfile*files)
	for f := 0; f < files; f++ {
		for s := 0; s < perfile; s++ {
			b := make([]byte, size)
			if _, err := rand.Read(b); err != nil {
				panic(err)
			}
			mh, err := multihash.Sum(b, multihash.SHA2_256, -1)
			if err != nil {
				panic(err)
			}
			out = append(out, block{
				cid:    cid.NewCidV1(cid.Raw, mh),
				fileID: f,
				seq:    s,
				data:   b,
			})
		}
	}
	return out
}

type result struct {
	name        string
	writeDur    time.Duration
	readDur     time.Duration
	byCidDur    time.Duration // random lookup BY content hash (FindId or indexed Find)
	hasDur      time.Duration
	sizeBytes   int64
	totalBytes  int64
	reads       int
	byCidOK     bool
}

func (r result) writeMBs() float64 { return mbps(r.totalBytes, r.writeDur) }
func (r result) readMBs(perRead int) float64 {
	return mbps(int64(r.reads)*int64(perRead), r.readDur)
}

func mbps(bytes int64, d time.Duration) float64 {
	if d == 0 {
		return 0
	}
	return (float64(bytes) / 1e6) / d.Seconds()
}

// dirSize sums ACTUAL on-disk allocation (st_blocks*512), not logical file
// size — so a store of many tiny files (flatfs) is charged for block rounding,
// directory entries, and inodes, exactly as the disk sees it.
func dirSize(path string) int64 {
	var total int64
	_ = filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if st, ok := info.Sys().(*syscall.Stat_t); ok {
			total += st.Blocks * 512
		} else if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total
}

// store is the minimal surface every candidate implements.
type store interface {
	put(ctx context.Context, blocks []block) error
	get(ctx context.Context, b block) ([]byte, error)
	has(ctx context.Context, b block) (bool, error)
	// getByCID resolves bytes by content hash (the dedup / BlobCheck / P2P
	// access path). ok=false means the layout has no by-CID path.
	getByCID(ctx context.Context, b block) (data []byte, ok bool, err error)
	close() error
}

func benchStore(ctx context.Context, name, path string, st store, blocks []block, readIdx []int) (result, error) {
	res := result{name: name, reads: len(readIdx)}
	for _, b := range blocks {
		res.totalBytes += int64(len(b.data))
	}

	t0 := time.Now()
	if err := st.put(ctx, blocks); err != nil {
		return res, fmt.Errorf("put: %w", err)
	}
	res.writeDur = time.Since(t0)

	// random reads
	t1 := time.Now()
	for _, i := range readIdx {
		got, err := st.get(ctx, blocks[i])
		if err != nil {
			return res, fmt.Errorf("get: %w", err)
		}
		if len(got) != len(blocks[i].data) {
			return res, fmt.Errorf("get %s: len %d != %d", blocks[i].cid, len(got), len(blocks[i].data))
		}
	}
	res.readDur = time.Since(t1)

	// random lookup BY content hash (dedup / BlobCheck / P2P path)
	t1b := time.Now()
	for _, i := range readIdx {
		got, ok, err := st.getByCID(ctx, blocks[i])
		if err != nil {
			return res, fmt.Errorf("getByCID: %w", err)
		}
		if !ok {
			res.byCidOK = false
			break
		}
		res.byCidOK = true
		if len(got) != len(blocks[i].data) {
			return res, fmt.Errorf("getByCID %s: len %d != %d", blocks[i].cid, len(got), len(blocks[i].data))
		}
	}
	if res.byCidOK {
		res.byCidDur = time.Since(t1b)
	}

	// existence checks
	t2 := time.Now()
	for _, i := range readIdx {
		if _, err := st.has(ctx, blocks[i]); err != nil {
			return res, fmt.Errorf("has: %w", err)
		}
	}
	res.hasDur = time.Since(t2)

	if err := st.close(); err != nil {
		return res, fmt.Errorf("close: %w", err)
	}
	res.sizeBytes = dirSize(path)
	return res, nil
}

func main() {
	blockSize := flag.Int("block", 256*1024, "block size in bytes")
	perFile := flag.Int("perfile", 16, "blocks per file")
	files := flag.Int("files", 64, "number of files")
	reads := flag.Int("reads", 1024, "number of random reads/has checks")
	keep := flag.Bool("keep", false, "keep temp dirs")
	flag.Parse()

	ctx := context.Background()
	blocks := genBlocks(*blockSize, *perFile, *files)
	fmt.Printf("dataset: %d blocks x %d bytes = %d MiB (%d files x %d blocks)\n\n",
		len(blocks), *blockSize, len(blocks)*(*blockSize)/(1024*1024), *files, *perFile)

	// deterministic-ish read pattern: stride across the keyspace
	readIdx := make([]int, *reads)
	for i := range readIdx {
		readIdx[i] = (i * 7919) % len(blocks)
	}

	root, err := os.MkdirTemp("", "blobbench-")
	if err != nil {
		panic(err)
	}
	if !*keep {
		defer os.RemoveAll(root)
	} else {
		fmt.Println("temp root:", root)
	}

	type cand struct {
		name string
		make func(path string) (store, error)
	}
	cands := []cand{
		{"flatfs", func(p string) (store, error) { return newFlatStore(p) }},
		{"anystore/cid-key", func(p string) (store, error) {
			return newAnyStore(ctx, p, keyCID, false)
		}},
		{"anystore/fileid-prefix (no cid idx)", func(p string) (store, error) {
			return newAnyStore(ctx, p, keyFileIDPrefix, false)
		}},
		{"anystore/fileid-prefix + cid idx", func(p string) (store, error) {
			return newAnyStore(ctx, p, keyFileIDPrefix, true)
		}},
		{"anystore/alloc-key + cid idx", func(p string) (store, error) {
			return newAnyStore(ctx, p, keyAlloc, true)
		}},
		{"v0-sqlite/cid-key", func(p string) (store, error) {
			return newAnyStoreV0(ctx, p, keyCID, false)
		}},
		{"v0-sqlite/fileid-prefix (no idx)", func(p string) (store, error) {
			return newAnyStoreV0(ctx, p, keyFileIDPrefix, false)
		}},
		{"v0-sqlite/fileid-prefix + cid idx", func(p string) (store, error) {
			return newAnyStoreV0(ctx, p, keyFileIDPrefix, true)
		}},
		{"v0-sqlite/alloc-key + cid idx", func(p string) (store, error) {
			return newAnyStoreV0(ctx, p, keyAlloc, true)
		}},
	}

	var results []result
	for _, c := range cands {
		path := filepath.Join(root, strings.NewReplacer("/", "_").Replace(c.name))
		if err := os.MkdirAll(path, 0o755); err != nil {
			panic(err)
		}
		st, err := c.make(path)
		if err != nil {
			fmt.Printf("%-36s SETUP ERROR: %v\n", c.name, err)
			continue
		}
		r, err := benchStore(ctx, c.name, path, st, blocks, readIdx)
		if err != nil {
			fmt.Printf("%-36s ERROR: %v\n", c.name, err)
			continue
		}
		results = append(results, r)
	}

	printTable(results, *blockSize)
}

func printTable(results []result, perRead int) {
	sort.Slice(results, func(i, j int) bool { return results[i].sizeBytes < results[j].sizeBytes })
	fmt.Printf("\n%-38s %11s %11s %11s %12s %10s\n",
		"store", "write MB/s", "read/op", "byCID/op", "byCID MB/s", "disk MiB")
	fmt.Println(strings.Repeat("-", 100))
	for _, r := range results {
		byCidPerOp, byCidMBs := "n/a", "n/a"
		if r.byCidOK {
			byCidPerOp = perOp(r.byCidDur, r.reads)
			byCidMBs = fmt.Sprintf("%.1f", mbps(int64(r.reads)*int64(perRead), r.byCidDur))
		}
		fmt.Printf("%-38s %11.1f %11s %11s %12s %10.1f\n",
			r.name,
			r.writeMBs(),
			perOp(r.readDur, r.reads),
			byCidPerOp,
			byCidMBs,
			float64(r.sizeBytes)/(1024*1024),
		)
	}
}

func perOp(d time.Duration, n int) string {
	if n == 0 {
		return "-"
	}
	return (d / time.Duration(n)).String()
}

// ---------- flatfs candidate ----------

type flatStore struct{ ds *flatfs.Datastore }

func newFlatStore(path string) (*flatStore, error) {
	ds, err := flatfs.CreateOrOpen(path, flatfs.IPFS_DEF_SHARD, false)
	if err != nil {
		return nil, err
	}
	return &flatStore{ds: ds}, nil
}

func flatKey(c cid.Cid) datastore.Key { return datastore.NewKey(strings.ToUpper(c.String())) }

func (f *flatStore) put(ctx context.Context, blocks []block) error {
	for _, b := range blocks {
		if err := f.ds.Put(ctx, flatKey(b.cid), b.data); err != nil {
			return err
		}
	}
	return f.ds.Sync(ctx, datastore.NewKey("/"))
}
func (f *flatStore) get(ctx context.Context, b block) ([]byte, error) { return f.ds.Get(ctx, flatKey(b.cid)) }
func (f *flatStore) has(ctx context.Context, b block) (bool, error)   { return f.ds.Has(ctx, flatKey(b.cid)) }

// flatfs is content-addressed by construction: the key IS the CID.
func (f *flatStore) getByCID(ctx context.Context, b block) ([]byte, bool, error) {
	d, err := f.ds.Get(ctx, flatKey(b.cid))
	return d, true, err
}
func (f *flatStore) close() error { return f.ds.Close() }

// ---------- any-store candidates ----------

type keyMode int

const (
	keyCID          keyMode = iota // id = CID string (content-addressed, random key)
	keyFileIDPrefix                // id = "f<fileId>/<seq>" (monotonic, clustered)
	keyAlloc                       // id = "b<global seq>" (allocated surrogate; cid demoted to a field)
)

type anyStore struct {
	db       anystore.DB
	coll     anystore.Collection
	keyMode  keyMode
	indexCid bool
	parser   *anyenc.Parser // reused across reads to avoid per-read allocs
}

// docID is the PRIMARY key. keyAlloc/keyFileIDPrefix carry the CID in a
// separate field (optionally indexed); keyCID is content-addressed directly.
func docID(b block, mode keyMode) string {
	switch mode {
	case keyFileIDPrefix:
		return fmt.Sprintf("f%08d/%06d", b.fileID, b.seq)
	case keyAlloc:
		return fmt.Sprintf("b%010d", b.fileID*100000+b.seq) // monotonic surrogate
	default:
		return b.cid.String()
	}
}

func newAnyStore(ctx context.Context, path string, mode keyMode, indexCid bool) (*anyStore, error) {
	db, err := anystore.Open(ctx, filepath.Join(path, "blobs.db"), &anystore.Config{})
	if err != nil {
		return nil, err
	}
	coll, err := db.CreateCollection(ctx, "blobs", anystore.CollectionOptions{
		Compression: anystore.NoCompression, // ciphertext is incompressible
		PrimaryKey:  "id",
	})
	if err != nil {
		return nil, err
	}
	if indexCid {
		if err := coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "cid", Fields: []string{"cid"}}); err != nil {
			return nil, err
		}
	}
	return &anyStore{db: db, coll: coll, keyMode: mode, indexCid: indexCid, parser: &anyenc.Parser{}}, nil
}

func (a *anyStore) put(ctx context.Context, blocks []block) error {
	tx, err := a.coll.WriteTx(ctx)
	if err != nil {
		return err
	}
	arena := &anyenc.Arena{}
	for _, b := range blocks {
		arena.Reset()
		doc := arena.NewObject()
		doc.Set("id", arena.NewString(docID(b, a.keyMode)))
		// surrogate-keyed layouts keep the content hash as a field for dedup/lookup
		if a.keyMode != keyCID {
			doc.Set("cid", arena.NewString(b.cid.String()))
		}
		doc.Set("data", arena.NewBinary(b.data))
		if err := a.coll.Insert(tx.Context(), doc); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (a *anyStore) get(ctx context.Context, b block) ([]byte, error) {
	doc, err := a.coll.FindIdWithParser(ctx, a.parser, docID(b, a.keyMode))
	if err != nil {
		return nil, err
	}
	return doc.Value().GetBytes("data"), nil
}

func (a *anyStore) has(ctx context.Context, b block) (bool, error) {
	_, err := a.coll.FindIdWithParser(ctx, a.parser, docID(b, a.keyMode))
	if err != nil {
		if err == anystore.ErrDocNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// getByCID resolves by content hash. keyCID is a direct primary-key lookup;
// surrogate-keyed layouts need the cid secondary index (no index -> n/a).
func (a *anyStore) getByCID(ctx context.Context, b block) ([]byte, bool, error) {
	if a.keyMode == keyCID {
		d, err := a.get(ctx, b)
		return d, true, err
	}
	if !a.indexCid {
		return nil, false, nil
	}
	filter := query.Key{Path: []string{"cid"}, Filter: query.NewComp(query.CompOpEq, b.cid.String())}
	iter, err := a.coll.Find(filter).Iter(ctx)
	if err != nil {
		return nil, false, err
	}
	defer iter.Close()
	if !iter.Next() {
		return nil, true, fmt.Errorf("cid %s not found via index", b.cid)
	}
	doc, err := iter.Doc()
	if err != nil {
		return nil, false, err
	}
	return doc.Value().GetBytes("data"), true, nil
}

func (a *anyStore) close() error { return a.db.Close() }

// ---------- any-store v0 (SQLite-backed) candidate ----------

type anyStoreV0 struct {
	db       anystorev0.DB
	coll     anystorev0.Collection
	keyMode  keyMode
	indexCid bool
	parser   *anyencv0.Parser
}

func newAnyStoreV0(ctx context.Context, path string, mode keyMode, indexCid bool) (*anyStoreV0, error) {
	db, err := anystorev0.Open(ctx, filepath.Join(path, "blobs.db"), nil)
	if err != nil {
		return nil, err
	}
	coll, err := db.CreateCollection(ctx, "blobs")
	if err != nil {
		return nil, err
	}
	if indexCid {
		if err := coll.EnsureIndex(ctx, anystorev0.IndexInfo{Name: "cid", Fields: []string{"cid"}}); err != nil {
			return nil, err
		}
	}
	return &anyStoreV0{db: db, coll: coll, keyMode: mode, indexCid: indexCid, parser: &anyencv0.Parser{}}, nil
}

func (a *anyStoreV0) put(ctx context.Context, blocks []block) error {
	tx, err := a.coll.WriteTx(ctx)
	if err != nil {
		return err
	}
	arena := &anyencv0.Arena{}
	for _, b := range blocks {
		arena.Reset()
		doc := arena.NewObject()
		doc.Set("id", arena.NewString(docID(b, a.keyMode)))
		if a.keyMode != keyCID {
			doc.Set("cid", arena.NewString(b.cid.String()))
		}
		doc.Set("data", arena.NewBinary(b.data))
		if err := a.coll.Insert(tx.Context(), doc); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (a *anyStoreV0) get(ctx context.Context, b block) ([]byte, error) {
	doc, err := a.coll.FindIdWithParser(ctx, a.parser, docID(b, a.keyMode))
	if err != nil {
		return nil, err
	}
	return doc.Value().GetBytes("data"), nil
}

func (a *anyStoreV0) has(ctx context.Context, b block) (bool, error) {
	_, err := a.coll.FindIdWithParser(ctx, a.parser, docID(b, a.keyMode))
	if err != nil {
		if err == anystorev0.ErrDocNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (a *anyStoreV0) getByCID(ctx context.Context, b block) ([]byte, bool, error) {
	if a.keyMode == keyCID {
		d, err := a.get(ctx, b)
		return d, true, err
	}
	if !a.indexCid {
		return nil, false, nil
	}
	filter := queryv0.Key{Path: []string{"cid"}, Filter: queryv0.NewComp(queryv0.CompOpEq, b.cid.String())}
	iter, err := a.coll.Find(filter).Iter(ctx)
	if err != nil {
		return nil, false, err
	}
	defer iter.Close()
	if !iter.Next() {
		return nil, true, fmt.Errorf("cid %s not found via index", b.cid)
	}
	doc, err := iter.Doc()
	if err != nil {
		return nil, false, err
	}
	return doc.Value().GetBytes("data"), true, nil
}

func (a *anyStoreV0) close() error { return a.db.Close() }
