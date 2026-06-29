//go:build filetreespike

// Spike: how big is a single "payloads" object tree at file scale, and how
// slow is a cold sync (= fresh device: download + verify-sig + decrypt-field +
// replay/fold every change to rebuild the tree and materialize the collection).
//
// Models the partially-encrypted payload row we groomed:
//
//	cleartext (node-readable):  id, size, cids[] (cids[0] = root), backupStatus
//	encrypted (member-only):    {key, name, meta{mime,imageSize}, objectIds}
//
// One cleartext (ShouldBeEncrypted:false) any-sync object tree holds all rows.
// Real objecttree + real any-store + real ed25519 signing + real AES-GCM on the
// encrypted field, so storage bytes and cold-replay CPU are the true numbers.
//
// Workload (params via env): N files; uploads batched 1..20 rows/change; a
// second filenode "backupStatus durable" change per file (also batched); then
// deletePct of files deleted as batched tombstone changes. Deletions only ADD
// changes — they never shrink tree history.
//
// Run:
//
//	go test -tags filetreespike ./internal/spikes/filetree -run TestFileTreeScale -v -timeout 30m
//	FILETREE_N=2000 go test -tags filetreespike ./internal/spikes/filetree -run TestFileTreeScale -v   # smoke
package filetree

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	mrand "math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store" // v1: objecttree storage uses it
	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync/commonspace/headsync/headstorage"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
)

// wire keys mirror internal/object/wire.go
const (
	kDataset = "d"
	kRecords = "r"
	kRecID   = "i"
	kRecOps  = "o"
	kOpType  = "t"
	kOpPath  = "p"
	kOpVal   = "v"
)

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

const b32 = "abcdefghijklmnopqrstuvwxyz234567"

// token makes a deterministic pseudo-id of the given length from a seed.
func token(seed uint64, length int) string {
	buf := make([]byte, length)
	x := seed*2862933555777941757 + 3037000493
	for i := range buf {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		buf[i] = b32[x&31]
	}
	return string(buf)
}

func cidsForFile(r *mrand.Rand) int {
	switch x := r.Float64(); {
	case x < 0.85:
		return 1
	case x < 0.99:
		return 2 + r.Intn(7) // 2..8
	default:
		return 50 + r.Intn(251) // 50..300
	}
}

func newSealer() cipher.AEAD {
	k := make([]byte, 32)
	_, _ = rand.Read(k)
	b, _ := aes.NewCipher(k)
	g, _ := cipher.NewGCM(b)
	return g
}

func seal(g cipher.AEAD, pt []byte) []byte {
	n := make([]byte, g.NonceSize())
	_, _ = rand.Read(n)
	return g.Seal(n, n, pt, nil)
}

type fileSpec struct {
	id     string
	size   int
	ncids  int
	objIds []string
}

func mem() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapInuse
}

func dirBytes(t *testing.T, dir string) int64 {
	var total int64
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if fi, err := e.Info(); err == nil {
			total += fi.Size()
		}
	}
	return total
}

func TestFileTreeScale(t *testing.T) {
	ctx := context.Background()
	N := envInt("FILETREE_N", 100_000)
	deletePct := envInt("FILETREE_DELETE_PCT", 25)
	maxBatch := envInt("FILETREE_MAX_BATCH", 20)
	r := mrand.New(mrand.NewSource(42))
	g := newSealer()

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "tree.db")

	keys, err := accountdata.NewRandom()
	noErr(t, err)
	spaceId := "spike.filetree"
	acl, err := list.NewInMemoryDerivedAcl(spaceId, keys)
	noErr(t, err)

	db, err := anystore.Open(ctx, dbPath, nil)
	noErr(t, err)
	hs, err := headstorage.New(ctx, db)
	noErr(t, err)
	root, err := objecttree.CreateObjectTreeRoot(objecttree.ObjectTreeCreatePayload{
		PrivKey:     keys.SignKey,
		ChangeType:  "payloads",
		SpaceId:     spaceId,
		IsEncrypted: false, // cleartext tree: node-readable rows
		Seed:        []byte("filetree-seed"),
		Timestamp:   1,
	}, acl)
	noErr(t, err)
	storage, err := objecttree.CreateStorage(ctx, root, hs, db)
	noErr(t, err)
	// per-space addSeq atomic (production wires it; we provide a fresh one)
	storage.(interface{ SetAddSeq(*atomic.Uint64) }).SetAddSeq(&atomic.Uint64{})
	treeId := storage.Id()
	tree, err := objecttree.BuildObjectTree(storage, acl)
	noErr(t, err)

	// ---- generate specs ----
	specs := make([]fileSpec, N)
	for i := range specs {
		nobj := 1 + r.Intn(2)
		objIds := make([]string, nobj)
		for j := range objIds {
			objIds[j] = token(uint64(i)*7+uint64(j)+1, 40)
		}
		specs[i] = fileSpec{
			id:     token(uint64(i)+1, 40),
			size:   1 + r.Intn(50_000_000),
			ncids:  cidsForFile(r),
			objIds: objIds,
		}
	}

	var arena anyenc.Arena
	var scratch, ptbuf, plainScratch []byte
	var changeCount, wireBytes, payloadBytes, plainBytes, compressedChanges int

	// add flushes the current batch (records) as one signed change.
	add := func(buildRecords func(recsArr *anyenc.Value)) {
		arena.Reset()
		rootObj := arena.NewObject()
		rootObj.Set(kDataset, arena.NewString("payloads"))
		recs := arena.NewArray()
		buildRecords(recs)
		rootObj.Set(kRecords, recs)
		// compression analysis: plain vs S2-compressed (what the SDK codec uses)
		plainScratch = rootObj.MarshalTo(plainScratch[:0])
		plain := len(plainScratch)
		plainBytes += plain
		var data []byte
		data, scratch = rootObj.MarshalCompressed(nil, scratch)
		if len(data) < plain {
			compressedChanges++
		}
		payloadBytes += len(data)
		tree.Lock()
		res, err := tree.AddContent(ctx, objecttree.SignableChangeContent{
			Data:              data,
			Key:               keys.SignKey,
			ShouldBeEncrypted: false,
			DataType:          "payloads",
			Timestamp:         int64(2 + changeCount),
		})
		tree.Unlock()
		noErr(t, err)
		for _, c := range res.Added {
			wireBytes += c.ChangeSize
			changeCount++
		}
	}

	// build one upload record (full row, encrypted blob field)
	uploadRec := func(recs *anyenc.Value, at, idx int, s fileSpec) {
		rec := arena.NewObject()
		rec.Set(kRecID, arena.NewString(s.id))
		ops := arena.NewArray()
		op := arena.NewObject()
		op.Set(kOpType, arena.NewString("set"))
		row := arena.NewObject()
		row.Set("s", arena.NewNumberInt(s.size))
		cidsArr := arena.NewArray()
		for c := 0; c < s.ncids; c++ {
			cidsArr.SetArrayItem(c, arena.NewString("bafy"+token(uint64(idx)*131+uint64(c)+1, 55)))
		}
		row.Set("c", cidsArr) // c[0] = root
		row.Set("b", arena.NewString("")) // backupStatus: pending
		// encrypted field
		ptbuf = buildEncPlaintext(ptbuf[:0], s)
		row.Set("e", arena.NewBinary(seal(g, ptbuf)))
		op.Set(kOpVal, row)
		ops.SetArrayItem(0, op)
		rec.Set(kRecOps, ops)
		recs.SetArrayItem(at, rec)
	}

	// ---- phase 1: uploads (batched 1..maxBatch) ----
	t0 := time.Now()
	for i := 0; i < N; {
		bs := 1 + r.Intn(maxBatch)
		if i+bs > N {
			bs = N - i
		}
		start := i
		add(func(recs *anyenc.Value) {
			for j := 0; j < bs; j++ {
				uploadRec(recs, j, start+j, specs[start+j])
			}
		})
		i += bs
		if start/5000 != (start+bs)/5000 {
			t.Logf("phase1 uploads: %d/%d files, %d changes", i, N, changeCount)
		}
	}
	uploadChanges := changeCount
	uploadWire := wireBytes
	uploadDur := time.Since(t0)

	// ---- phase 2: filenode backupStatus -> durable (batched) ----
	t0 = time.Now()
	for i := 0; i < N; {
		bs := 1 + r.Intn(maxBatch)
		if i+bs > N {
			bs = N - i
		}
		start := i
		add(func(recs *anyenc.Value) {
			for j := 0; j < bs; j++ {
				rec := arena.NewObject()
				rec.Set(kRecID, arena.NewString(specs[start+j].id))
				ops := arena.NewArray()
				op := arena.NewObject()
				op.Set(kOpType, arena.NewString("set"))
				p := arena.NewArray()
				p.SetArrayItem(0, arena.NewString("b"))
				op.Set(kOpPath, p)
				op.Set(kOpVal, arena.NewString("netStage1:"+token(uint64(start+j)*3+1, 88)))
				ops.SetArrayItem(0, op)
				rec.Set(kRecOps, ops)
				recs.SetArrayItem(j, rec)
			}
		})
		i += bs
	}
	statusChanges := changeCount - uploadChanges
	statusWire := wireBytes - uploadWire
	statusDur := time.Since(t0)

	// ---- phase 3: deletions (deletePct of files, batched) ----
	t0 = time.Now()
	delCount := N * deletePct / 100
	perm := r.Perm(N)[:delCount]
	for i := 0; i < delCount; {
		bs := 1 + r.Intn(maxBatch)
		if i+bs > delCount {
			bs = delCount - i
		}
		start := i
		add(func(recs *anyenc.Value) {
			for j := 0; j < bs; j++ {
				rec := arena.NewObject()
				rec.Set(kRecID, arena.NewString(specs[perm[start+j]].id))
				ops := arena.NewArray()
				op := arena.NewObject()
				op.Set(kOpType, arena.NewString("del"))
				ops.SetArrayItem(0, op)
				rec.Set(kRecOps, ops)
				recs.SetArrayItem(j, rec)
			}
		})
		i += bs
	}
	deleteChanges := changeCount - uploadChanges - statusChanges
	deleteWire := wireBytes - uploadWire - statusWire
	deleteDur := time.Since(t0)

	// flush to disk
	noErr(t, db.Close())
	onDisk := dirBytes(t, dir)

	// ---- cold sync: reopen, rebuild tree from storage (verify+order) ----
	runtime.GC()
	memBefore := mem()
	t0 = time.Now()
	db2, err := anystore.Open(ctx, dbPath, nil)
	noErr(t, err)
	hs2, err := headstorage.New(ctx, db2)
	noErr(t, err)
	storage2, err := objecttree.NewStorage(ctx, treeId, hs2, db2)
	noErr(t, err)
	tree2, err := objecttree.BuildObjectTree(storage2, acl)
	noErr(t, err)
	coldBuild := time.Since(t0)

	// ---- materialize: replay all changes, fold into the live collection ----
	// Payload bytes arrive as the `decrypted` arg of the convert callback (the
	// tree is cleartext, so decrypted == change.Data). We fold there directly.
	uploaded := make(map[string]struct{}, N)
	deleted := make(map[string]struct{})
	var parser anyenc.Parser
	t0 = time.Now()
	err = tree2.IterateRoot(
		func(ch *objecttree.Change, decrypted []byte) (any, error) {
			if len(decrypted) == 0 {
				return nil, nil
			}
			v, perr := parser.Parse(decrypted)
			if perr != nil {
				return nil, nil
			}
			for _, rec := range v.GetArray(kRecords) {
				id := string(rec.GetStringBytes(kRecID))
				if id == "" {
					continue
				}
				isDel := false
				for _, op := range rec.GetArray(kRecOps) {
					if string(op.GetStringBytes(kOpType)) == "del" {
						isDel = true
					}
				}
				if isDel {
					deleted[id] = struct{}{}
				} else {
					uploaded[id] = struct{}{}
				}
			}
			return nil, nil
		},
		func(ch *objecttree.Change) bool { return true },
	)
	noErr(t, err)
	materialize := time.Since(t0)
	memAfter := mem()

	live := 0
	for id := range uploaded {
		if _, d := deleted[id]; !d {
			live++
		}
	}

	// ---- report ----
	mb := func(b int64) float64 { return float64(b) / (1 << 20) }
	mbi := func(b int) float64 { return float64(b) / (1 << 20) }
	t.Logf("================ FILETREE SCALE (N=%d, delete=%d%%, batch 1..%d) ================", N, deletePct, maxBatch)
	t.Logf("changes total      : %d  (uploads %d, status %d, deletes %d)", changeCount, uploadChanges, statusChanges, deleteChanges)
	t.Logf("avg files/upload   : %.1f", float64(N)/float64(uploadChanges))
	t.Logf("payload PLAIN (MarshalTo)         : %.1f MiB  (%.0f B/change)", mbi(plainBytes), float64(plainBytes)/float64(changeCount))
	t.Logf("payload COMPRESSED (S2, codec out): %.1f MiB  (%.0f B/change)  ratio %.2fx  | %d/%d changes actually shrank",
		mbi(payloadBytes), float64(payloadBytes)/float64(changeCount), float64(plainBytes)/float64(payloadBytes), compressedChanges, changeCount)
	t.Logf("wire bytes (ChangeSize, signed)   : %.1f MiB  (%.0f B/change)  <- this is what syncs over the network", mbi(wireBytes), float64(wireBytes)/float64(changeCount))
	t.Logf("  wire by phase    : uploads %.1f MiB (%.0f B/ch) | status %.1f MiB (%.0f B/ch) | deletes %.1f MiB (%.0f B/ch)",
		mbi(uploadWire), float64(uploadWire)/float64(uploadChanges),
		mbi(statusWire), float64(statusWire)/float64(statusChanges),
		mbi(deleteWire), float64(deleteWire)/float64(deleteChanges))
	t.Logf("  per-file amortized: %.0f B/file wire (all phases)", float64(wireBytes)/float64(N))
	t.Logf("on-disk any-store  : %.1f MiB  (incl. snapshots, indexes, b-tree overhead)", mb(onDisk))
	t.Logf("write time         : uploads %s, status %s, deletes %s", uploadDur.Round(time.Millisecond), statusDur.Round(time.Millisecond), deleteDur.Round(time.Millisecond))
	t.Logf("COLD build tree    : %s   (verify sigs + order %d changes from storage)", coldBuild.Round(time.Millisecond), changeCount)
	t.Logf("COLD materialize   : %s   (parse + fold every change -> %d live rows)", materialize.Round(time.Millisecond), live)
	t.Logf("COLD total         : %s", (coldBuild + materialize).Round(time.Millisecond))
	t.Logf("materialize heap   : ~%.1f MiB for %d live rows", mb(int64(memAfter-memBefore)), live)
	_ = tree2
}

func buildEncPlaintext(dst []byte, s fileSpec) []byte {
	var a anyenc.Arena
	o := a.NewObject()
	key := make([]byte, 32)
	o.Set("k", a.NewBinary(key))
	o.Set("n", a.NewString("photo_"+token(uint64(s.size)+1, 12)+".png"))
	m := a.NewObject()
	m.Set("mime", a.NewString("image/png"))
	isz := a.NewArray()
	isz.SetArrayItem(0, a.NewNumberInt(100))
	isz.SetArrayItem(1, a.NewNumberInt(100))
	m.Set("isz", isz)
	o.Set("m", m)
	oid := a.NewArray()
	for i, id := range s.objIds {
		oid.SetArrayItem(i, a.NewString(id))
	}
	o.Set("o", oid)
	return o.MarshalTo(dst)
}

func noErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

var _ = fmt.Sprintf
