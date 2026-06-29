//go:build filetreespike

// Two decision benches for the per-owner derived-payloads design, at the
// objecttree layer (no network, real signed changes, real any-store):
//
//   TestFileTreeShardSweep — hold total rows fixed, sweep the number of TREES
//     (1..10000). Answers "how bad is many small trees vs one fat tree?" =
//     the binding-granularity decision (one payloads child per page/chat vs
//     per message). Measures on-disk + cold-rebuild + per-tree overhead.
//
//   TestFileTreeChurn — one tree, fixed LIVE row count, sweep CHURN rounds
//     (re-set a field on every row C times). Answers "does a live owner's DAG
//     grow forever, and how much does churn cost cold-rebuild?" = the
//     inline-threshold / churning-chat decision.
//
// Run:
//   go test -tags filetreespike ./internal/spikes/filetree -run 'TestFileTreeShardSweep|TestFileTreeChurn' -v -timeout 30m
package filetree

import (
	"context"
	"crypto/cipher"
	"fmt"
	mrand "math/rand"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync/commonspace/headsync/headstorage"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/util/crypto"
)

type addSeqSetter interface{ SetAddSeq(*atomic.Uint64) }

// newPayloadTree creates one cleartext payloads tree in db (shared headStorage/acl).
func newPayloadTree(t *testing.T, ctx context.Context, db anystore.DB, hs headstorage.HeadStorage, acl list.AclList, key crypto.PrivKey, seed string) (objecttree.ObjectTree, string) {
	t.Helper()
	root, err := objecttree.CreateObjectTreeRoot(objecttree.ObjectTreeCreatePayload{
		PrivKey: key, ChangeType: "payloads", SpaceId: "spike.filetree",
		IsEncrypted: false, Seed: []byte(seed), Timestamp: 1,
	}, acl)
	noErr(t, err)
	storage, err := objecttree.CreateStorage(ctx, root, hs, db)
	noErr(t, err)
	storage.(addSeqSetter).SetAddSeq(&atomic.Uint64{})
	tree, err := objecttree.BuildObjectTree(storage, acl)
	noErr(t, err)
	return tree, storage.Id()
}

// makeRecord builds one payload row record (explicit id so churn can target it).
func makeRecord(a *anyenc.Arena, idx int, g cipher.AEAD, ptbuf *[]byte) *anyenc.Value {
	rec := a.NewObject()
	rec.Set(kRecID, a.NewString(token(uint64(idx)+1, 40)))
	ops := a.NewArray()
	op := a.NewObject()
	op.Set(kOpType, a.NewString("set"))
	row := a.NewObject()
	row.Set("s", a.NewNumberInt(idx))
	nc := 1 + (idx % 4)
	cids := a.NewArray()
	for c := 0; c < nc; c++ {
		cids.SetArrayItem(c, a.NewString("bafy"+token(uint64(idx)*131+uint64(c)+1, 55)))
	}
	row.Set("c", cids)
	row.Set("b", a.NewString(""))
	*ptbuf = buildEncPlaintextIdx((*ptbuf)[:0], idx)
	row.Set("e", a.NewBinary(seal(g, *ptbuf)))
	op.Set(kOpVal, row)
	ops.SetArrayItem(0, op)
	rec.Set(kRecOps, ops)
	return rec
}

func buildEncPlaintextIdx(dst []byte, idx int) []byte {
	var a anyenc.Arena
	o := a.NewObject()
	o.Set("k", a.NewBinary(make([]byte, 32)))
	o.Set("n", a.NewString("file_"+token(uint64(idx)+5, 12)+".png"))
	o.Set("o", a.NewString(token(uint64(idx)*7+3, 40)))
	return o.MarshalTo(dst)
}

// fillTree writes count rows (ids idxBase..idxBase+count-1) batched 1..20.
func fillTree(t *testing.T, ctx context.Context, tree objecttree.ObjectTree, key crypto.PrivKey, g cipher.AEAD, r *mrand.Rand, count, idxBase, ts0 int) (changes, wireBytes, ts int) {
	var arena anyenc.Arena
	var scratch, ptbuf []byte
	ts = ts0
	for i := 0; i < count; {
		bs := 1 + r.Intn(20)
		if i+bs > count {
			bs = count - i
		}
		arena.Reset()
		rootObj := arena.NewObject()
		rootObj.Set(kDataset, arena.NewString("payloads"))
		recs := arena.NewArray()
		for j := 0; j < bs; j++ {
			recs.SetArrayItem(j, makeRecord(&arena, idxBase+i+j, g, &ptbuf))
		}
		rootObj.Set(kRecords, recs)
		var data []byte
		data, scratch = rootObj.MarshalCompressed(nil, scratch)
		tree.Lock()
		res, err := tree.AddContent(ctx, objecttree.SignableChangeContent{
			Data: data, Key: key, ShouldBeEncrypted: false, DataType: "payloads", Timestamp: int64(ts),
		})
		tree.Unlock()
		noErr(t, err)
		ts++
		for _, c := range res.Added {
			wireBytes += c.ChangeSize
			changes++
		}
		i += bs
	}
	return
}

// coldRebuild reopens the db and rebuilds every tree from storage; returns total wall time.
func coldRebuild(t *testing.T, ctx context.Context, dbPath string, acl list.AclList, treeIds []string) time.Duration {
	t.Helper()
	db, err := anystore.Open(ctx, dbPath, nil)
	noErr(t, err)
	defer func() { _ = db.Close() }()
	hs, err := headstorage.New(ctx, db)
	noErr(t, err)
	t0 := time.Now()
	for _, id := range treeIds {
		st, err := objecttree.NewStorage(ctx, id, hs, db)
		noErr(t, err)
		_, err = objecttree.BuildObjectTree(st, acl)
		noErr(t, err)
	}
	return time.Since(t0)
}

func TestFileTreeShardSweep(t *testing.T) {
	ctx := context.Background()
	N := envInt("FILETREE_N", 20000)
	Ks := []int{1, 10, 100, 1000, 10000}
	keys, err := accountdata.NewRandom()
	noErr(t, err)
	g := newSealer()

	t.Logf("===== SHARD SWEEP: N=%d total rows, vary tree count K =====", N)
	t.Logf("%-8s %-10s %-10s %-12s %-14s %-12s %-10s", "K", "rows/tree", "changes", "wire MiB", "on-disk MiB", "cold ms", "ms/tree")
	for _, K := range Ks {
		if K > N {
			continue
		}
		dir := t.TempDir()
		dbPath := filepath.Join(dir, "t.db")
		db, err := anystore.Open(ctx, dbPath, nil)
		noErr(t, err)
		acl, err := list.NewInMemoryDerivedAcl("spike.filetree", keys)
		noErr(t, err)
		hs, err := headstorage.New(ctx, db)
		noErr(t, err)

		treeIds := make([]string, K)
		trees := make([]objecttree.ObjectTree, K)
		for k := 0; k < K; k++ {
			trees[k], treeIds[k] = newPayloadTree(t, ctx, db, hs, acl, keys.SignKey, fmt.Sprintf("payloads-%d", k))
		}
		r := mrand.New(mrand.NewSource(42))
		per := N / K
		ts := 2
		changes, wire, idxBase := 0, 0, 0
		for k := 0; k < K; k++ {
			cnt := per
			if k == K-1 {
				cnt = N - per*(K-1)
			}
			c, w, nts := fillTree(t, ctx, trees[k], keys.SignKey, g, r, cnt, idxBase, ts)
			changes += c
			wire += w
			ts = nts
			idxBase += cnt
		}
		noErr(t, db.Close())
		onDisk := dirBytes(t, dir)
		cold := coldRebuild(t, ctx, dbPath, acl, treeIds)

		t.Logf("%-8d %-10d %-10d %-12.1f %-14.1f %-12d %-10.3f",
			K, per, changes, float64(wire)/(1<<20), float64(onDisk)/(1<<20),
			cold.Milliseconds(), float64(cold.Microseconds())/1000.0/float64(K))
	}
}

func TestFileTreeChurn(t *testing.T) {
	ctx := context.Background()
	N := envInt("FILETREE_N", 2000) // live rows
	Cs := []int{0, 5, 20}           // churn rounds: re-set "b" on every row C times
	keys, err := accountdata.NewRandom()
	noErr(t, err)
	g := newSealer()

	t.Logf("===== CHURN: N=%d live rows in ONE tree, vary churn rounds C =====", N)
	t.Logf("%-6s %-10s %-12s %-14s %-12s", "churn", "changes", "wire MiB", "on-disk MiB", "cold ms")
	for _, C := range Cs {
		dir := t.TempDir()
		dbPath := filepath.Join(dir, "t.db")
		db, err := anystore.Open(ctx, dbPath, nil)
		noErr(t, err)
		acl, err := list.NewInMemoryDerivedAcl("spike.filetree", keys)
		noErr(t, err)
		hs, err := headstorage.New(ctx, db)
		noErr(t, err)
		tree, id := newPayloadTree(t, ctx, db, hs, acl, keys.SignKey, "payloads-churn")
		r := mrand.New(mrand.NewSource(7))

		changes, wire, ts := fillTree(t, ctx, tree, keys.SignKey, g, r, N, 0, 2)
		// churn: C rounds, each re-sets "b" on all N rows (LWW update, grows DAG, live count unchanged)
		var arena anyenc.Arena
		var scratch []byte
		for round := 0; round < C; round++ {
			for i := 0; i < N; {
				bs := 1 + r.Intn(20)
				if i+bs > N {
					bs = N - i
				}
				arena.Reset()
				ro := arena.NewObject()
				ro.Set(kDataset, arena.NewString("payloads"))
				recs := arena.NewArray()
				for j := 0; j < bs; j++ {
					rec := arena.NewObject()
					rec.Set(kRecID, arena.NewString(token(uint64(i+j)+1, 40)))
					ops := arena.NewArray()
					op := arena.NewObject()
					op.Set(kOpType, arena.NewString("set"))
					p := arena.NewArray()
					p.SetArrayItem(0, arena.NewString("b"))
					op.Set(kOpPath, p)
					op.Set(kOpVal, arena.NewString("durable:"+token(uint64(round*N+i+j), 24)))
					ops.SetArrayItem(0, op)
					rec.Set(kRecOps, ops)
					recs.SetArrayItem(j, rec)
				}
				ro.Set(kRecords, recs)
				var data []byte
				data, scratch = ro.MarshalCompressed(nil, scratch)
				tree.Lock()
				res, err := tree.AddContent(ctx, objecttree.SignableChangeContent{
					Data: data, Key: keys.SignKey, ShouldBeEncrypted: false, DataType: "payloads", Timestamp: int64(ts),
				})
				tree.Unlock()
				noErr(t, err)
				ts++
				for _, c := range res.Added {
					wire += c.ChangeSize
					changes++
				}
				i += bs
			}
		}
		noErr(t, db.Close())
		onDisk := dirBytes(t, dir)
		cold := coldRebuild(t, ctx, dbPath, acl, []string{id})
		t.Logf("%-6d %-10d %-12.1f %-14.1f %-12d", C, changes, float64(wire)/(1<<20), float64(onDisk)/(1<<20), cold.Milliseconds())
	}
}
