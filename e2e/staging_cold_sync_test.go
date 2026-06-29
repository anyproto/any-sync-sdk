//go:build stagingcoldsync

// Real cold sync against the staging network, for the "files as one
// payloads tree" question. Device A creates a space + ONE object and
// writes N file-rows into a custom `payloads` dataset (one object tree,
// many records, batched 1..20 records/change — the shape from
// internal/spikes/filetree). Device B then opens a FRESH DataDir with
// the SAME account and we time how long the full cold pull of that tree
// takes off the real network, plus the on-disk footprint it lands.
//
// This is the network-validated companion to the local objecttree spike:
// the spike measured tree size + local replay with no network and a
// cleartext tree; here the SDK encrypts every change and the bytes cross
// real staging nodes, so absolute numbers are larger — that's expected.
//
// Run:
//
//	cp ../testetc/staging.yml e2e/staging.yml   # already done if present
//	COLDSYNC_N=5    go test -tags stagingcoldsync ./e2e -run TestStagingColdSync -v -timeout 20m   # smoke
//	COLDSYNC_N=2000 go test -tags stagingcoldsync ./e2e -run TestStagingColdSync -v -timeout 30m
package e2e

import (
	"context"
	"fmt"
	"io/fs"
	mrand "math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/space"
)

const (
	payloadsTypeId  = "payloadsType"
	payloadsDataset = "payloads"
)

func coldEnvInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

const coldB32 = "abcdefghijklmnopqrstuvwxyz234567"

func coldToken(seed uint64, n int) string {
	buf := make([]byte, n)
	x := seed*2862933555777941757 + 3037000493
	for i := range buf {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		buf[i] = coldB32[x&31]
	}
	return string(buf)
}

func coldCidsCount(r *mrand.Rand) int {
	switch x := r.Float64(); {
	case x < 0.85:
		return 1
	case x < 0.99:
		return 2 + r.Intn(7)
	default:
		return 50 + r.Intn(251)
	}
}

func dirSizeRecursive(root string) int64 {
	var total int64
	_ = filepath.WalkDir(root, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if fi, e := d.Info(); e == nil {
			total += fi.Size()
		}
		return nil
	})
	return total
}

// dirBreakdown returns "relpath=KiB" for every file >= 64 KiB, biggest first.
func dirBreakdown(root string) string {
	type fe struct {
		name string
		size int64
	}
	var files []fe
	_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if fi, e := d.Info(); e == nil && fi.Size() >= 64*1024 {
			rel, _ := filepath.Rel(root, p)
			files = append(files, fe{rel, fi.Size()})
		}
		return nil
	})
	sort.Slice(files, func(i, j int) bool { return files[i].size > files[j].size })
	out := ""
	for i, f := range files {
		if i >= 14 {
			out += " …"
			break
		}
		out += "\n    " + f.name + " = " + strconv.FormatInt(f.size/1024, 10) + " KiB"
	}
	return out
}

// waitSynced polls the SDK's own SyncStatus for objId until the object's
// tree reports Synced (converged with a responsible node) AND at least
// wantRows are materialized locally. kick forces a head-sync each tick.
// Logs object/space state + row count live to stderr so a background run
// is observable and a stall is self-explaining (e.g. obj=synced but
// rows<want => the node simply doesn't have the rest).
func waitAllSynced(ctx context.Context, tag string, sp space.Space, objIds []string, dataset string, wantTotalRows int, deadline time.Duration, kick bool) (bool, int) {
	end := time.Now().Add(deadline)
	lastLog := time.Now().Add(-5 * time.Second)
	total := 0
	for ctx.Err() == nil && time.Now().Before(end) {
		if kick {
			_ = sp.SyncHeads(ctx)
		}
		allSynced := true
		total = 0
		for _, oid := range objIds {
			if sp.SyncStatus().Object(oid).State != space.SyncStateSynced {
				allSynced = false
			}
			if n, err := sp.Query(oid, dataset).Count(ctx); err == nil {
				total += n
			}
		}
		if time.Since(lastLog) >= 5*time.Second {
			ss := sp.SyncStatus().Space()
			fmt.Fprintf(os.Stderr, "[%s] allTreesSynced=%v space=%s synced=%d/%d rows=%d/%d trees=%d\n",
				tag, allSynced, ss.State, ss.Synced, ss.Total, total, wantTotalRows, len(objIds))
			lastLog = time.Now()
		}
		if allSynced && total >= wantTotalRows {
			return true, total
		}
		select {
		case <-time.After(1 * time.Second):
		case <-ctx.Done():
		}
	}
	return false, total
}

func TestStagingColdSync(t *testing.T) {
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("staging cold-sync is slow; rerun without -short")
	}

	N := coldEnvInt("COLDSYNC_N", 2000)
	timeoutMin := coldEnvInt("COLDSYNC_TIMEOUT_MIN", 30)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutMin)*time.Minute)
	defer cancel()

	// pprof: curl http://<addr>/debug/pprof/goroutine?debug=2 etc. while running.
	pprofAddr := os.Getenv("COLDSYNC_PPROF")
	if pprofAddr == "" {
		pprofAddr = "localhost:6060"
	}
	go func() { _ = http.ListenAndServe(pprofAddr, nil) }()
	fmt.Fprintf(os.Stderr, "[coldsync] pprof on http://%s/debug/pprof/\n", pprofAddr)

	// One identity, two devices.
	provider := newFixedSeedProvider(t)

	// Register the payloads type so its dataset is writable. Zero Schema
	// => dynamic free-form synced keyspace (our s/c/b/e fields).
	payloadsType := handler.Type{
		Id:   payloadsTypeId,
		Name: "Payloads",
		Datasets: []handler.Dataset{{
			Name:        payloadsDataset,
			DataVersion: "payloads-v1",
			Handler:     handler.DefaultHandler{},
		}},
	}

	// ---- Device A: write the payloads tree ----
	cfgA := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
		Types:   []handler.Type{payloadsType},
	}
	sdkA, err := anysyncsdk.Open(ctx, cfgA, provider)
	require.NoError(t, err, "device A: Open")
	closedA := false
	closeA := func() {
		if !closedA {
			closedA = true
			_ = sdkA.Close()
		}
	}
	t.Cleanup(closeA)

	sp, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "ColdSyncFiles"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("device A: Spaces().Create: %v", err)
	}
	spaceId := sp.Id()
	trees := coldEnvInt("COLDSYNC_TREES", 1)
	if trees < 1 {
		trees = 1
	}
	objIds := make([]string, trees)
	for k := range objIds {
		oid, cErr := sp.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{payloadsTypeId}})
		require.NoError(t, cErr, "device A: Objects().Create #%d", k)
		objIds[k] = oid
	}
	t.Logf("device A: spaceId=%s trees=%d, writing N=%d file rows...", spaceId, trees, N)

	r := mrand.New(mrand.NewSource(42))
	tWrite := time.Now()
	changes := 0
	pace := coldEnvInt("COLDSYNC_PACE", 5000) // SyncHeads every ~pace rows to drain the broadcast buffer
	nextPace := pace
	g := 0       // global row index (unique ids/seeds across trees)
	written := 0 // total rows written so far (for pacing)
	for k := 0; k < trees; k++ {
		cnt := N / trees
		if k == trees-1 {
			cnt = N - (N/trees)*(trees-1) // remainder lands on the last tree
		}
		for i := 0; i < cnt; {
			bs := 1 + r.Intn(20)
			if i+bs > cnt {
				bs = cnt - i
			}
			recs := make([]space.RecordModify, bs)
			for j := 0; j < bs; j++ {
				nc := coldCidsCount(r)
				cids := make([]any, nc)
				for c := 0; c < nc; c++ {
					cids[c] = "bafy" + coldToken(uint64(g)*131+uint64(c)+1, 55)
				}
				row := map[string]any{
					"s": r.Intn(50_000_000),
					"c": cids, // c[0] = root
					"b": "",   // backupStatus: pending
					"e": coldToken(uint64(g)*99+7, 280), // ~encrypted-blob stand-in
				}
				recs[j] = space.RecordModify{
					Id:     coldToken(uint64(g)+1, 40),
					Upsert: true,
					Ops:    []space.Op{{Type: space.OpSet, Path: "", Value: row}},
				}
				g++
			}
			res, mErr := sp.Modify(ctx, space.ModifyBatch{ObjectId: objIds[k], Dataset: payloadsDataset, Records: recs})
			require.NoError(t, mErr, "device A: Modify tree=%d i=%d (dataset-membership? see Risk 1)", k, i)
			require.Empty(t, res.Rejections, "device A: op rejections tree=%d i=%d", k, i)
			changes++
			i += bs
			written += bs
			if pace > 0 && written >= nextPace {
				_ = sp.SyncHeads(ctx) // drain: let reactive sync push before the mb buffer overflows
				nextPace += pace
			}
		}
	}
	writeDur := time.Since(tWrite)
	t.Logf("device A: wrote %d rows in %d changes across %d trees (%s)", N, changes, trees, writeDur.Round(time.Millisecond))

	// Local sanity: A can read back all N rows across all trees.
	localN := 0
	for _, oid := range objIds {
		n, qErr := sp.Query(oid, payloadsDataset).Count(ctx)
		require.NoError(t, qErr, "device A: Query payloads")
		localN += n
	}
	require.GreaterOrEqual(t, localN, N, "device A should see all N rows locally")

	// Push to the network before B pulls, and confirm via SyncStatus that
	// every tree CONVERGED with the node (= durably on a responsible node),
	// not just that we called SyncHeads.
	_ = sdkA.Spaces().SyncSpaceList(ctx)
	tPush := time.Now()
	aSynced, _ := waitAllSynced(ctx, "push-A", sp, objIds, payloadsDataset, N, 10*time.Minute, true)
	require.True(t, aSynced, "device A trees never all reached Synced with the node — push incomplete")
	t.Logf("device A: pushed all %d trees to node (Synced) in %s", trees, time.Since(tPush).Round(time.Millisecond))

	// ---- Device B: cold sync (TIMED) ----
	dataDirB := t.TempDir()
	cfgB := config.Config{
		Storage: config.Storage{DataDir: dataDirB, Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
		Types:   []handler.Type{payloadsType}, // must re-register to replay the dataset
	}
	tCold := time.Now()
	sdkB, err := anysyncsdk.Open(ctx, cfgB, provider)
	require.NoError(t, err, "device B: Open")
	closedB := false
	closeB := func() {
		if !closedB {
			closedB = true
			_ = sdkB.Close()
		}
	}
	t.Cleanup(closeB)
	require.Equal(t, sdkA.Account().Id(), sdkB.Account().Id(), "same account on both devices")
	openDur := time.Since(tCold)

	// Step 1: space appears in the tech-space list.
	require.True(t, waitFor(ctx, 3*time.Minute, 250*time.Millisecond, func() bool {
		_ = sdkB.Spaces().SyncSpaceList(ctx)
		list, lErr := sdkB.Spaces().List(ctx)
		return lErr == nil && containsString(spaceIdsOnly(list), spaceId)
	}), "device B never saw the space in tech-space")
	listDur := time.Since(tCold)

	// Step 2: space tree loads.
	var spB space.Space
	require.True(t, waitFor(ctx, 1*time.Minute, 100*time.Millisecond, func() bool {
		spB, err = sdkB.Spaces().Get(ctx, spaceId)
		return err == nil
	}), "device B: Spaces().Get never succeeded: %v", err)

	// Step 3: the payloads tree fully converges — gated on the SDK's own
	// SyncStatus (object reports Synced = converged with the node) AND all
	// N rows materialized locally. SyncStatus distinguishes "still pulling"
	// from "node doesn't have it"; the row count confirms materialization.
	converged, rows := waitAllSynced(ctx, "coldsync", spB, objIds, payloadsDataset, N, time.Duration(timeoutMin)*time.Minute, true)
	coldDur := time.Since(tCold)
	require.True(t, converged, "device B trees never all converged: got %d/%d rows in %s", rows, N, coldDur.Round(time.Millisecond))

	// Close (checkpoint WAL) before measuring so the footprint is the
	// true persisted size, not transient WAL. Measure A last so it stays
	// available to serve B's pull until now.
	closeB()
	dataB := dirSizeRecursive(dataDirB)
	bdB := dirBreakdown(dataDirB)
	closeA()
	dataA := dirSizeRecursive(cfgA.Storage.DataDir)

	treePull := coldDur - listDur
	t.Logf("================ STAGING COLD SYNC (N=%d, trees=%d, %d changes) ================", N, trees, changes)
	t.Logf("device A write        : %s (%d rows)", writeDur.Round(time.Millisecond), N)
	t.Logf("device B Open         : %s", openDur.Round(time.Millisecond))
	t.Logf("space-in-list (fixed) : %s   (tech-space propagation, ~headsync period, N-independent)", listDur.Round(time.Millisecond))
	t.Logf("tree pull (scales w/N): %s   (space visible -> all %d rows materialized)", treePull.Round(time.Millisecond), rows)
	t.Logf("FULL COLD SYNC        : %s", coldDur.Round(time.Millisecond))
	t.Logf("device B on-disk      : %.1f MiB (post-close)", float64(dataB)/(1<<20))
	t.Logf("device A on-disk      : %.1f MiB (post-close)", float64(dataA)/(1<<20))
	t.Logf("device B files >=64KiB:%s", bdB)
}
