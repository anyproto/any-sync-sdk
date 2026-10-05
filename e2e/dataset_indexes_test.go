package e2e

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// indexLabPart is sharedLabPart with fields to index on top: `rank`,
// `tag` and a create-time stamp `at` on samples, `score` on plain, and
// on both an array and an object field no index may name. samplesIdx
// and plainIdx are the datasets' declared indexes.
func indexLabPart(samplesIdx, plainIdx []space.IndexDraft) space.PartDraft {
	mutable := func(key string, kind space.PropertyKind) space.DatasetFieldDraft {
		return space.DatasetFieldDraft{Key: key, Kind: kind, MutableBy: space.MutableByAnyone}
	}
	p := sharedLabPart()
	p.Key, p.Name = "ix", "Indexed"
	for i := range p.Datasets {
		ds := &p.Datasets[i]
		switch ds.Key {
		case "samples":
			ds.Fields = append(ds.Fields,
				mutable("rank", space.PropertyKindNumber),
				mutable("tag", space.PropertyKindString),
				space.DatasetFieldDraft{Key: "at", Stamp: space.StampCreateTime},
				mutable("tags", space.PropertyKindArray),
				mutable("meta", space.PropertyKindObject),
			)
			ds.Indexes = samplesIdx
		case "plain":
			ds.Fields = append(ds.Fields,
				mutable("score", space.PropertyKindNumber),
				mutable("tags", space.PropertyKindArray),
			)
			ds.Indexes = plainIdx
		}
	}
	return p
}

// declareIndexLab declares a type with indexLabPart in base's space and
// returns the lab over it.
func declareIndexLab(t *testing.T, ctx context.Context, base *sharedLab, samplesIdx, plainIdx []space.IndexDraft) *sharedLab {
	t.Helper()
	typeId, err := base.sp.Types().Create(ctx, space.TypeCreateParams{Name: "Indexed"})
	require.NoError(t, err)
	partId, err := base.sp.Types().AddPart(ctx, typeId, indexLabPart(samplesIdx, plainIdx))
	require.NoError(t, err)
	return &sharedLab{sdk: base.sdk, sp: base.sp, typeId: typeId, partId: partId,
		samples: typeId + "_samples", events: typeId + "_events", plain: typeId + "_plain"}
}

// openIndexPair opens two devices of one account: A creates a space and
// declares the indexed type with the given indexes; B is returned once
// it sees the space. Skips when no network is reachable.
func openIndexPair(t *testing.T, ctx context.Context, name string, samplesIdx, plainIdx []space.IndexDraft) (labA, labB *sharedLab) {
	t.Helper()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("cold-sync e2e is slow; rerun without -short")
	}
	provider := newFixedSeedProvider(t)
	open := func() *anysyncsdk.SDK {
		sdk, oerr := anysyncsdk.Open(ctx, config.Config{
			Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: yaml},
		}, provider)
		require.NoError(t, oerr)
		t.Cleanup(func() { _ = sdk.Close() })
		return sdk
	}

	sdkA := open()
	spA, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: name})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatal(err)
	}
	labA = declareIndexLab(t, ctx, &sharedLab{sdk: sdkA, sp: spA}, samplesIdx, plainIdx)
	_ = sdkA.Spaces().SyncSpaceList(ctx)
	_ = spA.SyncHeads(ctx)

	sdkB := open()
	var spB space.Space
	require.True(t, waitFor(ctx, 90*time.Second, 250*time.Millisecond, func() bool {
		_ = sdkB.Spaces().SyncSpaceList(ctx)
		got, gerr := sdkB.Spaces().Get(ctx, spA.Id())
		if gerr != nil {
			return false
		}
		spB = got
		return true
	}), "device B must see the space")
	labB = &sharedLab{sdk: sdkB, sp: spB, typeId: labA.typeId, partId: labA.partId,
		samples: labA.samples, events: labA.events, plain: labA.plain}
	return labA, labB
}

// create is a record create with a multi-field $set.
func create(id string, fields map[string]any) space.RecordModify {
	return space.RecordModify{Id: id, Upsert: true, Ops: []space.Op{{Type: space.OpSet, Value: fields}}}
}

// modify writes recs to objectId's dataset in one change. Every record
// must land.
func (l *sharedLab) modify(t *testing.T, ctx context.Context, objectId, dataset string, recs ...space.RecordModify) {
	t.Helper()
	res, err := l.sp.Modify(ctx, space.ModifyBatch{ObjectId: objectId, Dataset: dataset, Records: recs})
	require.NoError(t, err, "write %d records on %s", len(recs), objectId)
	require.Empty(t, res.Rejections, "write %d records on %s", len(recs), objectId)
}

// putSamples writes rows to their objects' samples, one change per
// object.
func (l *sharedLab) putSamples(t *testing.T, ctx context.Context, rows ...labRow) {
	t.Helper()
	byObj := map[string][]space.RecordModify{}
	var order []string
	for _, r := range rows {
		if _, ok := byObj[r.obj]; !ok {
			order = append(order, r.obj)
		}
		byObj[r.obj] = append(byObj[r.obj], create(r.id, map[string]any{"label": r.label, "score": r.score, "kind": r.kind}))
	}
	for _, obj := range order {
		l.modify(t, ctx, obj, l.samples, byObj[obj]...)
	}
}

// samplesWhere runs a filtered QueryDataset over samples sorted by
// score then id.
func (l *sharedLab) samplesWhere(t *testing.T, ctx context.Context, filter map[string]any) []string {
	t.Helper()
	rows, err := l.sp.QueryDataset(l.samples).Filter(filter).Sort("score", "id").All(ctx)
	require.NoError(t, err)
	return idsOfInitial(rows)
}

// withScore returns rows with the row under key rescored; its label
// keeps the value it was written with.
func withScore(rows []labRow, key string, score int) []labRow {
	out := append([]labRow(nil), rows...)
	for i := range out {
		if out[i].key() == key {
			out[i].score = score
		}
	}
	return out
}

// buildFeed records a space's index-build reports in arrival order.
type buildFeed struct {
	mu  sync.Mutex
	evs []space.IndexBuild
}

// watchBuilds subscribes a feed to sp's index builds until the test
// ends.
func watchBuilds(t *testing.T, sp space.Space) *buildFeed {
	t.Helper()
	f := &buildFeed{}
	cancel := sp.Types().SubscribeIndexBuilds(func(b space.IndexBuild) {
		f.mu.Lock()
		f.evs = append(f.evs, b)
		f.mu.Unlock()
	})
	t.Cleanup(cancel)
	return f
}

// mark is the position the next report lands at.
func (f *buildFeed) mark() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.evs)
}

// since returns the reports from position from on.
func (f *buildFeed) since(from int) []space.IndexBuild {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]space.IndexBuild(nil), f.evs[from:]...)
}

// awaitBuilt waits for a build of dataset to end after from and checks
// that the reports since from are exactly its Started then Done.
func (f *buildFeed) awaitBuilt(t *testing.T, ctx context.Context, from int, dataset string) {
	t.Helper()
	ended := func() bool {
		for _, b := range f.since(from) {
			if b.Dataset == dataset && b.Phase != space.IndexBuildStarted {
				return true
			}
		}
		return false
	}
	require.True(t, waitFor(ctx, 60*time.Second, 25*time.Millisecond, ended),
		"no build of %s ended; reports %+v", dataset, f.since(from))
	assert.Equal(t, []space.IndexBuild{
		{Dataset: dataset, Phase: space.IndexBuildStarted},
		{Dataset: dataset, Phase: space.IndexBuildDone},
	}, f.since(from), "one build of %s, nothing else", dataset)
}

// assertQuiet checks that no report arrives after from within window.
func (f *buildFeed) assertQuiet(t *testing.T, ctx context.Context, from int, window time.Duration) {
	t.Helper()
	reported := waitFor(ctx, window, 25*time.Millisecond, func() bool { return len(f.since(from)) > 0 })
	assert.False(t, reported, "unexpected index builds %+v", f.since(from))
}

// storeName is the any-store name of a declared index: `dx_` and its
// fields, `~sparse` when sparse.
func storeName(d space.IndexDraft) string {
	name := "dx_" + strings.Join(d.Fields, ",")
	if d.Sparse {
		name += "~sparse"
	}
	return name
}

// planIndex is the index an access plan reads: the `Index:` of its
// header `Plan: <kind>  Index: <name>  Cost: <n>`; empty for a plan
// reading none.
func planIndex(plan string) string {
	header, _, _ := strings.Cut(plan, "\n")
	_, rest, ok := strings.Cut(header, "  Index: ")
	if !ok {
		return ""
	}
	name, _, _ := strings.Cut(rest, "  Cost: ")
	return name
}

// planNames reports whether a plan reads the index or weighs it as a
// candidate.
func planNames(plan, index string) bool {
	return planIndex(plan) == index || strings.Contains(plan, "("+index+")")
}

// datasetPlan is the access plan of pipeline over every object's
// records of a shared dataset.
func datasetPlan(t *testing.T, ctx context.Context, sp space.Space, dataset, pipeline string) string {
	t.Helper()
	plan, err := sp.AggregateDataset(dataset, pipeline).Explain(ctx)
	require.NoError(t, err)
	return plan
}

// objectPlan is the access plan of pipeline over one object's records.
func objectPlan(t *testing.T, ctx context.Context, sp space.Space, objectId, dataset, pipeline string) string {
	t.Helper()
	plan, err := sp.Aggregate(objectId, dataset, pipeline).Explain(ctx)
	require.NoError(t, err)
	return plan
}

// storedIndexes lists the declared indexes any-store holds on the named
// collection, with their entry counts; nil when the collection does
// not exist.
func storedIndexes(ctx context.Context, sdk *anysyncsdk.SDK, collection string) map[string]int {
	coll, err := sdk.Store().OpenCollection(ctx, collection)
	if err != nil {
		return nil
	}
	out := map[string]int{}
	for _, idx := range coll.GetIndexes() {
		name := idx.Info().Name
		if !strings.HasPrefix(name, "dx_") {
			continue
		}
		n, lerr := idx.Len(ctx)
		if lerr != nil {
			n = -1
		}
		out[name] = n
	}
	return out
}

// sharedIndexes lists the declared indexes of a shared dataset's
// per-space collection.
func (l *sharedLab) sharedIndexes(ctx context.Context, dataset string) map[string]int {
	return storedIndexes(ctx, l.sdk, l.sp.Id()+"_"+dataset)
}

// datasetDefs reads the type's dataset definitions, by key.
func datasetDefs(t *testing.T, ctx context.Context, sp space.Space, typeId string) map[string]space.DatasetDef {
	t.Helper()
	defs, err := sp.Types().Datasets(ctx, typeId)
	require.NoError(t, err)
	out := make(map[string]space.DatasetDef, len(defs))
	for _, d := range defs {
		out[d.Key] = d
	}
	require.Len(t, out, len(defs))
	return out
}

// indexListing reads the declared indexes of every dataset of the type,
// by dataset key.
func indexListing(ctx context.Context, sp space.Space, typeId string) (map[string][]space.IndexDef, error) {
	defs, err := sp.Types().Datasets(ctx, typeId)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]space.IndexDef, len(defs))
	for _, d := range defs {
		out[d.Key] = d.Indexes
	}
	return out, nil
}

// mustListing is indexListing failing the test on error.
func mustListing(t *testing.T, ctx context.Context, sp space.Space, typeId string) map[string][]space.IndexDef {
	t.Helper()
	out, err := indexListing(ctx, sp, typeId)
	require.NoError(t, err)
	return out
}

// indexDrafts reduces definitions to their declared shape, checking each
// is valid and carries an id of its own.
func indexDrafts(t *testing.T, defs []space.IndexDef) []space.IndexDraft {
	t.Helper()
	out := make([]space.IndexDraft, 0, len(defs))
	ids := map[string]bool{}
	for _, d := range defs {
		assert.NotEmpty(t, d.Id, "index %s has an id", d.Key)
		assert.False(t, ids[d.Id], "index %s has an id of its own", d.Key)
		ids[d.Id] = true
		assert.False(t, d.Invalid, "index %s: %s", d.Key, d.InvalidReason)
		assert.Empty(t, d.InvalidReason, "index %s", d.Key)
		out = append(out, space.IndexDraft{Key: d.Key, Fields: d.Fields, Sparse: d.Sparse})
	}
	return out
}

// indexDefByKey finds the dataset's index definition with the key.
func indexDefByKey(t *testing.T, defs []space.IndexDef, key string) space.IndexDef {
	t.Helper()
	for _, d := range defs {
		if d.Key == key {
			return d
		}
	}
	t.Fatalf("index %q not listed (%d indexes)", key, len(defs))
	return space.IndexDef{}
}

// discoveredIndexes is the discovery listing of a dataset's indexes.
func discoveredIndexes(t *testing.T, sp space.Space, dataset string) []space.IndexDraft {
	t.Helper()
	return findDataset(t, sp.Datasets(), dataset).Indexes
}

// spreadSamples is n samples rows over objs: record `r<i>` on
// objs[i%len(objs)], kind x/y/z by i%3, score i.
func spreadSamples(objs []string, n int) []labRow {
	kinds := []string{"x", "y", "z"}
	rows := make([]labRow, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, sample(objs[i%len(objs)], fmt.Sprintf("r%03d", i), kinds[i%3], i))
	}
	return rows
}

// TestE2E_DatasetIndexes_Declared: indexes declared with a shared and a
// per-object dataset list through the type surface, the parts and
// discovery; the shared collection is built once, in the background,
// and reported; each per-object collection carries its indexes; an
// indexed filter or sort returns the rows it would without an index and
// is planned on the declared index.
func TestE2E_DatasetIndexes_Declared(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	base := openSharedLab(t, ctx, "IndexDecl")
	builds := watchBuilds(t, base.sp)
	from := builds.mark()

	byKind := space.IndexDraft{Key: "by_kind", Fields: []string{"kind"}}
	byScoreObject := space.IndexDraft{Key: "by_score_object", Fields: []string{"score", space.IndexPathObject}}
	byObjectCreated := space.IndexDraft{Key: "by_object_created", Fields: []string{space.IndexPathObject, space.IndexPathCreated}}
	byAt := space.IndexDraft{Key: "by_at", Fields: []string{"-at"}}
	samplesIdx := []space.IndexDraft{byKind, byScoreObject, byObjectCreated, byAt}
	byScore := space.IndexDraft{Key: "by_score", Fields: []string{"score"}}
	byLabelCreated := space.IndexDraft{Key: "by_label_created", Fields: []string{"label", space.IndexPathCreated}}
	plainIdx := []space.IndexDraft{byScore, byLabelCreated}

	lab := declareIndexLab(t, ctx, base, samplesIdx, plainIdx)
	sp := lab.sp
	builds.awaitBuilt(t, ctx, from, lab.samples)

	// The type surface lists every index valid, with an id; the dataset
	// without indexes lists none.
	defs := datasetDefs(t, ctx, sp, lab.typeId)
	assert.ElementsMatch(t, samplesIdx, indexDrafts(t, defs["samples"].Indexes))
	assert.ElementsMatch(t, plainIdx, indexDrafts(t, defs["plain"].Indexes))
	assert.Empty(t, defs["events"].Indexes)
	parts, err := sp.Types().Parts(ctx, lab.typeId)
	require.NoError(t, err)
	require.Len(t, parts, 1)
	for _, d := range parts[0].Datasets {
		assert.Equal(t, defs[d.Key].Indexes, d.Indexes, "Parts lists the indexes of %s", d.Key)
	}

	// Discovery lists the same shapes.
	assert.ElementsMatch(t, samplesIdx, discoveredIndexes(t, sp, lab.samples))
	assert.ElementsMatch(t, plainIdx, discoveredIndexes(t, sp, lab.plain))
	assert.Empty(t, discoveredIndexes(t, sp, lab.events))

	// The shared collection holds the four indexes, each built.
	stored := lab.sharedIndexes(ctx, lab.samples)
	assert.Len(t, stored, len(samplesIdx), "stored %v", stored)
	for _, d := range samplesIdx {
		assert.Contains(t, stored, storeName(d))
	}

	a, b, c := lab.newObject(t, ctx), lab.newObject(t, ctx), lab.newObject(t, ctx)
	objs := []string{a, b, c}
	seed := spreadSamples(objs, 60)
	lab.putSamples(t, ctx, seed...)
	for _, obj := range []string{a, b} {
		recs := make([]space.RecordModify, 0, 20)
		for i := 0; i < 20; i++ {
			recs = append(recs, create(fmt.Sprintf("p%02d", i), map[string]any{"label": fmt.Sprintf("p%02d", i), "score": i}))
		}
		lab.modify(t, ctx, obj, lab.plain, recs...)
	}

	// Every write keeps every index whole.
	stored = lab.sharedIndexes(ctx, lab.samples)
	for _, d := range samplesIdx {
		assert.Equal(t, len(seed), stored[storeName(d)], "%s holds every row", storeName(d))
	}

	t.Run("SharedFilterOnKind", func(t *testing.T) {
		want := rowsWhere(seed, func(r labRow) bool { return r.kind == "y" })
		got := lab.samplesWhere(t, ctx, map[string]any{"kind": "y"})
		assert.Equal(t, sortedKeys(want, false), got)
		rows, err := sp.QueryDataset(lab.samples).Filter(map[string]any{"kind": "y"}).All(ctx)
		require.NoError(t, err)
		assertRows(t, mirrorOf(rows), want)
		plan := datasetPlan(t, ctx, sp, lab.samples, `[{"$match": {"kind": "y"}}]`)
		assert.Equal(t, storeName(byKind), planIndex(plan), plan)
	})

	t.Run("SharedRangeOnScore", func(t *testing.T) {
		in := func(r labRow) bool { return r.score >= 10 && r.score < 20 }
		filter := map[string]any{"score": map[string]any{"$gte": 10, "$lt": 20}}
		assert.Equal(t, sortedKeys(rowsWhere(seed, in), false), lab.samplesWhere(t, ctx, filter))
		plan := datasetPlan(t, ctx, sp, lab.samples, `[{"$match": {"score": {"$gte": 10, "$lt": 20}}}]`)
		assert.Equal(t, storeName(byScoreObject), planIndex(plan), plan)
	})

	t.Run("SharedObjectRows", func(t *testing.T) {
		want := rowsWhere(seed, func(r labRow) bool { return r.obj == b })
		rows, err := sp.QueryDataset(lab.samples).Filter(map[string]any{"_objectId": b}).All(ctx)
		require.NoError(t, err)
		assertRows(t, mirrorOf(rows), want)
		plan := datasetPlan(t, ctx, sp, lab.samples, fmt.Sprintf(`[{"$match": {"_objectId": %q}}]`, b))
		assert.Equal(t, storeName(byObjectCreated), planIndex(plan), plan)
	})

	t.Run("SharedSortOnStamp", func(t *testing.T) {
		rows, err := sp.QueryDataset(lab.samples).Sort("-at").Limit(5).All(ctx)
		require.NoError(t, err)
		assert.Len(t, rows, 5)
		plan := datasetPlan(t, ctx, sp, lab.samples, `[{"$sort": {"at": -1}}, {"$limit": 5}]`)
		assert.Equal(t, storeName(byAt), planIndex(plan), plan)
	})

	t.Run("PerObject", func(t *testing.T) {
		for _, obj := range []string{a, b} {
			rows, err := sp.Query(obj, lab.plain).Filter(map[string]any{"score": map[string]any{"$gte": 15}}).Sort("score").All(ctx)
			require.NoError(t, err)
			assert.Equal(t, []string{"p15", "p16", "p17", "p18", "p19"}, idsOfInitial(rows), "object %s", obj)
			plan := objectPlan(t, ctx, sp, obj, lab.plain, `[{"$match": {"score": {"$gte": 15}}}]`)
			assert.Equal(t, storeName(byScore), planIndex(plan), plan)

			row, err := sp.Query(obj, lab.plain).Filter(map[string]any{"label": "p03"}).One(ctx)
			require.NoError(t, err)
			assert.Equal(t, "p03", row.GetString("id"))
			plan = objectPlan(t, ctx, sp, obj, lab.plain, `[{"$match": {"label": "p03"}}]`)
			assert.Equal(t, storeName(byLabelCreated), planIndex(plan), plan)

			stored := storedIndexes(ctx, lab.sdk, obj+"_"+lab.plain)
			assert.Equal(t, map[string]int{storeName(byScore): 20, storeName(byLabelCreated): 20}, stored,
				"the per-object collection of %s carries its indexes", obj)
		}
		// An object that wrote nothing reads nothing.
		n, err := sp.Query(c, lab.plain).Filter(map[string]any{"score": map[string]any{"$gte": 0}}).Count(ctx)
		require.NoError(t, err)
		assert.Zero(t, n)
	})

	// Writes and the per-object collections built nothing more.
	assert.Len(t, builds.since(from), 2, "reports %+v", builds.since(from))
}

// TestE2E_DatasetIndexes_AddedToPopulatedShared: an index added to a
// shared dataset that already holds rows across objects is built once
// in the background and reported for that dataset only; range reads
// return the same rows before and after and are planned on it only
// after; creates, updates crossing the range and deletes keep it
// whole; a compound index with `_objectId` serves a filter on both.
func TestE2E_DatasetIndexes_AddedToPopulatedShared(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	lab := declareIndexLab(t, ctx, openSharedLab(t, ctx, "IndexAddShared"), nil, nil)
	sp := lab.sp

	objs := []string{lab.newObject(t, ctx), lab.newObject(t, ctx), lab.newObject(t, ctx), lab.newObject(t, ctx)}
	live := spreadSamples(objs, 300)
	lab.putSamples(t, ctx, live...)

	byScore := space.IndexDraft{Key: "by_score", Fields: []string{"score"}}
	in := func(r labRow) bool { return r.score >= 100 && r.score < 140 }
	rangeFilter := map[string]any{"score": map[string]any{"$gte": 100, "$lt": 140}}
	const rangePipeline = `[{"$match": {"score": {"$gte": 100, "$lt": 140}}}, {"$sort": {"score": 1}}]`

	before := lab.samplesWhere(t, ctx, rangeFilter)
	require.Equal(t, sortedKeys(rowsWhere(live, in), false), before)
	plan := datasetPlan(t, ctx, sp, lab.samples, rangePipeline)
	assert.False(t, planNames(plan, storeName(byScore)), plan)
	assert.Empty(t, lab.sharedIndexes(ctx, lab.samples))

	builds := watchBuilds(t, sp)
	from := builds.mark()
	samplesDef := datasetDefs(t, ctx, sp, lab.typeId)["samples"]
	scoreId, err := sp.Types().AddDatasetIndex(ctx, lab.typeId, samplesDef.Id, byScore)
	require.NoError(t, err)
	require.NotEmpty(t, scoreId)
	builds.awaitBuilt(t, ctx, from, lab.samples)

	listed := datasetDefs(t, ctx, sp, lab.typeId)["samples"].Indexes
	assert.Equal(t, []space.IndexDraft{byScore}, indexDrafts(t, listed))
	assert.Equal(t, scoreId, indexDefByKey(t, listed, "by_score").Id)
	assert.Equal(t, []space.IndexDraft{byScore}, discoveredIndexes(t, sp, lab.samples))
	assert.Equal(t, map[string]int{storeName(byScore): len(live)}, lab.sharedIndexes(ctx, lab.samples),
		"the build indexed every stored row")

	after := lab.samplesWhere(t, ctx, rangeFilter)
	assert.Equal(t, before, after, "the index changes no result")
	rows, err := sp.QueryDataset(lab.samples).Filter(rangeFilter).All(ctx)
	require.NoError(t, err)
	assertRows(t, mirrorOf(rows), rowsWhere(live, in))
	plan = datasetPlan(t, ctx, sp, lab.samples, rangePipeline)
	assert.Equal(t, storeName(byScore), planIndex(plan), plan)

	t.Run("Maintenance", func(t *testing.T) {
		// Creates inside and outside the range.
		fresh := []labRow{sample(objs[0], "n1", "x", 105), sample(objs[1], "n2", "y", 500), sample(objs[2], "n3", "z", 139)}
		lab.putSamples(t, ctx, fresh...)
		live = append(live, fresh...)
		// Updates moving a row into, out of and within the range.
		moves := []struct {
			row   labRow
			score int
		}{
			{live[10], 130},   // in
			{live[120], 1000}, // out
			{live[125], 101},  // within
		}
		for _, m := range moves {
			lab.set(t, ctx, m.row.obj, lab.samples, m.row.id, "score", m.score)
			live = withScore(live, m.row.key(), m.score)
		}
		// Deletes inside and outside the range.
		for _, r := range []labRow{live[110], live[200]} {
			lab.deleteSample(t, ctx, r.obj, r.id)
			live = withoutKeys(live, r.key())
		}

		got := lab.samplesWhere(t, ctx, rangeFilter)
		assert.Equal(t, sortedKeys(rowsWhere(live, in), false), got)
		rows, err := sp.QueryDataset(lab.samples).Filter(rangeFilter).All(ctx)
		require.NoError(t, err)
		assertRows(t, mirrorOf(rows), rowsWhere(live, in))
		plan := datasetPlan(t, ctx, sp, lab.samples, rangePipeline)
		assert.Equal(t, storeName(byScore), planIndex(plan), plan)

		// One entry per stored row, tombstones included.
		stored, err := sp.QueryDataset(lab.samples).Projection(space.ProjectionOpts{IncludeDeleted: true}).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, map[string]int{storeName(byScore): stored}, lab.sharedIndexes(ctx, lab.samples))

		// A sort on the indexed field reads every live row in order, and
		// pages over them.
		all := sortedKeys(live, false)
		rows, err = sp.QueryDataset(lab.samples).Sort("score", "id").All(ctx)
		require.NoError(t, err)
		assert.Equal(t, all, idsOfInitial(rows))
		snap, err := sp.QueryDataset(lab.samples).Sort("score", "id").Limit(10).Offset(len(all)-10).
			Snapshot(ctx, space.QueryOpts{IncludeTotal: true})
		require.NoError(t, err)
		assert.Equal(t, all[len(all)-10:], idsOfInitial(snap.Initial))
		assert.Equal(t, len(all), snap.Total)
		assert.Empty(t, builds.since(from)[2:], "writes build nothing")
	})

	t.Run("CompoundWithObject", func(t *testing.T) {
		byKindObject := space.IndexDraft{Key: "by_kind_object", Fields: []string{"kind", space.IndexPathObject}}
		from := builds.mark()
		_, err := sp.Types().AddDatasetIndex(ctx, lab.typeId, samplesDef.Id, byKindObject)
		require.NoError(t, err)
		builds.awaitBuilt(t, ctx, from, lab.samples)
		assert.ElementsMatch(t, []space.IndexDraft{byScore, byKindObject},
			indexDrafts(t, datasetDefs(t, ctx, sp, lab.typeId)["samples"].Indexes))

		for _, obj := range objs[:2] {
			want := rowsWhere(live, func(r labRow) bool { return r.kind == "y" && r.obj == obj })
			require.NotEmpty(t, want)
			filter := map[string]any{"kind": "y", "_objectId": obj}
			assert.Equal(t, sortedKeys(want, false), lab.samplesWhere(t, ctx, filter))
			rows, err := sp.QueryDataset(lab.samples).Filter(filter).All(ctx)
			require.NoError(t, err)
			assertRows(t, mirrorOf(rows), want)
			plan := datasetPlan(t, ctx, sp, lab.samples, fmt.Sprintf(`[{"$match": {"kind": "y", "_objectId": %q}}]`, obj))
			assert.Equal(t, storeName(byKindObject), planIndex(plan), plan)
		}
		// The range read keeps its own index.
		plan := datasetPlan(t, ctx, sp, lab.samples, rangePipeline)
		assert.Equal(t, storeName(byScore), planIndex(plan), plan)
	})
}

// TestE2E_DatasetIndexes_SparseAndDescending: a sparse index holds only
// the rows carrying its field, yet every read that does not filter on
// the field still returns the rows without it; a descending index
// serves a descending sort.
func TestE2E_DatasetIndexes_SparseAndDescending(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	base := openSharedLab(t, ctx, "IndexSparseDesc")
	builds := watchBuilds(t, base.sp)
	from := builds.mark()
	byTag := space.IndexDraft{Key: "by_tag", Fields: []string{"tag"}, Sparse: true}
	byRankDesc := space.IndexDraft{Key: "by_rank_desc", Fields: []string{"-rank"}}
	lab := declareIndexLab(t, ctx, base, []space.IndexDraft{byTag, byRankDesc}, nil)
	sp := lab.sp
	builds.awaitBuilt(t, ctx, from, lab.samples)
	assert.Equal(t, "dx_tag~sparse", storeName(byTag))

	// 30 rows over three objects: unique ranks 1..30; every third row
	// tagged, alternately red and blue.
	type row struct {
		key, obj, id, tag string
		rank              int
	}
	objs := []string{lab.newObject(t, ctx), lab.newObject(t, ctx), lab.newObject(t, ctx)}
	var all []row
	byObj := map[string][]space.RecordModify{}
	for i := 0; i < 30; i++ {
		r := row{obj: objs[i%3], id: fmt.Sprintf("t%02d", i), rank: (i*7)%30 + 1}
		r.key = r.obj + "/" + r.id
		fields := map[string]any{"label": r.id, "score": i, "kind": "x", "rank": r.rank}
		if i%3 == 0 {
			r.tag = "blue"
			if i%2 == 0 {
				r.tag = "red"
			}
			fields["tag"] = r.tag
		}
		all = append(all, r)
		byObj[r.obj] = append(byObj[r.obj], create(r.id, fields))
	}
	for _, obj := range objs {
		lab.modify(t, ctx, obj, lab.samples, byObj[obj]...)
	}
	keysWhere := func(rows []row, keep func(row) bool) []string {
		var out []string
		for _, r := range rows {
			if keep(r) {
				out = append(out, r.key)
			}
		}
		return out
	}
	everyKey := keysWhere(all, func(row) bool { return true })
	tagged := func(tag string) func(row) bool { return func(r row) bool { return r.tag == tag } }

	t.Run("Sparse", func(t *testing.T) {
		assert.Equal(t, map[string]int{storeName(byTag): 10, storeName(byRankDesc): 30}, lab.sharedIndexes(ctx, lab.samples),
			"the sparse index holds the tagged rows only")

		rows, err := sp.QueryDataset(lab.samples).All(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, everyKey, idsOfInitial(rows), "an unfiltered read returns the untagged rows")
		n, err := sp.QueryDataset(lab.samples).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 30, n)
		rows, err = sp.QueryDataset(lab.samples).Sort("tag", "id").All(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, everyKey, idsOfInitial(rows), "a sort on the sparse field keeps the rows without it")
		n, err = sp.AggregateDataset(lab.samples, `[{"$sort": {"tag": 1}}]`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 30, n, "a pipeline sorted on the sparse field keeps the rows without it")

		rows, err = sp.QueryDataset(lab.samples).Filter(map[string]any{"tag": "red"}).All(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, keysWhere(all, tagged("red")), idsOfInitial(rows))
		plan := datasetPlan(t, ctx, sp, lab.samples, `[{"$match": {"tag": "red"}}]`)
		assert.Equal(t, storeName(byTag), planIndex(plan), plan)

		rows, err = sp.QueryDataset(lab.samples).Filter(map[string]any{"tag": map[string]any{"$exists": false}}).All(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, keysWhere(all, tagged("")), idsOfInitial(rows), "the rows the sparse index leaves out")

		// Unsetting a tag and setting one keep the sparse index exact.
		var red, plain row
		for _, r := range all {
			if r.tag == "red" && red.key == "" {
				red = r
			}
			if r.tag == "" && plain.key == "" {
				plain = r
			}
		}
		res, err := sp.Modify(ctx, space.ModifyBatch{ObjectId: red.obj, Dataset: lab.samples, Records: []space.RecordModify{
			{Id: red.id, Ops: []space.Op{{Type: space.OpUnset, Path: "tag"}}},
		}})
		require.NoError(t, err)
		require.Empty(t, res.Rejections)
		lab.set(t, ctx, plain.obj, lab.samples, plain.id, "tag", "red")
		for i := range all {
			switch all[i].key {
			case red.key:
				all[i].tag = ""
			case plain.key:
				all[i].tag = "red"
			}
		}
		rows, err = sp.QueryDataset(lab.samples).Filter(map[string]any{"tag": "red"}).All(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, keysWhere(all, tagged("red")), idsOfInitial(rows))
		assert.Contains(t, idsOfInitial(rows), plain.key)
		assert.NotContains(t, idsOfInitial(rows), red.key)
		assert.Equal(t, 10, lab.sharedIndexes(ctx, lab.samples)[storeName(byTag)])
		rows, err = sp.QueryDataset(lab.samples).Sort("tag", "id").All(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, everyKey, idsOfInitial(rows))
	})

	t.Run("Descending", func(t *testing.T) {
		byRank := append([]row(nil), all...)
		sort.Slice(byRank, func(i, j int) bool { return byRank[i].rank > byRank[j].rank })
		top := make([]string, 0, 5)
		for _, r := range byRank[:5] {
			top = append(top, r.key)
		}
		rows, err := sp.QueryDataset(lab.samples).Sort("-rank").Limit(5).All(ctx)
		require.NoError(t, err)
		assert.Equal(t, top, idsOfInitial(rows))
		plan := datasetPlan(t, ctx, sp, lab.samples, `[{"$sort": {"rank": -1}}, {"$limit": 5}]`)
		assert.Equal(t, storeName(byRankDesc), planIndex(plan), plan)

		rows, err = sp.QueryDataset(lab.samples).Filter(map[string]any{"rank": map[string]any{"$gte": 26}}).Sort("-rank").All(ctx)
		require.NoError(t, err)
		assert.Equal(t, top, idsOfInitial(rows))

		bottom := make([]string, 0, 3)
		for i := len(byRank) - 1; i >= len(byRank)-3; i-- {
			bottom = append(bottom, byRank[i].key)
		}
		rows, err = sp.QueryDataset(lab.samples).Sort("rank").Limit(3).All(ctx)
		require.NoError(t, err)
		assert.Equal(t, bottom, idsOfInitial(rows), "an ascending sort over a descending index")
	})
}

// TestE2E_DatasetIndexes_Removed: removing an index takes it out of the
// listing and discovery and, in the background, off the shared
// collection, without a build report; reads return the same rows and
// stop being planned on it; the key is free for an index of other
// fields, which builds.
func TestE2E_DatasetIndexes_Removed(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	base := openSharedLab(t, ctx, "IndexRemoved")
	builds := watchBuilds(t, base.sp)
	from := builds.mark()
	byScore := space.IndexDraft{Key: "by_score", Fields: []string{"score"}}
	byKind := space.IndexDraft{Key: "by_kind", Fields: []string{"kind"}}
	lab := declareIndexLab(t, ctx, base, []space.IndexDraft{byScore, byKind}, nil)
	sp := lab.sp
	builds.awaitBuilt(t, ctx, from, lab.samples)

	objs := []string{lab.newObject(t, ctx), lab.newObject(t, ctx), lab.newObject(t, ctx)}
	live := spreadSamples(objs, 90)
	lab.putSamples(t, ctx, live...)
	in := func(r labRow) bool { return r.score >= 20 && r.score < 35 }
	rangeFilter := map[string]any{"score": map[string]any{"$gte": 20, "$lt": 35}}
	const rangePipeline = `[{"$match": {"score": {"$gte": 20, "$lt": 35}}}]`
	const kindPipeline = `[{"$match": {"kind": "x"}}]`

	before := lab.samplesWhere(t, ctx, rangeFilter)
	require.Equal(t, sortedKeys(rowsWhere(live, in), false), before)
	plan := datasetPlan(t, ctx, sp, lab.samples, rangePipeline)
	require.Equal(t, storeName(byScore), planIndex(plan), plan)

	samplesDef := datasetDefs(t, ctx, sp, lab.typeId)["samples"]
	scoreDef := indexDefByKey(t, samplesDef.Indexes, "by_score")
	from = builds.mark()
	require.NoError(t, sp.Types().RemoveDatasetIndex(ctx, lab.typeId, scoreDef.Id))

	assert.Equal(t, []space.IndexDraft{byKind}, indexDrafts(t, datasetDefs(t, ctx, sp, lab.typeId)["samples"].Indexes))
	assert.Equal(t, []space.IndexDraft{byKind}, discoveredIndexes(t, sp, lab.samples))
	require.True(t, waitFor(ctx, 30*time.Second, 50*time.Millisecond, func() bool {
		_, held := lab.sharedIndexes(ctx, lab.samples)[storeName(byScore)]
		return !held
	}), "the shared collection drops the removed index; stored %v", lab.sharedIndexes(ctx, lab.samples))
	assert.Equal(t, map[string]int{storeName(byKind): len(live)}, lab.sharedIndexes(ctx, lab.samples),
		"the other index stays")
	require.True(t, waitFor(ctx, 30*time.Second, 50*time.Millisecond, func() bool {
		p, err := sp.AggregateDataset(lab.samples, rangePipeline).Explain(ctx)
		return err == nil && !planNames(p, storeName(byScore))
	}), "reads stop being planned on the removed index")

	assert.Equal(t, before, lab.samplesWhere(t, ctx, rangeFilter), "the removal changes no result")
	rows, err := sp.QueryDataset(lab.samples).Filter(rangeFilter).All(ctx)
	require.NoError(t, err)
	assertRows(t, mirrorOf(rows), rowsWhere(live, in))
	plan = datasetPlan(t, ctx, sp, lab.samples, kindPipeline)
	assert.Equal(t, storeName(byKind), planIndex(plan), plan)
	builds.assertQuiet(t, ctx, from, 500*time.Millisecond)

	// Writes after the removal land and read back.
	fresh := sample(objs[0], "late", "x", 25)
	lab.putSamples(t, ctx, fresh)
	live = append(live, fresh)
	assert.Equal(t, sortedKeys(rowsWhere(live, in), false), lab.samplesWhere(t, ctx, rangeFilter))

	// The key takes an index of other fields, which builds.
	byScoreAgain := space.IndexDraft{Key: "by_score", Fields: []string{"kind", "score"}}
	from = builds.mark()
	againId, err := sp.Types().AddDatasetIndex(ctx, lab.typeId, samplesDef.Id, byScoreAgain)
	require.NoError(t, err)
	assert.NotEqual(t, scoreDef.Id, againId)
	builds.awaitBuilt(t, ctx, from, lab.samples)
	listed := datasetDefs(t, ctx, sp, lab.typeId)["samples"].Indexes
	assert.ElementsMatch(t, []space.IndexDraft{byKind, byScoreAgain}, indexDrafts(t, listed))
	assert.Equal(t, againId, indexDefByKey(t, listed, "by_score").Id)
	assert.Equal(t, map[string]int{storeName(byKind): len(live), storeName(byScoreAgain): len(live)},
		lab.sharedIndexes(ctx, lab.samples))

	want := rowsWhere(live, func(r labRow) bool { return r.kind == "x" && in(r) })
	filter := map[string]any{"kind": "x", "score": map[string]any{"$gte": 20, "$lt": 35}}
	assert.Equal(t, sortedKeys(want, false), lab.samplesWhere(t, ctx, filter))
	plan = datasetPlan(t, ctx, sp, lab.samples, `[{"$match": {"kind": "x", "score": {"$gte": 20, "$lt": 35}}}]`)
	assert.Equal(t, storeName(byScoreAgain), planIndex(plan), plan)

	// Two definitions of one shape share one stored index: it stays
	// while either is declared.
	kindDef := indexDefByKey(t, listed, "by_kind")
	byKindTwin := space.IndexDraft{Key: "by_kind_twin", Fields: []string{"kind"}}
	from = builds.mark()
	twinId, err := sp.Types().AddDatasetIndex(ctx, lab.typeId, samplesDef.Id, byKindTwin)
	require.NoError(t, err)
	require.NoError(t, sp.Types().RemoveDatasetIndex(ctx, lab.typeId, kindDef.Id))
	assert.ElementsMatch(t, []space.IndexDraft{byScoreAgain, byKindTwin},
		indexDrafts(t, datasetDefs(t, ctx, sp, lab.typeId)["samples"].Indexes))
	dropped := waitFor(ctx, 500*time.Millisecond, 25*time.Millisecond, func() bool {
		_, held := lab.sharedIndexes(ctx, lab.samples)[storeName(byKind)]
		return !held
	})
	assert.False(t, dropped, "the twin keeps the stored index")
	builds.assertQuiet(t, ctx, from, 200*time.Millisecond)
	plan = datasetPlan(t, ctx, sp, lab.samples, kindPipeline)
	assert.Equal(t, storeName(byKindTwin), planIndex(plan), plan)
	require.NoError(t, sp.Types().RemoveDatasetIndex(ctx, lab.typeId, twinId))
	require.True(t, waitFor(ctx, 30*time.Second, 50*time.Millisecond, func() bool {
		_, held := lab.sharedIndexes(ctx, lab.samples)[storeName(byKind)]
		return !held
	}), "the last definition of the shape takes the stored index with it")
	assert.Equal(t, sortedKeys(rowsWhere(live, func(r labRow) bool { return r.kind == "x" }), false),
		lab.samplesWhere(t, ctx, map[string]any{"kind": "x"}))
}

// TestE2E_DatasetIndexes_PerObjectAddedLater: an index added to a
// per-object dataset that already holds rows on two objects reaches
// each object's collection on its next access, with no build report;
// its removal leaves each collection on the next access the same way.
func TestE2E_DatasetIndexes_PerObjectAddedLater(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	lab := declareIndexLab(t, ctx, openSharedLab(t, ctx, "IndexPerObject"), nil, nil)
	sp := lab.sp
	builds := watchBuilds(t, sp)

	objs := []string{lab.newObject(t, ctx), lab.newObject(t, ctx)}
	for _, obj := range objs {
		recs := make([]space.RecordModify, 0, 25)
		for i := 0; i < 25; i++ {
			recs = append(recs, create(fmt.Sprintf("p%02d", i), map[string]any{"label": fmt.Sprintf("p%02d", i), "score": i}))
		}
		lab.modify(t, ctx, obj, lab.plain, recs...)
	}
	byScore := space.IndexDraft{Key: "by_score", Fields: []string{"score"}}
	const pipeline = `[{"$match": {"score": {"$gte": 20}}}]`
	want := []string{"p20", "p21", "p22", "p23", "p24"}
	read := func(obj string) []string {
		t.Helper()
		rows, err := sp.Query(obj, lab.plain).Filter(map[string]any{"score": map[string]any{"$gte": 20}}).Sort("score").All(ctx)
		require.NoError(t, err)
		return idsOfInitial(rows)
	}
	for _, obj := range objs {
		require.Equal(t, want, read(obj))
		plan := objectPlan(t, ctx, sp, obj, lab.plain, pipeline)
		assert.False(t, planNames(plan, storeName(byScore)), plan)
		assert.Empty(t, storedIndexes(ctx, lab.sdk, obj+"_"+lab.plain))
	}

	from := builds.mark()
	plainDef := datasetDefs(t, ctx, sp, lab.typeId)["plain"]
	scoreId, err := sp.Types().AddDatasetIndex(ctx, lab.typeId, plainDef.Id, byScore)
	require.NoError(t, err)
	assert.Equal(t, []space.IndexDraft{byScore}, indexDrafts(t, datasetDefs(t, ctx, sp, lab.typeId)["plain"].Indexes))
	assert.Equal(t, []space.IndexDraft{byScore}, discoveredIndexes(t, sp, lab.plain))

	// The next access to each object opens its collection with the index.
	for _, obj := range objs {
		plan := objectPlan(t, ctx, sp, obj, lab.plain, pipeline)
		assert.Equal(t, storeName(byScore), planIndex(plan), "object %s: %s", obj, plan)
		assert.Equal(t, map[string]int{storeName(byScore): 25}, storedIndexes(ctx, lab.sdk, obj+"_"+lab.plain), "object %s", obj)
		assert.Equal(t, want, read(obj), "object %s", obj)
	}
	builds.assertQuiet(t, ctx, from, 500*time.Millisecond)

	// Writes after the index keep it whole.
	lab.modify(t, ctx, objs[0], lab.plain, create("p99", map[string]any{"label": "p99", "score": 99}))
	assert.Equal(t, append(append([]string(nil), want...), "p99"), read(objs[0]))
	assert.Equal(t, map[string]int{storeName(byScore): 26}, storedIndexes(ctx, lab.sdk, objs[0]+"_"+lab.plain))

	require.NoError(t, sp.Types().RemoveDatasetIndex(ctx, lab.typeId, scoreId))
	assert.Empty(t, datasetDefs(t, ctx, sp, lab.typeId)["plain"].Indexes)
	assert.Empty(t, discoveredIndexes(t, sp, lab.plain))
	for _, obj := range objs {
		plan := objectPlan(t, ctx, sp, obj, lab.plain, pipeline)
		assert.False(t, planNames(plan, storeName(byScore)), "object %s: %s", obj, plan)
		assert.Empty(t, storedIndexes(ctx, lab.sdk, obj+"_"+lab.plain), "object %s drops the index", obj)
	}
	assert.Equal(t, append(append([]string(nil), want...), "p99"), read(objs[0]))
	assert.Equal(t, want, read(objs[1]))
	builds.assertQuiet(t, ctx, from, 200*time.Millisecond)
}

// TestE2E_DatasetIndexes_Refusals: every declaration no collection could
// build is refused, at AddDatasetIndex and inside a dataset or part
// draft, and declares nothing; an indexed field cannot be removed until
// its index is; an unknown id removes nothing.
func TestE2E_DatasetIndexes_Refusals(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	lab := declareIndexLab(t, ctx, openSharedLab(t, ctx, "IndexRefusals"), nil, nil)
	sp := lab.sp
	summaryId, err := sp.Types().AddDataset(ctx, lab.typeId, lab.partId, space.DatasetDraft{Key: "summary", Module: "notes"})
	require.NoError(t, err)
	defs := datasetDefs(t, ctx, sp, lab.typeId)
	samplesId, plainId, eventsId := defs["samples"].Id, defs["plain"].Id, defs["events"].Id

	// expect is the listing every refusal must leave as it is.
	expect := mustListing(t, ctx, sp, lab.typeId)
	discovered := func() map[string][]space.IndexDraft {
		out := map[string][]space.IndexDraft{}
		for _, d := range sp.Datasets() {
			if strings.HasPrefix(d.Name, lab.typeId+"_") {
				out[d.Name] = d.Indexes
			}
		}
		return out
	}
	expectDisc := discovered()
	parts, err := sp.Types().Parts(ctx, lab.typeId)
	require.NoError(t, err)
	partCount := len(parts)
	unchanged := func(t *testing.T) {
		t.Helper()
		assert.Equal(t, expect, mustListing(t, ctx, sp, lab.typeId), "a refusal declares nothing")
		assert.Equal(t, expectDisc, discovered(), "a refusal changes no discovery entry")
		parts, err := sp.Types().Parts(ctx, lab.typeId)
		require.NoError(t, err)
		assert.Len(t, parts, partCount)
	}
	accepted := func(t *testing.T) {
		t.Helper()
		expect = mustListing(t, ctx, sp, lab.typeId)
		expectDisc = discovered()
	}

	t.Run("AddDatasetIndex", func(t *testing.T) {
		for _, tc := range []struct {
			name  string
			defId string
			draft space.IndexDraft
			msg   string
		}{
			{"UndeclaredField", samplesId, space.IndexDraft{Key: "by_nope", Fields: []string{"nope"}}, "is not a declared field"},
			{"PathIntoObject", samplesId, space.IndexDraft{Key: "by_meta_x", Fields: []string{"meta.x"}}, "is not a declared field"},
			{"ArrayField", samplesId, space.IndexDraft{Key: "by_tags", Fields: []string{"tags"}}, "is not a string, number, boolean or datetime"},
			{"ObjectField", samplesId, space.IndexDraft{Key: "by_meta", Fields: []string{"meta"}}, "is not a string, number, boolean or datetime"},
			{"ArrayFieldPerObject", plainId, space.IndexDraft{Key: "by_tags", Fields: []string{"score", "tags"}}, "is not a string, number, boolean or datetime"},
			{"ObjectIdOnPerObject", plainId, space.IndexDraft{Key: "by_object", Fields: []string{space.IndexPathObject}}, "exists on a shared dataset only"},
			{"ObjectIdLaterOnPerObject", plainId, space.IndexDraft{Key: "by_score_object", Fields: []string{"score", "-" + space.IndexPathObject}}, "exists on a shared dataset only"},
			{"FiveFields", samplesId, space.IndexDraft{Key: "by_five", Fields: []string{"label", "score", "kind", "rank", "tag"}}, "at most 4"},
			{"NoFields", samplesId, space.IndexDraft{Key: "by_none"}, "names no fields"},
			{"EmptyFieldList", samplesId, space.IndexDraft{Key: "by_none", Fields: []string{}}, "names no fields"},
			{"NonSlugKey", samplesId, space.IndexDraft{Key: "By Score", Fields: []string{"score"}}, "must match"},
			{"DashedKey", samplesId, space.IndexDraft{Key: "by-score", Fields: []string{"score"}}, "must match"},
			{"EmptyKey", samplesId, space.IndexDraft{Fields: []string{"score"}}, "must be non-empty"},
			{"RepeatedPath", samplesId, space.IndexDraft{Key: "by_twice", Fields: []string{"score", "-score"}}, "twice"},
			{"BareDash", samplesId, space.IndexDraft{Key: "by_dash", Fields: []string{"-"}}, "invalid field"},
			{"EmptyPath", samplesId, space.IndexDraft{Key: "by_empty", Fields: []string{""}}, "invalid field"},
			{"UnknownDataset", "no-such-dataset", space.IndexDraft{Key: "by_score", Fields: []string{"score"}}, ""},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, err := sp.Types().AddDatasetIndex(ctx, lab.typeId, tc.defId, tc.draft)
				require.Error(t, err)
				if tc.msg != "" {
					assert.ErrorContains(t, err, tc.msg)
				}
				unchanged(t)
			})
		}
	})

	t.Run("ModuleDataset", func(t *testing.T) {
		textIdx := []space.IndexDraft{{Key: "by_text", Fields: []string{"text"}}}
		_, err := sp.Types().AddDatasetIndex(ctx, lab.typeId, summaryId, textIdx[0])
		assert.ErrorIs(t, err, space.ErrModuleOwned)
		_, err = sp.Types().AddDataset(ctx, lab.typeId, lab.partId, space.DatasetDraft{Key: "digest", Module: "notes", Indexes: textIdx})
		assert.ErrorIs(t, err, space.ErrModuleOwned)
		_, err = sp.Types().AddPart(ctx, lab.typeId, space.PartDraft{
			Key: "digest", Datasets: []space.DatasetDraft{{Key: "digest", Module: "notes", Indexes: textIdx}},
		})
		assert.ErrorIs(t, err, space.ErrModuleOwned)
		unchanged(t)
	})

	t.Run("InDraft", func(t *testing.T) {
		label := []space.DatasetFieldDraft{{Key: "label", Kind: space.PropertyKindString, MutableBy: space.MutableByAnyone}}
		var nine []space.IndexDraft
		for _, f := range []string{"label", "-label", space.IndexPathCreated, "-" + space.IndexPathCreated} {
			for _, second := range []string{"", space.IndexPathObject} {
				d := space.IndexDraft{Key: fmt.Sprintf("by_%d", len(nine)), Fields: []string{f}}
				if second != "" {
					d.Fields = append(d.Fields, second)
				}
				nine = append(nine, d)
			}
		}
		nine = append(nine, space.IndexDraft{Key: "by_8", Fields: []string{space.IndexPathObject}})
		require.Len(t, nine, space.MaxDatasetIndexes+1)
		for _, tc := range []struct {
			name    string
			shared  bool
			indexes []space.IndexDraft
		}{
			{"UndeclaredField", true, []space.IndexDraft{{Key: "by_nope", Fields: []string{"nope"}}}},
			{"ObjectIdOnPerObject", false, []space.IndexDraft{{Key: "by_object", Fields: []string{space.IndexPathObject}}}},
			{"DuplicateKey", true, []space.IndexDraft{{Key: "by_label", Fields: []string{"label"}}, {Key: "by_label", Fields: []string{"-label"}}}},
			{"NineIndexes", true, nine},
		} {
			t.Run(tc.name, func(t *testing.T) {
				draft := space.DatasetDraft{Key: "extra", Shared: tc.shared, IdRule: space.IdUser, Fields: label, Indexes: tc.indexes}
				_, err := sp.Types().AddDataset(ctx, lab.typeId, lab.partId, draft)
				assert.Error(t, err, "AddDataset")
				_, err = sp.Types().AddPart(ctx, lab.typeId, space.PartDraft{Key: "extra", Datasets: []space.DatasetDraft{draft}})
				assert.Error(t, err, "AddPart")
				unchanged(t)
			})
		}
		// The eight that fit declare.
		_, err := sp.Types().AddDataset(ctx, lab.typeId, lab.partId, space.DatasetDraft{
			Key: "extra", Shared: true, IdRule: space.IdUser, Fields: label, Indexes: nine[:space.MaxDatasetIndexes],
		})
		require.NoError(t, err)
		extra := datasetDefs(t, ctx, sp, lab.typeId)["extra"]
		assert.ElementsMatch(t, nine[:space.MaxDatasetIndexes], indexDrafts(t, extra.Indexes))
		accepted(t)
		_, err = sp.Types().AddDatasetIndex(ctx, lab.typeId, extra.Id, nine[space.MaxDatasetIndexes])
		assert.ErrorContains(t, err, "at most 8 indexes")
		unchanged(t)
	})

	t.Run("DuplicateKey", func(t *testing.T) {
		_, err := sp.Types().AddDatasetIndex(ctx, lab.typeId, samplesId, space.IndexDraft{Key: "by_score", Fields: []string{"score"}})
		require.NoError(t, err)
		accepted(t)
		for _, fields := range [][]string{{"label"}, {"score"}} {
			_, err = sp.Types().AddDatasetIndex(ctx, lab.typeId, samplesId, space.IndexDraft{Key: "by_score", Fields: fields})
			assert.ErrorContains(t, err, "already declared", "fields %v", fields)
			unchanged(t)
		}
		// The key is the dataset's own: another dataset takes it.
		_, err = sp.Types().AddDatasetIndex(ctx, lab.typeId, eventsId, space.IndexDraft{Key: "by_score", Fields: []string{"label"}})
		require.NoError(t, err)
		accepted(t)
	})

	t.Run("NinthIndex", func(t *testing.T) {
		eight := []space.IndexDraft{
			{Key: "by_label", Fields: []string{"label"}},
			{Key: "by_label_desc", Fields: []string{"-label"}},
			{Key: "by_score", Fields: []string{"score"}},
			{Key: "by_score_desc", Fields: []string{"-score"}},
			{Key: "by_label_score", Fields: []string{"label", "score"}},
			{Key: "by_score_label", Fields: []string{"score", "label"}},
			{Key: "by_created", Fields: []string{space.IndexPathCreated}},
			{Key: "by_label_created", Fields: []string{"label", space.IndexPathCreated}},
		}
		require.Len(t, eight, space.MaxDatasetIndexes)
		ids := make([]string, 0, len(eight))
		for _, d := range eight {
			id, err := sp.Types().AddDatasetIndex(ctx, lab.typeId, plainId, d)
			require.NoError(t, err, d.Key)
			ids = append(ids, id)
		}
		listed := datasetDefs(t, ctx, sp, lab.typeId)["plain"].Indexes
		assert.Equal(t, eight, indexDrafts(t, listed), "listed in creation order")
		accepted(t)
		ninth := space.IndexDraft{Key: "by_score_created", Fields: []string{"score", space.IndexPathCreated}}
		_, err := sp.Types().AddDatasetIndex(ctx, lab.typeId, plainId, ninth)
		assert.ErrorContains(t, err, "at most 8 indexes")
		unchanged(t)

		// A removal makes room.
		require.NoError(t, sp.Types().RemoveDatasetIndex(ctx, lab.typeId, ids[0]))
		_, err = sp.Types().AddDatasetIndex(ctx, lab.typeId, plainId, ninth)
		require.NoError(t, err)
		assert.Equal(t, append(eight[1:], ninth), indexDrafts(t, datasetDefs(t, ctx, sp, lab.typeId)["plain"].Indexes))
		accepted(t)
	})

	t.Run("RemoveIndexedField", func(t *testing.T) {
		rankIdx := space.IndexDraft{Key: "by_score_rank", Fields: []string{"score", "-rank"}}
		rankIdxId, err := sp.Types().AddDatasetIndex(ctx, lab.typeId, samplesId, rankIdx)
		require.NoError(t, err)
		accepted(t)
		var rankFieldId string
		for _, f := range datasetDefs(t, ctx, sp, lab.typeId)["samples"].Fields {
			if f.Key == "rank" {
				rankFieldId = f.Id
			}
		}
		require.NotEmpty(t, rankFieldId)

		err = sp.Types().RemoveDatasetField(ctx, lab.typeId, rankFieldId)
		assert.ErrorContains(t, err, "remove the index first")
		unchanged(t)
		fieldKeys := func() []string {
			var out []string
			for _, f := range datasetDefs(t, ctx, sp, lab.typeId)["samples"].Fields {
				out = append(out, f.Key)
			}
			return out
		}
		assert.Contains(t, fieldKeys(), "rank", "the refused removal keeps the field")

		require.NoError(t, sp.Types().RemoveDatasetIndex(ctx, lab.typeId, rankIdxId))
		require.NoError(t, sp.Types().RemoveDatasetField(ctx, lab.typeId, rankFieldId))
		assert.NotContains(t, fieldKeys(), "rank")
		accepted(t)
		_, err = sp.Types().AddDatasetIndex(ctx, lab.typeId, samplesId, space.IndexDraft{Key: "by_rank", Fields: []string{"rank"}})
		assert.ErrorContains(t, err, "is not a declared field", "a removed field takes no index")
		unchanged(t)

		// The same index id removes nothing twice.
		assert.Error(t, sp.Types().RemoveDatasetIndex(ctx, lab.typeId, rankIdxId))
		unchanged(t)
	})

	t.Run("RemoveUnknownIndex", func(t *testing.T) {
		labelFieldId := ""
		for _, f := range datasetDefs(t, ctx, sp, lab.typeId)["samples"].Fields {
			if f.Key == "label" {
				labelFieldId = f.Id
			}
		}
		require.NotEmpty(t, labelFieldId)
		for _, id := range []string{"no-such-index", "", samplesId, labelFieldId, summaryId} {
			assert.Error(t, sp.Types().RemoveDatasetIndex(ctx, lab.typeId, id), "id %q", id)
			unchanged(t)
		}
		assert.Len(t, datasetDefs(t, ctx, sp, lab.typeId), len(expect), "no dataset went with a refused index removal")
	})
}

// TestE2E_DatasetIndexes_SecondDevice: index definitions converge on a
// second device of the account, which builds them on its own copy of
// the shared collection — reported on its build feed — plans reads on
// them and returns the first device's rows through them, and drops an
// index the first device removes.
func TestE2E_DatasetIndexes_SecondDevice(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	byKind := space.IndexDraft{Key: "by_kind", Fields: []string{"kind"}}
	byScore := space.IndexDraft{Key: "by_score", Fields: []string{"score"}}
	labA, labB := openIndexPair(t, ctx, "IndexSync", []space.IndexDraft{byKind}, []space.IndexDraft{byScore})
	spA, spB := labA.sp, labB.sp

	objs := []string{labA.newObject(t, ctx), labA.newObject(t, ctx), labA.newObject(t, ctx)}
	live := spreadSamples(objs, 45)
	labA.putSamples(t, ctx, live...)
	recs := make([]space.RecordModify, 0, 20)
	for i := 0; i < 20; i++ {
		recs = append(recs, create(fmt.Sprintf("p%02d", i), map[string]any{"label": fmt.Sprintf("p%02d", i), "score": i}))
	}
	labA.modify(t, ctx, objs[0], labA.plain, recs...)
	_ = spA.SyncHeads(ctx)

	// Device B converges on the declaration, the rows and the index built
	// at declaration.
	require.True(t, waitFor(ctx, 150*time.Second, 500*time.Millisecond, func() bool {
		_ = spB.SyncHeads(ctx)
		n, qerr := spB.QueryDataset(labA.samples).Count(ctx)
		_, built := labB.sharedIndexes(ctx, labA.samples)[storeName(byKind)]
		return qerr == nil && n == len(live) && built
	}), "device B must converge on the rows and build the declared index")
	assert.Equal(t, mustListing(t, ctx, spA, labA.typeId), mustListing(t, ctx, spB, labA.typeId))
	assert.Equal(t, []space.IndexDraft{byKind}, discoveredIndexes(t, spB, labA.samples))
	assert.Equal(t, []space.IndexDraft{byScore}, discoveredIndexes(t, spB, labA.plain))
	assert.Equal(t, map[string]int{storeName(byKind): len(live)}, labB.sharedIndexes(ctx, labA.samples))
	wantY := rowsWhere(live, func(r labRow) bool { return r.kind == "y" })
	assert.Equal(t, sortedKeys(wantY, false), labB.samplesWhere(t, ctx, map[string]any{"kind": "y"}))
	plan := datasetPlan(t, ctx, spB, labA.samples, `[{"$match": {"kind": "y"}}]`)
	assert.Equal(t, storeName(byKind), planIndex(plan), plan)

	// The per-object index reaches device B's copy of the object.
	require.True(t, waitFor(ctx, 60*time.Second, 500*time.Millisecond, func() bool {
		_ = spB.SyncHeads(ctx)
		n, qerr := spB.Query(objs[0], labA.plain).Count(ctx)
		return qerr == nil && n == len(recs)
	}), "device B must converge on the per-object rows")
	plan = objectPlan(t, ctx, spB, objs[0], labA.plain, `[{"$match": {"score": {"$gte": 15}}}]`)
	assert.Equal(t, storeName(byScore), planIndex(plan), plan)
	rows, err := spB.Query(objs[0], labA.plain).Filter(map[string]any{"score": map[string]any{"$gte": 15}}).Sort("score").All(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"p15", "p16", "p17", "p18", "p19"}, idsOfInitial(rows))

	// An index added on device A: both devices build it, each once.
	buildsA, buildsB := watchBuilds(t, spA), watchBuilds(t, spB)
	fromA, fromB := buildsA.mark(), buildsB.mark()
	byScoreObject := space.IndexDraft{Key: "by_score_object", Fields: []string{"score", space.IndexPathObject}}
	samplesDef := datasetDefs(t, ctx, spA, labA.typeId)["samples"]
	laterId, err := spA.Types().AddDatasetIndex(ctx, labA.typeId, samplesDef.Id, byScoreObject)
	require.NoError(t, err)
	buildsA.awaitBuilt(t, ctx, fromA, labA.samples)
	_ = spA.SyncHeads(ctx)
	want := mustListing(t, ctx, spA, labA.typeId)
	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		_ = spB.SyncHeads(ctx)
		got, lerr := indexListing(ctx, spB, labA.typeId)
		return lerr == nil && assert.ObjectsAreEqual(want, got)
	}), "device B must converge on the added index")
	buildsB.awaitBuilt(t, ctx, fromB, labA.samples)
	assert.ElementsMatch(t, []space.IndexDraft{byKind, byScoreObject}, discoveredIndexes(t, spB, labA.samples))
	assert.Equal(t, map[string]int{storeName(byKind): len(live), storeName(byScoreObject): len(live)},
		labB.sharedIndexes(ctx, labA.samples))
	const rangePipeline = `[{"$match": {"score": {"$gte": 10, "$lt": 25}}}]`
	in := func(r labRow) bool { return r.score >= 10 && r.score < 25 }
	rangeFilter := map[string]any{"score": map[string]any{"$gte": 10, "$lt": 25}}
	plan = datasetPlan(t, ctx, spB, labA.samples, rangePipeline)
	assert.Equal(t, storeName(byScoreObject), planIndex(plan), plan)
	rows, err = spB.QueryDataset(labA.samples).Filter(rangeFilter).All(ctx)
	require.NoError(t, err)
	assertRows(t, mirrorOf(rows), rowsWhere(live, in))

	// Device A's later rows reach device B's indexed reads.
	fresh := []labRow{sample(objs[1], "n1", "y", 12), sample(objs[2], "n2", "x", 99)}
	labA.putSamples(t, ctx, fresh...)
	live = append(live, fresh...)
	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		_ = spA.SyncHeads(ctx)
		_ = spB.SyncHeads(ctx)
		n, qerr := spB.QueryDataset(labA.samples).Count(ctx)
		return qerr == nil && n == len(live)
	}), "device B must converge on device A's later rows")
	assert.Equal(t, sortedKeys(rowsWhere(live, in), false), labB.samplesWhere(t, ctx, rangeFilter))
	assert.Equal(t, sortedKeys(rowsWhere(live, func(r labRow) bool { return r.kind == "y" }), false),
		labB.samplesWhere(t, ctx, map[string]any{"kind": "y"}))
	assert.Equal(t, map[string]int{storeName(byKind): len(live), storeName(byScoreObject): len(live)},
		labB.sharedIndexes(ctx, labA.samples))

	// Device A removes the later index; device B drops it without a build.
	fromB = buildsB.mark()
	require.NoError(t, spA.Types().RemoveDatasetIndex(ctx, labA.typeId, laterId))
	_ = spA.SyncHeads(ctx)
	want = mustListing(t, ctx, spA, labA.typeId)
	assert.Equal(t, []space.IndexDraft{byKind}, indexDrafts(t, want["samples"]))
	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		_ = spB.SyncHeads(ctx)
		got, lerr := indexListing(ctx, spB, labA.typeId)
		return lerr == nil && assert.ObjectsAreEqual(want, got)
	}), "device B must converge on the removal")
	require.True(t, waitFor(ctx, 30*time.Second, 50*time.Millisecond, func() bool {
		_, held := labB.sharedIndexes(ctx, labA.samples)[storeName(byScoreObject)]
		return !held
	}), "device B drops the removed index")
	assert.Equal(t, []space.IndexDraft{byKind}, discoveredIndexes(t, spB, labA.samples))
	plan = datasetPlan(t, ctx, spB, labA.samples, rangePipeline)
	assert.False(t, planNames(plan, storeName(byScoreObject)), plan)
	assert.Equal(t, sortedKeys(rowsWhere(live, in), false), labB.samplesWhere(t, ctx, rangeFilter))
	buildsB.assertQuiet(t, ctx, fromB, 500*time.Millisecond)
}

// TestE2E_DatasetIndexes_Restart: after the SDK reopens on the same data
// dir the listing is intact, reads are planned on the indexes on the
// first access, and the build feed reports nothing for indexes the
// collections already hold — until an index is added.
func TestE2E_DatasetIndexes_Restart(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)

	cfg := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	provider := newFixedSeedProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// First boot: one index declared with each dataset, one added to the
	// shared dataset once it holds rows.
	sdk1, err := anysyncsdk.Open(ctx, cfg, provider)
	require.NoError(t, err)
	sp1, err := sdk1.Spaces().Create(ctx, space.CreateRequest{Name: "IndexRestart"})
	if err != nil {
		_ = sdk1.Close()
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatal(err)
	}
	byKind := space.IndexDraft{Key: "by_kind", Fields: []string{"kind"}}
	byScore := space.IndexDraft{Key: "by_score", Fields: []string{"score"}}
	byScoreObject := space.IndexDraft{Key: "by_score_object", Fields: []string{"score", space.IndexPathObject}}
	builds1 := watchBuilds(t, sp1)
	from := builds1.mark()
	lab1 := declareIndexLab(t, ctx, &sharedLab{sdk: sdk1, sp: sp1}, []space.IndexDraft{byKind}, []space.IndexDraft{byScore})
	builds1.awaitBuilt(t, ctx, from, lab1.samples)
	objs := []string{lab1.newObject(t, ctx), lab1.newObject(t, ctx)}
	live := spreadSamples(objs, 40)
	lab1.putSamples(t, ctx, live...)
	recs := make([]space.RecordModify, 0, 20)
	for i := 0; i < 20; i++ {
		recs = append(recs, create(fmt.Sprintf("p%02d", i), map[string]any{"label": fmt.Sprintf("p%02d", i), "score": i}))
	}
	lab1.modify(t, ctx, objs[0], lab1.plain, recs...)
	from = builds1.mark()
	_, err = sp1.Types().AddDatasetIndex(ctx, lab1.typeId, datasetDefs(t, ctx, sp1, lab1.typeId)["samples"].Id, byScoreObject)
	require.NoError(t, err)
	builds1.awaitBuilt(t, ctx, from, lab1.samples)
	plan := objectPlan(t, ctx, sp1, objs[0], lab1.plain, `[{"$match": {"score": {"$gte": 15}}}]`)
	require.Equal(t, storeName(byScore), planIndex(plan), plan)
	listing := mustListing(t, ctx, sp1, lab1.typeId)
	spaceId := sp1.Id()
	require.NoError(t, sdk1.Close())

	// Second boot: the build feed opens before any read.
	sdk2, err := anysyncsdk.Open(ctx, cfg, provider)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk2.Close() })
	sp2, err := sdk2.Spaces().Get(ctx, spaceId)
	require.NoError(t, err)
	builds := watchBuilds(t, sp2)
	lab := &sharedLab{sdk: sdk2, sp: sp2, typeId: lab1.typeId, partId: lab1.partId,
		samples: lab1.samples, events: lab1.events, plain: lab1.plain}

	assert.Equal(t, listing, mustListing(t, ctx, sp2, lab.typeId), "the listing survives the restart")
	assert.ElementsMatch(t, []space.IndexDraft{byKind, byScoreObject}, discoveredIndexes(t, sp2, lab.samples))
	assert.Equal(t, []space.IndexDraft{byScore}, discoveredIndexes(t, sp2, lab.plain))

	plan = datasetPlan(t, ctx, sp2, lab.samples, `[{"$match": {"kind": "z"}}]`)
	assert.Equal(t, storeName(byKind), planIndex(plan), plan)
	plan = datasetPlan(t, ctx, sp2, lab.samples, `[{"$match": {"score": {"$gte": 5, "$lt": 15}}}]`)
	assert.Equal(t, storeName(byScoreObject), planIndex(plan), plan)
	plan = objectPlan(t, ctx, sp2, objs[0], lab.plain, `[{"$match": {"score": {"$gte": 15}}}]`)
	assert.Equal(t, storeName(byScore), planIndex(plan), plan)
	assert.Equal(t, map[string]int{storeName(byKind): len(live), storeName(byScoreObject): len(live)},
		lab.sharedIndexes(ctx, lab.samples))
	assert.Equal(t, sortedKeys(rowsWhere(live, func(r labRow) bool { return r.kind == "z" }), false),
		lab.samplesWhere(t, ctx, map[string]any{"kind": "z"}))
	assert.Equal(t, sortedKeys(rowsWhere(live, func(r labRow) bool { return r.score >= 5 && r.score < 15 }), false),
		lab.samplesWhere(t, ctx, map[string]any{"score": map[string]any{"$gte": 5, "$lt": 15}}))
	builds.assertQuiet(t, ctx, 0, time.Second)

	// A catalog change starts a pass that has nothing to build.
	_, err = sp2.Types().AddDatasetField(ctx, lab.typeId, datasetDefs(t, ctx, sp2, lab.typeId)["samples"].Id,
		space.DatasetFieldDraft{Key: "note", Kind: space.PropertyKindString, MutableBy: space.MutableByAnyone})
	require.NoError(t, err)
	builds.assertQuiet(t, ctx, 0, time.Second)

	// The feed is live: an index added after the restart builds.
	byLabel := space.IndexDraft{Key: "by_label", Fields: []string{"label"}}
	_, err = sp2.Types().AddDatasetIndex(ctx, lab.typeId, datasetDefs(t, ctx, sp2, lab.typeId)["samples"].Id, byLabel)
	require.NoError(t, err)
	builds.awaitBuilt(t, ctx, 0, lab.samples)
	plan = datasetPlan(t, ctx, sp2, lab.samples, fmt.Sprintf(`[{"$match": {"label": %q}}]`, live[7].label))
	assert.Equal(t, storeName(byLabel), planIndex(plan), plan)
}

// TestE2E_DatasetIndexes_RebuiltAtOpen: a declared index missing from
// the store when the SDK opens — on the shared collection or on an
// object's collection — is back once the space opens and the object is
// next read.
func TestE2E_DatasetIndexes_RebuiltAtOpen(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)

	cfg := config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}
	provider := newFixedSeedProvider(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	sdk1, err := anysyncsdk.Open(ctx, cfg, provider)
	require.NoError(t, err)
	sp1, err := sdk1.Spaces().Create(ctx, space.CreateRequest{Name: "IndexRebuiltAtOpen"})
	if err != nil {
		_ = sdk1.Close()
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatal(err)
	}
	byKind := space.IndexDraft{Key: "by_kind", Fields: []string{"kind"}}
	byScore := space.IndexDraft{Key: "by_score", Fields: []string{"score"}}
	builds1 := watchBuilds(t, sp1)
	lab1 := declareIndexLab(t, ctx, &sharedLab{sdk: sdk1, sp: sp1}, []space.IndexDraft{byKind}, []space.IndexDraft{byScore})
	builds1.awaitBuilt(t, ctx, 0, lab1.samples)
	obj := lab1.newObject(t, ctx)
	live := spreadSamples([]string{obj}, 30)
	lab1.putSamples(t, ctx, live...)
	recs := make([]space.RecordModify, 0, 10)
	for i := 0; i < 10; i++ {
		recs = append(recs, create(fmt.Sprintf("p%d", i), map[string]any{"label": fmt.Sprintf("p%d", i), "score": i}))
	}
	lab1.modify(t, ctx, obj, lab1.plain, recs...)
	plainColl := obj + "_" + lab1.plain
	require.Equal(t, map[string]int{storeName(byScore): 10}, storedIndexes(ctx, sdk1, plainColl))

	// Both indexes go from the store behind the SDK's back.
	for coll, name := range map[string]string{
		sp1.Id() + "_" + lab1.samples: storeName(byKind),
		plainColl:                     storeName(byScore),
	} {
		c, oerr := sdk1.Store().OpenCollection(ctx, coll)
		require.NoError(t, oerr)
		require.NoError(t, c.DropIndex(ctx, name))
		assert.Empty(t, storedIndexes(ctx, sdk1, coll))
	}
	spaceId := sp1.Id()
	require.NoError(t, sdk1.Close())

	sdk2, err := anysyncsdk.Open(ctx, cfg, provider)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk2.Close() })
	sp2, err := sdk2.Spaces().Get(ctx, spaceId)
	require.NoError(t, err)
	lab := &sharedLab{sdk: sdk2, sp: sp2, typeId: lab1.typeId, partId: lab1.partId,
		samples: lab1.samples, events: lab1.events, plain: lab1.plain}

	require.True(t, waitFor(ctx, 30*time.Second, 50*time.Millisecond, func() bool {
		return lab.sharedIndexes(ctx, lab.samples)[storeName(byKind)] == len(live)
	}), "the shared collection's index is rebuilt at open; stored %v", lab.sharedIndexes(ctx, lab.samples))
	plan := datasetPlan(t, ctx, sp2, lab.samples, `[{"$match": {"kind": "x"}}]`)
	assert.Equal(t, storeName(byKind), planIndex(plan), plan)
	assert.Equal(t, sortedKeys(rowsWhere(live, func(r labRow) bool { return r.kind == "x" }), false),
		lab.samplesWhere(t, ctx, map[string]any{"kind": "x"}))

	plan = objectPlan(t, ctx, sp2, obj, lab.plain, `[{"$match": {"score": {"$gte": 8}}}]`)
	assert.Equal(t, storeName(byScore), planIndex(plan), plan)
	assert.Equal(t, map[string]int{storeName(byScore): 10}, storedIndexes(ctx, sdk2, plainColl),
		"the object's collection gets its index back when next opened")
	rows, err := sp2.Query(obj, lab.plain).Filter(map[string]any{"score": map[string]any{"$gte": 8}}).Sort("score").All(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"p8", "p9"}, idsOfInitial(rows))
}

// TestE2E_DatasetIndexes_DataNotGated: an index definition does not
// hold back the data written after it. Rows device A writes right after
// adding indexes reach device B, and rows device B writes right after
// applying index definitions reach device A — for a shared and a
// per-object dataset alike.
func TestE2E_DatasetIndexes_DataNotGated(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	labA, labB := openIndexPair(t, ctx, "IndexNotGated", nil, nil)
	spA, spB := labA.sp, labB.sp

	o1, o2 := labA.newObject(t, ctx), labA.newObject(t, ctx)
	live := []labRow{sample(o1, "r1", "x", 1), sample(o1, "r2", "y", 2), sample(o2, "r1", "z", 3)}
	labA.putSamples(t, ctx, live...)
	labA.put(t, ctx, o1, labA.plain, "p1", map[string]any{"label": "p1", "score": 1})
	_ = spA.SyncHeads(ctx)
	plainIds := func(sp space.Space, obj string) []string {
		rows, err := sp.Query(obj, labA.plain).Sort("score").All(ctx)
		if err != nil {
			return nil
		}
		return idsOfInitial(rows)
	}
	// converged reports whether sp reads exactly rows in samples and
	// plain on each object.
	converged := func(sp space.Space, rows []labRow, plain map[string][]string) bool {
		got, err := sp.QueryDataset(labA.samples).All(ctx)
		if err != nil || !assert.ObjectsAreEqual(sortedKeys(rows, false), sortedKeys(rowsOf(got), false)) {
			return false
		}
		for obj, ids := range plain {
			if !assert.ObjectsAreEqual(ids, plainIds(sp, obj)) {
				return false
			}
		}
		return true
	}
	plain := map[string][]string{o1: {"p1"}, o2: {}}
	require.True(t, waitFor(ctx, 150*time.Second, 500*time.Millisecond, func() bool {
		_ = spB.SyncHeads(ctx)
		return converged(spB, live, plain)
	}), "device B must converge on the seed rows")

	defs := datasetDefs(t, ctx, spA, labA.typeId)
	samplesId, plainId := defs["samples"].Id, defs["plain"].Id
	addBoth := func(d space.IndexDraft) {
		t.Helper()
		_, err := spA.Types().AddDatasetIndex(ctx, labA.typeId, samplesId, d)
		require.NoError(t, err)
		_, err = spA.Types().AddDatasetIndex(ctx, labA.typeId, plainId, d)
		require.NoError(t, err)
	}

	// Device A adds indexes and writes at once.
	byLabel := space.IndexDraft{Key: "by_label", Fields: []string{"label"}}
	addBoth(byLabel)
	fromA := []labRow{sample(o1, "a1", "x", 10), sample(o2, "a2", "y", 11)}
	labA.putSamples(t, ctx, fromA...)
	labA.put(t, ctx, o2, labA.plain, "pa", map[string]any{"label": "pa", "score": 10})
	live = append(live, fromA...)
	plain[o2] = []string{"pa"}
	_ = spA.SyncHeads(ctx)
	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		_ = spB.SyncHeads(ctx)
		return converged(spB, live, plain)
	}), "device B must receive the rows written right after the index definitions")
	assertRows(t, labB.spaceRows(t, ctx, labA.samples), live)
	require.True(t, waitFor(ctx, 60*time.Second, 500*time.Millisecond, func() bool {
		_ = spB.SyncHeads(ctx)
		got, err := indexListing(ctx, spB, labA.typeId)
		return err == nil && assert.ObjectsAreEqual(mustListing(t, ctx, spA, labA.typeId), got)
	}), "device B must converge on the index definitions")

	// Device B writes as soon as it has applied the next definitions.
	byScore := space.IndexDraft{Key: "by_score", Fields: []string{"score"}}
	addBoth(byScore)
	_ = spA.SyncHeads(ctx)
	want := mustListing(t, ctx, spA, labA.typeId)
	require.True(t, waitFor(ctx, 120*time.Second, 100*time.Millisecond, func() bool {
		_ = spB.SyncHeads(ctx)
		got, err := indexListing(ctx, spB, labA.typeId)
		return err == nil && assert.ObjectsAreEqual(want, got)
	}), "device B must apply the second index definitions")
	fromB := []labRow{sample(o1, "b1", "z", 20), sample(o2, "b2", "x", 21)}
	labB.putSamples(t, ctx, fromB...)
	labB.put(t, ctx, o1, labA.plain, "pb", map[string]any{"label": "pb", "score": 20})
	live = append(live, fromB...)
	plain[o1] = []string{"p1", "pb"}
	_ = spB.SyncHeads(ctx)
	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		_ = spA.SyncHeads(ctx)
		return converged(spA, live, plain)
	}), "device A must receive the rows device B wrote right after applying the index definitions")
	assertRows(t, labA.spaceRows(t, ctx, labA.samples), live)

	// Both devices read the rows through the indexes.
	for _, sp := range []space.Space{spA, spB} {
		require.True(t, waitFor(ctx, 30*time.Second, 100*time.Millisecond, func() bool {
			p, err := sp.AggregateDataset(labA.samples, `[{"$match": {"score": {"$gte": 10}}}]`).Explain(ctx)
			return err == nil && planIndex(p) == storeName(byScore)
		}), "reads are planned on the shared index")
		rows, err := sp.QueryDataset(labA.samples).Filter(map[string]any{"score": map[string]any{"$gte": 10}}).Sort("score").All(ctx)
		require.NoError(t, err)
		assert.Equal(t, sortedKeys(rowsWhere(live, func(r labRow) bool { return r.score >= 10 }), false), idsOfInitial(rows))
		plan := objectPlan(t, ctx, sp, o1, labA.plain, `[{"$match": {"label": "pb"}}]`)
		assert.Equal(t, storeName(byLabel), planIndex(plan), plan)
	}
}

// rowsOf turns read rows back into labRows, enough for sortedKeys.
func rowsOf(rows []*anyenc.Value) []labRow {
	out := make([]labRow, 0, len(rows))
	for _, r := range rows {
		obj := r.GetString("_objectId")
		out = append(out, labRow{obj: obj, id: strings.TrimPrefix(r.GetString("id"), obj+"/"), score: int(r.GetFloat64("score"))})
	}
	return out
}
