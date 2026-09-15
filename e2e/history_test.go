package e2e

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/space"
)

// Version-history e2e (docs/version-history-proposal.md): a single
// offline device (dead loopback nodeconf, same trick as the p2p suite)
// writes a document-shaped dataset, then exercises the whole
// Space.History() surface — listing with filters and pagination,
// coalescing, ViewAt/RecordAt at intermediate versions, and diffs
// (range, per-change effect, record-scoped). Offline is the point:
// history is a purely local feature over the local DAG.

const (
	histDataset     = "hist_notes"
	histDataVersion = "hist_notes-v1"
	histSkipDataset = "hist_presence"
	histTypeId      = "hist-note-type"
)

func newHistoryType() handler.Type {
	return handler.Type{
		Id:   histTypeId,
		Name: "Hist Note",
		Datasets: []handler.Dataset{
			{
				Name:        histDataset,
				DataVersion: histDataVersion,
				Handler:     handler.DefaultHandler{},
				Schema:      handler.Schema{Dynamic: true},
			},
			{
				Name:        histSkipDataset,
				DataVersion: histSkipDataset + "-v1",
				Handler:     handler.DefaultHandler{},
				Schema:      handler.Schema{Dynamic: true},
				SkipHistory: true,
			},
		},
	}
}

func TestSDK_History(t *testing.T) {
	t.Parallel()
	yaml, err := loadLocalNetwork()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
		Types:   []handler.Type{newHistoryType()},
	}, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "History"})
	require.NoError(t, err)
	objId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: histTypeId})
	require.NoError(t, err)
	self := sdk.Account().Id()
	require.NotEmpty(t, self)

	write := func(recId string, upsert bool, path string, value any, traces ...string) space.Version {
		t.Helper()
		res, werr := sp.Modify(ctx, space.ModifyBatch{
			ObjectId: objId,
			Dataset:  histDataset,
			TraceIds: traces,
			Records: []space.RecordModify{{
				Id: recId, Upsert: upsert,
				Ops: []space.Op{{Type: space.OpSet, Path: path, Value: value}},
			}},
		})
		require.NoError(t, werr)
		require.NotEmpty(t, res.ChangeId)
		return res.ChangeId
	}

	// History: create n1, create n2, edit n1 (traced), edit n1 again,
	// delete n2.
	v1 := write("n1", true, "title", "first")
	v2 := write("n2", true, "title", "second")
	v3 := write("n1", false, "title", "renamed", "trace-ai-1")
	v4 := write("n1", false, "count", 42)
	delRes, err := sp.Delete(ctx, space.DeleteBatch{
		ObjectId: objId, Dataset: histDataset, RecordIds: []string{"n2"},
	})
	require.NoError(t, err)
	v5 := delRes.ChangeId
	require.NotEmpty(t, v5)

	hist := sp.History()

	// ---- ListChanges: descending, dataset filter ----
	list, err := hist.ListChanges(ctx, objId, space.HistoryFilter{Dataset: histDataset}, 0, "")
	require.NoError(t, err)
	require.Len(t, list.Changes, 5)
	assert.Empty(t, list.Cursor)
	assert.Equal(t, v5, list.Changes[0].Version) // newest first
	assert.Equal(t, v1, list.Changes[4].Version)
	for _, c := range list.Changes {
		assert.Equal(t, self, c.Author)
		assert.Equal(t, histDataset, c.Dataset)
		assert.Equal(t, 1, c.GroupSize)
	}
	require.Len(t, list.Changes[0].Touched, 1)
	assert.Equal(t, "n2", list.Changes[0].Touched[0].RecordId)
	assert.Equal(t, []string{"delete"}, list.Changes[0].Touched[0].Ops)

	// ---- pagination ----
	page1, err := hist.ListChanges(ctx, objId, space.HistoryFilter{Dataset: histDataset}, 2, "")
	require.NoError(t, err)
	require.Len(t, page1.Changes, 2)
	require.NotEmpty(t, page1.Cursor)
	page2, err := hist.ListChanges(ctx, objId, space.HistoryFilter{Dataset: histDataset}, 10, page1.Cursor)
	require.NoError(t, err)
	require.Len(t, page2.Changes, 3)
	assert.Equal(t, v3, page2.Changes[0].Version)

	// ---- record + trace filters ----
	recList, err := hist.ListChanges(ctx, objId, space.HistoryFilter{Dataset: histDataset, RecordId: "n1"}, 0, "")
	require.NoError(t, err)
	require.Len(t, recList.Changes, 3)
	assert.Equal(t, v4, recList.Changes[0].Version)
	assert.Equal(t, v1, recList.Changes[2].Version)

	traceList, err := hist.ListChanges(ctx, objId, space.HistoryFilter{TraceId: "trace-ai-1"}, 0, "")
	require.NoError(t, err)
	require.Len(t, traceList.Changes, 1)
	assert.Equal(t, v3, traceList.Changes[0].Version)
	assert.Equal(t, []string{"trace-ai-1"}, traceList.Changes[0].TraceIds)

	// ---- coalescing: the 5 rapid same-author writes group ----
	grouped, err := hist.ListChanges(ctx, objId,
		space.HistoryFilter{Dataset: histDataset, Coalesce: &space.CoalesceOpts{}}, 0, "")
	require.NoError(t, err)
	require.Len(t, grouped.Changes, 1)
	assert.Equal(t, v5, grouped.Changes[0].Version) // head = newest
	assert.Equal(t, 5, grouped.Changes[0].GroupSize)
	assert.Contains(t, grouped.Changes[0].TraceIds, "trace-ai-1")

	// ---- ViewAt: object as of v2 (before rename), then v4 ----
	viewV2, err := hist.ViewAt(ctx, objId, v2)
	require.NoError(t, err)
	defer viewV2.Close()
	n1, err := viewV2.Record(ctx, histDataset, "n1")
	require.NoError(t, err)
	require.NotNil(t, n1)
	assert.Equal(t, "first", string(n1.GetStringBytes("title")))
	assert.Nil(t, n1.Get("count"))
	live, err := viewV2.Records(ctx, histDataset)
	require.NoError(t, err)
	assert.Len(t, live, 2) // n1 + n2 both alive at v2

	viewV4, err := hist.ViewAt(ctx, objId, v4)
	require.NoError(t, err)
	defer viewV4.Close()
	n1v4, err := viewV4.Record(ctx, histDataset, "n1")
	require.NoError(t, err)
	require.NotNil(t, n1v4)
	assert.Equal(t, "renamed", string(n1v4.GetStringBytes("title")))
	assert.Equal(t, 42, n1v4.GetInt("count"))

	// ---- RecordAt (fast path) matches ViewAt ----
	recV2, err := hist.RecordAt(ctx, objId, histDataset, "n1", v2)
	require.NoError(t, err)
	require.NotNil(t, recV2)
	assert.Equal(t, n1.String(), recV2.String())

	// absent record at early cut; tombstone at the delete cut
	recMissing, err := hist.RecordAt(ctx, objId, histDataset, "n2", v1)
	require.NoError(t, err)
	assert.Nil(t, recMissing)
	recDead, err := hist.RecordAt(ctx, objId, histDataset, "n2", v5)
	require.NoError(t, err)
	require.NotNil(t, recDead)
	assert.NotNil(t, recDead.Get("_deletedAt"))

	// ---- Diff v2..v5: n1 changed (title + count), n2 deleted ----
	diff, err := hist.Diff(ctx, objId, v2, v5, space.DiffFilter{Dataset: histDataset})
	require.NoError(t, err)
	require.Len(t, diff.Datasets, 1)
	kinds := map[string]space.DiffKind{}
	var n1Fields []space.FieldDiff
	for _, rd := range diff.Datasets[0].Records {
		kinds[rd.Id] = rd.Kind
		if rd.Id == "n1" {
			n1Fields = rd.Fields
		}
	}
	assert.Equal(t, space.DiffChanged, kinds["n1"])
	assert.Equal(t, space.DiffDeleted, kinds["n2"])
	fieldPaths := map[string]space.FieldDiff{}
	for _, fd := range n1Fields {
		require.NotEmpty(t, fd.Path)
		fieldPaths[fd.Path[0]] = fd
	}
	require.Contains(t, fieldPaths, "title")
	assert.Equal(t, `"first"`, fieldPaths["title"].Before.String())
	assert.Equal(t, `"renamed"`, fieldPaths["title"].After.String())
	require.Contains(t, fieldPaths, "count")
	assert.Nil(t, fieldPaths["count"].Before)

	// ---- per-change effect diff (base = "") ----
	effect, err := hist.Diff(ctx, objId, "", v3, space.DiffFilter{Dataset: histDataset})
	require.NoError(t, err)
	require.Len(t, effect.Datasets, 1)
	require.Len(t, effect.Datasets[0].Records, 1)
	assert.Equal(t, "n1", effect.Datasets[0].Records[0].Id)
	assert.Equal(t, space.DiffChanged, effect.Datasets[0].Records[0].Kind)

	// record-scoped diff
	recDiff, err := hist.Diff(ctx, objId, v1, v4, space.DiffFilter{Dataset: histDataset, RecordIds: []string{"n1"}})
	require.NoError(t, err)
	require.Len(t, recDiff.Datasets, 1)
	require.Len(t, recDiff.Datasets[0].Records, 1)

	// ---- SkipHistory dataset stays invisible ----
	_, err = sp.Modify(ctx, space.ModifyBatch{
		ObjectId: objId,
		Dataset:  histSkipDataset,
		Records: []space.RecordModify{{
			Id: "p1", Upsert: true,
			Ops: []space.Op{{Type: space.OpSet, Path: "state", Value: "online"}},
		}},
	})
	require.NoError(t, err)
	skipList, err := hist.ListChanges(ctx, objId, space.HistoryFilter{Dataset: histSkipDataset}, 0, "")
	require.NoError(t, err)
	assert.Empty(t, skipList.Changes)

	// RecordAt on the SkipHistory dataset must take the decode-and-
	// filter fallback even when an indexed dataset shares the record id
	// (regression: the index pre-filter fed the other dataset's change
	// set and the replay returned nil for an existing record).
	collideRes, err := sp.Modify(ctx, space.ModifyBatch{
		ObjectId: objId,
		Dataset:  histSkipDataset,
		Records: []space.RecordModify{{
			Id: "n1", Upsert: true, // "n1" also exists in histDataset
			Ops: []space.Op{{Type: space.OpSet, Path: "state", Value: "away"}},
		}},
	})
	require.NoError(t, err)
	recSkip, err := hist.RecordAt(ctx, objId, histSkipDataset, "n1", collideRes.ChangeId)
	require.NoError(t, err)
	require.NotNil(t, recSkip, "skip-history record must reconstruct via fallback replay")
	assert.Equal(t, "away", string(recSkip.GetStringBytes("state")))

	// ---- unknown version ----
	_, err = hist.ViewAt(ctx, objId, "not-a-change-id")
	require.Error(t, err)
	assert.True(t, errors.Is(err, space.ErrVersionNotFound))
}

// TestSDK_HistoryDiffValues locks the exact Before/After bytes of a
// multi-change Diff. Regression for the DiffRange double-walk bug:
// objecttree caches decoded models whose op payloads alias the codec's
// parser arena, so a second tree walk applied delta changes with bytes
// later decodes had overwritten — diffs came back with another
// record's values and no error. Needs a delta of several changes with
// distinctive payloads; the surrounding TestSDK_History deltas are too
// small to shift the arena.
func TestSDK_HistoryDiffValues(t *testing.T) {
	t.Parallel()
	yaml, err := loadLocalNetwork()
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
		Types:   []handler.Type{newHistoryType()},
	}, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })

	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "DiffValues"})
	require.NoError(t, err)
	objId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: histTypeId})
	require.NoError(t, err)

	var versions []space.Version
	write := func(rec string, upsert bool, val string) {
		res, werr := sp.Modify(ctx, space.ModifyBatch{
			ObjectId: objId, Dataset: histDataset,
			Records: []space.RecordModify{{Id: rec, Upsert: upsert,
				Ops: []space.Op{{Type: space.OpSet, Path: "text", Value: val}}}},
		})
		require.NoError(t, werr)
		versions = append(versions, res.ChangeId)
	}
	for i := 0; i < 4; i++ {
		write(fmt.Sprintf("r%d", i), true, fmt.Sprintf("initial-%d", i))
	}
	base := versions[len(versions)-1]
	// 8 distinctive edits round-robin over the records: each record's
	// final value comes from a different arena generation than its
	// diff-time read, so any stale-alias regression changes the bytes.
	want := map[string]string{}
	for i := 0; i < 8; i++ {
		rec := fmt.Sprintf("r%d", i%4)
		val := fmt.Sprintf("payload-distinct-%02d-abcdefghijklmnopqrstuvwxyz", i)
		write(rec, false, val)
		want[rec] = val
	}
	head := versions[len(versions)-1]

	diff, err := sp.History().Diff(ctx, objId, base, head, space.DiffFilter{Dataset: histDataset})
	require.NoError(t, err)
	require.Len(t, diff.Datasets, 1)
	require.Len(t, diff.Datasets[0].Records, 4)
	for _, rd := range diff.Datasets[0].Records {
		require.Len(t, rd.Fields, 1)
		fd := rd.Fields[0]
		require.Equal(t, []string{"text"}, fd.Path)
		require.NotNil(t, fd.Before)
		require.NotNil(t, fd.After)
		assert.Equal(t, fmt.Sprintf(`"initial-%s"`, rd.Id[1:]), fd.Before.String(), "record %s Before", rd.Id)
		assert.Equal(t, fmt.Sprintf("%q", want[rd.Id]), fd.After.String(), "record %s After", rd.Id)
	}
}
