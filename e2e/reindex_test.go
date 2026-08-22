package e2e

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/space"
)

const (
	reindexTypeId  = "reindex-notes-type"
	reindexDataset = "reindex_notes"
	reindexDataVer = "reindex-notes-v1"
	reindexRecA    = "rec-a"
	reindexRecB    = "rec-b"
)

// stampHandler derives a per-record `stamp` carrying the handler's tag,
// so a rebuild is directly observable: rows written by the old handler
// carry the old tag until the object is replayed through the new one.
// applies counts BeforeCreate calls — zero on a load that replays
// nothing.
type stampHandler struct {
	tag     string
	applies atomic.Int64
}

func (h *stampHandler) Init(context.Context) error { return nil }

func (h *stampHandler) BeforeCreate(_ *handler.ChangeCtx, _ *handler.RecordChange, sink *handler.Sink) error {
	h.applies.Add(1)
	a := &anyenc.Arena{}
	sink.Derive(handler.Op{Type: handler.OpSet, Path: []string{"stamp"}, Payload: a.NewString(h.tag)})
	return nil
}

func (h *stampHandler) BeforeModify(_ *handler.ChangeCtx, _ *handler.RecordChange, _ *handler.Op, _ *handler.Sink) error {
	return nil
}

func (h *stampHandler) BeforeDelete(_ *handler.ChangeCtx, _ *handler.RecordChange, _ *handler.Sink) error {
	return nil
}

func reindexType(h *stampHandler, version int) handler.Type {
	return handler.Type{
		Id:   reindexTypeId,
		Name: "Reindex notes",
		Datasets: []handler.Dataset{{
			Name:           reindexDataset,
			DataVersion:    reindexDataVer,
			Handler:        h,
			HandlerVersion: version,
			Schema: handler.Schema{
				Fields: []handler.Field{
					{Id: "text", Name: "Text", Schema: handler.Leaf(handler.PropertyKindString), Scope: handler.ScopeSynced},
					{Id: "stamp", Name: "Stamp", Schema: handler.Leaf(handler.PropertyKindString), Scope: handler.ScopeDerived},
					{Id: "pinned", Name: "Pinned", Schema: handler.Leaf(handler.PropertyKindBoolean), Scope: handler.ScopeLocal},
				},
			},
		}},
	}
}

// TestE2E_ReindexOnHandlerVersionBump: a handler whose compiled-in logic
// changed rebuilds every object it materialized, and nothing else.
//
// Three boots on one data dir:
//  1. HandlerVersion 1, tag "v1" — write two records, flag one locally.
//  2. HandlerVersion 2, tag "v2" — the bump wipes and replays the tree,
//     so both records carry the new stamp; the synced text is unchanged
//     and the device-local flag (which never entered the DAG, so no
//     replay can reproduce it) survives the wipe.
//  3. HandlerVersion 2, tag "v3" — logic changed WITHOUT a version bump:
//     no rebuild, rows keep the v2 stamp, and the handler never fires.
func TestE2E_ReindexOnHandlerVersionBump(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	dir := t.TempDir()
	provider := newFixedSeedProvider(t)
	open := func(h *stampHandler, version int) *anysyncsdk.SDK {
		t.Helper()
		sdk, oerr := anysyncsdk.Open(ctx, config.Config{
			Storage: config.Storage{DataDir: dir, Topology: config.StorageShared},
			Network: config.Network{NodeConfYAML: yaml},
			Types:   []handler.Type{reindexType(h, version)},
		}, provider)
		require.NoError(t, oerr, "Open")
		return sdk
	}

	// ---- Boot 1: write through handler v1 ----
	h1 := &stampHandler{tag: "v1"}
	sdk1 := open(h1, 1)

	sp1, err := sdk1.Spaces().Create(ctx, space.CreateRequest{Name: "Reindex"})
	if err != nil {
		if isNoNetworkErr(err) {
			_ = sdk1.Close()
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("Spaces().Create: %v", err)
	}
	spaceId := sp1.Id()

	objId, err := sp1.Objects().Create(ctx, space.CreateObjectOpts{Types: []string{reindexTypeId}})
	require.NoError(t, err, "Objects().Create")

	for _, id := range []string{reindexRecA, reindexRecB} {
		_, err = sp1.Modify(ctx, space.ModifyBatch{
			ObjectId: objId,
			Dataset:  reindexDataset,
			Records: []space.RecordModify{{
				Id:     id,
				Upsert: true,
				Ops:    []space.Op{{Type: space.OpSet, Path: "text", Value: "text of " + id}},
			}},
		})
		require.NoError(t, err, "seed %s", id)
	}
	res, err := sp1.Modify(ctx, space.ModifyBatch{
		ObjectId: objId,
		Dataset:  reindexDataset,
		Scope:    space.ScopeLocal,
		Records: []space.RecordModify{{
			Id:  reindexRecA,
			Ops: []space.Op{{Type: space.OpSet, Path: "pinned", Value: true}},
		}},
	})
	require.NoError(t, err, "local-scope write")
	require.Empty(t, res.Rejections)

	readRecords := func(sp space.Space) map[string]*anyenc.Value {
		out := map[string]*anyenc.Value{}
		recs, qerr := sp.Query(objId, reindexDataset).All(ctx)
		require.NoError(t, qerr)
		for _, rec := range recs {
			out[string(rec.GetStringBytes("id"))] = rec
		}
		return out
	}

	rows := readRecords(sp1)
	require.Len(t, rows, 2)
	assert.Equal(t, "v1", rows[reindexRecA].GetString("stamp"))
	assert.True(t, rows[reindexRecA].GetBool("pinned"))
	require.NoError(t, sdk1.Close())

	// ---- Boot 2: the version bump rebuilds the object ----
	h2 := &stampHandler{tag: "v2"}
	sdk2 := open(h2, 2)
	sp2, err := sdk2.Spaces().Get(ctx, spaceId)
	require.NoError(t, err, "Spaces().Get after bump")

	rows = readRecords(sp2)
	require.Len(t, rows, 2, "the replay must rebuild every record")
	for _, id := range []string{reindexRecA, reindexRecB} {
		assert.Equal(t, "v2", rows[id].GetString("stamp"), "%s rebuilt by the current handler", id)
		assert.Equal(t, "text of "+id, rows[id].GetString("text"), "%s synced content survives", id)
	}
	assert.True(t, rows[reindexRecA].GetBool("pinned"),
		"device-local state never entered the DAG — the rebuild must carry it across")
	assert.Nil(t, rows[reindexRecB].Get("pinned"), "records without a local value gain none")
	assert.Positive(t, h2.applies.Load(), "the tree was replayed through the new handler")
	require.NoError(t, sdk2.Close())

	// ---- Boot 3: changed logic, unchanged version — no rebuild ----
	h3 := &stampHandler{tag: "v3"}
	sdk3 := open(h3, 2)
	sp3, err := sdk3.Spaces().Get(ctx, spaceId)
	require.NoError(t, err, "Spaces().Get without a bump")

	rows = readRecords(sp3)
	require.Len(t, rows, 2)
	assert.Equal(t, "v2", rows[reindexRecA].GetString("stamp"),
		"an unchanged HandlerVersion must not rebuild — that is the whole point of the counter")
	assert.True(t, rows[reindexRecA].GetBool("pinned"))
	assert.Zero(t, h3.applies.Load(), "nothing replays on a load with no version mismatch")
	require.NoError(t, sdk3.Close())
}
