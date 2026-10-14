package anysyncsdk

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/query"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/space"
)

// ReadClassification.TrackSelfAuthored: a verdict may opt the signer's
// own change out of born-read. Offline (local.yml is only a valid
// nodeconf shape; writes are local-first), single account — the
// scenario is precisely "my key wrote it, I have not seen it".

const (
	rsType    = "rs-chat-type"
	rsDataset = "rs_messages"
	rsAgent   = "agent"
	rsUnread  = "unread"
	rsCount   = "unreadCount"
)

func rsConfig(t *testing.T, dataDir string) config.Config {
	t.Helper()
	yaml, err := os.ReadFile("e2e/local.yml")
	if err != nil {
		t.Skipf("nodeconf not available: %v", err)
	}
	return config.Config{
		Storage: config.Storage{DataDir: dataDir, Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
		Types: []handler.Type{{
			Id:   rsType,
			Name: "RS Chat",
			Datasets: []handler.Dataset{{
				Name:        rsDataset,
				DataVersion: "rs-v1",
				Handler:     handler.DefaultHandler{},
				Schema: handler.Schema{
					Dynamic: true,
					Fields: []handler.Field{
						{Id: rsUnread, Name: "Unread", Schema: handler.Leaf(handler.PropertyKindBoolean), Scope: handler.ScopeLocal},
					},
				},
				ReadTracking: &handler.ReadTracking{
					// Creates track as "message"; one carrying the
					// create-only `agent` stamp counts unread for its own
					// author too.
					Classify: func(ctx *handler.ChangeCtx, rec *handler.RecordChange) handler.ReadClassification {
						if !rec.Upsert {
							return handler.ReadClassification{}
						}
						cl := handler.ReadClassification{Track: true, Tags: []string{"message"}}
						if ctx.Get != nil {
							cl.TrackSelfAuthored = ctx.Get(rsDataset, ctx.RecordId).GetBool(rsAgent)
						}
						return cl
					},
					Seed:          handler.ReadSeedAtFirstSight,
					RecordFlags:   map[string]string{"message": rsUnread},
					CounterFields: map[string]string{"message": rsCount},
				},
			}},
			Properties: []handler.PropertyDecl{
				{Id: rsCount, Name: "Unread Count", Kind: handler.PropertyKindNumber, Scope: handler.ScopeLocal},
			},
		}},
	}
}

func TestReadTracking_TrackSelfAuthored(t *testing.T) {
	dataDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sdk, err := Open(ctx, rsConfig(t, dataDir), wmProvider(t, dataDir))
	require.NoError(t, err)
	defer func() { _ = sdk.Close() }()
	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "RS"})
	require.NoError(t, err)
	objId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: rsType})
	require.NoError(t, err)

	send := func(id, text string, agent bool) {
		t.Helper()
		ops := []space.Op{{Type: space.OpSet, Path: "text", Value: text}}
		if agent {
			ops = append(ops, space.Op{Type: space.OpSet, Path: rsAgent, Value: true})
		}
		_, err := sp.Modify(ctx, space.ModifyBatch{
			ObjectId: objId, Dataset: rsDataset,
			Records: []space.RecordModify{{Id: id, Upsert: true, Ops: ops}},
		})
		require.NoError(t, err)
	}
	count := func() int {
		t.Helper()
		c, err := sp.ReadState().UnreadCounts(ctx, objId)
		require.NoError(t, err)
		return c["message"]
	}
	unreadFlag := func(id string) bool {
		doc, err := sp.Query(objId, rsDataset).Filter(query.Key{
			Path: []string{"id"}, Filter: query.NewComp(query.CompOpEq, id),
		}).One(ctx)
		return err == nil && doc.GetBool(rsUnread)
	}
	rowCount := func() int {
		row, err := sp.Properties().Get(ctx, objId)
		if err != nil || row == nil {
			return -1
		}
		return row.GetInt(rsType, rsCount)
	}

	// Own plain message: born read.
	send("h1", "hello", false)
	require.Equal(t, 0, count(), "own plain message is born read")

	// Own agent message: unread for its own author.
	send("a1", "answer", true)
	require.Equal(t, 1, count(), "own agent message tracks unread")
	snap, _, err := sp.ReadState().UnreadSnapshot(ctx, objId)
	require.NoError(t, err)
	require.Len(t, snap, 1)
	require.Equal(t, []string{"a1"}, snap[0].RecordIds)

	// A later own message must not advance the frontier past it.
	send("h2", "thanks", false)
	require.Equal(t, 1, count(), "own follow-up must not swallow the agent entry")

	// Flag and counter materialize on the record and the row.
	require.Eventually(t, func() bool {
		return unreadFlag("a1") && !unreadFlag("h1") && !unreadFlag("h2") && rowCount() == 1
	}, 15*time.Second, 50*time.Millisecond, "unread flag / unreadCount row property")

	// An ordinary mark clears it like any foreign entry.
	require.NoError(t, sp.ReadState().MarkReadUpTo(ctx, objId, ""))
	require.Equal(t, 0, count())
	require.Eventually(t, func() bool {
		return !unreadFlag("a1") && rowCount() == 0
	}, 15*time.Second, 50*time.Millisecond, "read-all clears flag and row property")
}
