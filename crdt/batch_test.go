package crdt

import (
	"fmt"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/lexid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBatch_ManyChangesOneTransaction applies 200 changes inside a single
// WriteTx and verifies all records land atomically.
func TestBatch_ManyChangesOneTransaction(t *testing.T) {
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "batch.db"), nil)
	require.NoError(t, err)
	defer db.Close()
	metaColl, err := db.Collection(ctx, MetaCollectionName)
	require.NoError(t, err)

	ctrl, err := NewController(ctx, "obj1", db, DefaultHandler{DatasetName: testDS})
	require.NoError(t, err)

	lx := lexid.Must(lexid.CharsAllNoEscape, 4, 100)
	var prev string
	arena := &anyenc.Arena{}

	// Batch: one outer WriteTx, 200 changes.
	tx, err := db.WriteTx(ctx)
	require.NoError(t, err)
	txCtx := tx.Context()

	const N = 200
	for i := 0; i < N; i++ {
		prev = lx.Next(prev)
		ch := Change{
			ObjectId:    "obj1",
			Dataset:     testDS,
			ChangeId:    fmt.Sprintf("ch%d", i),
			VersionId:   VersionId(prev),
			DataVersion: testDataVersion,
			AddSeq:      uint64(i + 1),
			Records: []RecordChange{{
				Id:     fmt.Sprintf("r%d", i),
				Upsert: true,
				Ops: []Op{{
					Type:    OpSet,
					Payload: recordPayload(arena, map[string]any{"idx": i}),
				}},
			}},
		}
		require.NoError(t, ctrl.ApplyChange(txCtx, ch))
	}

	// Persist meta inside the same tx.
	require.NoError(t, ctrl.PersistMeta(txCtx, metaColl))
	require.NoError(t, tx.Commit())

	// Verify all records exist.
	for i := 0; i < N; i++ {
		rec := ctrl.Get(ctx, testDS, fmt.Sprintf("r%d", i))
		require.NotNilf(t, rec, "r%d missing", i)
		assert.Equal(t, i, rec.GetInt("idx"))
	}
	assert.Equal(t, uint64(N), ctrl.MaxAddSeq())

	// Verify meta persisted.
	seq, _, err := LoadMeta(ctx, metaColl, "obj1")
	require.NoError(t, err)
	assert.Equal(t, uint64(N), seq)
}

// TestBatch_RollbackLeavesDBClean verifies that a rolled-back batch
// leaves zero trace in the database.
func TestBatch_RollbackLeavesDBClean(t *testing.T) {
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "rollback.db"), nil)
	require.NoError(t, err)
	defer db.Close()

	ctrl, err := NewController(ctx, "obj1", db, DefaultHandler{DatasetName: testDS})
	require.NoError(t, err)
	arena := &anyenc.Arena{}

	// Begin a tx, apply some changes, then rollback.
	tx, err := db.WriteTx(ctx)
	require.NoError(t, err)
	txCtx := tx.Context()

	require.NoError(t, ctrl.ApplyChange(txCtx, Change{
		ObjectId: "obj1", Dataset: testDS, DataVersion: testDataVersion,
		ChangeId: "ch1", VersionId: "v1", AddSeq: 1,
		Records: []RecordChange{{
			Id: "r1", Upsert: true,
			Ops: []Op{{Type: OpSet, Payload: recordPayload(arena, map[string]any{"name": "ghost"})}},
		}},
	}))
	require.NoError(t, tx.Rollback())

	// Record should NOT exist after rollback.
	assert.Nil(t, ctrl.Get(ctx, testDS, "r1"))
}

// TestBatch_ReopenPersistsState verifies that records and metadata survive
// a DB close + reopen cycle.
func TestBatch_ReopenPersistsState(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "reopen.db")
	arena := &anyenc.Arena{}

	// Phase 1: write data.
	{
		db, err := anystore.Open(ctx, dbPath, nil)
		require.NoError(t, err)
		metaColl, err := db.Collection(ctx, MetaCollectionName)
		require.NoError(t, err)

		ctrl, err := NewController(ctx, "obj1", db, DefaultHandler{DatasetName: testDS})
		require.NoError(t, err)

		ch := Change{
			ObjectId: "obj1", Dataset: testDS, DataVersion: testDataVersion,
			ChangeId: "ch1", VersionId: "v1", AddSeq: 42,
			Records: []RecordChange{{
				Id: "r1", Upsert: true,
				Ops: []Op{{Type: OpSet, Payload: recordPayload(arena, map[string]any{"name": "persistent"})}},
			}},
		}
		require.NoError(t, ctrl.ApplyChange(ctx, ch))
		require.NoError(t, ctrl.PersistMeta(ctx, metaColl))
		require.NoError(t, db.Close())
	}

	// Phase 2: reopen and verify.
	{
		db, err := anystore.Open(ctx, dbPath, nil)
		require.NoError(t, err)
		defer db.Close()
		metaColl, err := db.Collection(ctx, MetaCollectionName)
		require.NoError(t, err)

		ctrl, err := NewController(ctx, "obj1", db, DefaultHandler{DatasetName: testDS})
		require.NoError(t, err)

		// Load metadata.
		_, err = ctrl.LoadAndSeedMeta(ctx, metaColl)
		require.NoError(t, err)
		assert.Equal(t, uint64(42), ctrl.MaxAddSeq())

		// Record survives.
		rec := ctrl.Get(ctx, testDS, "r1")
		require.NotNil(t, rec)
		assert.Equal(t, "persistent", rec.GetString("name"))
		assert.Equal(t, VersionId("v1"), GetRecordVersion(rec, "name"))
	}
}
