package techspace_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/space"
)

func newCRDTVersionController(t *testing.T, onVersion func(int)) *crdt.Controller {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := anystore.Open(context.Background(), dbPath, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctrl, err := crdt.NewController(context.Background(), "index-1", db,
		crdt.HandlerReg{Name: techspace.CRDTVersionDataset, Handler: techspace.CRDTVersionHandler{OnVersion: onVersion}, Schema: techspace.CRDTVersionSchema()},
	)
	require.NoError(t, err)
	return ctrl
}

func crdtVersionChange(versionId crdt.VersionId, recId string, upsert bool, ops ...crdt.Op) crdt.Change {
	return crdt.Change{
		ObjectId:    "index-1",
		Dataset:     techspace.CRDTVersionDataset,
		ChangeId:    "ch-" + string(versionId),
		VersionId:   versionId,
		DataVersion: techspace.CRDTVersionHandlerVersion,
		Records:     []crdt.RecordChange{{Id: recId, Upsert: upsert, Ops: ops}},
	}
}

func setVersion(a *anyenc.Arena, v float64) crdt.Op {
	return crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldCRDTVersion}, Payload: a.NewNumberFloat64(v)}
}

func storedVersion(t *testing.T, ctrl *crdt.Controller) int {
	t.Helper()
	v := ctrl.Get(context.Background(), techspace.CRDTVersionDataset, techspace.CRDTVersionRecordId)
	require.NotNil(t, v)
	return int(v.GetFloat64(techspace.FieldCRDTVersion))
}

// The mark only goes up: a raise applies, a lower write is dropped on
// every replica, and the handler tells the service every version it
// admitted — including the one that exceeds what this SDK supports.
func TestCRDTVersion_Monotonic(t *testing.T) {
	var seen []int
	ctrl := newCRDTVersionController(t, func(v int) { seen = append(seen, v) })
	ctx := context.Background()
	a := &anyenc.Arena{}
	id := techspace.CRDTVersionRecordId

	require.NoError(t, ctrl.ApplyChange(ctx, crdtVersionChange("v1", id, true, setVersion(a, 1))))
	assert.Equal(t, 1, storedVersion(t, ctrl))

	require.NoError(t, ctrl.ApplyChange(ctx, crdtVersionChange("v2", id, true, setVersion(a, 3))))
	assert.Equal(t, 3, storedVersion(t, ctrl))

	// A concurrent older device's lower stamp: rejected, the stored
	// value stays at the maximum.
	err := ctrl.ApplyChange(ctx, crdtVersionChange("v3", id, true, setVersion(a, 2)))
	if err != nil {
		require.ErrorIs(t, err, crdt.ErrValidation)
	}
	assert.Equal(t, 3, storedVersion(t, ctrl))
	// Equal re-stamps are admitted (idempotent raise).
	require.NoError(t, ctrl.ApplyChange(ctx, crdtVersionChange("v4", id, true, setVersion(a, 3))))
	assert.Equal(t, 3, storedVersion(t, ctrl))

	assert.Equal(t, []int{1, 3, 3}, seen)
}

// Shape rules: one record id, a positive integer version, nothing
// else in the record, never deleted.
func TestCRDTVersion_Shape(t *testing.T) {
	ctrl := newCRDTVersionController(t, nil)
	ctx := context.Background()
	a := &anyenc.Arena{}
	id := techspace.CRDTVersionRecordId

	bad := []struct {
		name string
		ch   crdt.Change
	}{
		{"other id", crdtVersionChange("b1", "other", true, setVersion(a, 1))},
		{"zero", crdtVersionChange("b2", id, true, setVersion(a, 0))},
		{"fraction", crdtVersionChange("b3", id, true, setVersion(a, 1.5))},
		{"string", crdtVersionChange("b4", id, true, crdt.Op{Type: crdt.OpSet, Path: []string{techspace.FieldCRDTVersion}, Payload: a.NewString("1")})},
		{"extra field", crdtVersionChange("b5", id, true, crdt.Op{Type: crdt.OpSet, Path: []string{"writer"}, Payload: a.NewString("x")})},
		{"no version", crdtVersionChange("b6", id, true)},
	}
	for _, tc := range bad {
		err := ctrl.ApplyChange(ctx, tc.ch)
		if err != nil {
			require.ErrorIs(t, err, crdt.ErrValidation, tc.name)
		}
		assert.Nil(t, ctrl.Get(ctx, techspace.CRDTVersionDataset, tc.ch.Records[0].Id), tc.name)
	}

	// The multi-field create form (one object payload) is the other
	// accepted spelling.
	multi := a.NewObject()
	multi.Set(techspace.FieldCRDTVersion, a.NewNumberInt(2))
	require.NoError(t, ctrl.ApplyChange(ctx, crdtVersionChange("m1", id, true, crdt.Op{Type: crdt.OpSet, Payload: multi})))
	assert.Equal(t, 2, storedVersion(t, ctrl))

	err := ctrl.ApplyChange(ctx, crdt.Change{
		ObjectId: "index-1", Dataset: techspace.CRDTVersionDataset, ChangeId: "d1", VersionId: "d1",
		DataVersion: techspace.CRDTVersionHandlerVersion,
		Records:     []crdt.RecordChange{{Id: id, Ops: []crdt.Op{{Type: crdt.OpDelete}}}},
	})
	if err != nil {
		require.ErrorIs(t, err, crdt.ErrValidation)
	}
	assert.Equal(t, 2, storedVersion(t, ctrl), "the mark survives a delete")
}

// The service side without a network: the verdict on a stored mark and
// the write gate that follows a newer one.
func TestCRDTVersion_VerdictAndGate(t *testing.T) {
	var s techspace.Service
	assert.Nil(t, s.WriteGate())
	st := s.CRDTVersion()
	assert.Equal(t, space.CRDTVersion, st.Supported)
	assert.Equal(t, 0, st.Stored)
	assert.False(t, st.Newer)

	// An admitted version at or below the supported one: writable.
	techspace.CRDTVersionHandler{OnVersion: s.OnCRDTVersionForTest()}.
		BeforeCreateForTest(space.CRDTVersion)
	assert.Nil(t, s.WriteGate())

	// A newer one flips the account read-only, with the versions.
	techspace.CRDTVersionHandler{OnVersion: s.OnCRDTVersionForTest()}.
		BeforeCreateForTest(space.CRDTVersion + 2)
	err := s.WriteGate()
	require.Error(t, err)
	require.ErrorIs(t, err, space.ErrCRDTVersionNewer)
	var newer *space.CRDTVersionNewerError
	require.True(t, errors.As(err, &newer))
	assert.Equal(t, space.CRDTVersion+2, newer.Stored)
	assert.Equal(t, space.CRDTVersion, newer.Supported)
	assert.True(t, s.CRDTVersion().Newer)

	// The verdict an Open takes on a stored mark.
	write, err := techspace.CRDTVersionVerdictForTest(0, 1)
	require.NoError(t, err)
	assert.True(t, write, "absent mark is raised")
	write, err = techspace.CRDTVersionVerdictForTest(1, 1)
	require.NoError(t, err)
	assert.False(t, write, "equal mark is left alone")
	_, err = techspace.CRDTVersionVerdictForTest(2, 1)
	require.ErrorIs(t, err, space.ErrCRDTVersionNewer)
}
