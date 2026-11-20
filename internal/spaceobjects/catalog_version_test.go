package spaceobjects

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/schema"
	"github.com/anyproto/any-sync-sdk/internal/types"
)

// A runtime records dataset's registration version composes the layout
// bits its rows were built with: shared rows live per space, and a
// Dynamic dataset's additive fields are free keyspace at apply. An
// object stamped without a bit rebuilds; a plain per-object non-Dynamic
// dataset keeps the bare handler version.
func TestInstanceReg_VersionCarriesLayoutBits(t *testing.T) {
	s := &Store{}
	reg := func(dynamic, shared bool) crdt.HandlerReg {
		t.Helper()
		ds := types.CompiledDataset{
			Key: "k", Name: "t_k", Module: types.RecordsModule, TypeId: "t", Shared: shared,
			Schema: schema.Dataset{Dynamic: dynamic, Fields: []schema.Field{{Id: "a", Schema: schema.Leaf(schema.KindString)}}},
		}
		r, err := s.instanceReg(ds)
		require.NoError(t, err)
		return r
	}
	assert.Equal(t, crdt.SchemaHandlerVersion, reg(false, false).Version)
	assert.Equal(t, crdt.ComposeVersion(crdt.SchemaHandlerVersion, dynamicAdditiveVersion), reg(true, false).Version)
	assert.Equal(t, crdt.ComposeVersion(crdt.SchemaHandlerVersion, sharedLayoutVersion), reg(false, true).Version)
	assert.Equal(t, crdt.ComposeVersion(crdt.SchemaHandlerVersion, sharedLayoutVersion|dynamicAdditiveVersion), reg(true, true).Version)
}
