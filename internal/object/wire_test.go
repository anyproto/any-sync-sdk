package object

import (
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
)

// TestCodec_Roundtrip exercises every field in the wire format.
func TestCodec_Roundtrip(t *testing.T) {
	a := &anyenc.Arena{}

	payloadObj := a.NewObject()
	payloadObj.Set("type", a.NewString("regular"))
	payloadObj.Set("name", a.NewString("My Space"))

	tagsArr := a.NewArray()
	tagsArr.SetArrayItem(0, a.NewString("personal"))

	in := crdt.Change{
		Dataset:     "spaces",
		DataVersion: "spaceIndexHandler-v1",
		TraceIds:    []string{"trace-A", "trace-B"},
		Records: []crdt.RecordChange{
			{
				Id:     "space-1",
				Upsert: true,
				Ops: []crdt.Op{
					{Type: crdt.OpSet, Payload: payloadObj},
					{Type: crdt.OpAddToSet, Path: []string{"tags"}, Payload: a.NewString("personal")},
				},
			},
			{
				Id: "space-2",
				Ops: []crdt.Op{
					{Type: crdt.OpUnset, Path: []string{"name"}},
				},
			},
		},
	}

	c := NewCodec()
	raw, err := c.Encode(&in)
	require.NoError(t, err)
	require.NotEmpty(t, raw)

	out, err := c.Decode(raw)
	require.NoError(t, err)

	assert.Equal(t, in.Dataset, out.Dataset)
	assert.Equal(t, in.DataVersion, out.DataVersion)
	assert.Equal(t, in.TraceIds, out.TraceIds)
	require.Len(t, out.Records, 2)

	// Record 0
	r0 := out.Records[0]
	assert.Equal(t, "space-1", r0.Id)
	assert.True(t, r0.Upsert)
	require.Len(t, r0.Ops, 2)
	assert.Equal(t, crdt.OpSet, r0.Ops[0].Type)
	require.NotNil(t, r0.Ops[0].Payload)
	assert.Equal(t, "regular", string(r0.Ops[0].Payload.GetStringBytes("type")))
	assert.Equal(t, crdt.OpAddToSet, r0.Ops[1].Type)
	assert.Equal(t, []string{"tags"}, r0.Ops[1].Path)
	require.NotNil(t, r0.Ops[1].Payload)
	assert.Equal(t, "personal", string(r0.Ops[1].Payload.GetStringBytes()))

	// Record 1
	r1 := out.Records[1]
	assert.Equal(t, "space-2", r1.Id)
	assert.False(t, r1.Upsert)
	require.Len(t, r1.Ops, 1)
	assert.Equal(t, crdt.OpUnset, r1.Ops[0].Type)
	assert.Equal(t, []string{"name"}, r1.Ops[0].Path)
}

func TestCodec_DecodeEmpty(t *testing.T) {
	c := NewCodec()
	_, err := c.Decode(nil)
	assert.ErrorIs(t, err, ErrEmptyPayload)
}
