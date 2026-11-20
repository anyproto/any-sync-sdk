package subscribe

import (
	"errors"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/space"
)

// A delete the handler refused leaves the record live: the event keeps
// its slot with no id, and a subscription keeps holding it. Later
// records still read their own post values, and a delete that landed in
// the same change still removes its record.
func TestBuildEvent_RejectedDeleteKeepsRecord(t *testing.T) {
	eng := New("test")
	defer eng.Close()
	arena := newArena()
	kept, edited, gone := makeRow(arena, "kept", 1, nil), makeRow(arena, "edited", 2, nil), makeRow(arena, "gone", 3, nil)
	sub := subscribeSorted(t, eng, Scope{ObjectId: "obj1", Dataset: "chat"}, nil, 5, []row{kept, edited, gone})

	del := []crdt.Op{{Type: crdt.OpDelete}}
	setTitle := arena.NewObject()
	setTitle.Set("title", arena.NewString("new"))
	ch := &crdt.Change{
		SpaceId: "test", ObjectId: "obj1", Dataset: "chat", VersionId: "v1",
		Records: []crdt.RecordChange{
			{Id: "kept", Ops: del},
			{Id: "edited", Ops: []crdt.Op{{Type: crdt.OpSet, Payload: setTitle}}},
			{Id: "gone", Ops: del},
		},
	}
	rejections := []crdt.OpRejection{{RecordIndex: 0, OpIndex: -1, RecordId: "kept", Err: errors.New("not the author")}}
	editedPost := makeRow(arena, "edited", 2, map[string]any{"title": "new"}).d
	post := []*anyenc.Value{kept.d, editedPost, nil}
	postValue := func(i int) (*anyenc.Value, error) { return post[i], nil }

	ev := BuildEvent(ch, []string{"kept", "edited", "gone"}, nil, rejections, postValue)
	require.Len(t, ev.Records, 3)
	assert.Empty(t, ev.Records[0].Id, "the refused delete keeps its slot without an id")
	assert.Equal(t, "edited", ev.Records[1].Id)
	assert.Equal(t, EventRecord{Id: "gone", Deleted: true}, ev.Records[2])

	eng.OnApply(ev, postValue)
	got, err := waitOne(t, sub)
	require.NoError(t, err)
	assert.Equal(t, []space.RemovedRecord{{Id: "gone", Reason: space.RemoveDeleted}}, got.Removed)
	require.Len(t, got.Updated, 1)
	assert.Equal(t, "edited", got.Updated[0].Id)
	assert.Equal(t, "new", got.Updated[0].Doc.GetString("title"), "the edited record carries its own post value")
}
