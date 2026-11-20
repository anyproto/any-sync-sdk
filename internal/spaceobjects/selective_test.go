package spaceobjects

import (
	"errors"
	"testing"

	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	"github.com/anyproto/any-sync/commonspace/object/tree/treestorage"
	"github.com/anyproto/any-sync/util/cidutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rawRoot builds a cid-valid raw root change for the given changeType.
func rawRoot(t *testing.T, changeType string) *treechangeproto.RawTreeChangeWithId {
	t.Helper()
	root := &treechangeproto.RootChange{
		ChangeType: changeType,
		SpaceId:    "space",
	}
	payload, err := root.MarshalVT()
	require.NoError(t, err)
	raw := &treechangeproto.RawTreeChange{Payload: payload}
	rawBytes, err := raw.MarshalVT()
	require.NoError(t, err)
	id, err := cidutil.NewCidFromBytes(rawBytes)
	require.NoError(t, err)
	return &treechangeproto.RawTreeChangeWithId{RawChange: rawBytes, Id: id}
}

func selectiveStore(types ...string) *Store {
	s := &Store{spaceId: "space"}
	if len(types) > 0 {
		s.selective = make(map[string]struct{}, len(types))
		for _, tp := range types {
			s.selective[tp] = struct{}{}
		}
	}
	return s
}

func TestParseVerifiedRoot(t *testing.T) {
	raw := rawRoot(t, "payloads")
	root, err := parseVerifiedRoot(raw)
	require.NoError(t, err)
	assert.Equal(t, "payloads", root.ChangeType)

	t.Run("nil and empty roots rejected", func(t *testing.T) {
		_, err := parseVerifiedRoot(nil)
		assert.Error(t, err)
		_, err = parseVerifiedRoot(&treechangeproto.RawTreeChangeWithId{Id: "x"})
		assert.Error(t, err)
	})

	t.Run("cid mismatch rejected — type can't be spoofed", func(t *testing.T) {
		forged := &treechangeproto.RawTreeChangeWithId{
			RawChange: raw.RawChange,
			Id:        "bafyforgedid",
		}
		_, err := parseVerifiedRoot(forged)
		assert.Error(t, err)
	})
}

func TestSelectiveTreeValidator_ProbeSelected(t *testing.T) {
	s := selectiveStore("payloads")
	raw := rawRoot(t, "payloads")

	// A probe response for a selected type carries no change bodies —
	// the validator must demand a full re-fetch, not accept or skip.
	v := s.selectiveTreeValidator(true)
	_, err := v(treestorage.TreeStorageCreatePayload{
		RootRawChange: raw,
		Heads:         []string{"headA"},
	}, nil, nil)
	assert.True(t, errors.Is(err, errProbeSelectedType), "got %v", err)

	// Unverifiable root: rejected outright, nothing persisted.
	forged := &treechangeproto.RawTreeChangeWithId{RawChange: raw.RawChange, Id: "bafyforged"}
	_, err = v(treestorage.TreeStorageCreatePayload{RootRawChange: forged}, nil, nil)
	assert.Error(t, err)
	assert.False(t, errors.Is(err, errProbeSelectedType))
}

func TestSelectiveModeOff(t *testing.T) {
	s := selectiveStore()
	assert.False(t, s.SelectiveMode())
	// PullFilter decision without selective mode: always pull; must not
	// touch the app/space (nil here — a panic would fail the test).
	assert.True(t, s.ShouldPullTree(t.Context(), "tree", nil, nil))
	skipped, err := s.IsTreeSkipped(t.Context(), "tree")
	require.NoError(t, err)
	assert.False(t, skipped)
}

func TestShouldPullTree_Selected(t *testing.T) {
	s := selectiveStore("payloads")
	// Selected type: pull, no skip bookkeeping (app is nil — any write
	// attempt would panic).
	assert.True(t, s.ShouldPullTree(t.Context(), "tree", rawRoot(t, "payloads"), []string{"h"}))
	// Unverifiable root: pull and let the fetch validator decide.
	assert.True(t, s.ShouldPullTree(t.Context(), "tree", nil, nil))
	forged := &treechangeproto.RawTreeChangeWithId{RawChange: []byte("junk"), Id: "bafyforged"}
	assert.True(t, s.ShouldPullTree(t.Context(), "tree", forged, nil))
}
