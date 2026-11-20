package spaceimpl

import (
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"

	typetype "github.com/anyproto/any-sync-sdk/internal/types/type"
	"github.com/anyproto/any-sync-sdk/space"
)

// The head record carries every non-default delete policy: a dataset
// installed without its policy would be pinned anyone for good.
func TestEncodeDatasetHead_DeletePolicy(t *testing.T) {
	arena := &anyenc.Arena{}
	for _, tc := range []struct {
		policy space.DeletePolicy
		want   string
	}{
		{space.DeleteByAnyone, ""},
		{space.DeleteByAuthor, "author"},
		{space.DeleteByNone, "none"},
	} {
		head := encodeDatasetHead(arena, "part-1", &space.DatasetDraft{Key: "entries", DeleteBy: tc.policy}, false)
		assert.Equal(t, tc.want, string(head.GetStringBytes(typetype.DefFieldDeleteBy)), "policy %v", tc.policy)
	}
}

// A module-served dataset keeps its module's delete rule: a policy on
// the draft is refused, never written to the head and ignored.
func TestDraftToDecl_ModuleRefusesDeletePolicy(t *testing.T) {
	for _, module := range []string{"editor", "chat"} {
		_, err := draftToDecl(&space.DatasetDraft{Key: "notes", Module: module, DeleteBy: space.DeleteByNone})
		assert.ErrorIs(t, err, space.ErrModuleOwned, module)
		_, err = draftToDecl(&space.DatasetDraft{Key: "notes", Module: module})
		assert.NoError(t, err, module)
	}
}
