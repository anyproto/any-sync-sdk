package schema

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// DeleteBy: none is no author gate: it needs no creator stamp, and it
// rides the schema document like the other non-default policy.
func TestDeleteByNone_DeclAndSchema(t *testing.T) {
	ds := Dataset{Fields: []Field{{Id: "b", MutableBy: MutableByAnyone}}, DeleteBy: DeleteByNone, IdRule: IdUser}
	require.NoError(t, ValidateDatasetDecl(ds))

	raw, err := json.Marshal(ds)
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, json.Unmarshal(raw, &doc))
	assert.Equal(t, "none", doc["x-delete-by"])
}
