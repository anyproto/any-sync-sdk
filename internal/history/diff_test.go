package history

import (
	"strings"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func val(t *testing.T, json string) *anyenc.Value {
	t.Helper()
	v, err := anyenc.ParseJson(json)
	require.NoError(t, err)
	return v
}

func pathStr(f FieldDiff) string { return strings.Join(f.Path, ".") }

func TestDiffRecordsEqual(t *testing.T) {
	a := val(t, `{"id":"r1","name":"x","n":1}`)
	b := val(t, `{"id":"r1","name":"x","n":1}`)
	assert.Nil(t, DiffRecords("r1", a, b))
}

func TestDiffRecordsIgnoresBookkeeping(t *testing.T) {
	a := val(t, `{"id":"r1","name":"x","_ver":{"*":"aaa"},"_traces":{"t1":"v1"},"_applySeq":1,"_addSeq":10}`)
	b := val(t, `{"id":"r1","name":"x","_ver":{"*":"bbb"},"_traces":{"t2":"v2"},"_applySeq":9,"_addSeq":99}`)
	assert.Nil(t, DiffRecords("r1", a, b))
}

func TestDiffRecordsChanged(t *testing.T) {
	a := val(t, `{"id":"r1","name":"x","keep":true,"gone":1}`)
	b := val(t, `{"id":"r1","name":"y","keep":true,"new":2}`)
	d := DiffRecords("r1", a, b)
	require.NotNil(t, d)
	assert.Equal(t, KindChanged, d.Kind)

	byPath := map[string]FieldDiff{}
	for _, f := range d.Fields {
		byPath[pathStr(f)] = f
	}
	require.Len(t, byPath, 3)

	assert.Equal(t, "\"x\"", byPath["name"].Before.String())
	assert.Equal(t, "\"y\"", byPath["name"].After.String())
	assert.NotNil(t, byPath["gone"].Before)
	assert.Nil(t, byPath["gone"].After)
	assert.Nil(t, byPath["new"].Before)
	assert.NotNil(t, byPath["new"].After)
}

func TestDiffRecordsNestedLeafPaths(t *testing.T) {
	a := val(t, `{"id":"r1","meta":{"a":1,"deep":{"x":"old"}}}`)
	b := val(t, `{"id":"r1","meta":{"a":1,"deep":{"x":"new","y":2}}}`)
	d := DiffRecords("r1", a, b)
	require.NotNil(t, d)

	paths := make([]string, 0, len(d.Fields))
	for _, f := range d.Fields {
		paths = append(paths, pathStr(f))
	}
	assert.ElementsMatch(t, []string{"meta.deep.x", "meta.deep.y"}, paths)
}

func TestDiffRecordsArraysAreLeaves(t *testing.T) {
	a := val(t, `{"id":"r1","tags":["a","b"]}`)
	b := val(t, `{"id":"r1","tags":["a","c"]}`)
	d := DiffRecords("r1", a, b)
	require.NotNil(t, d)
	require.Len(t, d.Fields, 1)
	assert.Equal(t, "tags", pathStr(d.Fields[0]))
	assert.Equal(t, `["a","b"]`, d.Fields[0].Before.String())
	assert.Equal(t, `["a","c"]`, d.Fields[0].After.String())
}

func TestDiffRecordsTypeChangeIsLeaf(t *testing.T) {
	// object -> scalar on the same field must be one leaf diff, not a
	// per-key explosion of the former object
	a := val(t, `{"id":"r1","v":{"a":1}}`)
	b := val(t, `{"id":"r1","v":"flat"}`)
	d := DiffRecords("r1", a, b)
	require.NotNil(t, d)
	// object side recurses: v.a removed, and... v becomes a leaf-change
	// too? Contract: one-sided objects recurse per key; scalar vs object
	// is NOT both-objects so it is a single leaf.
	require.Len(t, d.Fields, 1)
	assert.Equal(t, "v", pathStr(d.Fields[0]))
}

func TestDiffRecordsAddedRemovedDeleted(t *testing.T) {
	live := val(t, `{"id":"r1","name":"x"}`)
	dead := val(t, `{"id":"r1","_deletedAt":123}`)

	d := DiffRecords("r1", nil, live)
	require.NotNil(t, d)
	assert.Equal(t, KindAdded, d.Kind)
	require.NotEmpty(t, d.Fields)

	d = DiffRecords("r1", live, nil)
	require.NotNil(t, d)
	assert.Equal(t, KindRemoved, d.Kind)

	d = DiffRecords("r1", live, dead)
	require.NotNil(t, d)
	assert.Equal(t, KindDeleted, d.Kind)
	assert.Empty(t, d.Fields)

	// dead at both ends, or never existing: no diff
	assert.Nil(t, DiffRecords("r1", dead, dead))
	assert.Nil(t, DiffRecords("r1", dead, nil))
	assert.Nil(t, DiffRecords("r1", nil, nil))

	// tombstone -> live (concurrent-create edge): Added
	d = DiffRecords("r1", dead, live)
	require.NotNil(t, d)
	assert.Equal(t, KindAdded, d.Kind)
}

func TestDiffRecordSets(t *testing.T) {
	base := []*anyenc.Value{
		val(t, `{"id":"a","v":1}`),
		val(t, `{"id":"b","v":1}`),
		val(t, `{"id":"c","v":1}`),
	}
	version := []*anyenc.Value{
		val(t, `{"id":"b","v":2}`),
		val(t, `{"id":"c","v":1}`),
		val(t, `{"id":"d","v":1}`),
	}
	dd := DiffRecordSets("notes", base, version)
	assert.Equal(t, "notes", dd.Dataset)
	require.Len(t, dd.Records, 3) // a removed, b changed, d added; c untouched

	kinds := map[string]DiffKind{}
	for _, r := range dd.Records {
		kinds[r.Id] = r.Kind
	}
	assert.Equal(t, KindRemoved, kinds["a"])
	assert.Equal(t, KindChanged, kinds["b"])
	assert.Equal(t, KindAdded, kinds["d"])

	// sorted by id
	assert.Equal(t, "a", dd.Records[0].Id)
	assert.Equal(t, "b", dd.Records[1].Id)
	assert.Equal(t, "d", dd.Records[2].Id)
}

func TestDiffObjects(t *testing.T) {
	baseSets := map[string][]*anyenc.Value{
		"notes": {val(t, `{"id":"a","v":1}`)},
		"same":  {val(t, `{"id":"s","v":1}`)},
	}
	versionSets := map[string][]*anyenc.Value{
		"notes": {val(t, `{"id":"a","v":2}`)},
		"same":  {val(t, `{"id":"s","v":1}`)},
		"tasks": {val(t, `{"id":"t","v":1}`)},
	}
	res := DiffObjects("v1", "v2", baseSets, versionSets)
	assert.Equal(t, "v1", res.Base)
	assert.Equal(t, "v2", res.Version)
	require.Len(t, res.Datasets, 2) // "same" omitted
	assert.Equal(t, "notes", res.Datasets[0].Dataset)
	assert.Equal(t, "tasks", res.Datasets[1].Dataset)
}

func TestDiffKindString(t *testing.T) {
	assert.Equal(t, "added", KindAdded.String())
	assert.Equal(t, "removed", KindRemoved.String())
	assert.Equal(t, "changed", KindChanged.String())
	assert.Equal(t, "deleted", KindDeleted.String())
}
