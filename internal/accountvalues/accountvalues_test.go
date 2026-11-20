package accountvalues

import (
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/schema"
)

func TestKeyRoundTrip(t *testing.T) {
	k := Key("bafyObj", "objects", "bafyObj")
	obj, ds, rec, ok := ParseKey(k)
	require.True(t, ok)
	assert.Equal(t, "bafyObj", obj)
	assert.Equal(t, "objects", ds)
	assert.Equal(t, "bafyObj", rec)

	for _, bad := range []string{"", "a", "a:b", "a:b:", ":b:c", "a::c"} {
		_, _, _, ok := ParseKey(bad)
		assert.False(t, ok, "key %q must not parse", bad)
	}
	assert.Equal(t, "bafyObj:", KeyPrefixForObject("bafyObj"))
}

// rec builds a record-shaped value: values is {typeId:{propId:val}},
// vers is path -> versionId stamped through the real _ver machinery so
// collapsed/default shapes behave like production rows.
func rec(arena *anyenc.Arena, values map[string]map[string]any, vers map[string]string) *anyenc.Value {
	r := arena.NewObject()
	r.Set("id", arena.NewString("r1"))
	for typeId, props := range values {
		ns := arena.NewObject()
		for propId, v := range props {
			switch x := v.(type) {
			case string:
				ns.Set(propId, arena.NewString(x))
			case bool:
				if x {
					ns.Set(propId, arena.NewTrue())
				} else {
					ns.Set(propId, arena.NewFalse())
				}
			case float64:
				ns.Set(propId, arena.NewNumberFloat64(x))
			}
		}
		r.Set(typeId, ns)
	}
	for path, ver := range vers {
		parts := []string{}
		for _, p := range splitDot(path) {
			parts = append(parts, p)
		}
		crdt.SetRecordVersion(arena, r, crdt.VersionId(ver), parts...)
	}
	return r
}

func splitDot(s string) []string {
	out := []string{}
	cur := ""
	for _, c := range s {
		if c == '.' {
			out = append(out, cur)
			cur = ""
			continue
		}
		cur += string(c)
	}
	return append(out, cur)
}

// resolver: movie.read + movie.rating are account; movie.title synced;
// anything else unknown.
func testResolver(typeId, propId string) (schema.Scope, bool) {
	if typeId != "movie" {
		return 0, false
	}
	switch propId {
	case "read", "rating":
		return schema.ScopeAccount, true
	case "title":
		return schema.ScopeSynced, true
	}
	return 0, false
}

func TestDiff_SetsAndSkips(t *testing.T) {
	a := &anyenc.Arena{}
	carrier := rec(a,
		map[string]map[string]any{"movie": {
			"read":   true,      // account, target stale → $set
			"rating": float64(8), // account, target already newer → skip
			"title":  "Heat",    // synced-scoped — defensive skip
			"ghost":  "x",       // unknown definition — Hole A skip
		}},
		map[string]string{
			"movie.read":   "t5",
			"movie.rating": "t3",
			"movie.title":  "t9",
			"movie.ghost":  "t9",
		})
	target := rec(a,
		map[string]map[string]any{"movie": {"rating": float64(7)}},
		map[string]string{"movie.rating": "t4"})

	batches := Diff(a, carrier, target, testResolver)
	require.Len(t, batches, 1)
	assert.Equal(t, crdt.VersionId("t5"), batches[0].VersionId)
	require.Len(t, batches[0].Ops, 1)
	op := batches[0].Ops[0]
	assert.Equal(t, crdt.OpSet, op.Type)
	assert.Equal(t, []string{"movie", "read"}, op.Path)
	assert.Equal(t, anyenc.TypeTrue, op.Payload.Type())
}

// Wait — target rating at t4 vs carrier t3: carrier is OLDER, so the
// carrier write must NOT come back (the target's t4 was itself a
// mirror of a newer carrier state this device already applied).
func TestDiff_StaleCarrierValueSkipped(t *testing.T) {
	a := &anyenc.Arena{}
	carrier := rec(a,
		map[string]map[string]any{"movie": {"rating": float64(8)}},
		map[string]string{"movie.rating": "t3"})
	target := rec(a,
		map[string]map[string]any{"movie": {"rating": float64(9)}},
		map[string]string{"movie.rating": "t4"})
	assert.Nil(t, Diff(a, carrier, target, testResolver))
}

func TestDiff_UnsetPropagation(t *testing.T) {
	a := &anyenc.Arena{}
	// Carrier: read was set at t2 then $unset at t6 — value gone,
	// version retained.
	carrier := rec(a, map[string]map[string]any{}, map[string]string{"movie.read": "t6"})
	// Target still holds the old value from t2.
	target := rec(a,
		map[string]map[string]any{"movie": {"read": true}},
		map[string]string{"movie.read": "t2"})

	batches := Diff(a, carrier, target, testResolver)
	require.Len(t, batches, 1)
	assert.Equal(t, crdt.VersionId("t6"), batches[0].VersionId)
	require.Len(t, batches[0].Ops, 1)
	assert.Equal(t, crdt.OpUnset, batches[0].Ops[0].Type)
	assert.Equal(t, []string{"movie", "read"}, batches[0].Ops[0].Path)
}

// A target path the carrier never claimed (no _ver entry) is left
// alone — the mirror has no authority to clear it.
func TestDiff_NoAuthorityNoUnset(t *testing.T) {
	a := &anyenc.Arena{}
	carrier := rec(a, map[string]map[string]any{}, map[string]string{})
	target := rec(a,
		map[string]map[string]any{"movie": {"read": true}},
		map[string]string{"movie.read": "t2"})
	assert.Nil(t, Diff(a, carrier, target, testResolver))
}

// Distinct carrier versions yield distinct batches, ascending — the
// mirror replays the carrier's order exactly, never one max-claim.
func TestDiff_GroupsByVersion(t *testing.T) {
	a := &anyenc.Arena{}
	carrier := rec(a,
		map[string]map[string]any{"movie": {"read": true, "rating": float64(8)}},
		map[string]string{"movie.read": "t2", "movie.rating": "t7"})
	target := rec(a, map[string]map[string]any{}, map[string]string{})

	batches := Diff(a, carrier, target, testResolver)
	require.Len(t, batches, 2)
	assert.Equal(t, crdt.VersionId("t2"), batches[0].VersionId)
	assert.Equal(t, []string{"movie", "read"}, batches[0].Ops[0].Path)
	assert.Equal(t, crdt.VersionId("t7"), batches[1].VersionId)
	assert.Equal(t, []string{"movie", "rating"}, batches[1].Ops[0].Path)
}

// In-sync records produce no work (idempotent double-apply is the
// watcher/inline-write overlap case).
func TestDiff_InSyncIsNil(t *testing.T) {
	a := &anyenc.Arena{}
	carrier := rec(a,
		map[string]map[string]any{"movie": {"read": true}},
		map[string]string{"movie.read": "t5"})
	target := rec(a,
		map[string]map[string]any{"movie": {"read": true}},
		map[string]string{"movie.read": "t5"})
	assert.Nil(t, Diff(a, carrier, target, testResolver))
}

// Nil target (row missing entirely) — sets still emit; the mirror's
// caller decides whether a strict apply is appropriate (it isn't for
// absent rows — the row-created hook replays instead), but Diff itself
// reports the full pending state.
func TestDiff_NilTarget(t *testing.T) {
	a := &anyenc.Arena{}
	carrier := rec(a,
		map[string]map[string]any{"movie": {"read": true}},
		map[string]string{"movie.read": "t5"})
	batches := Diff(a, carrier, nil, testResolver)
	require.Len(t, batches, 1)
	assert.Equal(t, crdt.OpSet, batches[0].Ops[0].Type)
}

// An account-scoped datetime value has to survive the carrier→target
// clone. A hand-rolled type switch used to return nil for anything it
// didn't list, and a nil payload is dropped silently by the apply path
// — the value would vanish on every device with no error.
func TestDiff_CarriesDatetimeValues(t *testing.T) {
	a := &anyenc.Arena{}
	const millis int64 = 1786014230123

	carrier := rec(a, nil, map[string]string{"movie.read": "t5"})
	ns := a.NewObject()
	ns.Set("read", a.NewDateTimeMillis(millis))
	carrier.Set("movie", ns)
	crdt.SetRecordVersion(a, carrier, crdt.VersionId("t5"), "movie", "read")

	batches := Diff(a, carrier, rec(a, nil, nil), testResolver)
	require.Len(t, batches, 1)
	require.Len(t, batches[0].Ops, 1)
	op := batches[0].Ops[0]
	require.NotNil(t, op.Payload, "a nil payload is dropped by the apply path")
	require.Equal(t, anyenc.TypeDateTime, op.Payload.Type())
	got, err := op.Payload.DateTimeMillis()
	require.NoError(t, err)
	assert.Equal(t, millis, got)
}
