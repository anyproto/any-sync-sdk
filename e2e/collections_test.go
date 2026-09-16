package e2e

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/handler"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_Collections covers the collections surface on one device: the
// CRUD round-trip and the marker semantics, the property-definition
// surface shared with types, object membership and its value
// namespaces, the slot rule, and queries over `any.type` /
// `any.collections`. All writes are local — no network round-trip.
func TestE2E_Collections(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	sdk := collDevice(t, ctx, yaml, newFixedSeedProvider(t), "device")
	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "Collections"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("Spaces().Create: %v", err)
	}

	// CRUD: a collection is a definition object carrying the collection
	// marker in `any.type`, listed by Collections() and never by
	// Types(); the two surfaces refuse each other's ids.
	t.Run("CRUD", func(t *testing.T) {
		cId, err := sp.Collections().Create(ctx, space.CollectionCreateParams{
			Name:        "Tag",
			Description: "A label",
			IconCID:     "bafyicon",
			XKey:        "tag",
			Hidden:      true,
			Meta:        map[string]any{"index": "basic", "rank": 3, "beta": true},
		})
		require.NoError(t, err)
		require.NotEmpty(t, cId)

		info, err := sp.Collections().Get(ctx, cId)
		require.NoError(t, err)
		assert.Equal(t, cId, info.Id)
		assert.Equal(t, "Tag", info.Name)
		assert.Equal(t, "A label", info.Description)
		assert.Equal(t, "bafyicon", info.IconCID)
		assert.Equal(t, "tag", info.XKey)
		assert.True(t, info.Hidden)
		assert.Equal(t, map[string]any{"index": "basic", "rank": float64(3), "beta": true}, info.Meta)
		assert.False(t, info.BuiltIn)

		// The row: the marker alone in the type slot, nothing in the
		// collections slot, the handle and flags under `collection.*`.
		row, err := sp.Objects().Get(ctx, cId)
		require.NoError(t, err)
		require.NotNil(t, row)
		assert.Equal(t, space.CollectionMarker, row.GetString("any", "type"))
		assert.Equal(t, "__collection__", space.CollectionMarker, "the marker's wire value is pinned")
		assert.Empty(t, row.GetArray("any", "collections"))
		assert.Equal(t, "Tag", row.GetString("any", "name"))
		assert.Equal(t, "tag", row.GetString("collection", "xkey"))
		assert.True(t, row.GetBool("collection", "hidden"))

		// List: the meta `collection` built-in first, the user
		// collection among the rows (hidden ones included).
		list, err := sp.Collections().List(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, list)
		assert.Equal(t, "collection", list[0].Id)
		assert.True(t, list[0].BuiltIn)
		assert.Contains(t, collCollectionIds(list), cId)

		// Types().List carries the `collection` meta-type but never a
		// collection object.
		typeList, err := sp.Types().List(ctx)
		require.NoError(t, err)
		typeIds := collTypeIds(typeList)
		assert.Contains(t, typeIds, "collection", "the meta-type is a built-in type")
		assert.NotContains(t, typeIds, cId, "a collection object is not a type")

		// The surfaces never alias: each refuses the other's id.
		typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Movie", XKey: "movie"})
		require.NoError(t, err)
		_, err = sp.Types().Get(ctx, cId)
		assert.ErrorIs(t, err, space.ErrNotAType)
		_, err = sp.Collections().Get(ctx, typeId)
		assert.ErrorIs(t, err, space.ErrNotACollection)
		_, err = sp.Collections().Get(ctx, "no-such-object")
		assert.ErrorIs(t, err, space.ErrNotFound)

		// The type-only surface refuses a collection id — a collection
		// has no parts and no layout.
		_, err = sp.Types().Parts(ctx, cId)
		assert.ErrorIs(t, err, space.ErrNotAType)
		_, err = sp.Types().AddPart(ctx, cId, collNotesPart())
		assert.ErrorIs(t, err, space.ErrNotAType)
		err = sp.Types().Patch(ctx, cId, space.TypePatch{Name: collPtr("Nope")})
		assert.ErrorIs(t, err, space.ErrNotAType)
		err = sp.Collections().Patch(ctx, typeId, space.CollectionPatch{Name: collPtr("Nope")})
		assert.ErrorIs(t, err, space.ErrNotACollection)

		// Patch: name, hidden and per-key meta round-trip; a nil meta
		// value unsets the key, the untouched keys and the handle stay.
		unhide := false
		require.NoError(t, sp.Collections().Patch(ctx, cId, space.CollectionPatch{
			Name:   collPtr("Tags"),
			Hidden: &unhide,
			Meta:   map[string]any{"index": "full", "beta": nil},
		}))
		info, err = sp.Collections().Get(ctx, cId)
		require.NoError(t, err)
		assert.Equal(t, "Tags", info.Name)
		assert.False(t, info.Hidden)
		assert.Equal(t, map[string]any{"index": "full", "rank": float64(3)}, info.Meta)
		assert.Equal(t, "tag", info.XKey, "the handle is untouched by Patch")
		assert.Equal(t, "A label", info.Description)
	})

	// Property definitions: one owner-agnostic surface reachable
	// through either accessor.
	t.Run("Definitions", func(t *testing.T) {
		cId, err := sp.Collections().Create(ctx, space.CollectionCreateParams{Name: "Shelf", XKey: "shelf"})
		require.NoError(t, err)

		posProp, err := sp.Collections().AddProperty(ctx, cId, space.PropertyDraft{
			Name: "Position", XKey: "pos", Kind: space.PropertyKindString,
		})
		require.NoError(t, err)
		// The same surface through Types(), addressed by a collection id.
		rankProp, err := sp.Types().AddProperty(ctx, cId, space.PropertyDraft{
			Name: "Rank", XKey: "rank", Kind: space.PropertyKindNumber,
		})
		require.NoError(t, err)
		require.NotEqual(t, posProp, rankProp)

		viaColl, err := sp.Collections().Properties(ctx, cId)
		require.NoError(t, err)
		viaTypes, err := sp.Types().Properties(ctx, cId)
		require.NoError(t, err)
		assert.Equal(t, collPropNames(viaColl), collPropNames(viaTypes), "both accessors read one definition set")
		assert.Equal(t, map[string]string{posProp: "Position", rankProp: "Rank"}, collPropNames(viaColl))

		// Patch through one accessor, read through the other.
		require.NoError(t, sp.Types().PatchProperty(ctx, cId, posProp, space.PropertyPatch{
			Set: map[string]any{"name": "Pos", "x-key": "position"},
		}))
		viaColl, err = sp.Collections().Properties(ctx, cId)
		require.NoError(t, err)
		byId := collPropsById(viaColl)
		assert.Equal(t, "Pos", byId[posProp].Name)
		assert.Equal(t, "position", byId[posProp].XKey)
		assert.Equal(t, space.PropertyKindString, byId[posProp].Kind)

		// Remove through the other accessor.
		require.NoError(t, sp.Collections().RemoveProperty(ctx, cId, rankProp))
		viaTypes, err = sp.Types().Properties(ctx, cId)
		require.NoError(t, err)
		assert.Equal(t, map[string]string{posProp: "Pos"}, collPropNames(viaTypes))

		// The meta `collection` built-in answers its static table and
		// refuses mutation.
		metaProps, err := sp.Collections().Properties(ctx, "collection")
		require.NoError(t, err)
		assert.Equal(t, []string{"hidden", "meta", "xkey"}, collSortedPropIds(metaProps))
		_, err = sp.Collections().AddProperty(ctx, "collection", space.PropertyDraft{
			Name: "X", XKey: "x", Kind: space.PropertyKindString,
		})
		assert.ErrorIs(t, err, space.ErrTypeRegistered)
	})

	// Object membership: one type, many collections, one value
	// namespace per member.
	t.Run("ObjectMembership", func(t *testing.T) {
		typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Film"})
		require.NoError(t, err)
		titleProp, err := sp.Types().AddProperty(ctx, typeId, space.PropertyDraft{
			Name: "Title", XKey: "title", Kind: space.PropertyKindString,
		})
		require.NoError(t, err)

		cId, err := sp.Collections().Create(ctx, space.CollectionCreateParams{Name: "Watchlist"})
		require.NoError(t, err)
		noteProp, err := sp.Collections().AddProperty(ctx, cId, space.PropertyDraft{
			Name: "Note", XKey: "note", Kind: space.PropertyKindString,
		})
		require.NoError(t, err)

		otherId, err := sp.Collections().Create(ctx, space.CollectionCreateParams{Name: "Archive"})
		require.NoError(t, err)
		archivedProp, err := sp.Collections().AddProperty(ctx, otherId, space.PropertyDraft{
			Name: "Archived", XKey: "archived", Kind: space.PropertyKindBoolean,
		})
		require.NoError(t, err)

		// Birth membership plus a value in each namespace, one change.
		objId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{
			Type:        typeId,
			Collections: []string{cId},
			InitialProperties: map[string]map[string]any{
				typeId: {titleProp: "Casablanca"},
				cId:    {noteProp: "classic"},
			},
		})
		require.NoError(t, err)

		row, err := sp.Objects().Get(ctx, objId)
		require.NoError(t, err)
		assert.Equal(t, typeId, row.GetString("any", "type"))
		assert.ElementsMatch(t, []string{cId}, collArrayOf(row, "any", "collections"))
		assert.Equal(t, "Casablanca", row.GetString(typeId, titleProp))
		assert.Equal(t, "classic", row.GetString(cId, noteProp))

		// A value write under an attached collection is an ordinary
		// write on the object's own CRDT.
		_, err = sp.Properties().Set(ctx, objId, cId, map[string]any{noteProp: "noir"})
		require.NoError(t, err)
		gotType, gotColls := collMembers(t, ctx, sp, objId)
		assert.Equal(t, typeId, gotType)
		assert.ElementsMatch(t, []string{cId}, gotColls)

		// A write under a collection the object is not in is refused by
		// the local pre-flight, naming both slots.
		_, err = sp.Properties().Set(ctx, objId, otherId, map[string]any{archivedProp: true})
		require.Error(t, err)
		reason, ok := handler.ClassifyValidation(err)
		require.True(t, ok, "rejection must classify: %v", err)
		assert.Equal(t, handler.ReasonTypeNotImplemented, reason)
		assert.ErrorIs(t, err, handler.ErrValidationTypeNotImplemented)
		assert.Contains(t, err.Error(), "any.collections",
			"the rejection must point at the collections slot")

		// Attach, then the same write lands. Attach is $addToSet —
		// idempotent.
		_, err = sp.Properties().AttachCollection(ctx, objId, otherId)
		require.NoError(t, err)
		_, err = sp.Properties().AttachCollection(ctx, objId, otherId)
		require.NoError(t, err)
		_, err = sp.Properties().Set(ctx, objId, otherId, map[string]any{archivedProp: true})
		require.NoError(t, err)
		_, gotColls = collMembers(t, ctx, sp, objId)
		assert.ElementsMatch(t, []string{cId, otherId}, gotColls)

		// Detach drops the membership; the namespace's values stay as
		// orphan data (read-tolerant).
		_, err = sp.Properties().DetachCollection(ctx, objId, otherId)
		require.NoError(t, err)
		row, err = sp.Objects().Get(ctx, objId)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{cId}, collArrayOf(row, "any", "collections"))
		assert.True(t, row.GetBool(otherId, archivedProp), "a detached namespace's values stay readable")

		// SetType replaces the one type; the previous type's values
		// stay in place, orphaned.
		bookType, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Book"})
		require.NoError(t, err)
		_, err = sp.Properties().SetType(ctx, objId, bookType)
		require.NoError(t, err)
		row, err = sp.Objects().Get(ctx, objId)
		require.NoError(t, err)
		assert.Equal(t, bookType, row.GetString("any", "type"))
		assert.Equal(t, "Casablanca", row.GetString(typeId, titleProp), "the old type's values orphan in place")
		assert.ElementsMatch(t, []string{cId}, collArrayOf(row, "any", "collections"), "retyping leaves collections alone")

		// The type slot cannot be emptied: a raw write that clears it
		// is refused, and the row keeps the type it had.
		_, err = sp.Modify(ctx, space.ModifyBatch{
			ObjectId: objId, Dataset: "objects",
			Records: []space.RecordModify{{
				Id: objId, Upsert: true,
				Ops: []space.Op{{Type: space.OpUnset, Path: "any.type"}},
			}},
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, space.ErrTypeRequired, "raw Modify must surface the public sentinel: %v", err)
		reason, ok = handler.ClassifyValidation(err)
		require.True(t, ok, "rejection must classify: %v", err)
		assert.Equal(t, handler.ReasonTypeRequired, reason)
		assert.ErrorIs(t, err, handler.ErrValidationTypeRequired)
		row, err = sp.Objects().Get(ctx, objId)
		require.NoError(t, err)
		assert.Equal(t, bookType, row.GetString("any", "type"), "the refused op must not land")
		assert.ElementsMatch(t, []string{cId}, collArrayOf(row, "any", "collections"))
	})

	// Every object has exactly one type, so Create refuses without one
	// — and mints nothing on the way out.
	t.Run("TypeRequired", func(t *testing.T) {
		allIds := func() []string {
			rows, err := sp.QueryObjects().All(ctx)
			require.NoError(t, err)
			return collRowIds(rows)
		}
		before := allIds()
		id, err := sp.Objects().Create(ctx, space.CreateObjectOpts{})
		assert.ErrorIs(t, err, space.ErrTypeRequired)
		assert.Empty(t, id, "a refused Create returns no id")
		assert.ElementsMatch(t, before, allIds(), "a refused Create leaves no object behind")

		// Collections stay optional: a type alone is a whole object.
		typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Typed"})
		require.NoError(t, err)
		id, err = sp.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId})
		require.NoError(t, err)
		row, err := sp.Objects().Get(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, typeId, row.GetString("any", "type"))
		assert.Empty(t, collArrayOf(row, "any", "collections"))
	})

	// The slot rule: a known id goes where its kind belongs; an id this
	// device cannot resolve passes.
	t.Run("WrongSlot", func(t *testing.T) {
		typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Slotted"})
		require.NoError(t, err)
		cId, err := sp.Collections().Create(ctx, space.CollectionCreateParams{Name: "Slots"})
		require.NoError(t, err)
		objId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId})
		require.NoError(t, err)

		_, err = sp.Properties().SetType(ctx, objId, cId)
		assert.ErrorIs(t, err, space.ErrWrongSlot, "a collection cannot be the type")
		_, err = sp.Properties().AttachCollection(ctx, objId, typeId)
		assert.ErrorIs(t, err, space.ErrWrongSlot, "a type cannot be a collection")

		// The rule lives in the pre-flight, so the raw write path is
		// refused too — and refused with the public sentinel, which is
		// what space.ErrWrongSlot documents for a raw Modify on the
		// objects row.
		_, err = sp.Modify(ctx, space.ModifyBatch{
			ObjectId: objId, Dataset: "objects",
			Records: []space.RecordModify{{
				Id: objId, Upsert: true,
				Ops: []space.Op{{Type: space.OpAddToSet, Path: "any.collections", Value: typeId}},
			}},
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, space.ErrWrongSlot, "raw Modify must surface the public sentinel: %v", err)
		_, gotColls := collMembers(t, ctx, sp, objId)
		assert.Empty(t, gotColls, "the refused op must not land")

		// Creating with the slots crossed is refused the same way.
		_, err = sp.Objects().Create(ctx, space.CreateObjectOpts{Type: cId})
		assert.ErrorIs(t, err, space.ErrWrongSlot)
		_, err = sp.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId, Collections: []string{typeId}})
		assert.ErrorIs(t, err, space.ErrWrongSlot)

		// An id that resolves to neither passes — a definition that has
		// not synced here yet must not block membership.
		_, err = sp.Properties().AttachCollection(ctx, objId, "bafyunknowncollection")
		require.NoError(t, err)
		_, err = sp.Properties().SetType(ctx, objId, "bafyunknowntype")
		require.NoError(t, err)
		gotType, gotColls := collMembers(t, ctx, sp, objId)
		assert.Equal(t, "bafyunknowntype", gotType)
		assert.ElementsMatch(t, []string{"bafyunknowncollection"}, gotColls)
	})

	// Queries: the two membership fields select disjoint sets, and
	// neither ever returns the definition object.
	t.Run("Queries", func(t *testing.T) {
		typeId, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Queried"})
		require.NoError(t, err)
		cId, err := sp.Collections().Create(ctx, space.CollectionCreateParams{Name: "Queue"})
		require.NoError(t, err)

		memberId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{
			Type: typeId, Collections: []string{cId},
		})
		require.NoError(t, err)
		loneId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId})
		require.NoError(t, err)

		byType, err := sp.QueryObjects().Filter(map[string]any{"any.type": typeId}).All(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{memberId, loneId}, collRowIds(byType),
			"a type query returns its objects and never the type object")

		byCollection, err := sp.QueryObjects().Filter(map[string]any{"any.collections": cId}).All(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{memberId}, collRowIds(byCollection),
			"a collection query returns members only")

		// A live query over `any.collections` sees attach and detach.
		res, err := sp.QueryObjects().Filter(map[string]any{"any.collections": cId}).Subscribe(ctx, space.QueryOpts{})
		require.NoError(t, err)
		require.NotNil(t, res.Sub)
		t.Cleanup(func() { _ = res.Sub.Close() })
		assert.ElementsMatch(t, []string{memberId}, collRowIds(res.Initial))

		_, err = sp.Properties().AttachCollection(ctx, loneId, cId)
		require.NoError(t, err)
		added, _ := collDrainSub(res.Sub, 5*time.Second, func(added, _ []string) bool {
			return containsString(added, loneId)
		})
		assert.Contains(t, added, loneId, "attach must enter the window")

		_, err = sp.Properties().DetachCollection(ctx, loneId, cId)
		require.NoError(t, err)
		_, removed := collDrainSub(res.Sub, 5*time.Second, func(_, removed []string) bool {
			return containsString(removed, loneId)
		})
		assert.Contains(t, removed, loneId, "detach must leave the window")

		final, err := sp.QueryObjects().Filter(map[string]any{"any.collections": cId}).All(ctx)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{memberId}, collRowIds(final))
	})
}

// TestE2E_CollectionsTwoDevices: device A defines a collection, files
// an object under it and writes a value in its namespace; device B
// (same account, fresh storage) cold-syncs and must converge on the
// definition, the membership and the value. The value change on B is
// gated on the collection's schema state (the DataVersion / known-
// shortIds set), so it parks until the definition's tree lands and
// drains after — the ordering is the sync layer's to choose.
func TestE2E_CollectionsTwoDevices(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("cold-sync e2e is slow; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	provider := newFixedSeedProvider(t)

	sdkA := collDevice(t, ctx, yaml, provider, "device A")
	spA, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "CollectionSync"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("device A: Spaces().Create: %v", err)
	}

	typeId, err := spA.Types().Create(ctx, space.TypeCreateParams{Name: "Film"})
	require.NoError(t, err)
	cId, err := spA.Collections().Create(ctx, space.CollectionCreateParams{
		Name: "Watchlist", XKey: "watchlist", Meta: map[string]any{"index": "basic"},
	})
	require.NoError(t, err)
	noteProp, err := spA.Collections().AddProperty(ctx, cId, space.PropertyDraft{
		Name: "Note", XKey: "note", Kind: space.PropertyKindString,
	})
	require.NoError(t, err)

	objId, err := spA.Objects().Create(ctx, space.CreateObjectOpts{Type: typeId, Collections: []string{cId}})
	require.NoError(t, err)
	_, err = spA.Properties().Set(ctx, objId, cId, map[string]any{noteProp: "watch tonight"})
	require.NoError(t, err)
	t.Logf("device A: space=%s type=%s collection=%s prop=%s object=%s", spA.Id(), typeId, cId, noteProp, objId)

	// Push before B pulls (see cold_sync_test for why).
	_ = sdkA.Spaces().SyncSpaceList(ctx)
	_ = spA.SyncHeads(ctx)

	sdkB := collDevice(t, ctx, yaml, provider, "device B")
	require.Equal(t, sdkA.Account().Id(), sdkB.Account().Id())

	var spB space.Space
	require.True(t, waitFor(ctx, 90*time.Second, 250*time.Millisecond, func() bool {
		_ = sdkB.Spaces().SyncSpaceList(ctx)
		got, gerr := sdkB.Spaces().Get(ctx, spA.Id())
		if gerr != nil {
			return false
		}
		spB = got
		return true
	}), "device B must see the space")

	// The definition converges first — it is what the value write is
	// gated on.
	var lastInfo space.CollectionInfo
	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		_ = spB.SyncHeads(ctx)
		info, gerr := spB.Collections().Get(ctx, cId)
		if gerr != nil {
			return false
		}
		lastInfo = info
		defs, derr := spB.Collections().Properties(ctx, cId)
		return derr == nil && len(defs) == 1 && defs[0].Id == noteProp
	}), "device B never converged on the collection definition: %+v", lastInfo)
	assert.Equal(t, "Watchlist", lastInfo.Name)
	assert.Equal(t, "watchlist", lastInfo.XKey)
	assert.Equal(t, map[string]any{"index": "basic"}, lastInfo.Meta)
	assert.False(t, lastInfo.BuiltIn)

	// The collection object arrives as a collection, not a type.
	_, err = spB.Types().Get(ctx, cId)
	assert.ErrorIs(t, err, space.ErrNotAType, "the marker survives the sync")

	// Membership and the parked value drain together.
	var lastRow *anyenc.Value
	sawRowBeforeValue := false
	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		_ = spB.SyncHeads(ctx)
		row, gerr := spB.Objects().Get(ctx, objId)
		if gerr != nil || row == nil {
			return false
		}
		lastRow = row
		if row.Get(cId, noteProp) == nil {
			sawRowBeforeValue = true
			return false
		}
		return containsString(collArrayOf(row, "any", "collections"), cId)
	}), "device B never converged on the object's collection membership and value: %v", lastRow)
	t.Logf("device B: observed the row before its collection value drained: %v", sawRowBeforeValue)

	row, err := spB.Objects().Get(ctx, objId)
	require.NoError(t, err)
	assert.Equal(t, typeId, row.GetString("any", "type"))
	assert.ElementsMatch(t, []string{cId}, collArrayOf(row, "any", "collections"))
	assert.Equal(t, "watch tonight", row.GetString(cId, noteProp))

	// The membership query answers on B as it does on A.
	members, err := spB.QueryObjects().Filter(map[string]any{"any.collections": cId}).All(ctx)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{objId}, collRowIds(members))
}

// TestE2E_CollectionBundles: the bundle gate for collection roots, and
// the self-hosting a type-declaring root keeps — its own records and
// its own values, while answering no query for its own id.
func TestE2E_CollectionBundles(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	sdk := collDevice(t, ctx, yaml, newFixedSeedProvider(t), "device")
	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "CollectionBundles"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("Spaces().Create: %v", err)
	}

	movieType, err := sp.Types().Create(ctx, space.TypeCreateParams{Name: "Movie"})
	require.NoError(t, err)

	// A collection declares no parts, and a declaring root's type slot
	// holds its marker — RootType has nowhere to go.
	_, _, err = sp.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: "coll-parts/v1", DerivedRoot: true, Collection: true, XKey: "parts",
		Parts: []space.PartDraft{collNotesPart()},
	})
	assert.ErrorIs(t, err, space.ErrBundleBadRequest, "a collection declares no parts")
	_, _, err = sp.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: "coll-layout/v1", DerivedRoot: true, Collection: true, XKey: "layout",
		Layout: map[string]any{"type": "page"},
	})
	assert.ErrorIs(t, err, space.ErrBundleBadRequest, "a collection has no layout")
	_, _, err = sp.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: "roottype-decl/v1", DerivedRoot: true, XKey: "smuggle", RootType: movieType,
	})
	assert.ErrorIs(t, err, space.ErrBundleBadRequest, "RootType next to a declaration")
	for _, id := range []string{"coll-parts/v1", "coll-layout/v1", "roottype-decl/v1"} {
		_, err = sp.Bundles().Get(ctx, id)
		assert.ErrorIs(t, err, space.ErrBundleUnknown, "a refused request must mint nothing (%s)", id)
	}

	// A type-declaring root implements itself: it hosts its own
	// records and its own values without ever naming its id.
	inst, didInstall, err := sp.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: "notes/v1", Name: "Notes", DerivedRoot: true, XKey: "notes", Hidden: true,
		Properties: []space.PropertyDraft{{XKey: "pos", Name: "Position", Kind: space.PropertyKindString}},
		Parts:      []space.PartDraft{collNotesPart()},
	})
	require.NoError(t, err)
	require.True(t, didInstall)
	root := inst.RootId

	rootType, rootColls := collMembers(t, ctx, sp, root)
	assert.Equal(t, space.TypeMarker, rootType, "a declaring root carries the marker alone")
	assert.Empty(t, rootColls)

	up, err := sp.Upsert(ctx, space.UpsertBatch{
		ObjectId: root, Dataset: root + "_notes",
		Records: []space.UpsertRecord{{Id: "n-1", Fields: map[string]any{"title": "own record"}}},
	})
	require.NoError(t, err, "a declaring root hosts its own records")
	assert.Equal(t, 1, up.Created)
	assert.Empty(t, up.Rejections)

	rootProps, err := sp.Types().Properties(ctx, root)
	require.NoError(t, err)
	require.Len(t, rootProps, 1)
	_, err = sp.Properties().Set(ctx, root, root, map[string]any{rootProps[0].Id: "a0"})
	require.NoError(t, err, "a declaring root holds its own values")
	row, err := sp.Objects().Get(ctx, root)
	require.NoError(t, err)
	assert.Equal(t, "a0", row.GetString(root, rootProps[0].Id))
	assert.Equal(t, space.TypeMarker, row.GetString("any", "type"), "self-hosting never names the root's own id")

	selfMatch, err := sp.QueryObjects().Filter(map[string]any{"any.type": root}).All(ctx)
	require.NoError(t, err)
	assert.Empty(t, selfMatch, "a definition answers no query for its own id")

	// A collection-declaring root, single device: the collection
	// marker, the handle under `collection.*`, columns on the
	// collections surface.
	coll, didInstall, err := sp.Bundles().Ensure(ctx, space.EnsureBundleRequest{
		Id: "labels/v1", Name: "Labels", DerivedRoot: true, Collection: true, XKey: "labels", Hidden: true,
		Properties: []space.PropertyDraft{{XKey: "color", Name: "Color", Kind: space.PropertyKindString}},
	})
	require.NoError(t, err)
	require.True(t, didInstall)
	collType, _ := collMembers(t, ctx, sp, coll.RootId)
	assert.Equal(t, space.CollectionMarker, collType)
	info, err := sp.Collections().Get(ctx, coll.RootId)
	require.NoError(t, err)
	assert.Equal(t, "labels", info.XKey)
	assert.True(t, info.Hidden)
	_, err = sp.Types().Get(ctx, coll.RootId)
	assert.ErrorIs(t, err, space.ErrNotAType)
	collProps, err := sp.Collections().Properties(ctx, coll.RootId)
	require.NoError(t, err)
	require.Len(t, collProps, 1)
	assert.Equal(t, "color", collProps[0].XKey)

	// The collection is usable as one: objects file under it and take
	// values in its namespace.
	objId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{
		Type: movieType, Collections: []string{coll.RootId},
		InitialProperties: map[string]map[string]any{coll.RootId: {collProps[0].Id: "red"}},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{objId}, collRowIds(collQueryAll(t, ctx, sp, map[string]any{"any.collections": coll.RootId})))
}

// TestE2E_CollectionBundleTwoDevices: two devices install the same
// collection bundle on its canonical derived root without a
// convergence gate. They must land on ONE collection root and, because
// the property ids derive from (root, handle), on ONE column per
// handle.
func TestE2E_CollectionBundleTwoDevices(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	if testing.Short() {
		t.Skip("bundles e2e is slow; rerun without -short")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	provider := newFixedSeedProvider(t)

	const bundleId = "shared-labels/v1"
	request := func() space.EnsureBundleRequest {
		return space.EnsureBundleRequest{
			Id: bundleId, Name: "Labels", DerivedRoot: true, Collection: true, XKey: "labels",
			Properties: []space.PropertyDraft{
				{XKey: "color", Name: "Color", Kind: space.PropertyKindString},
				{XKey: "weight", Name: "Weight", Kind: space.PropertyKindNumber},
			},
		}
	}

	sdkA := collDevice(t, ctx, yaml, provider, "device A")
	spA, err := sdkA.Spaces().Create(ctx, space.CreateRequest{Name: "LabelBundle"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("device A: Spaces().Create: %v", err)
	}

	wantRoot, err := spA.Bundles().DerivedRootId(ctx, bundleId)
	require.NoError(t, err)
	instA, didInstall, err := spA.Bundles().Ensure(ctx, request())
	require.NoError(t, err, "device A: Ensure(collection bundle)")
	require.True(t, didInstall)
	require.Equal(t, wantRoot, instA.RootId)

	typeA, _ := collMembers(t, ctx, spA, wantRoot)
	assert.Equal(t, space.CollectionMarker, typeA)
	propsA, err := spA.Collections().Properties(ctx, wantRoot)
	require.NoError(t, err)
	require.Len(t, propsA, 2)
	idsA := collPropIdsByXKey(propsA)
	require.Len(t, idsA, 2)

	_ = sdkA.Spaces().SyncSpaceList(ctx)
	_ = spA.SyncHeads(ctx)

	sdkB := collDevice(t, ctx, yaml, provider, "device B")
	var spB space.Space
	require.True(t, waitFor(ctx, 90*time.Second, 250*time.Millisecond, func() bool {
		_ = sdkB.Spaces().SyncSpaceList(ctx)
		got, gerr := sdkB.Spaces().Get(ctx, spA.Id())
		if gerr != nil {
			return false
		}
		spB = got
		return true
	}), "device B must see the space")

	// No convergence gate: B installs blind, as an offline device would.
	instB, _, err := spB.Bundles().Ensure(ctx, request())
	require.NoError(t, err, "device B: Ensure without a convergence gate")
	require.Equal(t, wantRoot, instB.RootId, "the canonical root does not depend on the device")
	require.Equal(t, []string{wantRoot}, instB.Roots, "a second install must not claim a second root")
	require.Empty(t, instB.Losers)

	// One root, one column per handle, on both devices.
	var propsB []space.PropertyDef
	require.True(t, waitFor(ctx, 120*time.Second, 500*time.Millisecond, func() bool {
		_ = spA.SyncHeads(ctx)
		_ = spB.SyncHeads(ctx)
		a, aerr := spA.Bundles().Get(ctx, bundleId)
		b, berr := spB.Bundles().Get(ctx, bundleId)
		if aerr != nil || berr != nil || a.RootId != wantRoot || b.RootId != wantRoot {
			return false
		}
		if len(a.Roots) != 1 || len(b.Roots) != 1 || len(a.Losers) > 0 || len(b.Losers) > 0 {
			return false
		}
		var perr error
		propsB, perr = spB.Collections().Properties(ctx, wantRoot)
		if perr != nil || len(propsB) != 2 {
			return false
		}
		for _, p := range propsB {
			if idsA[p.XKey] != p.Id {
				return false
			}
		}
		defsA, aerr := spA.Collections().Properties(ctx, wantRoot)
		return aerr == nil && len(defsA) == 2
	}), "the collection bundle never converged on one root with one column per handle: B props=%+v", propsB)

	typeB, _ := collMembers(t, ctx, spB, wantRoot)
	assert.Equal(t, space.CollectionMarker, typeB, "the root is a collection on both devices")
	infoB, err := spB.Collections().Get(ctx, wantRoot)
	require.NoError(t, err)
	assert.Equal(t, "labels", infoB.XKey)
	_, err = spB.Types().Get(ctx, wantRoot)
	assert.ErrorIs(t, err, space.ErrNotAType)
}

// collDevice opens an SDK on a fresh DataDir for the given account.
func collDevice(t *testing.T, ctx context.Context, yaml []byte, provider *fixedSeedProvider, label string) *anysyncsdk.SDK {
	t.Helper()
	sdk, err := anysyncsdk.Open(ctx, config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}, provider)
	require.NoError(t, err, "%s: Open", label)
	t.Cleanup(func() { _ = sdk.Close() })
	return sdk
}

// collNotesPart is a one-dataset part — what a bundle root declares to
// host its own records.
func collNotesPart() space.PartDraft {
	return space.PartDraft{
		Key: "notes", Name: "Notes",
		Datasets: []space.DatasetDraft{{
			Key:    "notes",
			IdRule: space.IdUser,
			Fields: []space.DatasetFieldDraft{
				{Key: "title", Kind: space.PropertyKindString, Required: true},
			},
		}},
	}
}

// collMembers reads an object's membership off its row.
func collMembers(t *testing.T, ctx context.Context, sp space.Space, objectId string) (string, []string) {
	t.Helper()
	row, err := sp.Objects().Get(ctx, objectId)
	require.NoError(t, err, "Objects().Get(%s)", objectId)
	require.NotNil(t, row)
	return row.GetString("any", "type"), collArrayOf(row, "any", "collections")
}

// collArrayOf reads a string array off a row at the given path.
func collArrayOf(row *anyenc.Value, path ...string) []string {
	var out []string
	for _, v := range row.GetArray(path...) {
		out = append(out, string(v.GetStringBytes()))
	}
	return out
}

// collQueryAll runs a filter over the per-space objects collection.
func collQueryAll(t *testing.T, ctx context.Context, sp space.Space, filter map[string]any) []*anyenc.Value {
	t.Helper()
	rows, err := sp.QueryObjects().Filter(filter).All(ctx)
	require.NoError(t, err)
	return rows
}

// collRowIds pulls the `id` field off each row.
func collRowIds(rows []*anyenc.Value) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.GetString("id"))
	}
	return out
}

func collCollectionIds(infos []space.CollectionInfo) []string {
	out := make([]string, 0, len(infos))
	for _, i := range infos {
		out = append(out, i.Id)
	}
	return out
}

func collTypeIds(infos []space.TypeInfo) []string {
	out := make([]string, 0, len(infos))
	for _, i := range infos {
		out = append(out, i.Id)
	}
	return out
}

func collPropsById(defs []space.PropertyDef) map[string]space.PropertyDef {
	out := make(map[string]space.PropertyDef, len(defs))
	for _, d := range defs {
		out[d.Id] = d
	}
	return out
}

func collPropNames(defs []space.PropertyDef) map[string]string {
	out := make(map[string]string, len(defs))
	for _, d := range defs {
		out[d.Id] = d.Name
	}
	return out
}

func collPropIdsByXKey(defs []space.PropertyDef) map[string]string {
	out := make(map[string]string, len(defs))
	for _, d := range defs {
		out[d.XKey] = d.Id
	}
	return out
}

func collSortedPropIds(defs []space.PropertyDef) []string {
	out := make([]string, 0, len(defs))
	for _, d := range defs {
		out = append(out, d.Id)
	}
	slices.Sort(out)
	return out
}

// collDrainSub collects Added / Removed ids from sub until match holds
// or the window closes.
func collDrainSub(sub space.QuerySubscription, window time.Duration, match func(added, removed []string) bool) ([]string, []string) {
	ctx, cancel := context.WithTimeout(context.Background(), window)
	defer cancel()
	var added, removed []string
	for {
		if match(added, removed) {
			return added, removed
		}
		evs, err := sub.Events().Wait(ctx)
		if err != nil {
			return added, removed
		}
		for _, ev := range evs {
			for _, r := range ev.Added {
				added = append(added, r.Id)
			}
			for _, r := range ev.Removed {
				removed = append(removed, r.Id)
			}
		}
	}
}

func collPtr(s string) *string { return &s }

// TestE2E_BundleDeclarationTakesMarker pins the stamp on a root that
// already has a type: a later Ensure that gains a declaration puts the
// marker in `any.type` over the type the root had (a definition has no
// type of its own), and the root resolves as a type from then on.
func TestE2E_BundleDeclarationTakesMarker(t *testing.T) {
	t.Parallel()
	yaml, confPath, err := loadAnySyncNetwork()
	if err != nil {
		t.Skipf("no any-sync network config available: %v", err)
	}
	t.Logf("using any-sync network config from %s", confPath)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	sdk, err := anysyncsdk.Open(ctx, config.Config{
		Storage: config.Storage{DataDir: t.TempDir(), Topology: config.StorageShared},
		Network: config.Network{NodeConfYAML: yaml},
	}, newFixedSeedProvider(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sdk.Close() })
	sp, err := sdk.Spaces().Create(ctx, space.CreateRequest{Name: "MarkerHeal"})
	if err != nil {
		if isNoNetworkErr(err) {
			t.Skipf("network unreachable on space create: %v", err)
		}
		t.Fatalf("Spaces().Create: %v", err)
	}
	typeId, _ := setupMovieType(t, ctx, sp)

	const bundleId = "marker-heal/v1"
	b, _, err := sp.Bundles().Ensure(ctx, space.EnsureBundleRequest{Id: bundleId, DerivedRoot: true, RootType: typeId})
	require.NoError(t, err)
	row, err := sp.Objects().Get(ctx, b.RootId)
	require.NoError(t, err)
	require.Equal(t, typeId, row.GetString("any", "type"))

	// The next version declares: RootType must go, the marker takes
	// the slot, the handle lands, the root is a type.
	_, _, err = sp.Bundles().Ensure(ctx, space.EnsureBundleRequest{Id: bundleId, DerivedRoot: true, XKey: "marker_heal"})
	require.NoError(t, err)
	row, err = sp.Objects().Get(ctx, b.RootId)
	require.NoError(t, err)
	assert.Equal(t, space.TypeMarker, row.GetString("any", "type"))
	info, err := sp.Types().Get(ctx, b.RootId)
	require.NoError(t, err)
	assert.Equal(t, "marker_heal", info.XKey)
}
