package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

// childSpacesCreateAndLimit: the Owner, an Admin and a Writer
// of a parent create children with no approval. A child syncs to a
// second device of its creator and to a member added to it, and names
// its parent on both. The children of one owner fill its limit across
// two parents; the next child is refused with the limit reason, stays
// usable, shows the reason on the creator's second device, and is
// accepted by RetryPush once a place frees up. A Reader's child is
// refused for the role. A creator is not charged: the Writer's own
// limit stays whole.
func childSpacesCreateAndLimit(t *testing.T, net *childNet) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	owner := net.account(t, ctx, "owner")
	ownerB := net.device(t, ctx, owner, "owner's second device")
	admin := net.account(t, ctx, "admin")
	writer := net.account(t, ctx, "writer")
	reader := net.account(t, ctx, "reader")

	limits, err := owner.sdk.Account().Limits(ctx)
	require.NoError(t, err)
	require.NotNil(t, limits.ChildSpaces, "the coordinator reports a child spaces limit")
	require.EqualValues(t, 3, limits.ChildSpaces.Limit, "the suite expects a child spaces limit of 3")

	parent1 := owner.create(t, ctx, "Parent 1")
	parent2 := owner.create(t, ctx, "Parent 2")
	addMembers(t, ctx, parent1, grant{admin, space.PermissionAdmin}, grant{reader, space.PermissionReader})
	addMembers(t, ctx, parent2, grant{writer, space.PermissionWriter})

	// Three children of one owner, over two parents.
	ownersChild := owner.createChild(t, ctx, parent1.Id(), "Owner's child")
	adminsChild := admin.createChild(t, ctx, parent1.Id(), "Admin's child")
	writersChild := writer.createChild(t, ctx, parent2.Id(), "Writer's child")
	waitSigned(t, ctx, owner, parent1.Id(), ownersChild.Id())
	waitSigned(t, ctx, owner, parent1.Id(), adminsChild.Id())
	waitSigned(t, ctx, owner, parent2.Id(), writersChild.Id())
	for _, c := range []struct {
		by     *device
		parent string
		child  string
	}{
		{owner, parent1.Id(), ownersChild.Id()},
		{admin, parent1.Id(), adminsChild.Id()},
		{writer, parent2.Id(), writersChild.Id()},
	} {
		row, ok := c.by.row(ctx, c.child)
		require.True(t, ok, "%s holds a row of its child", c.by.name)
		assert.Equal(t, c.parent, row.ParentSpaceId, "%s: ParentSpaceId", c.by.name)
		assert.Equal(t, space.RefusalNone, row.Refusal, "%s: a signed child has no refusal", c.by.name)
		assert.Equal(t, space.SpaceTypeAny, row.Type, "%s: a child is a regular space", c.by.name)
	}

	t.Run("OnlyRegularTopLevelParents", func(t *testing.T) {
		derived, err := owner.spaces().Derive(ctx, space.DeriveRequest{Seed: []byte("not a parent")})
		require.NoError(t, err)
		for _, p := range []struct{ name, id string }{
			{"a child", ownersChild.Id()},
			{"a derived space", derived.Id()},
			{"the tech space", owner.sdk.TechSpaceId()},
		} {
			_, err := owner.spaces().CreateChild(ctx, space.CreateChildRequest{ParentSpaceId: p.id, Name: "Nested"})
			assert.ErrorIs(t, err, space.ErrParentSpaceInvalid, "a child of %s", p.name)
		}
		foreign := writer.create(t, ctx, "Not the owner's")
		_, err = owner.spaces().CreateChild(ctx, space.CreateChildRequest{ParentSpaceId: foreign.Id(), Name: "Orphan"})
		assert.ErrorIs(t, err, space.ErrSpaceUnknown, "a child of a space the account does not hold")
	})

	// The owner's child on its second device, and on a member added to it.
	note := writeNote(t, ctx, ownersChild, "written by the owner")
	rowB := ownerB.waitRow(t, ctx, ownersChild.Id(), "the child reaches the second device", func(si space.SpaceInfo) bool {
		return si.Status == space.StatusActive
	})
	assert.Equal(t, parent1.Id(), rowB.ParentSpaceId, "second device: ParentSpaceId")
	waitNote(t, ctx, ownerB.name, ownerB.get(t, ctx, ownersChild.Id()), note, "written by the owner")

	readerChild := addMembers(t, ctx, ownersChild, grant{reader, space.PermissionWriter})[0]
	waitNote(t, ctx, reader.name, readerChild, note, "written by the owner")
	joined := reader.waitRow(t, ctx, ownersChild.Id(), "the joined child names its parent", func(si space.SpaceInfo) bool {
		return si.ParentSpaceId != ""
	})
	assert.Equal(t, parent1.Id(), joined.ParentSpaceId, "joiner: ParentSpaceId")

	// A Reader of the parent creates a child: refused for the role.
	readersChild := reader.createChild(t, ctx, parent1.Id(), "Reader's child")
	refused := reader.waitRow(t, ctx, readersChild.Id(), "the reader's child is refused", func(si space.SpaceInfo) bool {
		return si.Refusal != space.RefusalNone
	})
	assert.Equal(t, space.RefusalParentPermissionDenied, refused.Refusal)
	assert.ErrorIs(t, reader.spaces().RetryPush(ctx, readersChild.Id()), space.ErrParentPermissionDenied)

	// The limit is full: the owner's next child is refused.
	over := owner.createChild(t, ctx, parent2.Id(), "Over the limit")
	refused = owner.waitRow(t, ctx, over.Id(), "the child over the limit is refused", func(si space.SpaceInfo) bool {
		return si.Refusal != space.RefusalNone
	})
	assert.Equal(t, space.RefusalChildLimitReached, refused.Refusal)
	assert.Equal(t, space.StatusActive, refused.Status, "a refused child stays an active space")
	assert.ErrorIs(t, owner.spaces().RetryPush(ctx, over.Id()), space.ErrChildSpacesLimitReached)
	assert.NotContains(t, children(t, ctx, owner, parent2.Id()), over.Id(), "a refused child is not listed")

	// Usable locally while refused.
	overNote := writeNote(t, ctx, over, "written while refused")
	assert.True(t, readsNote(ctx, over, overNote, "written while refused"))

	// The reason reaches the creator's second device, which holds no copy.
	onB := ownerB.waitRow(t, ctx, over.Id(), "the refusal reaches the second device", func(si space.SpaceInfo) bool {
		return si.Refusal == space.RefusalChildLimitReached
	})
	assert.Equal(t, parent2.Id(), onB.ParentSpaceId)
	assert.ErrorIs(t, ownerB.spaces().RetryPush(ctx, over.Id()), space.ErrSpaceNotStored)
	assert.ErrorIs(t, owner.spaces().RetryPush(ctx, parent1.Id()), space.ErrNotChildSpace)

	// The creators are not charged: the Writer, who created a child of
	// the owner's parent, still has a whole limit of its own.
	writersParent := writer.create(t, ctx, "Writer's own parent")
	var own []space.Space
	for _, name := range []string{"Own child 1", "Own child 2", "Own child 3"} {
		own = append(own, writer.createChild(t, ctx, writersParent.Id(), name))
	}
	for _, c := range own {
		waitSigned(t, ctx, writer, writersParent.Id(), c.Id())
		row, _ := writer.row(ctx, c.Id())
		assert.Equal(t, space.RefusalNone, row.Refusal, "the writer's own child %s", c.Info().Name)
	}

	// A place frees up once a child's deletion is final: the Writer
	// deletes the child it owns.
	require.NoError(t, writer.spaces().Delete(ctx, writersChild.Id()))
	waitDeletionFinal(t, ctx, owner, parent2.Id(), writersChild.Id())
	require.NoError(t, owner.spaces().RetryPush(ctx, over.Id()), "RetryPush once a place is free")
	cleared, _ := owner.row(ctx, over.Id())
	assert.Equal(t, space.RefusalNone, cleared.Refusal, "the refusal clears when the child is signed")
	waitSigned(t, ctx, owner, parent2.Id(), over.Id())
	ownerB.waitRow(t, ctx, over.Id(), "the cleared refusal reaches the second device", func(si space.SpaceInfo) bool {
		return si.Refusal == space.RefusalNone
	})
	waitNote(t, ctx, ownerB.name, ownerB.get(t, ctx, over.Id()), overNote, "written while refused")
}

// childSpacesCreateOffline: a child created offline under a
// parent the network holds is pushed when the network returns, and a
// parent and its child created together offline both arrive, with no
// refusal on the way. The offline device reaches the network through
// local proxies; a second device of the account reads what arrived.
func childSpacesCreateOffline(t *testing.T, net *childNet) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	proxied, routes := proxiedNetwork(t, net.yaml)
	stop := startTCPProxies(t, routes)
	defer func() { stop() }()
	owner := net.open(t, ctx, "owner behind proxies", newFixedSeedProvider(t), proxied)
	ownerB := net.device(t, ctx, owner, "owner's second device")

	parent := owner.create(t, ctx, "Online parent")
	require.True(t, waitFor(ctx, 60*time.Second, 250*time.Millisecond, func() bool {
		st := owner.spaces().Status(parent.Id())
		return st.State == space.SyncStateSynced && st.NetworkPeers > 0
	}), "the parent reaches the network: %+v", owner.spaces().Status(parent.Id()))

	stop()
	offline := func() context.Context {
		c, cancel := context.WithTimeout(ctx, 15*time.Second)
		t.Cleanup(cancel)
		return c
	}

	// A child of a parent the network holds.
	child := owner.createChild(t, ctx, parent.Id(), "Created offline")
	childNote := writeNote(t, ctx, child, "written offline in the child")
	assert.True(t, readsNote(ctx, child, childNote, "written offline in the child"))
	_, err := owner.spaces().Children(offline(), parent.Id())
	assert.ErrorIs(t, err, space.ErrOffline, "Children offline")
	assert.ErrorIs(t, owner.spaces().RetryPush(offline(), child.Id()), space.ErrOffline, "RetryPush offline")

	// A parent and its child, both created offline.
	parent2 := owner.create(t, ctx, "Offline parent")
	child2 := owner.createChild(t, ctx, parent2.Id(), "Child of the offline parent")
	parent2Note := writeNote(t, ctx, parent2, "written offline in the parent")
	child2Note := writeNote(t, ctx, child2, "written offline in its child")
	assert.ErrorIs(t, owner.spaces().RetryPush(offline(), child2.Id()), space.ErrParentSpaceNotAvailable,
		"a child of an own parent the network does not hold is not asked for")
	for _, id := range []string{child.Id(), child2.Id()} {
		row, _ := owner.row(ctx, id)
		assert.Equal(t, space.RefusalNone, row.Refusal, "no refusal recorded offline for %s", id)
	}

	watched := map[string]bool{child.Id(): true, parent2.Id(): true, child2.Id(): true}
	inWatched := func(id string) bool { return watched[id] }
	refusalsA := watchRefusals(t, owner, inWatched)
	refusalsB := watchRefusals(t, ownerB, inWatched)

	stop = startTCPProxies(t, routes)

	waitSigned(t, ctx, ownerB, parent.Id(), child.Id())
	waitSigned(t, ctx, ownerB, parent2.Id(), child2.Id())
	for _, c := range []struct {
		id, parent, note, text string
	}{
		{child.Id(), parent.Id(), childNote, "written offline in the child"},
		{parent2.Id(), "", parent2Note, "written offline in the parent"},
		{child2.Id(), parent2.Id(), child2Note, "written offline in its child"},
	} {
		row := ownerB.waitRow(t, ctx, c.id, "the space reaches the second device", func(si space.SpaceInfo) bool {
			return si.Status == space.StatusActive
		})
		assert.Equal(t, c.parent, row.ParentSpaceId, "ParentSpaceId of %s", c.id)
		waitNote(t, ctx, ownerB.name, ownerB.get(t, ctx, c.id), c.note, c.text)
	}
	for _, id := range []string{child.Id(), child2.Id()} {
		row, _ := owner.row(ctx, id)
		assert.Equal(t, space.RefusalNone, row.Refusal, "creator's row of %s", id)
	}
	assert.Empty(t, refusalsA.entries(), "no refusal on the creating device")
	assert.Empty(t, refusalsB.entries(), "no refusal on the second device")
}

// childSpacesSharedSpacesLimitUntouched: a child takes no place
// in anybody's shared-spaces limit. The Writer of a parent fills its own
// limit of shared spaces, then creates a child, which is signed and
// shared all the same.
func childSpacesSharedSpacesLimitUntouched(t *testing.T, net *childNet) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	owner := net.account(t, ctx, "owner")
	writer := net.account(t, ctx, "writer")
	parent := owner.create(t, ctx, "Parent")
	addMembers(t, ctx, parent, grant{writer, space.PermissionWriter})

	limits, err := writer.sdk.Account().Limits(ctx)
	require.NoError(t, err)
	require.NotNil(t, limits.SharedSpaces)
	shared := int(limits.SharedSpaces.Limit)
	require.Positive(t, shared)
	require.LessOrEqual(t, shared, 20, "the suite fills the shared-spaces limit; it expects a small one")

	share := func(sp space.Space) error {
		_, err := sp.ACL().CreateInvite(ctx)
		return err
	}
	for i := 0; i < shared; i++ {
		require.NoError(t, share(writer.create(t, ctx, "Shared")), "shared space %d of %d", i+1, shared)
	}
	require.ErrorIs(t, share(writer.create(t, ctx, "One too many")), space.ErrCoordinatorLimitReached,
		"the writer's shared-spaces limit is full")

	child := writer.createChild(t, ctx, parent.Id(), "Writer's child")
	waitSigned(t, ctx, owner, parent.Id(), child.Id())
	require.NoError(t, share(child), "a child is shared outside the limit")
}
