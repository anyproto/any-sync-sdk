package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/space"
)

// TestE2E_ChildSpaces_Scopes: a parent with three children used as
// access scopes. A member of two of them sees and reads exactly those
// two. The parent's Owner sees all three in Children and reads none of
// them, a copy it pulls included. Each child changes its read key on
// its own.
func TestE2E_ChildSpaces_Scopes(t *testing.T) {
	t.Parallel()
	net := childSpacesNetwork(t)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()

	owner := net.account(t, ctx, "parent's owner")
	w1 := net.account(t, ctx, "owner of two children")
	w2 := net.account(t, ctx, "owner of one child")
	m := net.account(t, ctx, "member of two children")

	parent := owner.create(t, ctx, "Parent")
	addMembers(t, ctx, parent,
		grant{w1, space.PermissionWriter},
		grant{w2, space.PermissionWriter},
		grant{m, space.PermissionReader})
	teamA := w1.createChild(t, ctx, parent.Id(), "Team A")
	teamB := w1.createChild(t, ctx, parent.Id(), "Team B")
	teamC := w2.createChild(t, ctx, parent.Id(), "Team C")
	scopes := []space.Space{teamA, teamB, teamC}
	notes := make([]string, len(scopes))
	for i, c := range scopes {
		waitSigned(t, ctx, owner, parent.Id(), c.Id())
		notes[i] = writeNote(t, ctx, c, "inside "+c.Info().Name)
	}
	mA := addMembers(t, ctx, teamA, grant{m, space.PermissionWriter})[0]
	mB := addMembers(t, ctx, teamB, grant{m, space.PermissionWriter})[0]

	t.Run("MemberOfTwo", func(t *testing.T) {
		waitNote(t, ctx, m.name, mA, notes[0], "inside Team A")
		waitNote(t, ctx, m.name, mB, notes[1], "inside Team B")
		for _, c := range []space.Space{teamA, teamB} {
			m.waitRow(t, ctx, c.Id(), "the child names the parent", func(si space.SpaceInfo) bool {
				return si.ParentSpaceId == parent.Id()
			})
		}
		infos, err := m.spaces().List(ctx)
		require.NoError(t, err)
		var underParent []string
		for _, si := range infos {
			if si.ParentSpaceId == parent.Id() {
				underParent = append(underParent, si.Id)
			}
		}
		assert.ElementsMatch(t, []string{teamA.Id(), teamB.Id()}, underParent)
		_, err = m.spaces().Get(ctx, teamC.Id())
		assert.ErrorIs(t, err, space.ErrSpaceUnknown, "the third child is not the member's")
	})

	t.Run("ParentOwnerSeesAllReadsNone", func(t *testing.T) {
		listed := children(t, ctx, owner, parent.Id())
		assert.Len(t, listed, 3)
		for _, c := range scopes {
			assert.Equal(t, space.ChildStatusActive, listed[c.Id()], "%s listed", c.Info().Name)
			_, held := owner.row(ctx, c.Id())
			assert.False(t, held, "the parent's owner holds no row of %s", c.Info().Name)
		}
		// A copy pulled by id stays sealed.
		require.NoError(t, owner.spaces().Track(ctx, teamA.Id()))
		pullCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		sealed, err := owner.spaces().Get(pullCtx, teamA.Id())
		if err != nil {
			t.Logf("the parent's owner does not load Team A: %v", err)
			return
		}
		neverNote(t, ctx, owner.name, sealed, notes[0], "inside Team A", 10*time.Second)
	})

	t.Run("KeysRotateOnTheirOwn", func(t *testing.T) {
		keyOf := func(d *device, c space.Space) string {
			t.Helper()
			row := d.waitRow(t, ctx, c.Id(), "push keys mirrored", func(si space.SpaceInfo) bool {
				return si.PushKeys != nil && si.PushKeys.EncKeyId != ""
			})
			return row.PushKeys.EncKeyId
		}
		keyA, keyB, keyC := keyOf(w1, teamA), keyOf(w1, teamB), keyOf(w2, teamC)

		require.NoError(t, owner.spaces().RemoveChildMembers(ctx, teamA.Id(), []string{m.id()}))
		w1.waitRow(t, ctx, teamA.Id(), "Team A changes its read key", func(si space.SpaceInfo) bool {
			return si.PushKeys != nil && si.PushKeys.EncKeyId != keyA
		})
		waitRotationPending(t, ctx, w1.name, teamA, false)
		after := writeNote(t, ctx, teamA, "Team A after its rotation")
		stillB := writeNote(t, ctx, teamB, "Team B after Team A's rotation")
		waitNote(t, ctx, m.name, mB, stillB, "Team B after Team A's rotation")
		neverNote(t, ctx, m.name+" (removed from Team A)", mA, after, "Team A after its rotation", 10*time.Second)

		for _, c := range []struct {
			d    *device
			sp   space.Space
			key  string
			name string
		}{{w1, teamB, keyB, "Team B"}, {w2, teamC, keyC, "Team C"}} {
			pending, err := c.sp.ACL().RotationPending(ctx)
			require.NoError(t, err)
			assert.False(t, pending, "%s has no pending rotation", c.name)
			assert.Equal(t, c.key, keyOf(c.d, c.sp), "%s keeps its read key", c.name)
		}
	})
}
