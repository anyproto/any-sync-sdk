package spaceimpl

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestMapStatus_AccountScopedResolutionBeatsLocalPending pins the
// "processed is account-scoped" rule: when a 1-1 carries a synced
// (account-wide) resolution AND a stale device-local pending — the case
// where an old inbox invite was replayed on a new/other device before the
// synced row arrived — the synced resolution wins, so the invite is never
// re-surfaced as a pending prompt.
func TestMapStatus_AccountScopedResolutionBeatsLocalPending(t *testing.T) {
	const oneToOne = space.SpaceTypeOneToOne
	pend := oneToOnePendingLocalStatus

	// Synced resolution + stale local pending → resolution wins.
	assert.Equal(t, space.StatusActive,
		mapStatus(oneToOne, pend, techspace.StatusActive), "accepted account-wide → Active")
	assert.Equal(t, space.StatusOneToOneDeclined,
		mapStatus(oneToOne, pend, oneToOneDeclinedRemoteStatus), "declined account-wide → Declined")
	assert.Equal(t, space.StatusDeleted,
		mapStatus(oneToOne, pend, techspace.OneToOneDeletedStatus), "deleted account-wide → Deleted")

	// A genuinely-unresolved incoming (pending, no synced resolution) still
	// surfaces as a prompt.
	assert.Equal(t, space.StatusOneToOnePending,
		mapStatus(oneToOne, pend, ""), "unresolved incoming stays Pending")
	// A bare synced 1-1 row (no local status yet) surfaces as Pending.
	assert.Equal(t, space.StatusOneToOnePending,
		mapStatus(oneToOne, "", ""), "bare 1-1 row → Pending")
}

// TestReconcileOneToOneInvites_OfflineThenOnline proves the send-side
// offline→online path: a 1-1 invite marked toSend while the coordinator is
// unreachable is NOT cleared (so it stays queued), and the next pass after
// reconnect sends it and clears the marker. This is the "create a 1-1
// offline, deliver the notification once online" guarantee.
func TestReconcileOneToOneInvites_OfflineThenOnline(t *testing.T) {
	rows := []techspace.SpaceIndexRecord{{
		Id:                  "s1",
		Type:                space.SpaceTypeOneToOne,
		OneToOneInviteState: oneToOneInviteToSend,
		OneToOnePeer:        "peerA",
	}}

	online := false
	cleared := map[string]bool{}
	var sends int
	send := func(_ context.Context, _ string) error {
		sends++
		if !online {
			return errors.New("offline: coordinator unreachable")
		}
		return nil
	}
	clear := func(_ context.Context, id string) error { cleared[id] = true; return nil }

	// Offline pass: send fails → marker must persist for retry.
	reconcileInviteOutbox(context.Background(), rows, send, clear, noRegularSend, noRegularClear)
	assert.Equal(t, 1, sends)
	assert.False(t, cleared["s1"], "offline send failure must leave the toSend marker for retry")

	// Online pass: send succeeds → marker cleared.
	online = true
	reconcileInviteOutbox(context.Background(), rows, send, clear, noRegularSend, noRegularClear)
	assert.Equal(t, 2, sends)
	assert.True(t, cleared["s1"], "online send must clear the marker")
}

// noRegularSend / noRegularClear are fail-loud stubs for tests that only
// exercise the 1-1 half of the outbox core.
func noRegularSend(_ context.Context, receiverId, spaceId string) error {
	panic("unexpected regular send: " + spaceId + " → " + receiverId)
}

func noRegularClear(_ context.Context, spaceId, receiverId string) error {
	panic("unexpected regular clear: " + spaceId + " → " + receiverId)
}

// TestReconcileOneToOneInvites_SelectsRowsAndClearsMalformed checks the
// row filtering: only active 1-1 rows still owing a notification send;
// non-1-1 / non-toSend rows are skipped; a 1-1 row with no peer identity
// is cleared so the loop can't spin on it.
func TestReconcileOneToOneInvites_SelectsRowsAndClearsMalformed(t *testing.T) {
	rows := []techspace.SpaceIndexRecord{
		{Id: "regular", Type: space.SpaceTypeAny, OneToOneInviteState: oneToOneInviteToSend, OneToOnePeer: "x"},
		{Id: "noMarker", Type: space.SpaceTypeOneToOne, OneToOnePeer: "x"},
		{Id: "malformed", Type: space.SpaceTypeOneToOne, OneToOneInviteState: oneToOneInviteToSend},
		{Id: "good", Type: space.SpaceTypeOneToOne, OneToOneInviteState: oneToOneInviteToSend, OneToOnePeer: "peerB"},
	}
	var sent []string
	cleared := map[string]bool{}
	send := func(_ context.Context, peer string) error { sent = append(sent, peer); return nil }
	clear := func(_ context.Context, id string) error { cleared[id] = true; return nil }

	reconcileInviteOutbox(context.Background(), rows, send, clear, noRegularSend, noRegularClear)

	assert.Equal(t, []string{"peerB"}, sent, "only the eligible 1-1 row sends")
	assert.True(t, cleared["malformed"], "a 1-1 row with no peer is cleared to stop spinning")
	assert.True(t, cleared["good"], "a delivered 1-1 row is cleared")
	assert.False(t, cleared["regular"], "non-1-1 rows untouched")
	assert.False(t, cleared["noMarker"], "1-1 rows without the marker untouched")
}

// TestReconcileInviteOutbox_RegularPerReceiver pins the direct-add half
// of the outbox core: every queued receiver on a regular row is sent and
// cleared independently — one receiver's transient failure must not
// block or clear the others (batch AddAccounts = N independent
// notifications).
func TestReconcileInviteOutbox_RegularPerReceiver(t *testing.T) {
	rows := []techspace.SpaceIndexRecord{
		{Id: "s1", Type: space.SpaceTypeAny, InviteNotifyPending: []string{"bob", "carol"}},
		{Id: "noOutbox", Type: space.SpaceTypeAny},
	}
	var sent []string
	cleared := map[string]bool{}
	send := func(_ context.Context, receiverId, spaceId string) error {
		sent = append(sent, spaceId+"→"+receiverId)
		if receiverId == "carol" {
			return errors.New("offline: coordinator unreachable")
		}
		return nil
	}
	clear := func(_ context.Context, spaceId, receiverId string) error {
		cleared[spaceId+"→"+receiverId] = true
		return nil
	}

	reconcileInviteOutbox(context.Background(), rows, noOneToOneSend, noOneToOneClear, send, clear)

	assert.Equal(t, []string{"s1→bob", "s1→carol"}, sent, "every queued receiver is attempted")
	assert.True(t, cleared["s1→bob"], "a delivered entry clears")
	assert.False(t, cleared["s1→carol"], "a transient failure keeps the entry for retry")
}

// TestReconcileInviteOutbox_UndeliverableClears proves that a PERMANENT
// send failure (errInviteUndeliverable) drops the outbox entry instead of
// retrying it forever.
func TestReconcileInviteOutbox_UndeliverableClears(t *testing.T) {
	rows := []techspace.SpaceIndexRecord{
		{Id: "s1", Type: space.SpaceTypeAny, InviteNotifyPending: []string{"junk-identity"}},
	}
	cleared := map[string]bool{}
	send := func(_ context.Context, receiverId, spaceId string) error {
		return fmt.Errorf("%w: bad identity", errInviteUndeliverable)
	}
	clear := func(_ context.Context, spaceId, receiverId string) error {
		cleared[spaceId+"→"+receiverId] = true
		return nil
	}

	reconcileInviteOutbox(context.Background(), rows, noOneToOneSend, noOneToOneClear, send, clear)
	assert.True(t, cleared["s1→junk-identity"], "an undeliverable entry is cleared, not retried")
}

// TestReconcileInviteOutbox_Mixed runs 1-1 and regular rows through one
// pass and checks the two halves don't interfere.
func TestReconcileInviteOutbox_Mixed(t *testing.T) {
	rows := []techspace.SpaceIndexRecord{
		{Id: "oto", Type: space.SpaceTypeOneToOne, OneToOneInviteState: oneToOneInviteToSend, OneToOnePeer: "peerA"},
		{Id: "reg", Type: space.SpaceTypeAny, InviteNotifyPending: []string{"bob"}},
	}
	var otoSent, regSent []string
	otoCleared, regCleared := map[string]bool{}, map[string]bool{}

	reconcileInviteOutbox(context.Background(), rows,
		func(_ context.Context, peer string) error { otoSent = append(otoSent, peer); return nil },
		func(_ context.Context, id string) error { otoCleared[id] = true; return nil },
		func(_ context.Context, receiverId, spaceId string) error {
			regSent = append(regSent, spaceId+"→"+receiverId)
			return nil
		},
		func(_ context.Context, spaceId, receiverId string) error {
			regCleared[spaceId+"→"+receiverId] = true
			return nil
		})

	assert.Equal(t, []string{"peerA"}, otoSent)
	assert.True(t, otoCleared["oto"])
	assert.Equal(t, []string{"reg→bob"}, regSent)
	assert.True(t, regCleared["reg→bob"])
}

// noOneToOneSend / noOneToOneClear are fail-loud stubs for tests that
// only exercise the regular half of the outbox core.
func noOneToOneSend(_ context.Context, receiverId string) error {
	panic("unexpected 1-1 send: " + receiverId)
}

func noOneToOneClear(_ context.Context, spaceId string) error {
	panic("unexpected 1-1 clear: " + spaceId)
}

// TestMapStatus_InvitePendingDeclined pins the direct-add status mapping:
// the synced invitePending/inviteDeclined remote statuses surface as the
// new public statuses, deletion still wins, the loading device shows
// Active, and a crash-window row (local=joining is impossible for
// direct adds, but pending must win over unrelated local values).
func TestMapStatus_InvitePendingDeclined(t *testing.T) {
	reg := space.SpaceTypeAny

	assert.Equal(t, space.StatusInvitePending,
		mapStatus(reg, "", techspace.InvitePendingRemoteStatus), "synced pending row → InvitePending")
	assert.Equal(t, space.StatusInviteDeclined,
		mapStatus(reg, "", techspace.InviteDeclinedRemoteStatus), "synced declined row → InviteDeclined")

	// The synced tombstone beats both. The device-local deleted marker
	// does not: nothing writes it any more — it is the legacy ended join
	// — and a direct add landing over one is the newer account-wide
	// truth (techspace.SpaceIndexRecord.LocalDeleteStands).
	assert.Equal(t, space.StatusDeleted,
		mapStatus(reg, "", techspace.StatusDeleted), "synced tombstone wins")
	assert.Equal(t, space.StatusInvitePending,
		mapStatus(reg, techspace.StatusDeleted, techspace.InvitePendingRemoteStatus), "direct add over a legacy ended join")
	assert.Equal(t, space.StatusInviteDeclined,
		mapStatus(reg, techspace.StatusDeleted, techspace.InviteDeclinedRemoteStatus), "decline over a legacy ended join")

	// Accepting device mid-pull: synced accept + device-local loading →
	// Active (matches what the account's other devices show).
	assert.Equal(t, space.StatusActive,
		mapStatus(reg, inviteLoadingLocalStatus, techspace.StatusActive), "loading device → Active")

	// Synced pending beats an unrelated device-local value.
	assert.Equal(t, space.StatusInvitePending,
		mapStatus(reg, joiningLocalStatus, techspace.InvitePendingRemoteStatus), "pending wins over stale local joining")
}
