package spaceimpl

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/space"
)

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
	reconcileOneToOneInvites(context.Background(), rows, send, clear)
	assert.Equal(t, 1, sends)
	assert.False(t, cleared["s1"], "offline send failure must leave the toSend marker for retry")

	// Online pass: send succeeds → marker cleared.
	online = true
	reconcileOneToOneInvites(context.Background(), rows, send, clear)
	assert.Equal(t, 2, sends)
	assert.True(t, cleared["s1"], "online send must clear the marker")
}

// TestReconcileOneToOneInvites_SelectsRowsAndClearsMalformed checks the
// row filtering: only active 1-1 rows still owing a notification send;
// non-1-1 / non-toSend rows are skipped; a 1-1 row with no peer identity
// is cleared so the loop can't spin on it.
func TestReconcileOneToOneInvites_SelectsRowsAndClearsMalformed(t *testing.T) {
	rows := []techspace.SpaceIndexRecord{
		{Id: "regular", Type: space.SpaceTypeRegular, OneToOneInviteState: oneToOneInviteToSend, OneToOnePeer: "x"},
		{Id: "noMarker", Type: space.SpaceTypeOneToOne, OneToOnePeer: "x"},
		{Id: "malformed", Type: space.SpaceTypeOneToOne, OneToOneInviteState: oneToOneInviteToSend},
		{Id: "good", Type: space.SpaceTypeOneToOne, OneToOneInviteState: oneToOneInviteToSend, OneToOnePeer: "peerB"},
	}
	var sent []string
	cleared := map[string]bool{}
	send := func(_ context.Context, peer string) error { sent = append(sent, peer); return nil }
	clear := func(_ context.Context, id string) error { cleared[id] = true; return nil }

	reconcileOneToOneInvites(context.Background(), rows, send, clear)

	assert.Equal(t, []string{"peerB"}, sent, "only the eligible 1-1 row sends")
	assert.True(t, cleared["malformed"], "a 1-1 row with no peer is cleared to stop spinning")
	assert.True(t, cleared["good"], "a delivered 1-1 row is cleared")
	assert.False(t, cleared["regular"], "non-1-1 rows untouched")
	assert.False(t, cleared["noMarker"], "1-1 rows without the marker untouched")
}
