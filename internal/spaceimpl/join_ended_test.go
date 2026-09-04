package spaceimpl

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestJoinEnded pins the row shapes Join revives: the synced ended
// marker a declined or withdrawn join leaves, and the legacy device-local
// deleted marker over the synced active the old Join wrote. Every other
// synced delete marker, the 1-1 and guest rows, and a live joining row
// are outside it — and both ended shapes still classify as StatusDeleted
// and IsDeleted, so Get keeps refusing them.
func TestJoinEnded(t *testing.T) {
	cases := []struct {
		name  string
		rec   techspace.SpaceIndexRecord
		ended bool
	}{
		{"declined or withdrawn (synced)", techspace.SpaceIndexRecord{Id: "s", RemoteStatus: techspace.JoinEndedRemoteStatus}, true},
		{"synced ended over a legacy local joining", techspace.SpaceIndexRecord{Id: "s", LocalStatus: joiningLocalStatus, RemoteStatus: techspace.JoinEndedRemoteStatus}, true},
		{"declined or withdrawn (legacy device-local)", techspace.SpaceIndexRecord{Id: "s", LocalStatus: techspace.StatusDeleted, RemoteStatus: techspace.StatusActive}, true},
		{"joining (synced)", techspace.SpaceIndexRecord{Id: "s", RemoteStatus: techspace.JoiningRemoteStatus}, false},
		{"joining (legacy)", techspace.SpaceIndexRecord{Id: "s", LocalStatus: joiningLocalStatus, RemoteStatus: techspace.StatusActive}, false},
		{"active member", techspace.SpaceIndexRecord{Id: "s", LocalStatus: techspace.StatusActive, RemoteStatus: techspace.StatusActive}, false},
		{"synced tombstone", techspace.SpaceIndexRecord{Id: "s", LocalStatus: techspace.StatusDeleted, RemoteStatus: techspace.StatusDeleted}, false},
		{"synced tombstone, local untouched", techspace.SpaceIndexRecord{Id: "s", RemoteStatus: techspace.StatusDeleted}, false},
		{"1-1 offload", techspace.SpaceIndexRecord{Id: "s", Type: space.SpaceTypeOneToOne, LocalStatus: techspace.StatusDeleted, RemoteStatus: techspace.OneToOneDeletedStatus}, false},
		{"1-1 with the legacy join shape", techspace.SpaceIndexRecord{Id: "s", Type: space.SpaceTypeOneToOne, LocalStatus: techspace.StatusDeleted, RemoteStatus: techspace.StatusActive}, false},
		{"1-1 with the synced ended marker", techspace.SpaceIndexRecord{Id: "s", Type: space.SpaceTypeOneToOne, RemoteStatus: techspace.JoinEndedRemoteStatus}, false},
		{"guest row with the legacy join shape", techspace.SpaceIndexRecord{Id: "s", GuestKey: "k", LocalStatus: techspace.StatusDeleted, RemoteStatus: techspace.StatusActive}, false},
		{"guest row with the synced ended marker", techspace.SpaceIndexRecord{Id: "s", GuestKey: "k", RemoteStatus: techspace.JoinEndedRemoteStatus}, false},
		{"guest offload", techspace.SpaceIndexRecord{Id: "s", GuestKey: "k", RemoteStatus: techspace.GuestDeletedRemoteStatus}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.rec.JoinEnded(); got != tc.ended {
				t.Fatalf("JoinEnded = %v, want %v", got, tc.ended)
			}
			if tc.ended {
				if st := mapStatus(tc.rec.Type, tc.rec.LocalStatus, tc.rec.RemoteStatus); st != space.StatusDeleted {
					t.Fatalf("ended join maps to %v, want StatusDeleted", st)
				}
				if !tc.rec.IsDeleted() {
					t.Fatal("ended join must count as deleted (Get refuses it, eager-load skips it)")
				}
			}
		})
	}
}

// TestMapStatus_JoinLifecycle pins the synced join lifecycle and its
// precedence: the synced pending / ended states are the account-wide
// truth and outrank the legacy device-local markers, the synced
// tombstones outrank everything, and the legacy shapes still read as
// they always did on their own.
func TestMapStatus_JoinLifecycle(t *testing.T) {
	reg := space.SpaceTypeAny

	// Synced lifecycle on a row with no local marker — what every
	// device of the account reads.
	assert.Equal(t, space.StatusJoining, mapStatus(reg, "", techspace.JoiningRemoteStatus), "synced joining")
	assert.Equal(t, space.StatusDeleted, mapStatus(reg, "", techspace.JoinEndedRemoteStatus), "synced ended")
	assert.Equal(t, space.StatusActive, mapStatus(reg, "", techspace.StatusActive), "accepted and flipped elsewhere")
	assert.Equal(t, space.StatusActive, mapStatus(reg, techspace.StatusActive, techspace.StatusActive), "loaded here")

	// Synced state outranks a stale legacy marker on this device: an
	// ended join re-requested from another device reads joining here, a
	// legacy pending marker never hides a synced verdict.
	assert.Equal(t, space.StatusJoining, mapStatus(reg, techspace.StatusDeleted, techspace.JoiningRemoteStatus), "legacy ended + synced joining")
	assert.Equal(t, space.StatusJoining, mapStatus(reg, joiningLocalStatus, techspace.JoiningRemoteStatus), "legacy joining + synced joining")
	assert.Equal(t, space.StatusDeleted, mapStatus(reg, joiningLocalStatus, techspace.JoinEndedRemoteStatus), "legacy joining + synced ended")

	// Synced tombstones outrank the join states.
	assert.Equal(t, space.StatusDeleted, mapStatus(reg, "", techspace.StatusDeleted), "tombstone")
	assert.Equal(t, space.StatusDeleted, mapStatus(reg, joiningLocalStatus, techspace.StatusDeleted), "tombstone over legacy joining")

	// Legacy shapes on their own keep their meaning.
	assert.Equal(t, space.StatusJoining, mapStatus(reg, joiningLocalStatus, techspace.StatusActive), "legacy joining")
	assert.Equal(t, space.StatusDeleted, mapStatus(reg, techspace.StatusDeleted, techspace.StatusActive), "legacy ended")

	// A direct add registered over an ended join reads as the invite
	// (the inbox path clears a legacy marker when it registers).
	assert.Equal(t, space.StatusInvitePending, mapStatus(reg, "", techspace.InvitePendingRemoteStatus), "direct add over ended join")
}
