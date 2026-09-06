package spaceimpl

import (
	"errors"
	"testing"

	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestMaterializeBlock pins which tech-space rows the pending guard
// refuses: every not-yet-accepted flavor blocks (wrapping
// space.ErrSpaceNotAccepted); active / loading / tracked rows pass.
func TestMaterializeBlock(t *testing.T) {
	cases := []struct {
		name    string
		rec     techspace.SpaceIndexRecord
		blocked bool
	}{
		{"active", techspace.SpaceIndexRecord{Id: "s", LocalStatus: techspace.StatusActive, RemoteStatus: techspace.StatusActive}, false},
		{"empty statuses (tracked/legacy row)", techspace.SpaceIndexRecord{Id: "s"}, false},
		// The synced pending join blocks on every device of the account —
		// the requesting one and the ones that only synced the row in.
		{"joining (synced)", techspace.SpaceIndexRecord{Id: "s", RemoteStatus: techspace.JoiningRemoteStatus}, true},
		{"joining (synced) over a legacy ended marker", techspace.SpaceIndexRecord{Id: "s", LocalStatus: techspace.StatusDeleted, RemoteStatus: techspace.JoiningRemoteStatus}, true},
		{"joining (legacy device-local)", techspace.SpaceIndexRecord{Id: "s", LocalStatus: joiningLocalStatus, RemoteStatus: techspace.StatusActive}, true},
		{"incoming 1-1 pending", techspace.SpaceIndexRecord{Id: "s", LocalStatus: oneToOnePendingLocalStatus}, true},
		// Acceptance is account-scoped: synced remote=active (accepted or
		// initiated on any device) wins over a stale device-local pending.
		{"1-1 accepted on another device", techspace.SpaceIndexRecord{Id: "s", Type: space.SpaceTypeOneToOne, LocalStatus: oneToOnePendingLocalStatus, RemoteStatus: techspace.StatusActive}, false},
		// A 1-1 row synced in before this device set any status is an
		// unresolved incoming request — must not materialize.
		{"bare 1-1 row (no statuses)", techspace.SpaceIndexRecord{Id: "s", Type: space.SpaceTypeOneToOne}, true},
		{"1-1 declined", techspace.SpaceIndexRecord{Id: "s", RemoteStatus: oneToOneDeclinedRemoteStatus}, true},
		{"direct-add invite pending", techspace.SpaceIndexRecord{Id: "s", RemoteStatus: techspace.InvitePendingRemoteStatus}, true},
		{"direct-add invite declined", techspace.SpaceIndexRecord{Id: "s", RemoteStatus: techspace.InviteDeclinedRemoteStatus}, true},
		// The accept paths flip (or stamp) these before loading — they
		// must pass so the post-acceptance load is never self-blocked.
		{"invite loading (accept in flight)", techspace.SpaceIndexRecord{Id: "s", LocalStatus: inviteLoadingLocalStatus, RemoteStatus: techspace.StatusActive}, false},
		// A joined space flipped active on another device loads here.
		{"join accepted elsewhere", techspace.SpaceIndexRecord{Id: "s", RemoteStatus: techspace.StatusActive}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := MaterializeBlock(tc.rec)
			if tc.blocked {
				if !errors.Is(err, space.ErrSpaceNotAccepted) {
					t.Fatalf("want ErrSpaceNotAccepted, got %v", err)
				}
			} else if err != nil {
				t.Fatalf("want nil, got %v", err)
			}
		})
	}
	// An ended join is not the guard's business — it is a deleted shape,
	// refused one step earlier by Get (IsDeleted) and skipped by the
	// eager-loader on the same predicate.
	ended := techspace.SpaceIndexRecord{Id: "s", RemoteStatus: techspace.JoinEndedRemoteStatus}
	if !ended.IsDeleted() {
		t.Fatal("ended join must be IsDeleted")
	}
}
