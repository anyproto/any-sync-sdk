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
		{"joining", techspace.SpaceIndexRecord{Id: "s", LocalStatus: joiningLocalStatus, RemoteStatus: techspace.StatusActive}, true},
		{"incoming 1-1 pending", techspace.SpaceIndexRecord{Id: "s", LocalStatus: oneToOnePendingLocalStatus}, true},
		{"1-1 declined", techspace.SpaceIndexRecord{Id: "s", RemoteStatus: oneToOneDeclinedRemoteStatus}, true},
		{"direct-add invite pending", techspace.SpaceIndexRecord{Id: "s", RemoteStatus: techspace.InvitePendingRemoteStatus}, true},
		{"direct-add invite declined", techspace.SpaceIndexRecord{Id: "s", RemoteStatus: techspace.InviteDeclinedRemoteStatus}, true},
		// The accept paths flip (or stamp) these before loading — they
		// must pass so the post-acceptance load is never self-blocked.
		{"invite loading (accept in flight)", techspace.SpaceIndexRecord{Id: "s", LocalStatus: inviteLoadingLocalStatus, RemoteStatus: techspace.StatusActive}, false},
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
}
