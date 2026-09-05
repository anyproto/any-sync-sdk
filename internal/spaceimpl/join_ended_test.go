package spaceimpl

import (
	"testing"

	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/space"
)

// TestJoinEnded pins the one row shape Join revives: the device-local
// deleted marker a declined or withdrawn join leaves over the synced
// remote=active that Join wrote. Every synced delete marker, the 1-1
// and guest rows, and a live joining row are outside it — and the
// shape still classifies as StatusDeleted, so Get keeps refusing it.
func TestJoinEnded(t *testing.T) {
	cases := []struct {
		name  string
		rec   techspace.SpaceIndexRecord
		ended bool
	}{
		{"declined or withdrawn join", techspace.SpaceIndexRecord{Id: "s", LocalStatus: techspace.StatusDeleted, RemoteStatus: techspace.StatusActive}, true},
		{"joining", techspace.SpaceIndexRecord{Id: "s", LocalStatus: joiningLocalStatus, RemoteStatus: techspace.StatusActive}, false},
		{"active member", techspace.SpaceIndexRecord{Id: "s", LocalStatus: techspace.StatusActive, RemoteStatus: techspace.StatusActive}, false},
		{"synced tombstone", techspace.SpaceIndexRecord{Id: "s", LocalStatus: techspace.StatusDeleted, RemoteStatus: techspace.StatusDeleted}, false},
		{"synced tombstone, local untouched", techspace.SpaceIndexRecord{Id: "s", RemoteStatus: techspace.StatusDeleted}, false},
		{"1-1 offload", techspace.SpaceIndexRecord{Id: "s", Type: space.SpaceTypeOneToOne, LocalStatus: techspace.StatusDeleted, RemoteStatus: techspace.OneToOneDeletedStatus}, false},
		{"1-1 with the join shape", techspace.SpaceIndexRecord{Id: "s", Type: space.SpaceTypeOneToOne, LocalStatus: techspace.StatusDeleted, RemoteStatus: techspace.StatusActive}, false},
		{"guest row with the join shape", techspace.SpaceIndexRecord{Id: "s", GuestKey: "k", LocalStatus: techspace.StatusDeleted, RemoteStatus: techspace.StatusActive}, false},
		{"guest offload", techspace.SpaceIndexRecord{Id: "s", GuestKey: "k", RemoteStatus: techspace.GuestDeletedRemoteStatus}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := joinEnded(tc.rec); got != tc.ended {
				t.Fatalf("joinEnded = %v, want %v", got, tc.ended)
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
