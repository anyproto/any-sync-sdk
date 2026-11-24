package spaceimpl

import (
	"errors"
	"strings"
	"testing"

	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/net/rpc/rpcerr"
	"storj.io/drpc/drpcerr"

	"github.com/anyproto/any-sync-sdk/space"
)

// The internal pending vars wrap the public sentinels, so callers on
// either side of the package boundary classify with errors.Is.
func TestPendingVarsWrapPublicSentinels(t *testing.T) {
	if !errors.Is(ErrJoinPending, space.ErrJoinPending) {
		t.Error("ErrJoinPending does not wrap space.ErrJoinPending")
	}
	if !errors.Is(ErrInviteAcceptPending, space.ErrInviteAcceptPending) {
		t.Error("ErrInviteAcceptPending does not wrap space.ErrInviteAcceptPending")
	}
	// Message phrases stay intact for consumers still string-matching.
	if !strings.Contains(ErrJoinPending.Error(), "join pending owner approval") {
		t.Errorf("ErrJoinPending message changed: %v", ErrJoinPending)
	}
	if !strings.Contains(ErrInviteAcceptPending.Error(), "invite accepted; space load pending") {
		t.Errorf("ErrInviteAcceptPending message changed: %v", ErrInviteAcceptPending)
	}
}

func TestDecodeIdentityBadIdentity(t *testing.T) {
	if _, err := decodeIdentity(""); !errors.Is(err, space.ErrBadIdentity) {
		t.Errorf("empty identity: want ErrBadIdentity, got %v", err)
	}
	_, err := decodeIdentity("definitely-not-an-account-address")
	if !errors.Is(err, space.ErrBadIdentity) {
		t.Errorf("garbage identity: want ErrBadIdentity, got %v", err)
	}
	if !strings.Contains(err.Error(), "decode identity") {
		t.Errorf("message lost %q phrase: %v", "decode identity", err)
	}
}

// The coordinator finds no log of an ACL when no node holds the space,
// and the nodes pass its answer on as an rpc code. What the node client
// makes of that code reads as an unknown space.
func TestNetworkErrAclNotOnTheNetwork(t *testing.T) {
	code := uint64(consensusproto.ErrCodes_ErrorOffset) + uint64(consensusproto.ErrCodes_LogNotFound)
	answer := rpcerr.Unwrap(drpcerr.WithCode(errors.New("log not found"), code))
	if err := networkErr("ChildMembers", answer); !errors.Is(err, space.ErrSpaceUnknown) {
		t.Errorf("want ErrSpaceUnknown, got %v", err)
	}
}
