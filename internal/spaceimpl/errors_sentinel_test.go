package spaceimpl

import (
	"errors"
	"strings"
	"testing"

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
