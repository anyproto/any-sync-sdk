package space

import (
	"strings"
	"testing"

	"github.com/anyproto/any-sync/commonspace/object/acl/list"
)

// TestSentinelMessages pins the message phrases downstream consumers
// historically string-matched. errors.Is is the supported contract;
// the phrases stay stable until those consumers migrate.
func TestSentinelMessages(t *testing.T) {
	cases := []struct {
		err    error
		phrase string
	}{
		{ErrSpaceUnknown, "unknown space"},
		{ErrJoinPending, "join pending owner approval"},
		{ErrInviteAcceptPending, "invite accepted; space load pending"},
		{ErrNotInvitePending, "not invite-pending"},
		{ErrIsOneToOne, "is a 1-1 space"},
		{ErrSelfPair, "cannot pair with self"},
		{ErrNoActiveGuestKey, "no active guest key"},
		{ErrBadIdentity, "bad identity"},
		{ErrDuplicateInvite, "duplicate invites"},
		{ErrInsufficientPermissions, "insufficient permissions"},
		{ErrAclRecordNotFound, "no such record"},
		{ErrUnsupported, "unsupported on this space"},
		{ErrObjectDeleted, "object deleted"},
		{ErrObjectNotFound, "object not found"},
		{ErrNotAType, "names a collection, not a type"},
		{ErrNotACollection, "names a type, not a collection"},
		{ErrWrongSlot, "a type goes in any.type, a collection in any.collections"},
	}
	for _, c := range cases {
		if !strings.Contains(c.err.Error(), c.phrase) {
			t.Errorf("%v: message lost phrase %q", c.err, c.phrase)
		}
	}
}

// TestAclSentinelAliases pins that the re-exports are the SAME error
// values any-sync raises, so they errors.Is through spaceimpl's %w
// chains without conversion.
func TestAclSentinelAliases(t *testing.T) {
	if ErrDuplicateInvite != list.ErrDuplicateInvites { //nolint:errorlint
		t.Error("ErrDuplicateInvite is not list.ErrDuplicateInvites")
	}
	if ErrInsufficientPermissions != list.ErrInsufficientPermissions { //nolint:errorlint
		t.Error("ErrInsufficientPermissions is not list.ErrInsufficientPermissions")
	}
	if ErrAclRecordNotFound != list.ErrNoSuchRecord { //nolint:errorlint
		t.Error("ErrAclRecordNotFound is not list.ErrNoSuchRecord")
	}
}
