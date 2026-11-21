package space

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestPermissionStringRoundTrip pins the canonical wire vocabulary:
// every enum value round-trips through its label, and unknown labels
// (including "") collapse to PermissionNone.
func TestPermissionStringRoundTrip(t *testing.T) {
	labels := map[Permission]string{
		PermissionNone:   "none",
		PermissionReader: "reader",
		PermissionGuest:  "guest",
		PermissionWriter: "writer",
		PermissionAdmin:  "admin",
		PermissionOwner:  "owner",
	}
	for p, label := range labels {
		assert.Equal(t, label, p.String())
		assert.Equal(t, p, ParsePermission(label))
	}
	assert.Equal(t, PermissionNone, ParsePermission(""))
	assert.Equal(t, PermissionNone, ParsePermission("superadmin"))
	assert.Equal(t, "none", Permission(250).String())
}

// TestRefusalStringRoundTrip pins the wire vocabulary of a refused
// child space: every value round-trips through its label, and unknown
// labels (including "") collapse to RefusalNone.
func TestRefusalStringRoundTrip(t *testing.T) {
	labels := map[Refusal]string{
		RefusalNone:                   "none",
		RefusalParentNotAvailable:     "parent_not_available",
		RefusalParentInvalid:          "parent_invalid",
		RefusalParentPermissionDenied: "parent_permission_denied",
		RefusalChildLimitReached:      "child_limit_reached",
	}
	for r, label := range labels {
		assert.Equal(t, label, r.String())
		assert.Equal(t, r, ParseRefusal(label))
	}
	assert.Equal(t, RefusalNone, ParseRefusal(""))
	assert.Equal(t, RefusalNone, ParseRefusal("parent_gone"))
	assert.Equal(t, "none", Refusal(250).String())
}
