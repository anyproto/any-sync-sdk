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
