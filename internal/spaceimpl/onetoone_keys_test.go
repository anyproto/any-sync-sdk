package spaceimpl

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/anyproto/any-sync-sdk/internal/techspace"
)

// TestPeerKeyAction pins the watcher's decision: the resolve follows
// the missing profile, not the key write. The key reaches the directory
// on three paths (this watcher, the inbox invite, the synced directory
// from another device); an invite landing between accept and the row's
// arrival caches the key first, and the peer must still resolve.
func TestPeerKeyAction(t *testing.T) {
	for _, tc := range []struct {
		name           string
		dir            techspace.IdentityRecord
		cache, resolve bool
	}{
		{"no directory row", techspace.IdentityRecord{}, true, true},
		{"key cached elsewhere, profile missing", techspace.IdentityRecord{SymKey: "k"}, false, true},
		{"key cached, profile resolved", techspace.IdentityRecord{SymKey: "k", Name: "Alice"}, false, false},
		{"different key", techspace.IdentityRecord{SymKey: "old", Name: "Alice"}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache, resolve := peerKeyAction("k", tc.dir)
			assert.Equal(t, tc.cache, cache, "cache")
			assert.Equal(t, tc.resolve, resolve, "resolve")
		})
	}
}
