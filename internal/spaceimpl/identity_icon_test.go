package spaceimpl

import "testing"

// A profile-supplied space id reaches a store path and a public URL,
// so only the space-id shape passes.
func TestSafeSpaceId(t *testing.T) {
	for id, want := range map[string]bool{
		"bafyreibls6gvs7tnqnbci2hvum3apmggkuqicuidkg2iwmivk47kqxride.1ufw88v1uozle": true,
		"":              false,
		"nodot":         false,
		"two.dots.here": false,
		".leading":      false,
		"trailing.":     false,
		"../blob/x":     false,
		"a.b/c":         false,
		"a.b?x=1":       false,
		"a b.c":         false,
		"bafy.1\x00":    false,
	} {
		if got := safeSpaceId(id); got != want {
			t.Errorf("safeSpaceId(%q) = %v, want %v", id, got, want)
		}
	}
}
