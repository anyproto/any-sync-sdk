package config

import "testing"

func ptr[T any](v T) *T { return &v }

func TestResolveP2P(t *testing.T) {
	cases := []struct {
		name     string
		headless bool
		enabled  *bool
		want     bool
	}{
		{"non-headless default is enabled", false, nil, true},
		{"non-headless explicit off", false, ptr(false), false},
		{"non-headless explicit on", false, ptr(true), true},
		{"headless default is disabled", true, nil, false},
		{"headless explicit opt-in", true, ptr(true), true},
		{"headless explicit off", true, ptr(false), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := Config{Headless: tc.headless, P2P: P2P{Enabled: tc.enabled}}
			if got := c.ResolveP2P().IsEnabled(); got != tc.want {
				t.Fatalf("ResolveP2P().IsEnabled() = %v, want %v", got, tc.want)
			}
		})
	}
}

// A headless resolve must not mutate the caller's P2P config in place.
func TestResolveP2PDoesNotMutate(t *testing.T) {
	c := Config{Headless: true}
	_ = c.ResolveP2P()
	if c.P2P.Enabled != nil {
		t.Fatalf("ResolveP2P mutated the source config: Enabled = %v", c.P2P.Enabled)
	}
}
