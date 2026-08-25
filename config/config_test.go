package config

import (
	"testing"
	"time"
)

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

func TestGlobalDefaultsAndEnable(t *testing.T) {
	var g Global
	if g.IsEnabled() {
		t.Fatal("global p2p is opt-in")
	}
	off := false
	if (Global{Enabled: &off}).IsEnabled() {
		t.Fatal("explicit false stays off")
	}
	on := true
	if !(Global{Enabled: &on}).IsEnabled() {
		t.Fatal("explicit true turns it on")
	}
	d := g.WithDefaults()
	if d.MaxConnections != DefaultGlobalMaxConnections || d.MaxInbound != DefaultGlobalMaxInbound ||
		d.MaxDialsPerMinute != DefaultGlobalMaxDialsPerMinute || d.DialTimeout != DefaultGlobalDialTimeout ||
		d.KeepAlive != DefaultGlobalKeepAlive || d.StaleAfter != DefaultGlobalStaleAfter ||
		d.DormantAfter != DefaultGlobalDormantAfter || d.DisableAfter != DefaultGlobalDisableAfter {
		t.Fatalf("defaults not applied: %+v", d)
	}
	custom := Global{MaxConnections: 2, DialTimeout: time.Second}.WithDefaults()
	if custom.MaxConnections != 2 || custom.DialTimeout != time.Second || custom.MaxInbound != DefaultGlobalMaxInbound {
		t.Fatalf("explicit values must survive: %+v", custom)
	}
	// ResolveP2P: headless turns the LAN default off but leaves an
	// explicit global opt-in alone, defaults filled.
	c := Config{Headless: true, P2P: P2P{Global: Global{Enabled: &on}}}
	r := c.ResolveP2P()
	if r.IsEnabled() || !r.Global.IsEnabled() || r.Global.MaxConnections != DefaultGlobalMaxConnections {
		t.Fatalf("unexpected resolve: %+v", r)
	}
}
