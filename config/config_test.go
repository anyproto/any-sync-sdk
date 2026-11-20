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
	var g GlobalP2P
	if g.IsEnabled() {
		t.Fatal("global p2p is opt-in")
	}
	off := false
	if (GlobalP2P{Enabled: &off}).IsEnabled() {
		t.Fatal("explicit false stays off")
	}
	on := true
	if !(GlobalP2P{Enabled: &on}).IsEnabled() {
		t.Fatal("explicit true turns it on")
	}
	d := g.WithDefaults()
	if d.MaxConnections != DefaultGlobalP2PMaxConnections || d.MaxInbound != DefaultGlobalP2PMaxInbound ||
		d.MaxDialsPerMinute != DefaultGlobalP2PMaxDialsPerMinute || d.DialTimeout != DefaultGlobalP2PDialTimeout ||
		d.KeepAlive != DefaultGlobalP2PKeepAlive || d.StaleAfter != DefaultGlobalP2PStaleAfter ||
		d.DormantAfter != DefaultGlobalP2PDormantAfter || d.DisableAfter != DefaultGlobalP2PDisableAfter {
		t.Fatalf("defaults not applied: %+v", d)
	}
	custom := GlobalP2P{MaxConnections: 2, DialTimeout: time.Second}.WithDefaults()
	if custom.MaxConnections != 2 || custom.DialTimeout != time.Second || custom.MaxInbound != DefaultGlobalP2PMaxInbound {
		t.Fatalf("explicit values must survive: %+v", custom)
	}
	// ResolveP2P: headless turns the LAN default off but leaves an
	// explicit global opt-in alone, defaults filled.
	c := Config{Headless: true, P2P: P2P{Global: GlobalP2P{Enabled: &on}}}
	r := c.ResolveP2P()
	if r.IsEnabled() || !r.Global.IsEnabled() || r.Global.MaxConnections != DefaultGlobalP2PMaxConnections {
		t.Fatalf("unexpected resolve: %+v", r)
	}
}

func TestGlobalPkarrValidation(t *testing.T) {
	on := true
	base := GlobalP2P{Enabled: &on, RelayURLs: []string{"https://relay.example"}}
	if base.AccountEnabled() {
		t.Fatal("no pkarr relay: account layer off")
	}
	g := base
	g.PkarrRelayURLs = []string{"https://dns.example"}
	if err := g.Validate(); err != nil || !g.AccountEnabled() {
		t.Fatalf("https pkarr relay must validate: %v", err)
	}
	g.PkarrRelayURLs = []string{"http://127.0.0.1:1"}
	if err := g.Validate(); err == nil {
		t.Fatal("http pkarr relay needs InsecurePkarr")
	}
	g.InsecurePkarr = true
	if err := g.Validate(); err != nil {
		t.Fatalf("insecure opt-in must validate: %v", err)
	}
	for _, bad := range []string{"ws://x", "not a url", ""} {
		g.PkarrRelayURLs = []string{bad}
		if err := g.Validate(); err == nil {
			t.Fatalf("%q must be rejected", bad)
		}
	}
	off := GlobalP2P{PkarrRelayURLs: []string{"https://dns.example"}}
	if off.AccountEnabled() {
		t.Fatal("account layer needs the global layer on")
	}
}
