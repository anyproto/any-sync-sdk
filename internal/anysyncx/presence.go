package anysyncx

import (
	"context"

	"github.com/anyproto/any-sync/nodeconf"

	"github.com/anyproto/any-sync-sdk/internal/p2p"
)

// networkRefusesClient reports whether the nodeconf compatibility
// status means the network turned this client's protocol version
// away at the handshake. NeedsUpdate is a newer network that still
// accepts this version and syncs; Error and Unknown are a check that
// did not run.
func networkRefusesClient(st nodeconf.NetworkCompatibilityStatus) bool {
	return st == nodeconf.NetworkCompatibilityStatusIncompatible
}

// pickNow reports whether a live pool connection to id exists right
// now. It never dials and never waits: a peer still being dialed is
// not connected, and a spent context makes pool.Pick answer at once
// instead of waiting on the dial. The sync-status presence poll asks
// this for every tracked space's nodes once a second.
func pickNow(p p2p.Picker, id string) bool {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pr, err := p.Pick(ctx, id)
	return err == nil && pr != nil
}
