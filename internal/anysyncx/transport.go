//go:build !js

package anysyncx

import (
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/net/transport/iroh"
	"github.com/anyproto/any-sync/net/transport/quic"
	"github.com/anyproto/any-sync/net/transport/webtransport"
	"github.com/anyproto/any-sync/net/transport/yamux"
)

// registerTransports is split off so we can later add a `_js` build
// variant that registers a different transport set (e.g. webtransport
// only). Native builds register the three node/LAN transports; the
// iroh transport joins only when the global p2p layer is on — it
// binds a UDP socket and keeps a relay session for the process
// lifetime, so an opted-out device must not pay for it.
func registerTransports(a *app.App, global bool) {
	a.Register(yamux.New()).
		Register(quic.New()).
		Register(webtransport.New())
	if global {
		a.Register(iroh.New())
	}
}
