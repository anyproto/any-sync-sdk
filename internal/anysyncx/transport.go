//go:build !js

package anysyncx

import (
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/net/transport/quic"
	"github.com/anyproto/any-sync/net/transport/webtransport"
	"github.com/anyproto/any-sync/net/transport/yamux"
)

// registerTransports is split off so we can later add a `_js` build
// variant that registers a different transport set (e.g. webtransport
// only). Native builds register all three.
func registerTransports(a *app.App) {
	a.Register(yamux.New()).
		Register(quic.New()).
		Register(webtransport.New())
}
