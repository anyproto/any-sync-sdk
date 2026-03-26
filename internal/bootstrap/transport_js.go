//go:build js

package bootstrap

import (
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/net/transport/webtransport"
)

func registerTransports(a *app.App) {
	// On js/wasm, yamux and quic are not available.
	// WebTransport is always available in browsers (including Web Workers).
	a.Register(webtransport.New())
}
