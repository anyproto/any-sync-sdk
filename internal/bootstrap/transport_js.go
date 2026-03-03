//go:build js

package bootstrap

import "github.com/anyproto/any-sync/app"

func registerTransports(_ *app.App) {
	// On js/wasm, yamux and quic are not available.
	// Only WebRTC is supported (registered separately when configured).
}
