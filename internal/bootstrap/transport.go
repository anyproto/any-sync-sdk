//go:build !js

package bootstrap

import (
	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/net/transport/quic"
	"github.com/anyproto/any-sync/net/transport/webtransport"
	"github.com/anyproto/any-sync/net/transport/yamux"
)

func registerTransports(a *app.App) {
	a.Register(yamux.New()).
		Register(quic.New()).
		Register(webtransport.New())
}
