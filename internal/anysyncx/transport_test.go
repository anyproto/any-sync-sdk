//go:build !js

package anysyncx

import (
	"testing"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/net/transport"
	"github.com/anyproto/any-sync/net/transport/yamux"
	"github.com/stretchr/testify/require"
)

func TestRegisterTransportsIrohOnlyWhenGlobal(t *testing.T) {
	a := new(app.App)
	registerTransports(a, false)
	require.NotNil(t, a.Component(yamux.CName))
	require.Nil(t, a.Component(transport.IrohCName), "an opted-out device pays nothing for iroh")

	a = new(app.App)
	registerTransports(a, true)
	require.NotNil(t, a.Component(transport.IrohCName))
}
