// Package client provides the constructor for the any-sync SDK client.
//
// Usage:
//
//	import (
//	    syncsdk "github.com/anyproto/any-sync-sdk"
//	    "github.com/anyproto/any-sync-sdk/client"
//	)
//
//	c, err := client.New(ctx, syncsdk.Config{...})
package client

import (
	"context"

	syncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/internal/clientimpl"
)

// New creates and starts a new SDK client backed by the full any-sync
// component graph. The Config must have at least SigningKey and StoragePath set.
// PeerKey is auto-generated if nil; MasterKey defaults to SigningKey.
func New(ctx context.Context, cfg syncsdk.Config) (syncsdk.Client, error) {
	return clientimpl.New(ctx, cfg)
}
