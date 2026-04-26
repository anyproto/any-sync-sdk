package anysyncsdk

import (
	"context"

	"github.com/anyproto/any-sync-sdk/auth"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

// SDK is the top-level handle held by middleware for the lifetime of
// use. Constructed by Open; torn down by Close.
type SDK struct {
	// unexported — populated by Open
	spaces  space.Service
	account AccountAPI
}

// Open brings up the SDK: initializes auth, opens storage, boots any-sync,
// loads (or derives) the tech space, runs any pending re-index, and
// returns a ready handle. Blocks until the SDK is ready to accept reads
// and writes.
//
// Open consumes both cfg (pure data) and a provider (behavior). The
// provider is called during boot to get the account and device keys;
// it may be invoked again later if the SDK needs to re-authenticate
// with a peer.
func Open(ctx context.Context, cfg config.Config, provider auth.Provider) (*SDK, error) {
	return nil, nil // TODO
}

// Close tears down the SDK: closes the event bus, flushes any-store
// databases, stops any-sync, and releases resources. After Close
// returns, the SDK handle is unusable.
func (s *SDK) Close() error { return nil }

// Spaces returns the space-level entrypoint (Create / Join / Derive /
// List / Delete / Subscribe).
func (s *SDK) Spaces() space.Service { return s.spaces }

// Account returns the account-level API: own identity, own metadata.
func (s *SDK) Account() AccountAPI { return s.account }

// AccountAPI exposes account-level operations outside any space — own
// identity string, own metadata (propagated to identityRepo so other
// peers can see it), and account-scope preferences (deferred).
type AccountAPI interface {
	// Id returns the account's identity string (StrKey-encoded).
	Id() string

	// UpdateMetadata updates the account's public metadata
	// (identityRepo-backed). Applies across all spaces.
	UpdateMetadata(ctx context.Context, meta space.AccountMetadata) error
}
