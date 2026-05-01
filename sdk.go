package anysyncsdk

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	anystore "github.com/anyproto/any-store/v2"

	"github.com/anyproto/any-sync-sdk/auth"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/anysyncx"
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/space"
)

// SDK is the top-level handle held by middleware for the lifetime of
// use. Constructed by Open; torn down by Close.
type SDK struct {
	app     *anysyncx.App
	db      anystore.DB
	tsp     *techspace.Service
	spaces  *spaceimpl.Service
	account *accountImpl
}

// Open brings up the SDK: initializes auth, opens storage, boots any-sync,
// derives the tech space, replays any unseen DAG changes, and returns a
// ready handle.
//
// Storage layout:
//
//	<DataDir>/anysync/<spaceId>.db   — any-sync per-space state (any-store v1)
//	<DataDir>/sdk.db                 — SDK CRDT collections (any-store v2, shared)
//
// any-sync uses v1 internally for its tree storage; the SDK uses v2
// for everything it owns (CRDT controller collections, _meta watermark,
// type registry, _detached parked changes, per-space objects values).
// The two coexist via the /v2 module path.
func Open(ctx context.Context, cfg config.Config, provider auth.Provider) (*SDK, error) {
	if cfg.Storage.DataDir == "" {
		return nil, errors.New("anysyncsdk: Storage.DataDir is required")
	}
	if err := spaceobjects.ValidateExternalTypes(cfg.Types); err != nil {
		return nil, fmt.Errorf("anysyncsdk: %w", err)
	}

	// any-sync stores its per-space state under <DataDir>/anysync (v1
	// DB, owned by any-sync). The SDK's CRDT state lives at
	// <DataDir>/sdk.db (v2 DB, owned by us) — separate file so the
	// two any-store versions don't share schema/state.
	cfg.Storage.DataDir = filepath.Join(cfg.Storage.DataDir, "anysync")

	app, err := anysyncx.New(ctx, cfg, provider)
	if err != nil {
		return nil, err
	}

	sdkDBPath := filepath.Join(filepath.Dir(cfg.Storage.DataDir), "sdk.db")
	db, err := anystore.Open(ctx, sdkDBPath, nil)
	if err != nil {
		_ = app.Close(ctx)
		return nil, fmt.Errorf("anysyncsdk: open sdk db: %w", err)
	}

	tsp := techspace.New(app, db)
	app.SetSpaceRegistry(tsp)

	if err := tsp.Open(ctx); err != nil {
		_ = db.Close()
		_ = app.Close(ctx)
		return nil, fmt.Errorf("anysyncsdk: open techspace: %w", err)
	}

	spaces := spaceimpl.New(app, tsp, db, cfg.Types)
	account := newAccountImpl(app)

	return &SDK{
		app:     app,
		db:      db,
		tsp:     tsp,
		spaces:  spaces,
		account: account,
	}, nil
}

// Close tears down the SDK: closes loaded spaces, the tech space, the
// SDK DB, and finally the any-sync app.
func (s *SDK) Close() error {
	ctx := context.Background()
	if s.spaces != nil {
		_ = s.spaces.Close(ctx)
	}
	if s.tsp != nil {
		_ = s.tsp.Close(ctx)
	}
	if s.db != nil {
		_ = s.db.Close()
	}
	if s.app != nil {
		return s.app.Close(ctx)
	}
	return nil
}

// Spaces returns the space-level entrypoint.
func (s *SDK) Spaces() space.Service { return s.spaces }

// Account returns the account-level API.
func (s *SDK) Account() AccountAPI { return s.account }

// AccountAPI exposes account-level operations outside any space.
type AccountAPI interface {
	// Id returns the account's identity string (StrKey-encoded).
	Id() string

	// UpdateMetadata updates the account's public metadata
	// (identityRepo-backed). Applies across all spaces.
	UpdateMetadata(ctx context.Context, meta space.AccountMetadata) error
}

// accountImpl is a placeholder: Id() returns the account's libp2p-
// style PeerId. UpdateMetadata is not yet wired (identityRepo
// integration is groomed in its own pass).
type accountImpl struct {
	app *anysyncx.App
}

func newAccountImpl(app *anysyncx.App) *accountImpl { return &accountImpl{app: app} }

func (a *accountImpl) Id() string {
	keys := a.app.AccountKeys()
	if keys == nil {
		return ""
	}
	return keys.SignKey.GetPublic().PeerId()
}

func (a *accountImpl) UpdateMetadata(_ context.Context, _ space.AccountMetadata) error {
	return errors.New("anysyncsdk: UpdateMetadata not implemented")
}
