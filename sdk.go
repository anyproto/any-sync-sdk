package anysyncsdk

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-sync/identityrepo/identityrepoproto"
	"github.com/anyproto/any-sync/net/pool"

	"github.com/anyproto/any-sync-sdk/auth"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/internal/anysyncx"
	"github.com/anyproto/any-sync-sdk/internal/files/broker"
	"github.com/anyproto/any-sync-sdk/internal/files/fetch"
	"github.com/anyproto/any-sync-sdk/internal/files/gc"
	"github.com/anyproto/any-sync-sdk/internal/files/status"
	filestore "github.com/anyproto/any-sync-sdk/internal/files/store"
	"github.com/anyproto/any-sync-sdk/internal/files/upload"
	"github.com/anyproto/any-sync-sdk/internal/spaceimpl"
	"github.com/anyproto/any-sync-sdk/internal/spaceobjects"
	"github.com/anyproto/any-sync-sdk/internal/spacesync"
	"github.com/anyproto/any-sync-sdk/internal/techspace"
	"github.com/anyproto/any-sync-sdk/space"
)

// SDK is the top-level handle held by middleware for the lifetime of
// use. Constructed by Open; torn down by Close.
type SDK struct {
	app        *anysyncx.App
	db         anystore.DB
	tsp        *techspace.Service
	spaces     *spaceimpl.Service
	account    *accountImpl
	filesQueue *status.Queue
	filesGC    *gc.Service
}

// FileCacheSize returns the local bytes currently held by file content
// across all spaces (complete + partial copies; inline files hold no
// cache bytes).
func (s *SDK) FileCacheSize(ctx context.Context) (int64, error) {
	return s.filesGC.CacheSize(ctx)
}

// FreeUpFileCache reclaims local file bytes until at least `bytes` are
// freed, least-recently-used first, dropping only content that is safe
// to drop (backed up on the network — a later Open refetches — or no
// longer referenced by any file). Returns the bytes actually freed,
// which is less than requested when nothing else is safely evictable.
func (s *SDK) FreeUpFileCache(ctx context.Context, bytes int64) (freed int64, err error) {
	return s.filesGC.FreeUp(ctx, bytes)
}

// SweepFileCache runs one file-cache safety pass: prunes references of
// deleted files, deletes content no file references anymore (past a
// grace period), and drops long-untouched partial downloads of
// backed-up files. Never touches content that is not safely
// refetchable. This is the manual trigger; the same pass runs
// periodically only when cfg.Files.GCInterval is set.
func (s *SDK) SweepFileCache(ctx context.Context) error {
	return s.filesGC.Sweep(ctx)
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
	spaces := spaceimpl.New(app, tsp, tsp, db, cfg.Types)

	// Files byte layer (SYN-25/27/28): CARv2s under <DataDir>/files (a
	// sibling of anysync/ and sdk.db — cfg.Storage.DataDir was
	// re-pointed to anysync/ above), metadata in the shared SDK DB.
	// Uploads go through the fileV2 broker; downloads are public-read
	// first ({base}/blob/…), with the base resolved once via the broker
	// Info RPC and persisted (or pinned by cfg.Files.PublicReadBaseUrl).
	filesRoot := filepath.Join(filepath.Dir(cfg.Storage.DataDir), "files")
	filesStore, err := filestore.New(ctx, filesRoot, db)
	if err != nil {
		_ = db.Close()
		_ = app.Close(ctx)
		return nil, fmt.Errorf("anysyncsdk: open files store: %w", err)
	}
	filesBroker := broker.New(app.Pool(), app.FileV2Peers, app.NetworkId(), app.FileNetworkId)
	baseURL := fetch.NewBaseURL(filesStore, app.NetworkId(), cfg.Files.PublicReadBaseUrl,
		func(ctx context.Context) (string, error) {
			info, err := filesBroker.Info(ctx)
			if err != nil {
				return "", err
			}
			return info.PublicReadBaseUrl, nil
		})
	filesUpload := upload.New(filesStore, filesBroker)
	// The persistent files work queue (SYN-29): drive-toward-durable
	// retries + Pin background fetches, surviving restarts. Runs after
	// spaces exist (jobs resolve rows through them); closed first.
	filesQueue, err := status.NewQueue(ctx, db, spaces.RunFileJob, spaces.OnFileJobChange)
	if err != nil {
		_ = db.Close()
		_ = app.Close(ctx)
		return nil, fmt.Errorf("anysyncsdk: open files queue: %w", err)
	}
	filesUpload.SetQueue(filesQueue)
	spaces.SetFiles(filesUpload, fetch.New(filesStore, baseURL), filesStore, filesQueue)
	// Cache reclamation (SYN-26): fully embedder-driven —
	// FileCacheSize/FreeUpFileCache/SweepFileCache and per-file
	// Offload. The periodic safety sweep runs ONLY when configured
	// (cfg.Files.GCInterval > 0); the default is no background GC.
	filesGC := gc.New(filesStore, spaces, func(ctx context.Context, spaceId, fileId string) error {
		return filesQueue.Enqueue(ctx, status.KindDurable, spaceId, fileId)
	})
	// Wire spaceimpl.Service as the space registry so any-sync's
	// treemanager-driven callbacks (deletion-manager DeleteTree,
	// space-sync PutTree, head-sync GetTree for arbitrary trees)
	// route to the per-space Store. spaceimpl internally delegates
	// the tech-space's own indexId back to techspace.
	app.SetSpaceRegistry(spaces)

	if err := tsp.Open(ctx); err != nil {
		_ = db.Close()
		_ = app.Close(ctx)
		return nil, fmt.Errorf("anysyncsdk: open techspace: %w", err)
	}
	// Start the background workers only after the last fallible Open
	// step — a failed Open must not leak goroutines polling a closed DB.
	filesQueue.Run()
	filesGC.Run(cfg.Files.GCInterval)
	account := newAccountImpl(app, tsp, spaces)

	// Headless: skip the account-facing boot work below — profile
	// republish, the 1-1 inbox, identity resolution, pending-join
	// resume, and the eager space-loading loop. A broker tracks foreign
	// spaces and opens them on demand via Get; nothing account-shaped
	// exists to resume, and eager-loading every tracked space defeats
	// open/close-on-demand. (The tech space itself was already pinned
	// local-only inside tsp.Open.)
	if cfg.Headless {
		return &SDK{
			app:        app,
			db:         db,
			tsp:        tsp,
			spaces:     spaces,
			account:    account,
			filesQueue: filesQueue,
			filesGC:    filesGC,
		}, nil
	}

	// Resume any join left pending from a previous session now that the
	// tech space is open (the join controller started in spaceimpl.New,
	// before this point, so its initial scan saw an empty index).
	spaces.ResumePendingJoins()

	// Start the Layer-2 1-1 inbox subsystem now that the tech space is
	// open: the receive notifier (coordinator push + poll → pending rows)
	// and the send-retry loop. No-op when the inbox transport is
	// unavailable; the out-of-band 1-1 path works without it.
	spaces.StartOneToOneInbox(ctx)

	// Cold-sync resolve: a fresh device syncs the identities directory's
	// symkeys but no profiles (those are device-local). Batch-fetch the
	// missing profiles from identityRepo in the background.
	go spaces.ResolveIdentityProfiles(context.Background())

	// Republish the locally-stored profile to identityRepo on every
	// boot. Heart's ownProfileSubscription does the equivalent (reads
	// the local profile object, calls IdentityRepoPut). Without this,
	// other peers only see our profile after we explicitly call
	// UpdateMetadata in this process — which never happens for a
	// freshly-started client that's only re-loading existing state.
	//
	// Best-effort: a failed push doesn't prevent SDK use. The next
	// successful push (next UpdateMetadata or next boot) heals it.
	if err := account.republishStoredProfile(ctx); err != nil {
		// Log via the any-sync log? We don't have one wired here.
		// Swallow — Open succeeds; the rest of the SDK is functional.
		_ = err
	}

	// Eager-load every non-deleted space from the tech-space index so
	// per-space headsync / syncacl start running at boot rather than
	// waiting for the first caller-driven access. Combined with the
	// disabled space-cache TTL (anysyncx/spacecache.go), this means
	// every joined space stays subscribed for the SDK's lifetime —
	// idle peers receive ACL updates and pushed changes without anyone
	// touching them first.
	//
	// Best-effort per space: a single failure (e.g. corrupted local
	// storage for one space) is logged and skipped so SDK.Open still
	// succeeds for the rest. Pending-join records (LocalStatus=joining)
	// are skipped too — Service.Join wrote those before any-sync
	// storage exists.
	for _, rec := range tsp.List(ctx) {
		// Skip deletion tombstones. Both delete paths — local
		// Service.Delete and inbound reconcile — record the delete via
		// the SYNCED RemoteStatus field (LocalStatus is never set to
		// "deleted"). Eager-loading a deleted space rebuilds its
		// commonspace and storage and starts periodic headsync, which
		// the node rejects with "space is deleted". If local storage
		// still lingers (e.g. an offload that was interrupted, or one
		// re-created by a previous build that eager-loaded tombstones),
		// reclaim it now — OffloadSpace is idempotent and best-effort.
		// Includes the 1-1 synced offload marker (oneToOneDeleted) — a
		// deleted 1-1 must be offloaded, not eager-loaded, on every device.
		if rec.IsDeleted() {
			if app.SpaceExists(rec.Id) {
				spaces.OffloadSpace(ctx, rec.Id)
			}
			continue
		}
		if !app.SpaceExists(rec.Id) {
			continue
		}
		if _, err := app.GetSpace(ctx, rec.Id); err != nil {
			_ = err
			continue
		}
		// Catch up any trees that advanced while we were offline (or
		// that we have never opened). Best-effort: a failure here
		// shouldn't block SDK.Open for the rest of the spaces — the
		// per-object lazy ColdRestore on first user touch still works.
		if err := spacesync.Run(ctx, app, db, spaces.StoreFor(rec.Id), rec.Id); err != nil {
			_ = err
		}
		// Backstop the sdk.db-rebuild deletion gap: purge any local row for
		// an object any-sync has flipped to Deleted (and stamp the consumer
		// deletion feed). Best-effort; runs after Run so it also cleans
		// anything the forward catch-up or a lazy Get re-materialized.
		if err := spacesync.ReconcileDeletions(ctx, app, db, spaces.StoreFor(rec.Id), rec.Id); err != nil {
			_ = err
		}
	}

	return &SDK{
		app:        app,
		db:         db,
		tsp:        tsp,
		spaces:     spaces,
		account:    account,
		filesQueue: filesQueue,
		filesGC:    filesGC,
	}, nil
}

// Close tears down the SDK: stops the files queue, closes loaded
// spaces, the tech space, the SDK DB, and finally the any-sync app.
func (s *SDK) Close() error {
	ctx := context.Background()
	if s.filesGC != nil {
		s.filesGC.Close()
	}
	if s.filesQueue != nil {
		s.filesQueue.Close()
	}
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

// Identities returns the account-global directory of identities this
// account has encountered (profiles + the spaces where each was seen).
func (s *SDK) Identities() space.IdentitiesAPI { return spaceimpl.NewIdentitiesAPI(s.tsp) }

// Account returns the account-level API.
func (s *SDK) Account() AccountAPI { return s.account }

// PoolInternal exposes the any-sync peer pool (dial by peerId with this
// account's identity in the handshake). Same-module internal surface —
// mirrors the PayloadsInternal pattern — used by the e2e suite to speak
// node-side protocols (e.g. fileprotov2 against a fileV2 broker).
func (s *SDK) PoolInternal() pool.Pool { return s.app.Pool() }

// AccountAPI exposes account-level operations outside any space.
type AccountAPI interface {
	// Id returns the account's identity string (StrKey-encoded).
	Id() string

	// Metadata returns the locally-stored profile (the source-of-truth
	// copy that's also pushed to identityRepo on UpdateMetadata).
	// Reading from the tech-space is deterministic — no coordinator
	// round-trip, no 60-second watcher tick — so callers that just want
	// to read back what they wrote (e.g. a settings UI rendering after a
	// page reload) don't depend on identityRepo being reachable.
	//
	// `present` is false when no profile has been written yet on this
	// device (fresh wallet). The same `present` and zero-value distinction
	// the SDK exposes via tsp.GetProfile.
	Metadata(ctx context.Context) (meta space.AccountMetadata, present bool, err error)

	// UpdateMetadata updates the account's public metadata
	// (identityRepo-backed). Applies across all spaces.
	UpdateMetadata(ctx context.Context, meta space.AccountMetadata) error
}

// accountImpl exposes the account-level surface. Id() returns the
// account's StrKey-encoded identity (PubKey.Account()), the same form
// used for change authorship and identityRepo; UpdateMetadata persists the profile
// to the tech-space and pushes to identityRepo, then kicks every
// running members watcher so the new profile becomes visible across
// already-loaded spaces without waiting for the slow tick.
type accountImpl struct {
	app    *anysyncx.App
	tsp    *techspace.Service
	spaces *spaceimpl.Service
}

func newAccountImpl(app *anysyncx.App, tsp *techspace.Service, spaces *spaceimpl.Service) *accountImpl {
	return &accountImpl{app: app, tsp: tsp, spaces: spaces}
}

func (a *accountImpl) Id() string {
	keys := a.app.AccountKeys()
	if keys == nil {
		return ""
	}
	return keys.SignKey.GetPublic().Account()
}

// Metadata reads the locally-persisted profile from the tech-space.
// Symmetric with UpdateMetadata's local-first write — never hits the
// network. See AccountAPI.Metadata for the (present, zero-value)
// contract.
func (a *accountImpl) Metadata(ctx context.Context) (space.AccountMetadata, bool, error) {
	if a.tsp == nil {
		return space.AccountMetadata{}, false, nil
	}
	rec, ok := a.tsp.GetProfile(ctx)
	if !ok {
		return space.AccountMetadata{}, false, nil
	}
	return space.AccountMetadata{
		Name:        rec.Name,
		Description: rec.Description,
		IconCID:     rec.IconCID,
	}, true, nil
}

// UpdateMetadata publishes the account's profile (name / description /
// icon CID) to identityRepo. The bytes are signed with the account
// signing key so other peers can verify authenticity at fetch time.
//
// Kind is "anysync-sdk.profile" — namespaced separately from
// anytype-heart's "profile" record because the on-the-wire format
// differs (this SDK uses a flat NUL-separated layout; heart uses an
// encrypted protobuf). Switching consumers between the two would
// require explicit format negotiation, which is out of scope.
//
// Any-store the encoded plaintext locally? Not yet — the fetcher
// reads its own profile back from identityRepo on next poll, same as
// any other member. Skipping the local cache keeps the code small.
func (a *accountImpl) UpdateMetadata(ctx context.Context, meta space.AccountMetadata) error {
	if a.app.AccountKeys() == nil {
		return errors.New("anysyncsdk: UpdateMetadata: no account keys")
	}
	if space.EncodeAccountMetadata(meta) == nil {
		return errors.New("anysyncsdk: UpdateMetadata: empty metadata")
	}
	// Persist locally first so a future boot can republish without
	// the user re-supplying the metadata. Tech-space writes are
	// owner-only and cheap (single CRDT row).
	if a.tsp != nil {
		if err := a.tsp.SetProfile(ctx, techspace.ProfileRecord{
			Name:        meta.Name,
			Description: meta.Description,
			IconCID:     meta.IconCID,
		}); err != nil {
			return fmt.Errorf("anysyncsdk: persist profile: %w", err)
		}
	}
	if err := a.pushToIdentityRepo(ctx, meta); err != nil {
		return err
	}
	// Kick every running members watcher so the just-published
	// profile is reflected in their snapshots without waiting for
	// the periodic identityRepo tick (60s).
	if a.spaces != nil {
		a.spaces.KickProfiles(ctx)
	}
	return nil
}

// republishStoredProfile reads the locally-stored profile (if any) and
// pushes it to identityRepo. Called on SDK boot to mirror heart's
// ownProfileSubscription.Run path. No-op when no profile has been
// written yet (fresh device, never called UpdateMetadata).
func (a *accountImpl) republishStoredProfile(ctx context.Context) error {
	if a.tsp == nil {
		return nil
	}
	rec, ok := a.tsp.GetProfile(ctx)
	if !ok || rec.IsEmpty() {
		return nil
	}
	return a.pushToIdentityRepo(ctx, space.AccountMetadata{
		Name:        rec.Name,
		Description: rec.Description,
		IconCID:     rec.IconCID,
	})
}

// pushToIdentityRepo signs and uploads the profile bytes to the
// coordinator's identityRepo. Returns the raw error from the RPC so
// callers can decide whether to surface it.
func (a *accountImpl) pushToIdentityRepo(ctx context.Context, meta space.AccountMetadata) error {
	keys := a.app.AccountKeys()
	symKey, err := space.DeriveAccountMetadataSymKey(keys.SignKey)
	if err != nil {
		return fmt.Errorf("anysyncsdk: derive metadata key: %w", err)
	}
	// Encrypt the profile with our account metadata symkey before upload —
	// only contacts who have received the key (via a shared space's ACL or
	// a 1-1 invite) can read it. The signature is over the CIPHERTEXT so
	// the fetch path verifies before it decrypts (matches
	// decodeIdentityRepoProfile).
	payload, err := space.EncryptProfile(meta, symKey)
	if err != nil {
		return fmt.Errorf("anysyncsdk: encrypt profile: %w", err)
	}
	if len(payload) == 0 {
		return nil
	}
	signature, err := keys.SignKey.Sign(payload)
	if err != nil {
		return fmt.Errorf("anysyncsdk: sign profile: %w", err)
	}
	// identityRepo on the coordinator expects the strkey-encoded
	// "account address" form for the identity, not the libp2p PeerId
	// our public Account.Id() returns. Translate at the boundary.
	identity := keys.SignKey.GetPublic().Account()
	return a.app.Coordinator().IdentityRepoPut(ctx, identity, []*identityrepoproto.Data{{
		Kind:      space.IdentityProfileKind,
		Data:      payload,
		Signature: signature,
	}})
}
