package space

import (
	"context"
	"errors"
)

// ErrSpaceNotAccepted is returned by Service.Get for a space this
// account knows of but has not accepted yet: a pending join
// (StatusJoining), an incoming or declined 1-1 (StatusOneToOnePending /
// StatusOneToOneDeclined), or a pending/declined direct-add invite.
// Nothing is downloaded for such spaces — a load with no local storage
// triggers any-sync's SpacePull bootstrap, so materializing a pending
// row would pull the whole space ciphertext before the user accepted
// it. Acceptance (owner approval of a join, AcceptOneToOne,
// AcceptInvite) is what authorizes materialization.
var ErrSpaceNotAccepted = errors.New("space: not accepted; not materialized")

// ErrSpaceDeleted is returned by Service.Get for a space whose index
// row carries a deletion marker (local or synced, including the 1-1
// offload marker). The storage is offloaded — loading would SpacePull
// a space the account removed, which the nodes reject anyway. A
// deleted 1-1 is re-creatable via Service.OneToOne; a regular deleted
// space is terminal.
var ErrSpaceDeleted = errors.New("space: deleted")

// ErrReadOnlySpace rejects synced writes into a space this account
// cannot write to: any space opened via a guest key, and any space
// where the ACL grants a role without write permission (reader /
// guest). The nodes would drop the records anyway — the SDK fails the
// write up front with a typed error instead of letting local state
// silently diverge or surfacing a low-level ACL rejection.
var ErrReadOnlySpace = errors.New("space: read-only")

// ErrGuestJoinPending is returned by Service.JoinGuest when the guest
// row was recorded durably but the space content isn't pullable yet.
// Loading continues in the background and across restarts; callers
// poll List/Get or Subscribe for the flip to StatusActive.
var ErrGuestJoinPending = errors.New("space: guest join recorded; space load pending")

// ErrSpaceUnknown is returned by id-addressed Service methods (Get,
// SetSettings, the accept/decline families) when the spaceId has no
// row in the account's space index.
var ErrSpaceUnknown = errors.New("unknown space")

// ErrJoinPending is returned by Join after the RequestToJoin was
// posted but the owner has not yet accepted. The space is recorded in
// the index with StatusJoining; callers poll List for the status flip
// and then call Get.
var ErrJoinPending = errors.New("join pending owner approval")

// ErrInviteAcceptPending is returned by AcceptInvite when the accept
// was recorded (synced account-wide) but the space content is not
// pullable yet. Loading continues durably in the background and across
// restarts; callers poll List/Get or Subscribe for the flip to
// StatusActive.
var ErrInviteAcceptPending = errors.New("invite accepted; space load pending")

// ErrNotInvitePending is returned by AcceptInvite / DeclineInvite when
// the space is not awaiting direct-add invite approval.
var ErrNotInvitePending = errors.New("not invite-pending")

// ErrIsOneToOne is returned when a regular-space invite op targets a
// 1-1 space — use the OneToOne accept/decline methods instead.
var ErrIsOneToOne = errors.New("is a 1-1 space")

// ErrSelfPair is returned by OneToOne / RegisterIncoming when the
// given identity is the caller's own account.
var ErrSelfPair = errors.New("cannot pair with self")

// ErrBadSpaceId is returned by Track when the given id does not have
// the any-sync spaceId shape (`<cid>.<replication key base36>`). A
// malformed id would otherwise sit in the index and fail every Get
// with an opaque remote error — Track rejects it up front instead.
var ErrBadSpaceId = errors.New("invalid space id")

// ErrIsTechSpace is returned by Track when the given id is the
// account's own tech space — the tech space is system-owned and never
// appears in the space list.
var ErrIsTechSpace = errors.New("cannot track the tech space")

// ErrBadSpaceType is returned by Create when CreateRequest.SpaceType
// is outside the allow-list (SpaceTypeAny or empty). The type is
// content-addressed into the immutable space header and coordinator-
// gated, so a bad value is rejected up front — classify with
// errors.Is to turn it into a caller-facing 4xx.
var ErrBadSpaceType = errors.New("unsupported space type")

// Settings-patch sentinels — wrapped by SetSettings validation errors
// so callers can classify with errors.Is.
var (
	ErrSettingsEmpty      = errors.New("settings patch is empty")
	ErrSettingsBadKey     = errors.New("invalid settings key")
	ErrSettingsBadValue   = errors.New("unsupported settings value")
	ErrSettingsKeyOverlap = errors.New("settings key in both set and unset")
)

// Service is the space-level entrypoint exposed by the top-level SDK.
// It owns lifecycle (Create / Join / Derive / Delete) and the space
// list; individual space operations live on Space.
type Service interface {
	// Create a new regular space owned by the authenticated account.
	Create(ctx context.Context, req CreateRequest) (Space, error)

	// Join a space via an invite. Depending on the invite key mode the
	// space is either immediately active or pending approval.
	Join(ctx context.Context, req JoinRequest) (Space, error)

	// JoinGuest adds a space via a guest invite (InviteKindGuest): the
	// invite carries the shared read-only guest identity, so there is no
	// join request and no owner approval — the space is pulled and opened
	// signing as that identity. One bounded synchronous load attempt is
	// made; if the content isn't pullable yet the row is recorded
	// durably, (nil, ErrGuestJoinPending) is returned, and loading
	// finishes in the background (resumed across restarts). The space is
	// read-only for life: synced writes fail with ErrReadOnlySpace and
	// Info reports OwnRole = PermissionGuest. Remove it with Delete
	// (local drop — a guest cannot write to the ACL).
	JoinGuest(ctx context.Context, invite string) (Space, error)

	// Derive a deterministic space from the account keys. Used for
	// the tech space (never returned here) and future derived spaces.
	Derive(ctx context.Context, req DeriveRequest) (Space, error)

	// DeriveId returns the deterministic spaceId for a DeriveRequest
	// without creating or loading the space. Same id as Derive(...).Id()
	// for the same request. Lets a consumer recompute a known derived
	// space's id (from its own seed) to recognize or filter it
	// client-side.
	DeriveId(ctx context.Context, req DeriveRequest) (string, error)

	// OneToOne reaches out to — or explicitly accepts / un-declines — the
	// derived 1-1 space shared with otherIdentity. Materializes and
	// activates it locally (implicit self-approval). Same id regardless of
	// key order — both peers land on the same space. Idempotent; overrides
	// a prior local decline.
	OneToOne(ctx context.Context, otherIdentity string) (Space, error)

	// AcceptOneToOne approves an incoming pending 1-1 by space id (as
	// surfaced in List with Status == StatusOneToOnePending): materializes
	// and activates it. The peer identity is read off the row, so the
	// caller needn't re-derive it. Equivalent to OneToOne(peer).
	AcceptOneToOne(ctx context.Context, spaceId string) (Space, error)

	// DeclineOneToOne rejects an incoming 1-1. Writes a synced sticky
	// marker so the request is suppressed on all the account's devices and
	// never auto-resurfaces; a later explicit OneToOne(peer) overrides it.
	DeclineOneToOne(ctx context.Context, spaceId string) error

	// RegisterIncoming records an incoming 1-1 request learned out-of-band
	// (no coordinator) as a pending row for the user to approve, without
	// materializing storage. displayHint is an optional name/icon snapshot
	// for the UI. No-op if a row for the derived space already exists.
	RegisterIncoming(ctx context.Context, peerIdentity string, displayHint AccountMetadata) error

	// AcceptInvite approves a direct-add invite (a space surfaced in List
	// with Status == StatusInvitePending or StatusInviteDeclined — the
	// account is already an ACL member; accept is a local materialization
	// gate). Flips the synced status to active (all the account's devices
	// converge) and loads the space. When the content isn't pullable yet
	// it returns ErrInviteAcceptPending and loading continues durably in
	// the background (crash/restart-safe); poll Get or Subscribe for the
	// flip. Idempotent; overrides a prior decline.
	AcceptInvite(ctx context.Context, spaceId string) (Space, error)

	// DeclineInvite rejects a direct-add invite. Writes a synced sticky
	// marker so the request is suppressed on all the account's devices; a
	// later AcceptInvite overrides it. No ACL write happens — the account
	// remains a member on the space's ACL. Nothing was materialized, so
	// nothing is removed.
	DeclineInvite(ctx context.Context, spaceId string) error

	// Get returns an already-joined space by id. Fails if the space is
	// unknown locally, with ErrSpaceNotAccepted for a known row whose
	// acceptance is still pending (joining / incoming 1-1 / direct-add
	// invite) — those must never be materialized by a read — and with
	// ErrSpaceDeleted for a deleted row. A 1-1 accepted or initiated on
	// another of the account's devices (synced remote=active) is adopted
	// transparently: Get materializes it locally, no per-device
	// re-accept needed.
	Get(ctx context.Context, spaceId string) (Space, error)

	// Track registers a foreign spaceId in the local space index without
	// joining it, so a later Get can open it — any-sync bootstraps the
	// space from its responsible nodes when local storage is missing.
	// The caller does not become a member and holds no keys: synced
	// content stays sealed. Idempotent; a no-op when the id is already
	// indexed (including own/joined spaces — Track never downgrades a
	// membership row).
	//
	// The broker path (headless + selective sync): Track the spaceId,
	// Get it, read the payloads index via Space.Payloads.
	Track(ctx context.Context, spaceId string) error

	// Evict closes a space without deleting anything: per-space watchers
	// stop, the in-memory store and the any-sync space are released. All
	// disk state stays — a later Get reopens the space from local
	// storage. Idempotent; evicting a space that isn't open is a no-op.
	//
	// This is close-on-demand for embedders that hold many spaces (the
	// filenode-v2 broker); Delete is the destructive sibling.
	Evict(ctx context.Context, spaceId string) error

	// List returns a point-in-time snapshot of all known spaces.
	// Mirror of the tech space's space index.
	List(ctx context.Context) ([]SpaceInfo, error)

	// SyncSpaceList forces an immediate head-sync round on the tech
	// space so spaces added or removed on other devices land in the
	// local index, instead of waiting for the periodic timer. Call it
	// before List to converge the space list on demand. Blocks until
	// the round completes.
	SyncSpaceList(ctx context.Context) error

	// Delete tears down a space locally. For regular spaces this also
	// flags the space as deleted on the network; for 1-1 spaces it is
	// local-only (the space is always re-derivable). The record stays
	// in List with Status = StatusDeleted.
	Delete(ctx context.Context, spaceId string) error

	// SetSettings patches the account-private per-space client settings
	// — a free-form object on the space's tech-space row, synced across
	// the account's devices (owner-only ACL: other space members never
	// see it). Each set entry lands as its own $set at settings.<key>
	// and each unset key as a $unset, all in one change — so devices
	// editing DIFFERENT keys converge without clobbering each other;
	// the same key is per-key LWW.
	//
	// Keys are the caller's vocabulary: non-empty, dot-free (one level
	// under `settings` in v1). Values are scalars — string, bool, or
	// any numeric type (stored as float64, JSON semantics). At least
	// one set or unset entry is required; a key may not appear in both.
	// The spaceId must be known to the account (any row in List,
	// deleted included — a tombstone's settings stay editable).
	//
	// Read back via List / Get → SpaceInfo.Settings, or live via the
	// raw spaces-dataset query (Query(SpaceIndexObjectId(), "spaces"))
	// where the subtree appears under the row's `settings` field.
	SetSettings(ctx context.Context, spaceId string, set map[string]any, unset []string) error

	// SetDevice upserts THIS device's row in the account's devices
	// registry — a system dataset in the tech space, one row per device,
	// synced account-wide (SYN-165). The row id is always the local peer
	// id (SDK.PeerId()), never caller-supplied. Only non-empty fields are
	// written; each Apps entry lands per-slug (nil value removes the
	// slug), so writes touching different fields merge. At least one
	// field must be non-empty (ErrDeviceEmptyUpsert). ErrDevicePruned
	// when this device's row was deleted — the sticky tombstone
	// absorbs the write and the id can never re-register.
	//
	// Read back via ListDevices, or generically via
	// Query(SpaceIndexObjectId(), "devices").
	SetDevice(ctx context.Context, up DeviceUpsert) error

	// ClaimActive marks THIS device as the active instance of app: it
	// writes an activeClaims.<app> = {seq, at} claim on the own row
	// (seq = max existing + 1). Conflict-resolution semantics live in
	// the reader — resolve the winner with ActiveDevice, never by
	// comparing claims ad hoc. There is no un-claim: only a higher
	// claim from another device or a row deletion moves the winner.
	// ErrDevicePruned when this device's row was deleted (see
	// SetDevice).
	ClaimActive(ctx context.Context, app string) error

	// DeleteDevice prunes peerId's row — the "device doesn't exist"
	// signal that moves the active election away from it. The tombstone
	// is sticky: the peer id can never re-register (a pruned device
	// that comes back stays unlisted until it re-derives its peer
	// keys). ErrDeviceUnknown when the row doesn't exist;
	// ErrDeviceSelfDelete for the local device's own row (self-pruning
	// would permanently lock this installation out of the registry —
	// prune it from another device).
	DeleteDevice(ctx context.Context, peerId string) error

	// ListDevices returns a point-in-time snapshot of the devices
	// registry (pruned rows excluded). Feed it to ActiveDevice to
	// resolve the active instance of an app. Unavailability (tech
	// space not open yet) is an error, never an empty snapshot — an
	// election consumer must not mistake a closed service for an
	// empty registry.
	ListDevices(ctx context.Context) ([]Device, error)

	// Subscribe delivers space-list changes (added / updated / removed).
	// Returns a cancel function.
	Subscribe(cb func(SpaceListEvent)) (cancel func())

	// SpaceIndexObjectId returns the id of the tech-space index object —
	// the handle for generic Query/Subscribe over the system datasets
	// (spaces, profile, devices). Future system objects expose their own ids.
	SpaceIndexObjectId() string

	// Query builds a generic read query over a system object's dataset
	// (e.g. SpaceIndexObjectId() + "spaces"), with the same chainable
	// Filter / Sort / Limit / Snapshot / Subscribe surface as
	// Space.Query. The bespoke List / Subscribe methods are convenience
	// wrappers over this.
	Query(objectId, dataset string) Query

	// Datasets returns the JSON-Schema description of the tech-space
	// system datasets (spaces, profile, devices) — field names, value shapes, and
	// per-field class (synced / derived / local) via `x-scope`. For
	// discovery, mirroring Space.Datasets.
	Datasets() []DatasetSchema

	// Status returns a snapshot of one space's rolled-up sync state.
	// Cheap; safe to call on every render tick. Spaces unknown to
	// the SDK return SpaceSyncStatus{SpaceId: spaceId,
	// State: SyncStateUnknown}.
	Status(spaceId string) SpaceSyncStatus

	// SubscribeStatus delivers SpaceSyncStatus events whenever any
	// known space's rollup transitions. Account-wide — one cb sees
	// every space. cb runs synchronously on the dispatcher
	// goroutine; keep work small or hand off.
	SubscribeStatus(cb func(SpaceSyncStatus)) (cancel func())
}

// CreateRequest is the input to Service.Create.
type CreateRequest struct {
	Name        string
	Description string
	IconCID     string
	// SpaceType is stamped into the space header at create time and
	// gated by the any-sync-coordinator. Must be SpaceTypeAny or empty
	// (same meaning). Anything else is rejected by Create with a clear
	// error; passing an invalid type would otherwise produce a space
	// the coordinator refuses to sync.
	SpaceType string
}

// JoinRequest is the input to Service.Join.
type JoinRequest struct {
	// Invite is the invite string produced by ACL.CreateInvite on the
	// host side. The SDK parses it internally; opaque to the caller.
	Invite string

	// Metadata is the joining account's metadata that will be attached
	// to the ACL join record (display name, icon, etc.).
	Metadata AccountMetadata
}

// DeriveRequest is the input to Service.Derive.
type DeriveRequest struct {
	// Seed is hashed into the derivation. Zero seed = account-root
	// derivation (tech space).
	Seed []byte

	// SpaceType is an app-level tag surfaced as SpaceInfo.SpaceType for
	// client-side filtering. It is NOT the on-wire header type (that
	// stays SpaceTypeAny and is coordinator-gated) and not stamped into
	// the header as the type. Empty defaults to SpaceTypeAny.
	SpaceType string
}

// SpaceListEvent is delivered to Service.Subscribe callbacks.
type SpaceListEvent struct {
	Added   []SpaceInfo
	Updated []SpaceInfo
	Removed []string
}

// AccountMetadata is the owner/member metadata attached to ACL join
// records (identityRepo-backed on the wire).
type AccountMetadata struct {
	Name        string
	Description string
	IconCID     string
}
