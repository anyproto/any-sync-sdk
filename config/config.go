package config

import (
	"time"

	"github.com/anyproto/any-sync-sdk/handler"
)

// Config is the full SDK configuration passed to sdk.Open. Pure data —
// zero behavior lives here. AuthProvider is NOT in this struct; it is
// a behavior and travels as a separate argument to sdk.Open.
//
// Logger setup is intentionally not part of Config. Callers should
// invoke (logger.Config{...}).ApplyGlobal() once at process start
// before Open. ApplyGlobal mutates already-handed-out named loggers in
// place, so calling it inside Open would race goroutines from a prior
// SDK instance in the same process.
type Config struct {
	Storage Storage
	Network Network
	Sync    Sync
	Files   Files
	P2P     P2P

	// Headless runs the SDK as an embedded backend service rather than
	// a user-facing client. Open skips the account-facing boot work —
	// profile republish, the 1-1 inbox subsystem, identity-profile
	// resolution, pending-join resume, and the eager space-loading loop
	// — and the tech space stays strictly local: it is still derived and
	// opened (it is the space registry Get depends on) but is never
	// pushed to the network and never requests a coordinator receipt.
	// Spaces are opened on demand via Get after Track.
	//
	// This is the embedding mode of the filenode-v2 broker, usually
	// combined with Sync.TreeTypes. Regular app embedders leave it false.
	Headless bool `yaml:"headless"`

	// Types is the optional list of caller-defined types extending
	// the SDK's built-in catalog. Each Type binds a typeId to the
	// dataset handlers it owns; every handler's Dataset() name must
	// be unique across the whole catalog (no collisions with the
	// built-in datasets "objects", "properties", "shortIds", and no
	// duplicates across other Types). Empty or nil = built-ins only.
	Types []handler.Type
}

// Storage controls on-disk layout.
type Storage struct {
	// DataDir is the root under which any-store databases live.
	DataDir string

	// Topology picks one-DB-for-everything vs one-DB-per-space. Shared
	// is the simpler default; PerSpace isolates parallel writers and
	// makes deletion cheap but rules out cross-space transactions.
	// See docs/06-data-structure.md §"Storage Topology".
	Topology StorageTopology

	// AnyStore is a reserved hook for per-DB tuning. v1: leave zero.
	AnyStore AnyStoreTuning
}

// StorageTopology selects the dbRouter policy.
type StorageTopology uint8

const (
	StorageShared StorageTopology = iota
	StoragePerSpace
)

// AnyStoreTuning is a placeholder for future any-store options (cache
// sizes, WAL mode, etc.). Zero-value means defaults.
type AnyStoreTuning struct{}

// Files tunes the files byte layer. All zero-valued fields fall back
// to SDK defaults.
type Files struct {
	// PublicReadBaseUrl overrides the network-advertised public read
	// base for durable file downloads ({base}/blob/{spaceId}/{rootCid}).
	// Normally left empty: the SDK resolves it once from the network's
	// fileV2 nodes and caches it. Set it for private deployments that
	// front the object store themselves.
	PublicReadBaseUrl string `yaml:"publicReadBaseUrl"`

	// GCInterval enables the periodic file-cache safety sweep (prune
	// refs of deleted files, delete unreferenced content past grace,
	// drop stale partials) at the given cadence. ZERO — the default —
	// means NO automatic sweep: reclamation is fully embedder-driven
	// via SDK.SweepFileCache / FreeUpFileCache / Files().Offload.
	GCInterval time.Duration `yaml:"gcInterval"`
}

// P2P controls local-network peer discovery and sync. The SDK
// announces itself over mDNS on the LAN, discovers other devices
// running the same network, and syncs shared spaces with them
// directly — including while sync nodes are unreachable.
type P2P struct {
	// Enabled is an opt-out: nil (the default) means enabled. False
	// disables the QUIC listener and discovery entirely; sync status
	// then reports P2PStateNotPossible for every space.
	Enabled *bool `yaml:"enabled"`

	// Port fixes the QUIC listen port. Zero — the default — reuses the
	// port persisted under DataDir from the previous run, or binds an
	// ephemeral one on first start (and persists it).
	Port int `yaml:"port"`

	// ServiceName is the mDNS service type used for announce/browse.
	// Empty means the SDK default. Override only to isolate networks —
	// e.g. e2e tests use a unique per-run name so developer machines on
	// the same LAN don't discover each other.
	ServiceName string `yaml:"serviceName"`
}

// IsEnabled resolves the opt-out tristate: nil = enabled.
func (p P2P) IsEnabled() bool { return p.Enabled == nil || *p.Enabled }

// Network is the any-sync network configuration. v1 is deliberately
// conservative — callers pass a serialized nodeconf blob and we decode
// it inside anysyncx so any-sync type changes don't leak into the
// public surface.
type Network struct {
	// NodeConfYAML is the serialized any-sync nodeconf. If empty, the
	// SDK refuses to start.
	NodeConfYAML []byte
}

// Sync tunes sync timeouts and retries. All zero-valued fields fall
// back to SDK defaults.
type Sync struct {
	DialTimeout     time.Duration
	ChangeBatchSize int

	// TreeTypes enables selective sync by tree type. Empty or nil (the
	// default) syncs and materializes everything. Non-empty, the SDK
	// still head-syncs every space in full — all tree ids and heads are
	// known and the sync diff converges — but downloads, stores and
	// materializes only trees whose root changeType is in the list;
	// other trees are recorded as heads-only stubs. ACL, settings and
	// key-value data are always fully synced, as is the tech space.
	//
	// This is the embedding mode of the filenode-v2 broker, which passes
	// the payloads tree type only. Regular app embedders leave it empty.
	TreeTypes []string
}
