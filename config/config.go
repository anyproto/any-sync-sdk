package config

import (
	"errors"
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
	Push    Push

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
	//
	// Headless mode (see Config.Headless) flips the default: a nil
	// Enabled resolves to disabled there, since an embedded backend has
	// no reason to announce over mDNS or accept LAN peers. A headless
	// broker that genuinely wants local-network sync must opt back in
	// with Enabled = &true; an explicit &false stays off in both modes.
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

	// Global is the internet-wide device-to-device layer (iroh: QUIC
	// with relay fallback and hole punching, peers discovered through
	// each space's key-value store). Independent of the LAN layer:
	// Enabled=false above with Global.Enabled=true is a valid setup.
	Global GlobalP2P `yaml:"global"`
}

// IsEnabled resolves the opt-out tristate: nil = enabled.
func (p P2P) IsEnabled() bool { return p.Enabled == nil || *p.Enabled }

// GlobalP2P configures the internet-wide p2p layer. Opt-in: nil Enabled
// means off until a relay is deployed for the network. Zero-valued
// budget fields take the DefaultGlobalP2P* values; see
// docs/18-global-p2p.md.
type GlobalP2P struct {
	// Enabled turns the layer on. nil and false both mean off.
	Enabled *bool `yaml:"enabled"`

	// RelayURLs are the home-relay candidates ("https://relay.example").
	// Required when enabled: without a relay the published ticket would
	// carry this device's IP addresses into every space's records.
	RelayURLs []string `yaml:"relayUrls"`

	// InsecureRelay admits http:// relay URLs (plaintext transport to
	// the relay). Development and tests only.
	InsecureRelay bool `yaml:"insecureRelay"`

	// Port fixes the UDP port of the iroh endpoint. Zero binds an
	// ephemeral port.
	Port int `yaml:"port"`

	// MaxConnections caps the global connections this device maintains
	// (outbound, chosen to cover the loaded spaces).
	MaxConnections int `yaml:"maxConnections"`

	// MaxInbound is the headroom above MaxConnections for connections
	// initiated by other devices: once MaxConnections+MaxInbound distinct
	// global peers hold a live connection, inbound ones are refused
	// before the handshake.
	MaxInbound int `yaml:"maxInbound"`

	// MaxDialsPerMinute rate-limits the connector; dials are sequential
	// (one in flight) regardless.
	MaxDialsPerMinute int `yaml:"maxDialsPerMinute"`

	// DialTimeout bounds one global dial (relay round trip included).
	// A relay dial either completes in about a round trip or dies at
	// QUIC's own handshake timeout of 5 s, so this only decides how long
	// the connector's single dial slot stays busy on a dead peer.
	DialTimeout time.Duration `yaml:"dialTimeout"`

	// KeepAlive is the QUIC keep-alive period of global connections.
	KeepAlive time.Duration `yaml:"keepAlive"`

	// StaleAfter / DormantAfter / DisableAfter are the liveness tiers:
	// a peer not seen for StaleAfter is probed on a slow cadence, past
	// DormantAfter only at startup and every few hours, past
	// DisableAfter never (its record is ignored until it moves).
	StaleAfter   time.Duration `yaml:"staleAfter"`
	DormantAfter time.Duration `yaml:"dormantAfter"`
	DisableAfter time.Duration `yaml:"disableAfter"`
}

// Defaults for the zero-valued GlobalP2P budget fields.
const (
	DefaultGlobalP2PMaxConnections    = 4
	DefaultGlobalP2PMaxInbound        = 8
	DefaultGlobalP2PMaxDialsPerMinute = 6
	DefaultGlobalP2PDialTimeout       = 6 * time.Second
	DefaultGlobalP2PKeepAlive         = 60 * time.Second
	DefaultGlobalP2PStaleAfter        = time.Hour
	DefaultGlobalP2PDormantAfter      = 7 * 24 * time.Hour
	DefaultGlobalP2PDisableAfter      = 30 * 24 * time.Hour
)

// IsEnabled reports whether the global layer is on (explicit opt-in).
func (g GlobalP2P) IsEnabled() bool { return g.Enabled != nil && *g.Enabled }

// Validate reports a configuration the layer cannot run with: enabled
// without a relay would publish this device's IP addresses into every
// space's records.
func (g GlobalP2P) Validate() error {
	if g.IsEnabled() && len(g.RelayURLs) == 0 {
		return errors.New("p2p.global: relayUrls is required when enabled")
	}
	return nil
}

// WithDefaults returns g with every zero budget field replaced by its
// default.
func (g GlobalP2P) WithDefaults() GlobalP2P {
	if g.MaxConnections <= 0 {
		g.MaxConnections = DefaultGlobalP2PMaxConnections
	}
	if g.MaxInbound <= 0 {
		g.MaxInbound = DefaultGlobalP2PMaxInbound
	}
	if g.MaxDialsPerMinute <= 0 {
		g.MaxDialsPerMinute = DefaultGlobalP2PMaxDialsPerMinute
	}
	if g.DialTimeout <= 0 {
		g.DialTimeout = DefaultGlobalP2PDialTimeout
	}
	if g.KeepAlive <= 0 {
		g.KeepAlive = DefaultGlobalP2PKeepAlive
	}
	if g.StaleAfter <= 0 {
		g.StaleAfter = DefaultGlobalP2PStaleAfter
	}
	if g.DormantAfter <= 0 {
		g.DormantAfter = DefaultGlobalP2PDormantAfter
	}
	if g.DisableAfter <= 0 {
		g.DisableAfter = DefaultGlobalP2PDisableAfter
	}
	return g
}

// ResolveP2P returns the effective P2P config for this Config, applying
// the headless default: in headless mode a nil (unset) Enabled resolves
// to disabled, because an embedded backend has no reason to announce
// over mDNS or accept LAN peers. An explicit Enabled — &true or &false —
// is always honored, so a headless broker can opt back into local sync.
// Global is opt-in in every mode, so it passes through with its budget
// defaults filled in.
func (c Config) ResolveP2P() P2P {
	p := c.P2P
	if c.Headless && p.Enabled == nil {
		off := false
		p.Enabled = &off
	}
	p.Global = p.Global.WithDefaults()
	return p
}

// Push configures the push-notification node. Unlike sync nodes it is
// NOT part of the nodeconf — it is a direct out-of-band peer: the SDK
// registers Addrs for PeerId on the peer service and dials it through
// the regular secure-channel pool, so RPCs carry the account identity
// the server authorizes by. Both fields empty (the default) disables
// push entirely — SDK.Push() methods then return
// space.ErrPushNotConfigured.
type Push struct {
	// PeerId is the push node's peer id (its device key identity).
	PeerId string `yaml:"peerId"`

	// Addrs are the node's dial addresses (same forms the nodeconf
	// uses, e.g. "quic://host:port" or "host:port").
	Addrs []string `yaml:"addrs"`
}

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
