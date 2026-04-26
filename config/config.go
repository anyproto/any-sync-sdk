package config

import (
	"time"

	"github.com/anyproto/any-sync/app/logger"
)

// Config is the full SDK configuration passed to sdk.Open. Pure data —
// zero behavior lives here. AuthProvider is NOT in this struct; it is
// a behavior and travels as a separate argument to sdk.Open.
type Config struct {
	Storage Storage
	Network Network
	Sync    Sync

	// Log is any-sync's logger configuration (production mode, default
	// level, per-name level overrides, output paths, format). The SDK
	// calls Log.ApplyGlobal() during Open; internal packages pull their
	// named loggers via logger.NewNamed.
	Log logger.Config
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
}
