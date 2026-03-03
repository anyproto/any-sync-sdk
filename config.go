package syncsdk

import (
	anystore "github.com/anyproto/any-store"
	"github.com/anyproto/any-sync-sdk/keys"
	"github.com/anyproto/any-sync/osfuncs"
	"go.yaml.in/yaml/v2"
)

type Config struct {
	SigningKey   keys.PrivateKey
	MasterKey   keys.PrivateKey
	PeerKey     keys.PrivateKey
	Network     NetworkConfig
	StoragePath string
	// LogPath sets the directory for log files. When set, logs are written
	// to files in this directory instead of stderr. If empty (default),
	// logging to stderr is disabled entirely.
	LogPath string
	// WebRTC enables the WebRTC transport for connecting to any-sync nodes.
	// When non-nil, the WebRTC transport component is registered alongside
	// yamux and quic. This is required for WASM builds where yamux/quic
	// are not available.
	WebRTC *WebRTCConfig
	// StoreConfig provides optional configuration for the underlying any-store
	// database. When nil, any-store defaults are used.
	StoreConfig *anystore.Config
}

// WebRTCConfig configures the WebRTC transport.
type WebRTCConfig struct {
	// ListenAddrs are the addresses for the HTTP signal endpoint (server only).
	// Example: ["0.0.0.0:8080"]
	ListenAddrs []string
	// SignalPort overrides the signal endpoint port (0 = same as ListenAddrs).
	SignalPort int
	// DialTimeoutSec is the timeout for ICE gathering and connection setup.
	// Defaults to 30 seconds if zero.
	DialTimeoutSec int
	// WriteTimeoutSec is the write deadline on DataChannel streams.
	WriteTimeoutSec int
	// CloseTimeoutSec is the graceful shutdown timeout. Defaults to 5 seconds if zero.
	CloseTimeoutSec int
	// ICEServers is a list of STUN/TURN server URLs for NAT traversal.
	// Example: ["stun:stun.l.google.com:19302"]
	ICEServers []string
}

type NetworkConfig struct {
	ID        string     `yaml:"-"`
	NetworkID string     `yaml:"networkId"`
	Nodes     []NodeInfo `yaml:"nodes"`
}

type NodeInfo struct {
	PeerID    string   `yaml:"peerId"`
	Addresses []string `yaml:"addresses"`
	Types     []string `yaml:"types"`
}

// NetworkConfigToYAML serializes a NetworkConfig to YAML.
func NetworkConfigToYAML(cfg NetworkConfig) ([]byte, error) {
	return yaml.Marshal(cfg)
}

// NetworkConfigFromYAML parses a NetworkConfig from YAML data.
// The ID field is not part of the YAML format and must be set separately.
func NetworkConfigFromYAML(data []byte) (NetworkConfig, error) {
	var cfg NetworkConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return NetworkConfig{}, err
	}
	return cfg, nil
}

// NetworkConfigFromFile reads a YAML file and parses a NetworkConfig from it.
// The ID field is not part of the YAML format and must be set separately.
func NetworkConfigFromFile(path string) (NetworkConfig, error) {
	data, err := osfuncs.ReadFile(path)
	if err != nil {
		return NetworkConfig{}, err
	}
	return NetworkConfigFromYAML(data)
}
