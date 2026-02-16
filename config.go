package syncsdk

import (
	"os"

	"github.com/anyproto/any-sync-sdk/keys"
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
	data, err := os.ReadFile(path)
	if err != nil {
		return NetworkConfig{}, err
	}
	return NetworkConfigFromYAML(data)
}
