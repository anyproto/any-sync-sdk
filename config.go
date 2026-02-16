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
