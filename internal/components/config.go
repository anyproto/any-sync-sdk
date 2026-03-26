package components

import (
	"path/filepath"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/config"
	"github.com/anyproto/any-sync/net/rpc"
	"github.com/anyproto/any-sync/net/secureservice"
	"github.com/anyproto/any-sync/net/streampool"
	"github.com/anyproto/any-sync/net/transport/quic"
	"github.com/anyproto/any-sync/net/transport/webtransport"
	"github.com/anyproto/any-sync/net/transport/yamux"
	"github.com/anyproto/any-sync/nodeconf"

	syncsdk "github.com/anyproto/any-sync-sdk"
)

type ConfigAdapter struct {
	sdkCfg syncsdk.Config
}

func NewConfig(cfg syncsdk.Config) *ConfigAdapter {
	return &ConfigAdapter{sdkCfg: cfg}
}

func (c *ConfigAdapter) Init(_ *app.App) error {
	return nil
}

func (c *ConfigAdapter) Name() string {
	return "config"
}

// config.ConfigGetter
func (c *ConfigAdapter) GetSpace() config.Config {
	return config.Config{
		SyncPeriod:           30, // seconds — periodic diff with peers
		KeepTreeDataInMemory: true,
	}
}

// nodeconf.ConfigGetter
func (c *ConfigAdapter) GetNodeConf() nodeconf.Configuration {
	nodes := make([]nodeconf.Node, len(c.sdkCfg.Network.Nodes))
	for i, n := range c.sdkCfg.Network.Nodes {
		types := make([]nodeconf.NodeType, len(n.Types))
		for j, t := range n.Types {
			types[j] = nodeconf.NodeType(t)
		}
		nodes[i] = nodeconf.Node{
			PeerId:    n.PeerID,
			Addresses: n.Addresses,
			Types:     types,
		}
	}
	return nodeconf.Configuration{
		Id:        c.sdkCfg.Network.ID,
		NetworkId: c.sdkCfg.Network.NetworkID,
		Nodes:     nodes,
	}
}

// rpc.ConfigGetter
func (c *ConfigAdapter) GetDrpc() rpc.Config {
	return rpc.Config{
		Stream: rpc.StreamConfig{
			MaxMsgSizeMb: 256,
		},
	}
}

// yamux configGetter
func (c *ConfigAdapter) GetYamux() yamux.Config {
	return yamux.Config{
		WriteTimeoutSec:    10,
		DialTimeoutSec:     10,
		KeepAlivePeriodSec: 25,
	}
}

// quic configGetter
func (c *ConfigAdapter) GetQuic() quic.Config {
	return quic.Config{
		WriteTimeoutSec:    10,
		DialTimeoutSec:     10,
		MaxStreams:          128,
		KeepAlivePeriodSec: 25,
	}
}

// webtransport configGetter
func (c *ConfigAdapter) GetWebTransport() webtransport.Config {
	return webtransport.Config{
		DialTimeoutSec: 30,
	}
}

// secureservice configGetter
func (c *ConfigAdapter) GetSecureService() secureservice.Config {
	return secureservice.Config{}
}

// streampool configGetter
func (c *ConfigAdapter) GetStreamConfig() streampool.StreamConfig {
	return streampool.StreamConfig{
		SendQueueSize:    100,
		DialQueueWorkers: 4,
		DialQueueSize:    100,
	}
}

// nodeconfstore configGetter
func (c *ConfigAdapter) GetNodeConfStorePath() string {
	return filepath.Join(c.sdkCfg.StoragePath, "nodeconf")
}
