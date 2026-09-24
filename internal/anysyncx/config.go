package anysyncx

import (
	"fmt"
	"math"
	"net"
	"net/netip"
	"path/filepath"
	"strconv"
	"time"

	"github.com/anyproto/any-sync/app"
	commonconfig "github.com/anyproto/any-sync/commonspace/config"
	"github.com/anyproto/any-sync/net/rpc"
	"github.com/anyproto/any-sync/net/secureservice"
	"github.com/anyproto/any-sync/net/streampool"
	"github.com/anyproto/any-sync/net/transport/iroh"
	"github.com/anyproto/any-sync/net/transport/quic"
	"github.com/anyproto/any-sync/net/transport/webtransport"
	"github.com/anyproto/any-sync/net/transport/yamux"
	"github.com/anyproto/any-sync/nodeconf"
	"gopkg.in/yaml.v3"

	"github.com/anyproto/any-sync-sdk/config"
)

// nodeConfYAML mirrors the on-disk format clients ship as
// config.Network.NodeConfYAML. Kept private so the public Config stays
// any-sync-agnostic.
type nodeConfYAML struct {
	ID string `yaml:"id"`
	// NetworkID identifies the tree/coordinator fleet; FileNetworkID is
	// the fileV2 fleet's receipt-signing identity — without it durable
	// receipts can't verify and files never leave the inflight state.
	NetworkID     string     `yaml:"networkId"`
	FileNetworkID string     `yaml:"fileNetworkId"`
	Nodes         []nodeYAML `yaml:"nodes"`
}

type nodeYAML struct {
	PeerID    string   `yaml:"peerId"`
	Addresses []string `yaml:"addresses"`
	Types     []string `yaml:"types"`
}

func parseNodeConf(raw []byte) (nodeconf.Configuration, error) {
	if len(raw) == 0 {
		return nodeconf.Configuration{}, fmt.Errorf("anysyncx: empty NodeConfYAML")
	}
	var doc nodeConfYAML
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nodeconf.Configuration{}, fmt.Errorf("anysyncx: parse nodeconf: %w", err)
	}
	nodes := make([]nodeconf.Node, len(doc.Nodes))
	for i, n := range doc.Nodes {
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
		Id:            doc.ID,
		NetworkId:     doc.NetworkID,
		FileNetworkId: doc.FileNetworkID,
		Nodes:         nodes,
	}, nil
}

// configAdapter satisfies all the GetX getters any-sync's components
// reach for during Init. Built from the SDK's public config.Config plus
// the parsed nodeconf.
type configAdapter struct {
	sdk      config.Config
	nodeConf nodeconf.Configuration
}

func newConfig(cfg config.Config, nc nodeconf.Configuration) *configAdapter {
	return &configAdapter{sdk: cfg, nodeConf: nc}
}

func (c *configAdapter) Init(_ *app.App) error { return nil }
func (c *configAdapter) Name() string          { return "config" }

func (c *configAdapter) GetSpace() commonconfig.Config {
	return commonconfig.Config{
		SyncPeriod:           30,
		KeepTreeDataInMemory: true,
	}
}

func (c *configAdapter) GetNodeConf() nodeconf.Configuration { return c.nodeConf }

func (c *configAdapter) GetDrpc() rpc.Config {
	return rpc.Config{Stream: rpc.StreamConfig{MaxMsgSizeMb: 256}}
}

func (c *configAdapter) GetYamux() yamux.Config {
	return yamux.Config{
		WriteTimeoutSec:    c.dialSeconds(),
		DialTimeoutSec:     c.dialSeconds(),
		KeepAlivePeriodSec: 25,
	}
}

func (c *configAdapter) GetQuic() quic.Config {
	return quic.Config{
		WriteTimeoutSec:    c.dialSeconds(),
		DialTimeoutSec:     c.dialSeconds(),
		MaxStreams:         128,
		KeepAlivePeriodSec: 25,
	}
}

// GetIroh maps the global p2p budget onto the iroh transport. The idle
// timeout is three keep-alive periods so a quiet relay path survives a
// missed probe; a dead peer is noticed within that window. Without a
// configured port the endpoint prefers the one persisted by the previous
// run (keepIrohPort) and falls back to an ephemeral one when it is taken.
func (c *configAdapter) GetIroh() iroh.Config {
	p2pCfg := c.sdk.ResolveP2P()
	g := p2pCfg.Global
	conf := iroh.Config{
		RelayURLs:          g.RelayURLs,
		InsecureRelay:      g.InsecureRelay,
		WriteTimeoutSec:    c.dialSeconds(),
		DialTimeoutSec:     wholeSeconds(g.DialTimeout),
		CloseTimeoutSec:    5,
		MaxStreams:         128,
		KeepAlivePeriodSec: wholeSeconds(g.KeepAlive),
		MaxIdleTimeoutSec:  wholeSeconds(3 * g.KeepAlive),
	}
	port := g.Port
	if port == 0 {
		port = readPortFile(c.irohPortFile())
		// The iroh endpoint binds before the LAN listener: a remembered
		// port equal to the LAN one would take it, and the LAN listener
		// has a single attempt at a configured port.
		lanPort := p2pCfg.Port
		if lanPort == 0 {
			lanPort = readPortFile(portFilePath(c.sdk.Storage.DataDir, portFileName))
		}
		if p2pCfg.IsEnabled() && port == lanPort {
			port = 0
		}
		conf.BindFallback = port != 0
	}
	if port != 0 {
		// dual-stack, like go-iroh's own default bind
		conf.BindAddr = net.JoinHostPort("::", strconv.Itoa(port))
	}
	return conf
}

func (c *configAdapter) GetWebTransport() webtransport.Config {
	return webtransport.Config{DialTimeoutSec: 30}
}

func (c *configAdapter) GetSecureService() secureservice.Config { return secureservice.Config{} }

// GetStreamConfig sizes the shared outgoing queues. The dial queue is
// one per process and takes every broadcast + head-sync send across all
// spaces at once; at boot the whole space list floods it while its 4
// workers may be stuck in slow dials, and an overflowed TryAdd DROPS
// the message (mitigated by the peer-manager park buffer, but the queue
// should rarely overflow in the first place). 300 matches heart.
// SendQueueSize is ignored for outgoing streams — the per-stream queue
// size is set at streamHandler.OpenStream (spacesync.go).
func (c *configAdapter) GetStreamConfig() streampool.StreamConfig {
	return streampool.StreamConfig{
		SendQueueSize:    outgoingQueueSize,
		DialQueueWorkers: 4,
		DialQueueSize:    outgoingQueueSize,
	}
}

// keepIrohPort persists the port the iroh endpoint bound so the next
// start prefers it (GetIroh). A configured port is never written.
func (c *configAdapter) keepIrohPort(ep interface{ LocalAddr() netip.AddrPort }) {
	if c.sdk.ResolveP2P().Global.Port != 0 {
		return
	}
	if port := int(ep.LocalAddr().Port()); port != 0 {
		savePortFile(c.irohPortFile(), port)
	}
}

func (c *configAdapter) irohPortFile() string {
	return portFilePath(c.sdk.Storage.DataDir, irohPortFileName)
}

func (c *configAdapter) GetNodeConfStorePath() string {
	return filepath.Join(c.sdk.Storage.DataDir, "nodeconf")
}

// dialSeconds returns Sync.DialTimeout in whole seconds, defaulting to
// 10s.
func (c *configAdapter) dialSeconds() int {
	d := c.sdk.Sync.DialTimeout
	if d <= 0 {
		d = 10 * time.Second
	}
	return wholeSeconds(d)
}

// wholeSeconds converts a positive duration for the transports' *Sec
// fields, rounding UP so a sub-second value never becomes 0: the
// transports read 0 as "use my default" (yamux 10s, iroh 15s) or, for
// the QUIC accept handshake and stream writes, as an already-expired
// deadline — either way not what the caller configured.
func wholeSeconds(d time.Duration) int {
	if d <= 0 {
		return 0
	}
	return int(math.Ceil(d.Seconds()))
}
