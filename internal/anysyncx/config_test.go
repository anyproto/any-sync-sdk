package anysyncx

import (
	"testing"
	"time"

	"github.com/anyproto/any-sync/nodeconf"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-sync-sdk/config"
)

func TestParseNodeConfKeepsIdentityFields(t *testing.T) {
	raw := []byte(`
id: 6a480c92689ecd56b48b4fe2
networkId: NTree123
fileNetworkId: NFile456
nodes:
  - peerId: 12D3KooWTree
    addresses: ["host:443"]
    types: ["tree"]
  - peerId: 12D3KooWFile
    addresses: ["host:455"]
    types: ["fileV2"]
`)
	conf, err := parseNodeConf(raw)
	require.NoError(t, err)
	require.Equal(t, "6a480c92689ecd56b48b4fe2", conf.Id)
	require.Equal(t, "NTree123", conf.NetworkId)
	// fileNetworkId is the fileV2 receipt-signing identity: dropping it
	// makes every durable receipt unverifiable, so files stay inflight
	// forever on networks whose coordinator doesn't re-supply it.
	require.Equal(t, "NFile456", conf.FileNetworkId)
	require.Len(t, conf.Nodes, 2)
}

// The iroh transport config: a pinned port binds dual-stack, the
// keep-alive stays below the idle timeout, the relay flags pass through.
func TestGetIroh(t *testing.T) {
	on := true
	cfg := config.Config{P2P: config.P2P{Global: config.GlobalP2P{
		Enabled:       &on,
		RelayURLs:     []string{"http://127.0.0.1:3340"},
		InsecureRelay: true,
		Port:          4321,
	}}}
	conf := newConfig(cfg, nodeconf.Configuration{}).GetIroh()
	require.Equal(t, "[::]:4321", conf.BindAddr)
	require.Equal(t, []string{"http://127.0.0.1:3340"}, conf.RelayURLs)
	require.True(t, conf.InsecureRelay)
	require.Equal(t, int(config.DefaultGlobalP2PDialTimeout/time.Second), conf.DialTimeoutSec)
	require.Greater(t, conf.KeepAlivePeriodSec, 0)
	require.Less(t, conf.KeepAlivePeriodSec, conf.MaxIdleTimeoutSec)

	cfg.P2P.Global.Port = 0
	require.Empty(t, newConfig(cfg, nodeconf.Configuration{}).GetIroh().BindAddr)
}
