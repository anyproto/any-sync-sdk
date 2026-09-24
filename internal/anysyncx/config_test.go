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
	cfg := config.Config{Storage: config.Storage{DataDir: t.TempDir()}, P2P: config.P2P{Global: config.GlobalP2P{
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

// Every transport takes whole seconds. A sub-second timeout rounds up
// to one, never down to zero: the transports read zero as their own
// default or as an expired deadline.
func TestTimeoutsRoundUpToWholeSeconds(t *testing.T) {
	for _, tc := range []struct {
		in   time.Duration
		want int
	}{
		{0, 10},
		{500 * time.Millisecond, 1},
		{time.Second, 1},
		{1500 * time.Millisecond, 2},
		{10 * time.Second, 10},
	} {
		cfg := config.Config{Sync: config.Sync{DialTimeout: tc.in}}
		c := newConfig(cfg, nodeconf.Configuration{})
		require.Equal(t, tc.want, c.GetYamux().DialTimeoutSec, "yamux dial %v", tc.in)
		require.Equal(t, tc.want, c.GetYamux().WriteTimeoutSec, "yamux write %v", tc.in)
		require.Equal(t, tc.want, c.GetQuic().DialTimeoutSec, "quic dial %v", tc.in)
		require.Equal(t, tc.want, c.GetQuic().WriteTimeoutSec, "quic write %v", tc.in)
		require.Equal(t, tc.want, c.GetIroh().WriteTimeoutSec, "iroh write %v", tc.in)
	}

	on := true
	cfg := config.Config{P2P: config.P2P{Global: config.GlobalP2P{
		Enabled:     &on,
		DialTimeout: 500 * time.Millisecond,
		KeepAlive:   700 * time.Millisecond,
	}}}
	conf := newConfig(cfg, nodeconf.Configuration{}).GetIroh()
	require.Equal(t, 1, conf.DialTimeoutSec)
	require.Equal(t, 1, conf.KeepAlivePeriodSec)
	require.Equal(t, 3, conf.MaxIdleTimeoutSec, "three keep-alives, rounded up")
}
