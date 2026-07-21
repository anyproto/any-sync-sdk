package anysyncx

import (
	"testing"

	"github.com/stretchr/testify/require"
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
