package p2p

import (
	"context"
	"testing"

	"github.com/anyproto/any-sync/commonspace/clientspaceproto"
	"github.com/anyproto/any-sync/net/peer"
	"github.com/stretchr/testify/require"
)

type fakeLANAddrs struct {
	addrs map[string][]string
}

func (f *fakeLANAddrs) SetLAN(peerId string, addrs []string) {
	if f.addrs == nil {
		f.addrs = map[string][]string{}
	}
	f.addrs[peerId] = addrs
}

// fakeDiscoveryKeys returns a static spaceId→key resolver for tests.
func fakeDiscoveryKeys(keys map[string][]byte) func(context.Context, []string) map[string][]byte {
	return func(_ context.Context, spaceIds []string) map[string][]byte {
		out := map[string][]byte{}
		for _, id := range spaceIds {
			if k, ok := keys[id]; ok {
				out[id] = k
			}
		}
		return out
	}
}

func testKey(seed byte) []byte {
	k := make([]byte, 32)
	for i := range k {
		k[i] = seed
	}
	return k
}

func TestSpaceExchangeV1AlwaysRefused(t *testing.T) {
	store := NewPeerStore()
	ex := NewExchange("self-peer", store, func() []string { return []string{"mine"} }, nil, nil)
	ex.addrs = &fakeLANAddrs{}

	// The legacy plaintext exchange must never be served — regardless
	// of who asks — and must record nothing.
	ctx := peer.CtxWithPeerId(context.Background(), "remote-peer")
	_, err := ex.SpaceExchange(ctx, &clientspaceproto.SpaceExchangeRequest{
		SpaceIds:    []string{"s"},
		LocalServer: &clientspaceproto.LocalServer{Ips: []string{"10.0.0.9"}, Port: 4242},
	})
	require.ErrorContains(t, err, "not supported")
	require.Empty(t, store.AllLocalPeers())
}

func TestSpaceExchangeV2RejectsBadAddresses(t *testing.T) {
	sharedKey := testKey(1)
	keys := map[string][]byte{"shared": sharedKey}
	store := NewPeerStore()
	ex := NewExchange("self-peer", store, func() []string { return []string{"shared"} },
		fakeDiscoveryKeys(keys), nil)
	ps := &fakeLANAddrs{}
	ex.addrs = ps
	ctx := peer.CtxWithPeerId(context.Background(), "p")

	// Out-of-range port: no addresses and no space set recorded, but
	// the proofs are still returned.
	nonce, tokens := buildV2Request(t, keys, "p", "self-peer")
	resp, err := ex.SpaceExchangeV2(ctx, &clientspaceproto.SpaceExchangeV2Request{
		Nonce:       nonce,
		SpaceTokens: tokens,
		LocalServer: &clientspaceproto.LocalServer{Ips: []string{"10.0.0.1"}, Port: 70000},
	})
	require.NoError(t, err)
	require.Len(t, resp.SpaceTokens, 1)
	require.Empty(t, ps.addrs["p"])
	require.Empty(t, store.AllLocalPeers())

	// Garbage / hostname IPs are dropped; only literal IPs survive.
	nonce, tokens = buildV2Request(t, keys, "p", "self-peer")
	_, err = ex.SpaceExchangeV2(ctx, &clientspaceproto.SpaceExchangeV2Request{
		Nonce:       nonce,
		SpaceTokens: tokens,
		LocalServer: &clientspaceproto.LocalServer{Ips: []string{"evil.example.com", "not-an-ip", "10.0.0.2"}, Port: 4242},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"yamux://10.0.0.2:4242", "quic://10.0.0.2:4242"}, ps.addrs["p"])
	require.ElementsMatch(t, []string{"p"}, store.LocalPeerIds("shared"))
}

// buildV2Request builds the caller side of a v2 exchange for tests:
// tokens for the given spaces, padded, plus the nonce.
func buildV2Request(t *testing.T, keys map[string][]byte, callerPeerId, responderPeerId string) (nonce []byte, tokens [][]byte) {
	nonce, err := clientspaceproto.NewNonceV2()
	require.NoError(t, err)
	for _, key := range keys {
		tokens = append(tokens, clientspaceproto.RequestTokenV2(key, nonce, callerPeerId, responderPeerId))
	}
	tokens, err = clientspaceproto.PadTokensV2(tokens)
	require.NoError(t, err)
	return nonce, tokens
}

func TestSpaceExchangeV2Inbound(t *testing.T) {
	sharedKey := testKey(1)
	responderOnlyKey := testKey(2)
	callerOnlyKey := testKey(3)

	store := NewPeerStore()
	var kicked []string
	ex := NewExchange("self-peer", store,
		func() []string { return []string{"shared", "respOnly", "keyless"} },
		fakeDiscoveryKeys(map[string][]byte{"shared": sharedKey, "respOnly": responderOnlyKey}),
		func(_ string, spaceIds []string) { kicked = spaceIds })
	ps := &fakeLANAddrs{}
	ex.addrs = ps

	nonce, tokens := buildV2Request(t, map[string][]byte{"shared": sharedKey, "callerOnly": callerOnlyKey}, "remote-peer", "self-peer")

	ctx := peer.CtxWithPeerId(context.Background(), "remote-peer")
	ctx = peer.CtxWithPeerAddr(ctx, "quic://192.168.1.5:51000")
	resp, err := ex.SpaceExchangeV2(ctx, &clientspaceproto.SpaceExchangeV2Request{
		Nonce:       nonce,
		SpaceTokens: tokens,
		LocalServer: &clientspaceproto.LocalServer{Ips: []string{"192.168.1.5"}, Port: 4242},
	})
	require.NoError(t, err)

	// Only the shared space is recorded — the caller's claim was proven
	// by its token, and respOnly/keyless stay invisible to it.
	require.ElementsMatch(t, []string{"remote-peer"}, store.LocalPeerIds("shared"))
	require.Empty(t, store.LocalPeerIds("respOnly"))
	require.Equal(t, []string{"shared"}, kicked)
	require.Equal(t, []string{"yamux://192.168.1.5:4242", "quic://192.168.1.5:4242"}, ps.addrs["remote-peer"])

	// The response proves membership for exactly the intersection.
	require.Len(t, resp.SpaceTokens, 1)
	require.Equal(t, clientspaceproto.ResponseTokenV2(sharedKey, nonce, "remote-peer", "self-peer"), resp.SpaceTokens[0])
}

func TestSpaceExchangeV2ProbeWithoutLocalServer(t *testing.T) {
	sharedKey := testKey(1)
	store := NewPeerStore()
	ex := NewExchange("self-peer", store,
		func() []string { return []string{"shared"} },
		fakeDiscoveryKeys(map[string][]byte{"shared": sharedKey}), nil)
	ex.addrs = &fakeLANAddrs{}

	nonce, tokens := buildV2Request(t, map[string][]byte{"shared": sharedKey}, "remote-peer", "self-peer")
	ctx := peer.CtxWithPeerId(context.Background(), "remote-peer")
	resp, err := ex.SpaceExchangeV2(ctx, &clientspaceproto.SpaceExchangeV2Request{Nonce: nonce, SpaceTokens: tokens})
	require.NoError(t, err)
	// Proofs are returned (the caller proved membership) but nothing is
	// recorded without dialable addresses.
	require.Len(t, resp.SpaceTokens, 1)
	require.Empty(t, store.AllLocalPeers())
}

func TestSpaceExchangeV2RejectsBadRequests(t *testing.T) {
	store := NewPeerStore()
	ex := NewExchange("self-peer", store, func() []string { return nil },
		fakeDiscoveryKeys(nil), nil)
	ex.addrs = &fakeLANAddrs{}
	ctx := peer.CtxWithPeerId(context.Background(), "remote-peer")

	_, err := ex.SpaceExchangeV2(ctx, &clientspaceproto.SpaceExchangeV2Request{Nonce: []byte("short")})
	require.ErrorContains(t, err, "nonce")

	nonce, err := clientspaceproto.NewNonceV2()
	require.NoError(t, err)
	tooMany := make([][]byte, clientspaceproto.MaxTokensV2+1)
	for i := range tooMany {
		tooMany[i] = testKey(byte(i))
	}
	_, err = ex.SpaceExchangeV2(ctx, &clientspaceproto.SpaceExchangeV2Request{Nonce: nonce, SpaceTokens: tooMany})
	require.ErrorContains(t, err, "too many tokens")

	// Own peer id refused in v2 as well.
	selfCtx := peer.CtxWithPeerId(context.Background(), "self-peer")
	_, err = ex.SpaceExchangeV2(selfCtx, &clientspaceproto.SpaceExchangeV2Request{Nonce: nonce})
	require.ErrorContains(t, err, "own peer id")
}

// TestSpaceExchangeV2ProbeColdRestore is the LAN cold-restore
// handshake half: a fresh device of the SAME account knows a space id
// (tech-space index) but has no ACL key yet, so it sends an
// account-keyed probe token. The responder proves it holds the space
// — the caller can then pull from it — but records the caller for
// NOTHING (a probe claims interest, not possession).
func TestSpaceExchangeV2ProbeColdRestore(t *testing.T) {
	accountKey := testKey(4)
	store := NewPeerStore()
	ex := NewExchange("self-peer", store,
		func() []string { return []string{"mine"} },
		fakeDiscoveryKeys(map[string][]byte{"mine": testKey(1)}), nil)
	ex.addrs = &fakeLANAddrs{}
	ex.SetAccountKeysFn(fakeDiscoveryKeys(map[string][]byte{"mine": accountKey}))

	nonce, err := clientspaceproto.NewNonceV2()
	require.NoError(t, err)
	tokens, err := clientspaceproto.PadTokensV2([][]byte{
		probeRequestTokenV2(accountKey, nonce, "fresh-device", "self-peer"),
	})
	require.NoError(t, err)

	ctx := peer.CtxWithPeerId(context.Background(), "fresh-device")
	ctx = peer.CtxWithPeerAddr(ctx, "quic://192.168.1.7:51000")
	resp, err := ex.SpaceExchangeV2(ctx, &clientspaceproto.SpaceExchangeV2Request{
		Nonce:       nonce,
		SpaceTokens: tokens,
		LocalServer: &clientspaceproto.LocalServer{Ips: []string{"192.168.1.7"}, Port: 4242},
	})
	require.NoError(t, err)

	// The proof lets the caller record US as holding the space...
	require.Len(t, resp.SpaceTokens, 1)
	require.Equal(t, clientspaceproto.ResponseTokenV2(accountKey, nonce, "fresh-device", "self-peer"), resp.SpaceTokens[0])
	// ...but we record the caller for nothing.
	require.Empty(t, store.LocalPeerIds("mine"))
}

// A probe keyed with the WRONG account key (a stranger who learned the
// space id) gets no proof and records nothing — the probe path must
// not weaken the v2 privacy contract.
func TestSpaceExchangeV2ProbeWrongAccountKey(t *testing.T) {
	store := NewPeerStore()
	ex := NewExchange("self-peer", store,
		func() []string { return []string{"mine"} },
		fakeDiscoveryKeys(map[string][]byte{"mine": testKey(1)}), nil)
	ex.addrs = &fakeLANAddrs{}
	ex.SetAccountKeysFn(fakeDiscoveryKeys(map[string][]byte{"mine": testKey(4)}))

	nonce, err := clientspaceproto.NewNonceV2()
	require.NoError(t, err)
	tokens, err := clientspaceproto.PadTokensV2([][]byte{
		probeRequestTokenV2(testKey(9), nonce, "stranger", "self-peer"),
	})
	require.NoError(t, err)

	ctx := peer.CtxWithPeerId(context.Background(), "stranger")
	ctx = peer.CtxWithPeerAddr(ctx, "quic://192.168.1.9:51000")
	resp, err := ex.SpaceExchangeV2(ctx, &clientspaceproto.SpaceExchangeV2Request{
		Nonce:       nonce,
		SpaceTokens: tokens,
		LocalServer: &clientspaceproto.LocalServer{Ips: []string{"192.168.1.9"}, Port: 4242},
	})
	require.NoError(t, err)
	require.Empty(t, resp.SpaceTokens)
	require.Empty(t, store.LocalPeerIds("mine"))
}

// probeSet: probes = (known ∪ stored) minus ACL-keyed, deduped; nil
// when the sources aren't wired.
func TestProbeSet(t *testing.T) {
	ex := NewExchange("self-peer", NewPeerStore(), func() []string { return nil }, nil, nil)
	ids, keys := ex.probeSet(context.Background(), []string{"stored"}, nil)
	require.Nil(t, ids)
	require.Nil(t, keys)

	ex.SetKnownSpaceIdsFn(func() []string { return []string{"known", "stored", "known"} })
	ex.SetAccountKeysFn(fakeDiscoveryKeys(map[string][]byte{"known": testKey(1), "storedKeyless": testKey(2)}))
	ids, _ = ex.probeSet(context.Background(),
		[]string{"stored", "storedKeyless"},
		map[string][]byte{"stored": testKey(3)})
	require.ElementsMatch(t, []string{"known", "storedKeyless"}, ids)
}

func TestSpaceExchangeV2StrangerLearnsNothing(t *testing.T) {
	sharedKey := testKey(1)
	store := NewPeerStore()
	ex := NewExchange("self-peer", store,
		func() []string { return []string{"shared"} },
		fakeDiscoveryKeys(map[string][]byte{"shared": sharedKey}), nil)
	ex.addrs = &fakeLANAddrs{}

	// A stranger who knows the space id but not the read key derives a
	// different discovery key: no intersection, empty response, nothing
	// recorded in the store.
	wrongKey := testKey(9)
	nonce, tokens := buildV2Request(t, map[string][]byte{"shared": wrongKey}, "stranger", "self-peer")
	ctx := peer.CtxWithPeerId(context.Background(), "stranger")
	ctx = peer.CtxWithPeerAddr(ctx, "quic://192.168.1.9:51000")
	resp, err := ex.SpaceExchangeV2(ctx, &clientspaceproto.SpaceExchangeV2Request{
		Nonce:       nonce,
		SpaceTokens: tokens,
		LocalServer: &clientspaceproto.LocalServer{Ips: []string{"192.168.1.9"}, Port: 4242},
	})
	require.NoError(t, err)
	require.Empty(t, resp.SpaceTokens)
	require.Empty(t, store.LocalPeerIds("shared"))
}
