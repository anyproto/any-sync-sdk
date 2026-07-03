package broker

import (
	"testing"

	"github.com/anyproto/any-sync/commonfile/fileproto/fileprotov2"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
)

func testRoot(t *testing.T) cid.Cid {
	t.Helper()
	mh, err := multihash.Sum([]byte("receipt test content"), multihash.SHA2_256, -1)
	require.NoError(t, err)
	return cid.NewCidV1(cid.Raw, mh)
}

// encodeReceipt builds the wire payload the filenode signs
// (field-number-stable protowire, mirroring receiptproto).
func encodeReceipt(r Receipt) []byte {
	var b []byte
	b = protowire.AppendTag(b, 1, protowire.BytesType)
	b = protowire.AppendString(b, r.NetworkId)
	b = protowire.AppendTag(b, 2, protowire.BytesType)
	b = protowire.AppendString(b, r.SpaceId)
	b = protowire.AppendTag(b, 3, protowire.BytesType)
	b = protowire.AppendBytes(b, r.RootCid)
	b = protowire.AppendTag(b, 4, protowire.VarintType)
	b = protowire.AppendVarint(b, r.Size)
	b = protowire.AppendTag(b, 5, protowire.VarintType)
	b = protowire.AppendVarint(b, uint64(r.SignedAt))
	b = protowire.AppendTag(b, 6, protowire.BytesType)
	b = protowire.AppendString(b, r.SignerPeerId)
	return b
}

func TestVerifyReceipt(t *testing.T) {
	// The fleet signing key is deliberately distinct from the signing
	// node's peer key — the real deployment shape: one shared account
	// (signing) key across the fleet, per-node transport peerIds.
	fleetPriv, fleetPub, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	nodePriv, nodePub, err := crypto.GenerateRandomEd25519KeyPair()
	require.NoError(t, err)
	fileNetworkId := fleetPub.Network()
	root := testRoot(t)

	base := Receipt{
		NetworkId:    "net1",
		SpaceId:      "space1",
		RootCid:      root.Bytes(),
		Size:         12345,
		SignedAt:     1_750_000_000,
		SignerPeerId: nodePub.PeerId(),
	}
	sign := func(r Receipt) *fileprotov2.NetworkSignReceipt {
		payload := encodeReceipt(r)
		sig, err := fleetPriv.Sign(payload)
		require.NoError(t, err)
		return &fileprotov2.NetworkSignReceipt{ReceiptPayload: payload, Signature: sig}
	}
	params := VerifyParams{
		NetworkId:     "net1",
		SpaceId:       "space1",
		Root:          root,
		ObjectSize:    12345,
		FileNetworkId: fileNetworkId,
	}

	t.Run("valid", func(t *testing.T) {
		got, err := VerifyReceipt(sign(base), params)
		require.NoError(t, err)
		require.Equal(t, NetworkSign(fileNetworkId, sign(base).Signature), got)
	})

	t.Run("signerPeerId is audit-only", func(t *testing.T) {
		r := base
		r.SignerPeerId = "12D3KooWreplacedNodeNotInAnyFleet"
		_, err := VerifyReceipt(sign(r), params)
		require.NoError(t, err)
	})

	t.Run("no fileNetworkId", func(t *testing.T) {
		p := params
		p.FileNetworkId = ""
		_, err := VerifyReceipt(sign(base), p)
		require.ErrorIs(t, err, ErrNoFileNetworkId)
	})

	t.Run("signed by peer key, not fleet key", func(t *testing.T) {
		payload := encodeReceipt(base)
		sig, err := nodePriv.Sign(payload)
		require.NoError(t, err)
		_, err = VerifyReceipt(&fileprotov2.NetworkSignReceipt{ReceiptPayload: payload, Signature: sig}, params)
		require.ErrorContains(t, err, "signature invalid")
	})

	t.Run("tampered payload", func(t *testing.T) {
		rcpt := sign(base)
		rcpt.ReceiptPayload[len(rcpt.ReceiptPayload)-1] ^= 0xff
		_, err := VerifyReceipt(rcpt, params)
		require.Error(t, err)
	})

	t.Run("wrong network", func(t *testing.T) {
		r := base
		r.NetworkId = "net2"
		_, err := VerifyReceipt(sign(r), params)
		require.ErrorContains(t, err, "networkId")
	})

	t.Run("wrong space", func(t *testing.T) {
		r := base
		r.SpaceId = "space2"
		_, err := VerifyReceipt(sign(r), params)
		require.ErrorContains(t, err, "spaceId")
	})

	t.Run("wrong root", func(t *testing.T) {
		r := base
		r.RootCid = append([]byte(nil), root.Bytes()...)
		r.RootCid[len(r.RootCid)-1] ^= 0xff
		_, err := VerifyReceipt(sign(r), params)
		require.ErrorContains(t, err, "rootCid")
	})

	t.Run("wrong size", func(t *testing.T) {
		r := base
		r.Size = 99
		_, err := VerifyReceipt(sign(r), params)
		require.ErrorContains(t, err, "size")
	})

	t.Run("size check skipped when zero", func(t *testing.T) {
		p := params
		p.ObjectSize = 0
		r := base
		r.Size = 99
		_, err := VerifyReceipt(sign(r), p)
		require.NoError(t, err)
	})

	t.Run("unknown fields skipped", func(t *testing.T) {
		payload := encodeReceipt(base)
		payload = protowire.AppendTag(payload, 9, protowire.BytesType)
		payload = protowire.AppendString(payload, "future extension")
		sig, err := fleetPriv.Sign(payload)
		require.NoError(t, err)
		_, err = VerifyReceipt(&fileprotov2.NetworkSignReceipt{ReceiptPayload: payload, Signature: sig}, params)
		require.NoError(t, err)
	})

	t.Run("empty receipt", func(t *testing.T) {
		_, err := VerifyReceipt(nil, params)
		require.Error(t, err)
		_, err = VerifyReceipt(&fileprotov2.NetworkSignReceipt{}, params)
		require.Error(t, err)
	})
}
