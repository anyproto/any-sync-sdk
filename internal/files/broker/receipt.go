package broker

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/anyproto/any-sync/commonfile/fileproto/fileprotov2"
	"github.com/anyproto/any-sync/util/crypto"
	"github.com/ipfs/go-cid"
	"google.golang.org/protobuf/encoding/protowire"
)

// Receipt is the decoded durable-custody receipt payload. Mirrors
// filenode2's receiptproto.ReceiptPayload wire contract; decoded with
// protowire (field-number-stable) so the SDK carries no dependency on
// the broker module's codegen types. Verify-then-unmarshal: the
// signature is over the payload bytes exactly.
type Receipt struct {
	NetworkId    string // 1
	SpaceId      string // 2
	RootCid      []byte // 3
	Size         uint64 // 4
	SignedAt     int64  // 5
	SignerPeerId string // 6
}

// ParseReceiptPayload decodes the receipt payload. Unknown fields are
// skipped (the node may extend the receipt without a wire break).
func ParseReceiptPayload(b []byte) (Receipt, error) {
	var r Receipt
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return Receipt{}, protowire.ParseError(n)
		}
		b = b[n:]
		switch typ {
		case protowire.BytesType:
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return Receipt{}, protowire.ParseError(n)
			}
			b = b[n:]
			switch num {
			case 1:
				r.NetworkId = string(v)
			case 2:
				r.SpaceId = string(v)
			case 3:
				r.RootCid = append([]byte(nil), v...)
			case 6:
				r.SignerPeerId = string(v)
			}
		case protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return Receipt{}, protowire.ParseError(n)
			}
			b = b[n:]
			switch num {
			case 4:
				r.Size = v
			case 5:
				r.SignedAt = int64(v)
			}
		default:
			n := protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return Receipt{}, protowire.ParseError(n)
			}
			b = b[n:]
		}
	}
	return r, nil
}

// VerifyParams pins what a receipt must attest to.
type VerifyParams struct {
	NetworkId string
	SpaceId   string
	Root      cid.Cid
	// ObjectSize is the stored S3 object size (the CAR file size, which
	// the node HEAD-measures after the PUT). 0 skips the size check.
	ObjectSize uint64
	// FleetPeers is the current fileV2 fleet; the signer must be one of
	// them (receipt signatures verify against the signer's peer key).
	FleetPeers []string
}

// VerifyReceipt checks the signature and every attested field, and
// returns the networkSign row value ("{signerPeerId}/{base64(sig)}").
func VerifyReceipt(rcpt *fileprotov2.NetworkSignReceipt, p VerifyParams) (string, error) {
	if rcpt == nil || len(rcpt.ReceiptPayload) == 0 || len(rcpt.Signature) == 0 {
		return "", errors.New("filebroker: empty receipt")
	}
	r, err := ParseReceiptPayload(rcpt.ReceiptPayload)
	if err != nil {
		return "", fmt.Errorf("filebroker: receipt payload: %w", err)
	}
	fleet := false
	for _, id := range p.FleetPeers {
		if id == r.SignerPeerId {
			fleet = true
			break
		}
	}
	if !fleet {
		return "", fmt.Errorf("filebroker: receipt signer %s is not a fileV2 fleet peer", r.SignerPeerId)
	}
	pub, err := crypto.DecodePeerId(r.SignerPeerId)
	if err != nil {
		return "", fmt.Errorf("filebroker: receipt signer peerId: %w", err)
	}
	ok, err := pub.Verify(rcpt.ReceiptPayload, rcpt.Signature)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", errors.New("filebroker: receipt signature invalid")
	}
	if r.NetworkId != p.NetworkId {
		return "", fmt.Errorf("filebroker: receipt networkId %q != %q", r.NetworkId, p.NetworkId)
	}
	if r.SpaceId != p.SpaceId {
		return "", fmt.Errorf("filebroker: receipt spaceId %q != %q", r.SpaceId, p.SpaceId)
	}
	if !bytes.Equal(r.RootCid, p.Root.Bytes()) {
		return "", errors.New("filebroker: receipt rootCid mismatch")
	}
	if p.ObjectSize != 0 && r.Size != p.ObjectSize {
		return "", fmt.Errorf("filebroker: receipt size %d != stored object size %d", r.Size, p.ObjectSize)
	}
	return NetworkSign(r.SignerPeerId, rcpt.Signature), nil
}

// NetworkSign formats the payloads-row receipt value.
func NetworkSign(signerPeerId string, signature []byte) string {
	return signerPeerId + "/" + base64.StdEncoding.EncodeToString(signature)
}
