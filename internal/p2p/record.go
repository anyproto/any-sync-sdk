package p2p

import (
	"errors"
	"fmt"
	"time"

	"github.com/anyproto/any-sync/net/transport/iroh"
	"github.com/tmc/go-iroh/endpointticket"
	"github.com/tmc/go-iroh/netaddr"
)

// RecordKey is the key-value key of a device's global p2p record. The
// store keeps one row per (key, peer), so every device of a space owns
// exactly one row: its endpoint ticket, re-set as a heartbeat.
const RecordKey = "p2p/iroh"

var (
	errRecordSelf     = errors.New("own record")
	errRecordIdentity = errors.New("ticket endpoint id does not belong to the row's peer")
)

// record is one validated row: the peer's ticket, the identity that
// signed it and the row timestamp (clamped by the status book).
type record struct {
	ticket   string
	identity string
	seen     time.Time
}

// parseTicket decodes a record value and checks that the ticket names
// the endpoint of the peer that signed the row — a member can publish
// only its own address, never point others at a third party.
func parseTicket(peerId, selfPeerId string, value []byte) (string, netaddr.EndpointAddr, error) {
	if peerId == selfPeerId {
		return "", netaddr.EndpointAddr{}, errRecordSelf
	}
	ticket := string(value)
	addr, err := endpointticket.Decode(ticket)
	if err != nil {
		return "", netaddr.EndpointAddr{}, fmt.Errorf("decode ticket: %w", err)
	}
	owner, err := iroh.PeerIdFromTicket(ticket)
	if err != nil {
		return "", netaddr.EndpointAddr{}, err
	}
	if owner != peerId {
		return "", netaddr.EndpointAddr{}, errRecordIdentity
	}
	return ticket, addr, nil
}

// homeRelay returns the first relay URL of a ticket, empty when the
// ticket carries none or does not parse.
func homeRelay(ticket string) string {
	addr, err := endpointticket.Decode(ticket)
	if err != nil {
		return ""
	}
	if urls := addr.RelayURLs(); len(urls) > 0 {
		return urls[0].String()
	}
	return ""
}
