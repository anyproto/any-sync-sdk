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

// maxTicketLen bounds a record value before it is decoded: a relay-only
// ticket is ~120 bytes, and the value is remote input.
const maxTicketLen = 512

var (
	errRecordTooLong  = errors.New("record value too long")
	errRecordIdentity = errors.New("ticket endpoint id does not belong to the row's peer")
	errRecordDirect   = errors.New("ticket carries direct addresses")
	errRecordNoRelay  = errors.New("ticket carries no relay")
)

// record is one validated row: the peer's ticket, the identity that
// signed it and the row timestamp (clamped to now at ingestion).
type record struct {
	ticket   string
	identity string
	seen     time.Time
}

// parseTicket validates a record value: bounded size, decodes once,
// names the endpoint of the peer that signed the row (a member can
// publish only its own address, never point others at a third party),
// and — with relays configured locally — carries a relay and no direct
// IP, so a row can never make this device send packets to an
// attacker-chosen host.
func parseTicket(peerId string, value []byte, requireRelay bool) (string, error) {
	if len(value) > maxTicketLen {
		return "", errRecordTooLong
	}
	ticket := string(value)
	addr, err := endpointticket.Decode(ticket)
	if err != nil {
		return "", fmt.Errorf("decode ticket: %w", err)
	}
	owner, err := iroh.PeerIdFromEndpointId(addr.ID)
	if err != nil {
		return "", err
	}
	if owner != peerId {
		return "", errRecordIdentity
	}
	if requireRelay {
		if len(addr.IPAddrs()) > 0 {
			return "", errRecordDirect
		}
		if len(addr.RelayURLs()) == 0 {
			return "", errRecordNoRelay
		}
	}
	return ticket, nil
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

var _ = netaddr.EndpointAddr{}
