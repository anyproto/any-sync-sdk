package p2p

import (
	"context"
	"errors"
	"net"
	"strconv"

	"github.com/brutella/dnssd"

	sdkp2p "github.com/anyproto/any-sync-sdk/p2p"
)

// dnssdDriver is the built-in discovery backend: mDNS/DNS-SD via
// brutella/dnssd (Bonjour-conformance-tested; handles per-interface
// address resolution and, on Linux, re-announces on link changes).
// Stateless — each Announce/Browse call builds its own responder or
// lookup, so the discovery supervisor can restart them freely.
type dnssdDriver struct{}

func (dnssdDriver) Announce(ctx context.Context, a sdkp2p.Announcement) error {
	rp, err := dnssd.NewResponder()
	if err != nil {
		return err
	}
	// Host is the peerId too: two SDK instances on one machine must
	// not probe/defend the same hostname's A records.
	svc, err := dnssd.NewService(dnssd.Config{
		Name:   a.PeerId,
		Type:   a.ServiceType,
		Domain: "local",
		Host:   a.PeerId,
		Port:   a.Port,
	})
	if err != nil {
		return err
	}
	if _, err = rp.Add(svc); err != nil {
		return err
	}
	err = rp.Respond(ctx)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (dnssdDriver) Browse(ctx context.Context, serviceType string, found func(sdkp2p.DiscoveredPeer), lost func(peerId string)) error {
	err := dnssd.LookupType(ctx, serviceType+".local.", func(e dnssd.BrowseEntry) {
		var addrs []string
		for _, ip := range e.IPs {
			// IPv4 only, matching the udp4 QUIC listener.
			if ip4 := ip.To4(); ip4 != nil {
				addrs = append(addrs, net.JoinHostPort(ip4.String(), strconv.Itoa(e.Port)))
			}
		}
		if len(addrs) == 0 {
			return
		}
		found(sdkp2p.DiscoveredPeer{PeerId: e.Name, Addrs: addrs})
	}, func(e dnssd.BrowseEntry) {
		lost(e.Name)
	})
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
