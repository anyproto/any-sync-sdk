package account

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/pkarr"
)

const (
	relayTimeout   = 10 * time.Second
	maxPayloadSize = 4096
	contentType    = "application/x-pkarr-signed-packet"
)

// ErrStale means every relay already holds a newer packet: another
// device published since this one resolved.
var ErrStale = errors.New("account record: relays hold a newer packet")

// Client talks to pkarr relays: PUT/GET of signed packets at
// <relay>/pkarr/<z32>. Relays verify the signature and refuse packets
// older than what they hold; they can withhold, never forge.
type Client struct {
	urls []*url.URL
	http *http.Client
}

// NewClient accepts relay base URLs with or without a /pkarr path.
func NewClient(relayURLs []string) (*Client, error) {
	c := &Client{http: &http.Client{Timeout: relayTimeout}}
	for _, raw := range relayURLs {
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" {
			return nil, fmt.Errorf("account record: relay url %q", raw)
		}
		u.Path = strings.TrimSuffix(u.Path, "/")
		if !strings.HasSuffix(u.Path, "/pkarr") {
			u.Path += "/pkarr"
		}
		c.urls = append(c.urls, u)
	}
	if len(c.urls) == 0 {
		return nil, errors.New("account record: no relay configured")
	}
	return c, nil
}

// Publish PUTs the packet to every relay. It fails only when no relay
// stored it; ErrStale when every relay answered that it holds a newer
// packet.
func (c *Client) Publish(ctx context.Context, packet *pkarr.SignedPacket) error {
	target := packet.PublicKey().EndpointID().Z32()
	var (
		stored bool
		stale  int
		last   error
	)
	for _, base := range c.urls {
		err := c.put(ctx, base, target, packet.RelayPayload())
		switch {
		case err == nil:
			stored = true
		case errors.Is(err, ErrStale):
			stale++
		default:
			last = err
		}
	}
	if stored {
		return nil
	}
	if stale == len(c.urls) {
		return ErrStale
	}
	return last
}

// Resolve GETs the record from every relay and returns the newest
// verified packet; nil when no relay holds one.
func (c *Client) Resolve(ctx context.Context, pub key.PublicKey) (*pkarr.SignedPacket, error) {
	target := pub.EndpointID().Z32()
	var (
		newest *pkarr.SignedPacket
		last   error
	)
	for _, base := range c.urls {
		p, err := c.get(ctx, base, target, pub)
		if err != nil {
			last = err
			continue
		}
		if p != nil && (newest == nil || p.MoreRecentThan(newest)) {
			newest = p
		}
	}
	if newest == nil && last != nil {
		return nil, last
	}
	return newest, nil
}

func (c *Client) put(ctx context.Context, base *url.URL, target string, payload []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, base.JoinPath(target).String(), bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	switch {
	case resp.StatusCode == http.StatusConflict:
		return ErrStale
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	}
	return fmt.Errorf("account record: %s: %s", base.Host, resp.Status)
}

func (c *Client) get(ctx context.Context, base *url.URL, target string, pub key.PublicKey) (*pkarr.SignedPacket, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base.JoinPath(target).String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return nil, fmt.Errorf("account record: %s: %s", base.Host, resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxPayloadSize))
	if err != nil {
		return nil, err
	}
	return pkarr.FromRelayPayload(pub, body)
}
