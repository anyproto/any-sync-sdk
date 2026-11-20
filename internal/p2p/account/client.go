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
	"sync"
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
// device published since this one resolved, or its clock runs ahead.
var ErrStale = errors.New("account record: relays hold a newer packet")

// Client talks to pkarr relays: PUT/GET of signed packets at
// <relay>/pkarr/<z32>. Relays verify the signature and refuse packets
// older than what they hold; they can withhold, never forge. Every
// relay is contacted in parallel; errors name the relay host only —
// the record address is the derived public key and stays out of logs.
type Client struct {
	urls []*url.URL
	http *http.Client
}

// Relays lists the configured relay hosts, for the status surface.
func (c *Client) Relays() []string {
	out := make([]string, 0, len(c.urls))
	for _, u := range c.urls {
		out = append(out, u.Host)
	}
	return out
}

// NewClient accepts relay base URLs with or without a /pkarr path.
func NewClient(relayURLs []string) (*Client, error) {
	c := &Client{http: &http.Client{
		Timeout: relayTimeout,
		// a relay must not steer a request elsewhere: a redirect would
		// carry the record address and the publisher's IP to its target
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
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
	payload := packet.RelayPayload()
	errs := c.each(func(base *url.URL) error { return c.put(ctx, base, target, payload) })
	var (
		stale int
		last  error
	)
	for _, err := range errs {
		switch {
		case err == nil:
			return nil
		case errors.Is(err, ErrStale):
			stale++
		default:
			last = err
		}
	}
	if stale == len(errs) {
		return ErrStale
	}
	return last
}

// Resolve GETs the record from every relay and returns the newest
// verified packet; nil when no relay holds one.
func (c *Client) Resolve(ctx context.Context, pub key.PublicKey) (*pkarr.SignedPacket, error) {
	target := pub.EndpointID().Z32()
	packets := make([]*pkarr.SignedPacket, len(c.urls))
	errs := c.each(func(base *url.URL) error {
		i := c.index(base)
		p, err := c.get(ctx, base, target, pub)
		if err != nil {
			return err
		}
		packets[i] = p
		return nil
	})
	var (
		newest *pkarr.SignedPacket
		last   error
	)
	for i, p := range packets {
		if errs[i] != nil {
			last = errs[i]
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

// each runs fn against every relay concurrently and returns one error
// slot per relay, in configuration order.
func (c *Client) each(fn func(base *url.URL) error) []error {
	errs := make([]error, len(c.urls))
	var wg sync.WaitGroup
	for i, base := range c.urls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = fn(base)
		}()
	}
	wg.Wait()
	return errs
}

func (c *Client) index(base *url.URL) int {
	for i, u := range c.urls {
		if u == base {
			return i
		}
	}
	return 0
}

func (c *Client) put(ctx context.Context, base *url.URL, target string, payload []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, base.JoinPath(target).String(), bytes.NewReader(payload))
	if err != nil {
		return relayError(base, err)
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := c.http.Do(req)
	if err != nil {
		return relayError(base, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxPayloadSize))
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
		return nil, relayError(base, err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, relayError(base, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxPayloadSize))
		return nil, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxPayloadSize))
		return nil, fmt.Errorf("account record: %s: %s", base.Host, resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxPayloadSize))
	if err != nil {
		return nil, relayError(base, err)
	}
	p, err := pkarr.FromRelayPayload(pub, body)
	if err != nil {
		return nil, fmt.Errorf("account record: %s: %w", base.Host, err)
	}
	return p, nil
}

// relayError strips the request URL (which carries the record address)
// from a transport error, keeping the relay host and the cause.
func relayError(base *url.URL, err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		err = urlErr.Err
	}
	return fmt.Errorf("account record: %s: %w", base.Host, err)
}
