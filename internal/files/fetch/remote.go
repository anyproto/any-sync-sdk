package fetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// headProbeLen is the first-touch Range: it always covers the CARv2
// pragma + v2 header + CARv1 header (root), and its Content-Range
// reveals the object size for the index tail read.
const headProbeLen = 4096

// promotionRetries × promotionDelay bounds the wait for a just-signed
// object to appear under blob/ (the staging→blob promotion window a
// reader can hit right after upload). Vars so tests shrink the wait.
var (
	promotionRetries = 5
	promotionDelay   = 2 * time.Second
)

// ErrRemoteGone — the object is not (or not yet) readable at its
// public URL.
var ErrRemoteGone = errors.New("filefetch: object not available at public url")

// remoteCar range-reads one CARv2 object over HTTP. The object is
// immutable and content-addressed, so responses need no validators.
type remoteCar struct {
	hc  *http.Client
	url string
}

// readRange fetches [off, off+length). A short read only happens at
// the object end; the caller knows the geometry, so short is an error
// except during the head probe (readProbe).
func (r *remoteCar) readRange(ctx context.Context, off, length int64) ([]byte, error) {
	data, _, err := r.get(ctx, off, length)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != length {
		return nil, fmt.Errorf("filefetch: range [%d,%d): got %d bytes", off, off+length, len(data))
	}
	return data, nil
}

// readProbe fetches the head range and the total object size, retrying
// through the staging→blob promotion window on 404.
func (r *remoteCar) readProbe(ctx context.Context) (head []byte, total int64, err error) {
	for attempt := 0; ; attempt++ {
		head, total, err = r.get(ctx, 0, headProbeLen)
		if err == nil || !errors.Is(err, ErrRemoteGone) || attempt >= promotionRetries {
			return head, total, err
		}
		select {
		case <-ctx.Done():
			return nil, 0, err
		case <-time.After(promotionDelay):
		}
	}
}

// get performs one Range GET. total is parsed from Content-Range when
// the server answers 206 (a 200 means the whole object fit the range).
func (r *remoteCar) get(ctx context.Context, off, length int64) (data []byte, total int64, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.url, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, off+length-1))
	resp, err := r.hc.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusPartialContent:
		// RFC 9110 requires Content-Range on a 206; the object size
		// drives the index-tail read, so a guess is not acceptable —
		// underestimating it makes seed treat a truncated probe as the
		// whole object.
		total = parseContentRangeTotal(resp.Header.Get("Content-Range"))
		if total <= 0 {
			return nil, 0, fmt.Errorf("filefetch: GET %s: 206 without a parseable Content-Range", r.url)
		}
	case http.StatusOK:
		// Whole-object answer (server ignored Range): only acceptable
		// from offset 0; the body is the entire object.
		if off != 0 {
			return nil, 0, fmt.Errorf("filefetch: server ignored range at offset %d", off)
		}
		total = resp.ContentLength
	case http.StatusNotFound, http.StatusForbidden:
		return nil, 0, fmt.Errorf("%w: %s (%d)", ErrRemoteGone, r.url, resp.StatusCode)
	default:
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, 0, fmt.Errorf("filefetch: GET %s: %d %s", r.url, resp.StatusCode, string(msg))
	}
	data, err = io.ReadAll(io.LimitReader(resp.Body, length))
	if err != nil {
		return nil, 0, err
	}
	if total <= 0 {
		total = off + int64(len(data))
	}
	return data, total, nil
}

// parseContentRangeTotal extracts the total size from
// "bytes start-end/total"; 0 when absent or unparseable.
func parseContentRangeTotal(v string) int64 {
	i := strings.LastIndexByte(v, '/')
	if i < 0 {
		return 0
	}
	n, err := strconv.ParseInt(v[i+1:], 10, 64)
	if err != nil {
		return 0
	}
	return n
}
