package broker

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/anyproto/any-sync/commonfile/fileproto/fileprotov2"
	"github.com/stretchr/testify/require"
)

// slowReader hands out one byte per step: slow, never stalled.
type slowReader struct {
	r    io.Reader
	step time.Duration
}

func (s *slowReader) Read(b []byte) (int, error) {
	time.Sleep(s.step)
	return s.r.Read(b[:1])
}

func TestPutAbortsStalledUpload(t *testing.T) {
	old := putStallTimeout
	putStallTimeout = 200 * time.Millisecond
	t.Cleanup(func() { putStallTimeout = old })

	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		<-release // the object store never answers
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })

	c := &Client{hc: http.DefaultClient}
	body := []byte("payload")
	start := time.Now()
	err := c.Put(context.Background(), &fileprotov2.PresignedUpload{Url: srv.URL}, bytes.NewReader(body), int64(len(body)))
	require.ErrorIs(t, err, ErrPutStalled)
	require.Less(t, time.Since(start), 5*time.Second)
}

func TestPutSlowUploadIsNotStalled(t *testing.T) {
	old := putStallTimeout
	putStallTimeout = 200 * time.Millisecond
	t.Cleanup(func() { putStallTimeout = old })

	var got []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = io.ReadAll(r.Body)
	}))
	t.Cleanup(srv.Close)

	c := &Client{hc: http.DefaultClient}
	body := bytes.Repeat([]byte("x"), 20)
	// 20 steps of 50ms: five times the stall window end to end.
	err := c.Put(context.Background(), &fileprotov2.PresignedUpload{Url: srv.URL},
		&slowReader{r: bytes.NewReader(body), step: 50 * time.Millisecond}, int64(len(body)))
	require.NoError(t, err)
	require.Equal(t, body, got)
}
