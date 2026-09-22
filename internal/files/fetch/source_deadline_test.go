package fetch

import (
	"context"
	"errors"
	"testing"
	"time"
)

// budgetCar declares a read budget and records the one each read got.
type budgetCar struct {
	roundTrip time.Duration
	rate      int64
	lastRange time.Duration
	lastProbe time.Duration
}

func (c *budgetCar) PeerReadBudget() (time.Duration, int64) { return c.roundTrip, c.rate }

// The assertions inspect the deadline the caller set, so the stub
// answers at once: sleeping to "prove" it would only buy timer slop on
// a loaded runner.
func (c *budgetCar) ReadRange(ctx context.Context, _, _ int64) ([]byte, error) {
	c.lastRange = budgetOf(ctx)
	return []byte{1}, nil
}

func (c *budgetCar) ReadProbe(ctx context.Context) ([]byte, int64, error) {
	c.lastProbe = budgetOf(ctx)
	return []byte{1}, 1, nil
}

func budgetOf(ctx context.Context) time.Duration {
	dl, ok := ctx.Deadline()
	if !ok {
		return 0
	}
	return time.Until(dl)
}

// about tolerates the clock ticks between setting a deadline and
// reading it back.
func about(t *testing.T, what string, got, want time.Duration) {
	t.Helper()
	if got > want || got < want-100*time.Millisecond {
		t.Errorf("%s budget = %v, want ~%v", what, got, want)
	}
}

// A declared budget is round trip + length/rate, so a 4 KiB probe and
// a multi-MiB range get budgets sized for what they move.
func TestPeerBudgetScalesWithLength(t *testing.T) {
	car := &budgetCar{roundTrip: 5 * time.Second, rate: 1 << 20}
	fs := &fetchSources{peer: car}

	if _, err := fs.readRange(context.Background(), 0, 4<<20, nil); err != nil {
		t.Fatalf("a read inside the declared budget must succeed: %v", err)
	}
	about(t, "4 MiB range", car.lastRange, 9*time.Second)

	if _, _, err := fs.readProbe(context.Background(), nil); err != nil {
		t.Fatalf("probe: %v", err)
	}
	about(t, "probe", car.lastProbe, 5*time.Second+headProbeLen*time.Second/(1<<20))
}

// A read that exhausts the peer's own budget while HTTP exists bans
// the peer, not just demotes it: selection admitted it and would admit
// it again, so a mere demotion re-pays the stall on every Open.
func TestExpiredPeerReadBansWhenHttpExists(t *testing.T) {
	banned := 0
	car := &stallingCar{}
	fs := &fetchSources{peer: car, http: &plainCar{}, banPeer: func() { banned++ }}
	car.roundTrip = 10 * time.Millisecond

	if _, _, err := fs.readProbe(context.Background(), nil); err != nil {
		t.Fatalf("HTTP must serve the probe: %v", err)
	}
	if banned != 1 {
		t.Errorf("banned %d times, want 1", banned)
	}
	if !fs.peerOff {
		t.Error("the peer must be demoted for the rest of this fetch")
	}
}

// A transport error that did not exhaust the budget only demotes; the
// peer may be back on the next Open.
func TestFailedPeerReadOnlyDemotesWhenHttpExists(t *testing.T) {
	banned := 0
	car := &failingCar{}
	fs := &fetchSources{peer: car, http: &plainCar{}, banPeer: func() { banned++ }}

	if _, err := fs.readRange(context.Background(), 0, 1, nil); err != nil {
		t.Fatalf("HTTP must serve the range: %v", err)
	}
	if banned != 0 {
		t.Errorf("banned %d times, want 0", banned)
	}
	if !fs.peerOff {
		t.Error("the peer must be demoted for the rest of this fetch")
	}
}

// The caller giving up mid-read says nothing about the peer.
func TestCancelledPeerReadDoesNotBan(t *testing.T) {
	banned := 0
	ctx, cancel := context.WithCancel(context.Background())
	car := &stallingCar{cancel: cancel}
	fs := &fetchSources{peer: car, http: &plainCar{}, banPeer: func() { banned++ }}

	if _, err := fs.readRange(ctx, 0, 1, nil); err != nil {
		t.Fatalf("HTTP stub still answers: %v", err)
	}
	if banned != 0 {
		t.Errorf("banned %d times, want 0", banned)
	}
}

// With no HTTP fallback an expired read surfaces and the peer stays:
// it is the only source, and the next block retries it.
func TestExpiredPeerReadWithoutHttpKeepsThePeer(t *testing.T) {
	banned := 0
	car := &stallingCar{roundTrip: 10 * time.Millisecond}
	fs := &fetchSources{peer: car, banPeer: func() { banned++ }}

	if _, err := fs.readRange(context.Background(), 0, 1, nil); err == nil {
		t.Fatal("an expired read with no fallback must surface")
	}
	if banned != 0 || fs.peerOff {
		t.Errorf("banned=%d peerOff=%v, want the only source kept", banned, fs.peerOff)
	}
}

// stallingCar never answers: it waits for its ctx, cancelling the
// caller first when asked to, and fails with err (default: the ctx
// error).
type stallingCar struct {
	roundTrip time.Duration
	cancel    context.CancelFunc
	err       error
}

func (c *stallingCar) PeerReadBudget() (time.Duration, int64) { return c.roundTrip, 0 }

func (c *stallingCar) ReadRange(ctx context.Context, _, _ int64) ([]byte, error) {
	if c.cancel != nil {
		c.cancel()
	}
	<-ctx.Done()
	if c.err != nil {
		return nil, c.err
	}
	return nil, ctx.Err()
}

func (c *stallingCar) ReadProbe(ctx context.Context) ([]byte, int64, error) {
	_, err := c.ReadRange(ctx, 0, 0)
	return nil, 0, err
}

// failingCar errors at once, well inside any budget.
type failingCar struct{}

func (failingCar) ReadRange(context.Context, int64, int64) ([]byte, error) {
	return nil, errors.New("connection reset")
}
func (failingCar) ReadProbe(context.Context) ([]byte, int64, error) {
	return nil, 0, errors.New("connection reset")
}

// A read is never budgeted past MaxPeerReadLen: the server refuses a
// longer read anyway, and seed sizes its index read from the total the
// peer itself reported.
func TestPeerBudgetIsClampedToMaxPeerReadLen(t *testing.T) {
	car := &budgetCar{roundTrip: 5 * time.Second, rate: 1 << 20}
	fs := &fetchSources{peer: car}
	if _, err := fs.readRange(context.Background(), 0, 1<<40, nil); err != nil {
		t.Fatalf("range: %v", err)
	}
	about(t, "1 TiB range", car.lastRange, 5*time.Second+MaxPeerReadLen*time.Second/(1<<20))
}

// A zero round trip keeps the LAN figure but a rate still charges per
// byte: a LAN peer's coalesced multi-MiB range cannot be expected to
// land inside one LAN round trip.
func TestPeerBudgetRateWithoutRoundTrip(t *testing.T) {
	car := &budgetCar{rate: 2 << 20}
	fs := &fetchSources{peer: car}
	if _, err := fs.readRange(context.Background(), 0, 4<<20, nil); err != nil {
		t.Fatalf("range: %v", err)
	}
	about(t, "4 MiB LAN range", car.lastRange, peerPreferDeadline+2*time.Second)
}

// The ban is about the budget being used up, not the error's type: a
// transport error surfacing after the deadline still bans.
func TestExpiredPeerReadBansOnAnyError(t *testing.T) {
	banned := 0
	car := &stallingCar{roundTrip: 10 * time.Millisecond, err: errors.New("stream reset")}
	fs := &fetchSources{peer: car, http: &plainCar{}, banPeer: func() { banned++ }}
	if _, err := fs.readRange(context.Background(), 0, 1, nil); err != nil {
		t.Fatalf("HTTP must serve the range: %v", err)
	}
	if banned != 1 {
		t.Errorf("banned %d times, want 1", banned)
	}
}

// A source that declares nothing keeps the LAN default.
func TestReadsFallBackToTheLanBudget(t *testing.T) {
	car := &plainCar{}
	fs := &fetchSources{peer: car}
	_, _ = fs.readRange(context.Background(), 0, 1, nil)
	about(t, "undeclared range", car.lastRange, peerPreferDeadline)
}

type plainCar struct{ lastRange time.Duration }

func (c *plainCar) ReadRange(ctx context.Context, _, length int64) ([]byte, error) {
	c.lastRange = budgetOf(ctx)
	return make([]byte, length), nil
}
func (c *plainCar) ReadProbe(context.Context) ([]byte, int64, error) { return nil, 0, nil }
