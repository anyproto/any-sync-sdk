package fetch

import (
	"context"
	"testing"
	"time"
)

// slowCar blocks past the LAN budget, then answers.
type slowCar struct {
	delay     time.Duration
	readFor   time.Duration
	probeFor  time.Duration
	lastRange time.Duration
	lastProbe time.Duration
}

func (c *slowCar) PeerReadDeadline() time.Duration  { return c.readFor }
func (c *slowCar) PeerProbeDeadline() time.Duration { return c.probeFor }

func (c *slowCar) ReadRange(ctx context.Context, _, length int64) ([]byte, error) {
	c.lastRange = budgetOf(ctx)
	select {
	case <-time.After(c.delay):
		return make([]byte, length), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *slowCar) ReadProbe(ctx context.Context) ([]byte, int64, error) {
	c.lastProbe = budgetOf(ctx)
	select {
	case <-time.After(c.delay):
		return []byte{1}, 1, nil
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}
}

func budgetOf(ctx context.Context) time.Duration {
	dl, ok := ctx.Deadline()
	if !ok {
		return 0
	}
	return time.Until(dl)
}

// A source that declares budgets must actually get them: without this,
// reverting readRange/readProbe to the LAN constant leaves every test
// green while every relayed read silently returns to 800ms.
func TestReadsHonourADeclaredPeerBudget(t *testing.T) {
	car := &slowCar{delay: peerPreferDeadline * 2, readFor: 6 * time.Second, probeFor: 2 * time.Second}
	fs := &fetchSources{peer: car}

	if _, err := fs.readRange(context.Background(), 0, 8, nil); err != nil {
		t.Fatalf("a read inside the declared budget must succeed: %v", err)
	}
	if car.lastRange < peerPreferDeadline*2 {
		t.Errorf("range budget = %v, want the declared %v", car.lastRange, car.readFor)
	}
	if _, _, err := fs.readProbe(context.Background(), nil); err != nil {
		t.Fatalf("probe: %v", err)
	}
	if car.lastProbe >= car.readFor {
		t.Errorf("probe budget = %v, want the smaller probe one (%v)", car.lastProbe, car.probeFor)
	}
}

// A source that declares nothing keeps the LAN default.
func TestReadsFallBackToTheLanBudget(t *testing.T) {
	car := &plainCar{}
	fs := &fetchSources{peer: car}
	_, _ = fs.readRange(context.Background(), 0, 1, nil)
	if car.lastRange > peerPreferDeadline {
		t.Errorf("range budget = %v, want the LAN default %v", car.lastRange, peerPreferDeadline)
	}
}

type plainCar struct{ lastRange time.Duration }

func (c *plainCar) ReadRange(ctx context.Context, _, length int64) ([]byte, error) {
	c.lastRange = budgetOf(ctx)
	return make([]byte, length), nil
}
func (c *plainCar) ReadProbe(context.Context) ([]byte, int64, error) { return nil, 0, nil }
