package anysyncx

import (
	"testing"
	"time"
)

func TestNextSubscribeDelay_RampTakesPrecedenceOverHealth(t *testing.T) {
	ramp := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}
	// During the ramp the schedule is followed regardless of health, so
	// cold-start convergence is not slowed by a (briefly) absent stream.
	for i, want := range ramp {
		if got := nextSubscribeDelay(i, ramp, false); got != want {
			t.Fatalf("ramp attempt %d unhealthy: got %v, want %v", i, got, want)
		}
		if got := nextSubscribeDelay(i, ramp, true); got != want {
			t.Fatalf("ramp attempt %d healthy: got %v, want %v", i, got, want)
		}
	}
}

func TestNextSubscribeDelay_SteadyStateAdaptsToHealth(t *testing.T) {
	ramp := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}
	// Past the ramp: slow refresh when a node stream is live, fast churn
	// cadence when it is down so a reopen+resubscribe follows promptly.
	if got := nextSubscribeDelay(len(ramp), ramp, true); got != subscribeRefresh {
		t.Fatalf("healthy steady state: got %v, want %v", got, subscribeRefresh)
	}
	if got := nextSubscribeDelay(len(ramp)+5, ramp, false); got != subscribeChurn {
		t.Fatalf("churn steady state: got %v, want %v", got, subscribeChurn)
	}
	// Sanity: churn must be meaningfully faster than the refresh, else
	// the fast path buys nothing over the old flat tick.
	if subscribeChurn >= subscribeRefresh {
		t.Fatalf("subscribeChurn (%v) must be < subscribeRefresh (%v)", subscribeChurn, subscribeRefresh)
	}
}
