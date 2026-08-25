package p2p

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var testThresholds = Thresholds{Stale: time.Hour, Dormant: 7 * 24 * time.Hour, Disable: 30 * 24 * time.Hour}

func TestTierFor(t *testing.T) {
	require.Equal(t, TierActive, TierFor(0, testThresholds))
	require.Equal(t, TierActive, TierFor(59*time.Minute, testThresholds))
	require.Equal(t, TierStale, TierFor(time.Hour, testThresholds))
	require.Equal(t, TierStale, TierFor(6*24*time.Hour, testThresholds))
	require.Equal(t, TierDormant, TierFor(7*24*time.Hour, testThresholds))
	require.Equal(t, TierDormant, TierFor(29*24*time.Hour, testThresholds))
	require.Equal(t, TierDisabled, TierFor(30*24*time.Hour, testThresholds))
	require.Equal(t, TierDisabled, TierFor(400*24*time.Hour, testThresholds))
}

func TestStatusBookSeenClampsAndAdvances(t *testing.T) {
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	b := NewStatusBook("", testThresholds)
	b.now = func() time.Time { return now }

	var advanced []string
	b.SetOnAdvance(func(id string) { advanced = append(advanced, id) })

	require.Equal(t, TierDisabled, b.Tier("p"), "unknown peers are disabled")

	// A publisher clock in the future is clamped to now.
	require.True(t, b.Seen("p", now.Add(time.Hour)))
	require.Equal(t, now, b.LastSeen("p"))
	require.Equal(t, TierActive, b.Tier("p"))
	require.Equal(t, []string{"p"}, advanced)

	// Older evidence never moves LastSeen back.
	require.False(t, b.Seen("p", now.Add(-time.Minute)))
	require.Equal(t, now, b.LastSeen("p"))

	// Failures accumulate and a newer sighting resets them.
	b.Attempt("p", false)
	b.Attempt("p", false)
	rec, _ := b.Get("p")
	require.Equal(t, 2, rec.Failures)
	require.Equal(t, now, rec.LastAttempt)
	now = now.Add(time.Minute)
	require.True(t, b.Seen("p", now))
	rec, _ = b.Get("p")
	require.Equal(t, 0, rec.Failures)

	// A successful attempt is liveness evidence too.
	now = now.Add(time.Minute)
	b.Attempt("p", true)
	require.Equal(t, now, b.LastSeen("p"))

	// Tiers follow the age.
	now = now.Add(2 * time.Hour)
	require.Equal(t, TierStale, b.Tier("p"))
	now = now.Add(8 * 24 * time.Hour)
	require.Equal(t, TierDormant, b.Tier("p"))
	now = now.Add(31 * 24 * time.Hour)
	require.Equal(t, TierDisabled, b.Tier("p"))
	// ... and fresh evidence reactivates.
	require.True(t, b.Seen("p", now))
	require.Equal(t, TierActive, b.Tier("p"))
}

func TestStatusBookPersistRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p2p_peers.json")
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)

	b := NewStatusBook(path, testThresholds)
	b.now = func() time.Time { return now }
	b.Seen("p1", now.Add(-time.Minute))
	b.Attempt("p2", false)
	b.Seen("p3", now)
	b.Forget("p3")
	require.NoError(t, b.Close())

	b2 := NewStatusBook(path, testThresholds)
	b2.now = func() time.Time { return now }
	require.NoError(t, b2.Load())
	rec, ok := b2.Get("p1")
	require.True(t, ok)
	require.True(t, rec.LastSeen.Equal(now.Add(-time.Minute)))
	rec, ok = b2.Get("p2")
	require.True(t, ok)
	require.Equal(t, 1, rec.Failures)
	_, ok = b2.Get("p3")
	require.False(t, ok)

	// A missing file is an empty book, not an error.
	b3 := NewStatusBook(filepath.Join(t.TempDir(), "none.json"), testThresholds)
	require.NoError(t, b3.Load())
	require.Equal(t, TierDisabled, b3.Tier("p1"))
}
