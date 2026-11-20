package fetch

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBaseURLOverrideWins(t *testing.T) {
	st := newStore(t)
	calls := 0
	b := NewBaseURL(st, "net1", "https://pinned.example", func(context.Context) (string, error) {
		calls++
		return "https://fromnode.example", nil
	})
	got, err := b(context.Background())
	require.NoError(t, err)
	require.Equal(t, "https://pinned.example", got)
	require.Zero(t, calls, "override must not touch the network")
}

func TestBaseURLResolveOncePersist(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	calls := 0
	info := func(context.Context) (string, error) {
		calls++
		return "https://cdn.example", nil
	}
	b := NewBaseURL(st, "net1", "", info)
	for i := 0; i < 3; i++ {
		got, err := b(ctx)
		require.NoError(t, err)
		require.Equal(t, "https://cdn.example", got)
	}
	require.Equal(t, 1, calls, "resolved once, then memory-cached")

	// A fresh provider (new process) reads the persisted value with no
	// network at all.
	b2 := NewBaseURL(st, "net1", "", func(context.Context) (string, error) {
		t.Fatal("must not re-resolve a persisted base")
		return "", nil
	})
	got, err := b2(ctx)
	require.NoError(t, err)
	require.Equal(t, "https://cdn.example", got)

	// Another network does not see net1's base.
	other := NewBaseURL(st, "net2", "", func(context.Context) (string, error) { return "", nil })
	got, err = other(ctx)
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestBaseURLErrorNotCached(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	calls := 0
	b := NewBaseURL(st, "net1", "", func(context.Context) (string, error) {
		calls++
		if calls == 1 {
			return "", errors.New("node unreachable")
		}
		return "https://cdn.example", nil
	})
	_, err := b(ctx)
	require.Error(t, err)
	got, err := b(ctx)
	require.NoError(t, err)
	require.Equal(t, "https://cdn.example", got)
}

func TestBaseURLEmptyCachedInMemoryOnly(t *testing.T) {
	ctx := context.Background()
	st := newStore(t)
	calls := 0
	b := NewBaseURL(st, "net1", "", func(context.Context) (string, error) {
		calls++
		return "", nil // network has no public read
	})
	for i := 0; i < 2; i++ {
		got, err := b(ctx)
		require.NoError(t, err)
		require.Empty(t, got)
	}
	require.Equal(t, 1, calls)

	// Not persisted: the next process retries (picks up a network that
	// turned public read on).
	calls2 := 0
	b2 := NewBaseURL(st, "net1", "", func(context.Context) (string, error) {
		calls2++
		return "https://cdn.example", nil
	})
	got, err := b2(ctx)
	require.NoError(t, err)
	require.Equal(t, "https://cdn.example", got)
	require.Equal(t, 1, calls2)
}
