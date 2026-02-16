package syncsdk_test

import (
	"context"
	"testing"
	"time"

	syncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/client"
	"github.com/anyproto/any-sync-sdk/keys"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testConfig builds a Config suitable for local-only integration tests.
// It uses real staging peer IDs mapped to localhost so chash validation
// passes without needing network connectivity.
func testConfig(t *testing.T) syncsdk.Config {
	t.Helper()
	signingKey, _, err := keys.GenerateRandomKey()
	require.NoError(t, err)
	masterKey, _, err := keys.GenerateRandomKey()
	require.NoError(t, err)

	return syncsdk.Config{
		SigningKey: signingKey,
		MasterKey: masterKey,
		Network: syncsdk.NetworkConfig{
			ID:        "test",
			NetworkID: "N9DU6hLkTAbvcpji3TCKPPd3UQWKGyzUxGmgJEyvhByqAjfD",
			Nodes: []syncsdk.NodeInfo{
				// 3 tree nodes (minimum for ReplicationFactor=3)
				{PeerID: "12D3KooWN6Wwdod3axHpfWq1anBCgYG1sZK42RpgEZEokT4KnMu7", Addresses: []string{"127.0.0.1:14830"}, Types: []string{"tree"}},
				{PeerID: "12D3KooWJuqyFQ2ZgnYFNhdHAddF7DrTPP2rNueLjw5BWDJa9kqg", Addresses: []string{"127.0.0.1:14831"}, Types: []string{"tree"}},
				{PeerID: "12D3KooWCub3vY3kWmAQ5qf9TtKBHKY6Yk7F6tGb13M1sXqvcbV4", Addresses: []string{"127.0.0.1:14832"}, Types: []string{"tree"}},
				// coordinator
				{PeerID: "12D3KooWHyWNKYPdYeFK9eQ32UK9uqM1yTdXZT6qTTbtCowCYBAp", Addresses: []string{"127.0.0.1:14833"}, Types: []string{"coordinator"}},
				// consensus
				{PeerID: "12D3KooWCCe34B5jMauqR8hQzm8XWwndrk8a3exsee93SWUe59TH", Addresses: []string{"127.0.0.1:14834"}, Types: []string{"consensus"}},
			},
		},
		StoragePath: t.TempDir(),
	}
}

// newTestClient is a helper that creates a client and registers cleanup.
func newTestClient(t *testing.T) syncsdk.Client {
	t.Helper()
	cfg := testConfig(t)
	return newTestClientWithConfig(t, cfg)
}

func newTestClientWithConfig(t *testing.T, cfg syncsdk.Config) syncsdk.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := client.New(ctx, cfg)
	require.NoError(t, err, "client.New should not fail")
	require.NotNil(t, c, "client should not be nil")
	t.Cleanup(func() {
		_ = c.Close(context.Background())
	})
	return c
}

func TestClientNew_InvalidConfig_MissingSigningKey(t *testing.T) {
	ctx := context.Background()
	cfg := syncsdk.Config{
		StoragePath: t.TempDir(),
	}
	c, err := client.New(ctx, cfg)
	assert.Nil(t, c)
	assert.ErrorIs(t, err, syncsdk.ErrInvalidConfig)
}

func TestClientNew_InvalidConfig_MissingStoragePath(t *testing.T) {
	ctx := context.Background()
	signingKey, _, err := keys.GenerateRandomKey()
	require.NoError(t, err)
	cfg := syncsdk.Config{
		SigningKey: signingKey,
	}
	c, err := client.New(ctx, cfg)
	assert.Nil(t, c)
	assert.ErrorIs(t, err, syncsdk.ErrInvalidConfig)
}

func TestClientCreateAndClose(t *testing.T) {
	c := newTestClient(t)
	assert.NotNil(t, c)
	// Close is handled by t.Cleanup
}

func TestDeriveSpace(t *testing.T) {
	c := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	space, err := c.DeriveSpace(ctx, "test.derive")
	require.NoError(t, err)
	require.NotNil(t, space)
	assert.NotEmpty(t, space.ID(), "derived space should have a non-empty ID")
}

func TestDeriveSpace_Idempotent(t *testing.T) {
	c := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sp1, err := c.DeriveSpace(ctx, "idempotent.type")
	require.NoError(t, err)

	sp2, err := c.DeriveSpace(ctx, "idempotent.type")
	require.NoError(t, err)

	assert.Equal(t, sp1.ID(), sp2.ID(), "deriving with the same type should give the same space ID")
}

func TestCreateSpace(t *testing.T) {
	c := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	space, err := c.CreateSpace(ctx)
	require.NoError(t, err)
	require.NotNil(t, space)
	assert.NotEmpty(t, space.ID(), "created space should have a non-empty ID")
}

func TestCreateObject(t *testing.T) {
	c := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	space, err := c.CreateSpace(ctx)
	require.NoError(t, err)

	obj, err := space.CreateObject(ctx)
	require.NoError(t, err)
	require.NotNil(t, obj)
	assert.NotEmpty(t, obj.ID(), "object ID should be non-empty")
	assert.Equal(t, space.ID(), obj.SpaceID(), "object's spaceID should match")
}

func TestObjectAddContentAndIterate(t *testing.T) {
	c := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	space, err := c.CreateSpace(ctx)
	require.NoError(t, err)

	obj, err := space.CreateObject(ctx)
	require.NoError(t, err)

	// Add multiple non-snapshot changes
	data1 := []byte("change-1")
	data2 := []byte("change-2")
	data3 := []byte("change-3")

	info1, err := obj.AddContent(ctx, data1)
	require.NoError(t, err)
	assert.NotEmpty(t, info1.ID)

	info2, err := obj.AddContent(ctx, data2, syncsdk.WithDataType("text"))
	require.NoError(t, err)
	assert.NotEmpty(t, info2.ID)

	info3, err := obj.AddContent(ctx, data3)
	require.NoError(t, err)
	assert.NotEmpty(t, info3.ID)

	// Verify heads updated
	heads := obj.Heads()
	assert.NotEmpty(t, heads)

	// Iterate and collect all changes
	var changes []syncsdk.ChangeInfo
	err = obj.Iterate(func(change syncsdk.ChangeInfo) bool {
		changes = append(changes, change)
		return true
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(changes), 3, "should see at least 3 changes")

	// Verify data is present in the changes
	var foundData []string
	for _, c := range changes {
		if len(c.Data) > 0 {
			foundData = append(foundData, string(c.Data))
		}
	}
	assert.Contains(t, foundData, "change-1")
	assert.Contains(t, foundData, "change-2")
	assert.Contains(t, foundData, "change-3")
}

func TestObjectSubscribe(t *testing.T) {
	c := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	space, err := c.CreateSpace(ctx)
	require.NoError(t, err)

	obj, err := space.CreateObject(ctx)
	require.NoError(t, err)

	// Subscribe should return a non-nil unsubscribe function
	var called bool
	unsub := obj.Subscribe(func(e syncsdk.Event) {
		called = true
	})
	assert.NotNil(t, unsub, "unsubscribe function should not be nil")

	// Unsubscribe should not panic
	unsub()

	// After unsubscribe, adding content should not call handler
	called = false
	_, _ = obj.AddContent(ctx, []byte("after-unsub"))
	assert.False(t, called, "handler should not be called after unsubscribe")
}

func TestSpaceSubscribe(t *testing.T) {
	c := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	space, err := c.CreateSpace(ctx)
	require.NoError(t, err)

	unsub := space.Subscribe(func(e syncsdk.Event) {})
	assert.NotNil(t, unsub)
	unsub() // should not panic
}

func TestClientSubscribe(t *testing.T) {
	c := newTestClient(t)

	unsub := c.Subscribe(func(e syncsdk.Event) {})
	assert.NotNil(t, unsub)
	unsub() // should not panic
}

func TestMultipleObjects(t *testing.T) {
	c := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	space, err := c.CreateSpace(ctx)
	require.NoError(t, err)

	const numObjects = 5
	createdIDs := make([]string, 0, numObjects)
	for i := 0; i < numObjects; i++ {
		obj, err := space.CreateObject(ctx)
		require.NoError(t, err)
		createdIDs = append(createdIDs, obj.ID())
	}

	// All IDs should be unique
	idSet := make(map[string]bool)
	for _, id := range createdIDs {
		assert.False(t, idSet[id], "duplicate object ID: %s", id)
		idSet[id] = true
	}
	assert.Len(t, idSet, numObjects, "all objects should have unique IDs")

	// Verify each object can be retrieved
	for _, id := range createdIDs {
		obj, err := space.GetObject(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, id, obj.ID())
	}
}

func TestSpaceOpenExisting(t *testing.T) {
	cfg := testConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a client, create a space and an object
	c1, err := client.New(ctx, cfg)
	require.NoError(t, err)

	space, err := c1.CreateSpace(ctx)
	require.NoError(t, err)
	spaceID := space.ID()

	obj, err := space.CreateObject(ctx)
	require.NoError(t, err)
	objID := obj.ID()

	_, err = obj.AddContent(ctx, []byte("persistent-data"))
	require.NoError(t, err)

	// Close the first client
	err = c1.Close(ctx)
	require.NoError(t, err)

	// Create a new client with the same config (same storage, same keys)
	c2, err := client.New(ctx, cfg)
	require.NoError(t, err)
	defer func() { _ = c2.Close(context.Background()) }()

	// Re-open the space
	space2, err := c2.OpenSpace(ctx, spaceID)
	require.NoError(t, err)
	assert.Equal(t, spaceID, space2.ID())

	// Re-open the object
	obj2, err := space2.GetObject(ctx, objID)
	require.NoError(t, err)
	assert.Equal(t, objID, obj2.ID())

	// Verify the content is still there via iteration
	var found bool
	err = obj2.Iterate(func(change syncsdk.ChangeInfo) bool {
		if string(change.Data) == "persistent-data" {
			found = true
			return false
		}
		return true
	})
	require.NoError(t, err)
	assert.True(t, found, "should find previously stored content")
}

func TestKeyValue(t *testing.T) {
	c := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	space, err := c.DeriveSpace(ctx, "kv.test")
	require.NoError(t, err)

	// Trigger lazy space initialization by creating an object
	_, err = space.CreateObject(ctx)
	require.NoError(t, err)

	kv := space.KeyValue()
	if kv == nil {
		t.Skip("KeyValue not available (space may not support it)")
	}

	// Set a value
	err = kv.Set(ctx, "greeting", []byte("hello"))
	require.NoError(t, err)

	// Get it back
	val, err := kv.Get(ctx, "greeting")
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), val)

	// Set another value
	err = kv.Set(ctx, "farewell", []byte("goodbye"))
	require.NoError(t, err)

	// Iterate and verify both keys
	collected := make(map[string]string)
	err = kv.Iterate(ctx, func(key string, value []byte) bool {
		collected[key] = string(value)
		return true
	})
	require.NoError(t, err)
	assert.Contains(t, collected, "greeting")
	assert.Contains(t, collected, "farewell")
	assert.Equal(t, "hello", collected["greeting"])
	assert.Equal(t, "goodbye", collected["farewell"])
}

func TestCreateObjectWithChangeType(t *testing.T) {
	c := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	space, err := c.CreateSpace(ctx)
	require.NoError(t, err)

	obj, err := space.CreateObject(ctx, syncsdk.WithChangeType("document"))
	require.NoError(t, err)
	assert.NotEmpty(t, obj.ID())
}

func TestAddContentWithOptions(t *testing.T) {
	c := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	space, err := c.CreateSpace(ctx)
	require.NoError(t, err)

	obj, err := space.CreateObject(ctx)
	require.NoError(t, err)

	// Add with snapshot flag
	info, err := obj.AddContent(ctx, []byte("snapshot-data"), syncsdk.WithSnapshot(), syncsdk.WithDataType("snap"))
	require.NoError(t, err)
	assert.NotEmpty(t, info.ID)
	assert.True(t, info.IsSnapshot)
	assert.Equal(t, "snap", info.DataType)
}

func TestClientClose_Idempotent(t *testing.T) {
	cfg := testConfig(t)
	ctx := context.Background()

	c, err := client.New(ctx, cfg)
	require.NoError(t, err)

	err = c.Close(ctx)
	require.NoError(t, err)

	// Second close should not error
	err = c.Close(ctx)
	assert.NoError(t, err)
}

func TestClientClose_OperationsAfterClose(t *testing.T) {
	cfg := testConfig(t)
	ctx := context.Background()

	c, err := client.New(ctx, cfg)
	require.NoError(t, err)

	err = c.Close(ctx)
	require.NoError(t, err)

	// Operations after close should fail
	_, err = c.CreateSpace(ctx)
	assert.ErrorIs(t, err, syncsdk.ErrClientClosed)

	_, err = c.DeriveSpace(ctx, "after-close")
	assert.ErrorIs(t, err, syncsdk.ErrClientClosed)

	_, err = c.OpenSpace(ctx, "nonexistent")
	assert.ErrorIs(t, err, syncsdk.ErrClientClosed)
}

func TestConcurrentGetObject(t *testing.T) {
	c := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	space, err := c.CreateSpace(ctx)
	require.NoError(t, err)

	// Create an object first
	obj, err := space.CreateObject(ctx)
	require.NoError(t, err)
	objectID := obj.ID()

	// Concurrently get the same object from multiple goroutines
	const goroutines = 10
	results := make(chan syncsdk.Object, goroutines)
	errs := make(chan error, goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			got, err := space.GetObject(ctx, objectID)
			errs <- err
			results <- got
		}()
	}

	for i := 0; i < goroutines; i++ {
		err := <-errs
		assert.NoError(t, err)
	}

	// All goroutines should get the same object
	var first syncsdk.Object
	for i := 0; i < goroutines; i++ {
		obj := <-results
		if obj == nil {
			continue
		}
		if first == nil {
			first = obj
		}
		assert.Equal(t, first.ID(), obj.ID())
	}
}

func TestDeriveObject(t *testing.T) {
	c := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	space, err := c.DeriveSpace(ctx, "derive.obj.test")
	require.NoError(t, err)

	// Derive an object
	obj, err := space.DeriveObject(ctx)
	require.NoError(t, err)
	require.NotNil(t, obj)
	assert.NotEmpty(t, obj.ID())
	assert.Equal(t, space.ID(), obj.SpaceID())

	// Derive again - should return the same object (idempotent from cache)
	obj2, err := space.DeriveObject(ctx)
	require.NoError(t, err)
	assert.Equal(t, obj.ID(), obj2.ID())
}

func TestDeleteObject(t *testing.T) {
	c := newTestClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	space, err := c.CreateSpace(ctx)
	require.NoError(t, err)

	obj, err := space.CreateObject(ctx)
	require.NoError(t, err)

	err = space.DeleteObject(ctx, obj.ID())
	assert.NoError(t, err)
}
