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

func loadStagingConfig(t *testing.T) syncsdk.NetworkConfig {
	t.Helper()
	cfg, err := syncsdk.NetworkConfigFromFile("staging.yml")
	require.NoError(t, err)
	cfg.ID = "staging"
	return cfg
}

func TestE2ECreateSpaceOnStaging(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	network := loadStagingConfig(t)

	signingKey, _, err := keys.GenerateRandomKey()
	require.NoError(t, err)
	masterKey, _, err := keys.GenerateRandomKey()
	require.NoError(t, err)

	cfg := syncsdk.Config{
		SigningKey:  signingKey,
		MasterKey:  masterKey,
		Network:    network,
		StoragePath: t.TempDir(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	c, err := client.New(ctx, cfg)
	require.NoError(t, err)
	defer func() { _ = c.Close(context.Background()) }()

	// Create a space on the staging network
	space, err := c.CreateSpace(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, space.ID())

	// Create an object in the space
	obj, err := space.CreateObject(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, obj.ID())

	// Add content
	info, err := obj.AddContent(ctx, []byte("e2e-test-data"))
	require.NoError(t, err)
	assert.NotEmpty(t, info.ID)

	// Verify content via iteration
	var found bool
	err = obj.Iterate(func(change syncsdk.ChangeInfo) bool {
		if string(change.Data) == "e2e-test-data" {
			found = true
			return false
		}
		return true
	})
	require.NoError(t, err)
	assert.True(t, found, "should find the content we just added")
}

func TestE2EDeriveSpaceOnStaging(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	network := loadStagingConfig(t)

	signingKey, _, err := keys.GenerateRandomKey()
	require.NoError(t, err)
	masterKey, _, err := keys.GenerateRandomKey()
	require.NoError(t, err)

	cfg := syncsdk.Config{
		SigningKey:  signingKey,
		MasterKey:  masterKey,
		Network:    network,
		StoragePath: t.TempDir(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	c, err := client.New(ctx, cfg)
	require.NoError(t, err)
	defer func() { _ = c.Close(context.Background()) }()

	// Derive a space
	space, err := c.DeriveSpace(ctx, "e2e.derive.test")
	require.NoError(t, err)
	assert.NotEmpty(t, space.ID())

	// Derive again - should be idempotent
	space2, err := c.DeriveSpace(ctx, "e2e.derive.test")
	require.NoError(t, err)
	assert.Equal(t, space.ID(), space2.ID())
}

func TestE2EDeleteSpaceOnStaging(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	network := loadStagingConfig(t)

	signingKey, _, err := keys.GenerateRandomKey()
	require.NoError(t, err)
	masterKey, _, err := keys.GenerateRandomKey()
	require.NoError(t, err)

	cfg := syncsdk.Config{
		SigningKey:   signingKey,
		MasterKey:   masterKey,
		Network:     network,
		StoragePath: t.TempDir(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	c, err := client.New(ctx, cfg)
	require.NoError(t, err)
	defer func() { _ = c.Close(context.Background()) }()

	// Create a space, push to coordinator, then delete it
	space, err := c.CreateSpace(ctx)
	require.NoError(t, err)

	err = space.Push(ctx)
	require.NoError(t, err)

	err = c.DeleteSpace(ctx, space.ID())
	require.NoError(t, err)
}

func TestE2EGenerateInviteOnStaging(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	network := loadStagingConfig(t)

	signingKey, _, err := keys.GenerateRandomKey()
	require.NoError(t, err)
	masterKey, _, err := keys.GenerateRandomKey()
	require.NoError(t, err)

	cfg := syncsdk.Config{
		SigningKey:   signingKey,
		MasterKey:   masterKey,
		Network:     network,
		StoragePath: t.TempDir(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	c, err := client.New(ctx, cfg)
	require.NoError(t, err)
	defer func() { _ = c.Close(context.Background()) }()

	space, err := c.CreateSpace(ctx)
	require.NoError(t, err)

	// Register the space with the coordinator before sharing
	err = space.Push(ctx)
	require.NoError(t, err)

	// Generate an invite and verify it round-trips
	invite, err := space.GenerateInvite(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, invite)

	spaceID, err := syncsdk.ParseInvite(invite)
	require.NoError(t, err)
	assert.Equal(t, space.ID(), spaceID)

	// Verify members shows the owner
	members, err := space.Members(ctx)
	require.NoError(t, err)
	require.Len(t, members, 1)
	assert.Equal(t, syncsdk.PermissionOwner, members[0].Permissions)
	assert.Equal(t, syncsdk.MemberStatusActive, members[0].Status)
}

func TestE2ENetworkConfigOnStaging(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	network := loadStagingConfig(t)

	signingKey, _, err := keys.GenerateRandomKey()
	require.NoError(t, err)

	cfg := syncsdk.Config{
		SigningKey:   signingKey,
		Network:     network,
		StoragePath: t.TempDir(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	c, err := client.New(ctx, cfg)
	require.NoError(t, err)
	defer func() { _ = c.Close(context.Background()) }()

	netCfg, err := c.NetworkConfig(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, netCfg.NetworkID)
	assert.NotEmpty(t, netCfg.Nodes)

	// Verify we got different node types
	typeSet := make(map[string]bool)
	for _, node := range netCfg.Nodes {
		assert.NotEmpty(t, node.PeerID)
		assert.NotEmpty(t, node.Addresses)
		for _, typ := range node.Types {
			typeSet[typ] = true
		}
	}
	assert.True(t, typeSet["tree"], "should have tree nodes")
	assert.True(t, typeSet["coordinator"], "should have coordinator nodes")

	// Verify it serializes to YAML
	data, err := syncsdk.NetworkConfigToYAML(netCfg)
	require.NoError(t, err)
	assert.Contains(t, string(data), "networkId:")
	t.Logf("Fresh network config:\n%s", data)
}

func newE2EClient(t *testing.T, ctx context.Context, network syncsdk.NetworkConfig) syncsdk.Client {
	t.Helper()
	signingKey, _, err := keys.GenerateRandomKey()
	require.NoError(t, err)
	masterKey, _, err := keys.GenerateRandomKey()
	require.NoError(t, err)
	c, err := client.New(ctx, syncsdk.Config{
		SigningKey:   signingKey,
		MasterKey:   masterKey,
		Network:     network,
		StoragePath: t.TempDir(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

func TestE2EMultiClientSync(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	network := loadStagingConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Client A: create space, object, and content
	clientA := newE2EClient(t, ctx, network)
	spaceA, err := clientA.CreateSpace(ctx)
	require.NoError(t, err)

	objA, err := spaceA.CreateObject(ctx)
	require.NoError(t, err)
	_, err = objA.AddContent(ctx, []byte("from A"))
	require.NoError(t, err)

	// Push space and generate invite
	err = spaceA.Push(ctx)
	require.NoError(t, err)
	invite, err := spaceA.GenerateInvite(ctx)
	require.NoError(t, err)

	// Client B: join space via invite
	clientB := newE2EClient(t, ctx, network)
	spaceB, err := clientB.JoinSpace(ctx, invite)
	require.NoError(t, err)
	assert.Equal(t, spaceA.ID(), spaceB.ID())

	// Client B: wait for object to appear via sync
	var objectIDs []string
	require.Eventually(t, func() bool {
		objectIDs, err = spaceB.ListObjectIDs(ctx)
		if err != nil {
			t.Logf("ListObjectIDs error: %v", err)
			return false
		}
		t.Logf("ListObjectIDs returned %d ids: %v (looking for %s)", len(objectIDs), objectIDs, objA.ID())
		for _, id := range objectIDs {
			if id == objA.ID() {
				return true
			}
		}
		return false
	}, 60*time.Second, 2*time.Second, "client B should see client A's object")

	// Client B: verify content from A
	objB, err := spaceB.GetObject(ctx, objA.ID())
	require.NoError(t, err)
	var foundA bool
	require.Eventually(t, func() bool {
		foundA = false
		_ = objB.Iterate(func(ci syncsdk.ChangeInfo) bool {
			if string(ci.Data) == "from A" {
				foundA = true
				return false
			}
			return true
		})
		return foundA
	}, 60*time.Second, 2*time.Second, "client B should see 'from A' content")

	// Client B: add content
	_, err = objB.AddContent(ctx, []byte("from B"))
	require.NoError(t, err)

	// Client A: wait for content from B
	require.Eventually(t, func() bool {
		var foundB bool
		_ = objA.Iterate(func(ci syncsdk.ChangeInfo) bool {
			if string(ci.Data) == "from B" {
				foundB = true
				return false
			}
			return true
		})
		return foundB
	}, 60*time.Second, 2*time.Second, "client A should see 'from B' content")
}

func TestE2EDelayedSync(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	network := loadStagingConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 210*time.Second)
	defer cancel()

	// Set up two clients sharing a space with one object.
	clientA := newE2EClient(t, ctx, network)
	spaceA, err := clientA.CreateSpace(ctx)
	require.NoError(t, err)

	objA, err := spaceA.CreateObject(ctx)
	require.NoError(t, err)
	_, err = objA.AddContent(ctx, []byte("initial"))
	require.NoError(t, err)

	err = spaceA.Push(ctx)
	require.NoError(t, err)
	invite, err := spaceA.GenerateInvite(ctx)
	require.NoError(t, err)

	clientB := newE2EClient(t, ctx, network)
	spaceB, err := clientB.JoinSpace(ctx, invite)
	require.NoError(t, err)

	// Wait for B to see the object and initial content.
	require.Eventually(t, func() bool {
		ids, _ := spaceB.ListObjectIDs(ctx)
		for _, id := range ids {
			if id == objA.ID() {
				return true
			}
		}
		return false
	}, 60*time.Second, 2*time.Second)

	objB, err := spaceB.GetObject(ctx, objA.ID())
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		var found bool
		_ = objB.Iterate(func(ci syncsdk.ChangeInfo) bool {
			if string(ci.Data) == "initial" {
				found = true
				return false
			}
			return true
		})
		return found
	}, 60*time.Second, 2*time.Second)
	t.Log("Both clients synced. Waiting 70 seconds before adding new content...")

	// Wait 70 seconds — well past the SyncPeriod window — to see if
	// connections are still alive.
	time.Sleep(70 * time.Second)

	// Client A adds new content after the delay.
	t.Log("Adding 'after delay' content from client A...")
	_, err = objA.AddContent(ctx, []byte("after delay"))
	require.NoError(t, err)

	// Measure how long it takes for Client B to receive it.
	start := time.Now()
	require.Eventually(t, func() bool {
		var found bool
		_ = objB.Iterate(func(ci syncsdk.ChangeInfo) bool {
			if string(ci.Data) == "after delay" {
				found = true
				return false
			}
			return true
		})
		if found {
			t.Logf("Client B received 'after delay' in %s", time.Since(start).Round(time.Millisecond))
		}
		return found
	}, 60*time.Second, 500*time.Millisecond, "client B should see 'after delay' content")
}

func TestE2EDeleteAccountOnStaging(t *testing.T) {
	// TODO: staging coordinator returns "account is deleted" for fresh accounts.
	// Need to investigate how the coordinator registers accounts before deletion
	// is allowed. Possibly requires a specific account registration RPC first.
	t.Skip("skipping: coordinator returns 'account is deleted' for unregistered accounts")

	if testing.Short() {
		t.Skip("skipping E2E test in short mode")
	}

	network := loadStagingConfig(t)

	signingKey, _, err := keys.GenerateRandomKey()
	require.NoError(t, err)
	masterKey, _, err := keys.GenerateRandomKey()
	require.NoError(t, err)

	cfg := syncsdk.Config{
		SigningKey:   signingKey,
		MasterKey:   masterKey,
		Network:     network,
		StoragePath: t.TempDir(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	c, err := client.New(ctx, cfg)
	require.NoError(t, err)

	// Delete account and verify we get a timestamp back.
	// DeleteAccount closes all connections, so the client is unusable after.
	ts, err := c.DeleteAccount(ctx)
	require.NoError(t, err)
	assert.Greater(t, ts, int64(0), "deletion timestamp should be positive")

	// Revert requires a fresh client (same identity, fresh connection).
	cfg.StoragePath = t.TempDir()
	c2, err := client.New(ctx, cfg)
	require.NoError(t, err)
	defer func() { _ = c2.Close(context.Background()) }()

	err = c2.RevertAccountDeletion(ctx)
	require.NoError(t, err)
}
