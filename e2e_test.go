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
	// TODO: tree nodes reject space receipt as invalid, so AclClient.AddRecord
	// fails with "log not found". Need to investigate why the SpaceSign receipt
	// is rejected by tree nodes — possibly a space type validation issue.
	t.Skip("skipping: tree nodes reject space receipt, AddRecord fails with 'log not found'")

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
