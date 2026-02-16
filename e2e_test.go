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

	// Create a space, then delete it
	space, err := c.CreateSpace(ctx)
	require.NoError(t, err)

	err = c.DeleteSpace(ctx, space.ID())
	require.NoError(t, err)
}

func TestE2EDeleteAccountOnStaging(t *testing.T) {
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

	// Delete account and verify we get a timestamp back
	ts, err := c.DeleteAccount(ctx)
	require.NoError(t, err)
	assert.Greater(t, ts, int64(0), "deletion timestamp should be positive")

	// Revert the deletion
	err = c.RevertAccountDeletion(ctx)
	require.NoError(t, err)
}
