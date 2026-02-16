// Package main demonstrates the basic usage of the any-sync SDK.
//
// This example creates a client, creates a space, creates an object within
// the space, adds content to the object, iterates the object's changes, and
// demonstrates the subscription API.
//
// Usage:
//
//	go run ./example
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	syncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/client"
	"github.com/anyproto/any-sync-sdk/keys"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// 1. Generate keys
	signingKey, _, err := keys.GenerateRandomKey()
	if err != nil {
		return fmt.Errorf("generate signing key: %w", err)
	}
	masterKey, _, err := keys.GenerateRandomKey()
	if err != nil {
		return fmt.Errorf("generate master key: %w", err)
	}

	// 2. Set up storage path
	storagePath := filepath.Join(os.TempDir(), "any-sync-sdk-example")
	if err := os.MkdirAll(storagePath, 0o755); err != nil {
		return fmt.Errorf("create storage dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(storagePath) }()

	// 3. Create config with staging network
	cfg := syncsdk.Config{
		SigningKey: signingKey,
		MasterKey: masterKey,
		Network: syncsdk.NetworkConfig{
			ID:        "staging",
			NetworkID: "N9DU6hLkTAbvcpji3TCKPPd3UQWKGyzUxGmgJEyvhByqAjfD",
			Nodes: []syncsdk.NodeInfo{
				{PeerID: "12D3KooWN6Wwdod3axHpfWq1anBCgYG1sZK42RpgEZEokT4KnMu7", Addresses: []string{"stage1-any-sync-node1.toolpad.org:443"}, Types: []string{"tree"}},
				{PeerID: "12D3KooWJuqyFQ2ZgnYFNhdHAddF7DrTPP2rNueLjw5BWDJa9kqg", Addresses: []string{"stage1-any-sync-node2.toolpad.org:443"}, Types: []string{"tree"}},
				{PeerID: "12D3KooWCub3vY3kWmAQ5qf9TtKBHKY6Yk7F6tGb13M1sXqvcbV4", Addresses: []string{"stage1-any-sync-node3.toolpad.org:443"}, Types: []string{"tree"}},
				{PeerID: "12D3KooWHyWNKYPdYeFK9eQ32UK9uqM1yTdXZT6qTTbtCowCYBAp", Addresses: []string{"stage1-any-sync-coordinator2.toolpad.org:443"}, Types: []string{"coordinator"}},
				{PeerID: "12D3KooWCCe34B5jMauqR8hQzm8XWwndrk8a3exsee93SWUe59TH", Addresses: []string{"stage1-any-sync-consensusnode1.toolpad.org:443"}, Types: []string{"consensus"}},
			},
		},
		StoragePath: storagePath,
	}

	// 4. Create the SDK client
	fmt.Println("Creating client...")
	c, err := client.New(ctx, cfg)
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	defer func() { _ = c.Close(context.Background()) }()
	fmt.Println("Client created.")

	// 5. Subscribe to global events
	unsub := c.Subscribe(func(e syncsdk.Event) {
		fmt.Printf("  [event] type=%d space=%s object=%s\n", e.Type, e.SpaceID, e.ObjectID)
	})
	defer unsub()

	// 6. Create a space
	fmt.Println("Creating space...")
	space, err := c.CreateSpace(ctx)
	if err != nil {
		return fmt.Errorf("create space: %w", err)
	}
	fmt.Printf("Space created: %s\n", space.ID())

	// 7. Create an object
	fmt.Println("Creating object...")
	obj, err := space.CreateObject(ctx)
	if err != nil {
		return fmt.Errorf("create object: %w", err)
	}
	fmt.Printf("Object created: %s\n", obj.ID())

	// 8. Subscribe to object events
	objUnsub := obj.Subscribe(func(e syncsdk.Event) {
		fmt.Printf("  [object event] type=%d heads=%v\n", e.Type, e.Heads)
	})
	defer objUnsub()

	// 9. Add content
	fmt.Println("Adding content...")
	info1, err := obj.AddContent(ctx, []byte("Hello, any-sync!"))
	if err != nil {
		return fmt.Errorf("add content 1: %w", err)
	}
	fmt.Printf("Change added: %s\n", info1.ID)

	info2, err := obj.AddContent(ctx, []byte("Second change"), syncsdk.WithDataType("text"))
	if err != nil {
		return fmt.Errorf("add content 2: %w", err)
	}
	fmt.Printf("Change added: %s (dataType=text)\n", info2.ID)

	// 10. Iterate all changes
	fmt.Println("Iterating changes:")
	err = obj.Iterate(func(change syncsdk.ChangeInfo) bool {
		fmt.Printf("  change=%s data=%q dataType=%s snapshot=%v\n",
			change.ID, string(change.Data), change.DataType, change.IsSnapshot)
		return true
	})
	if err != nil {
		return fmt.Errorf("iterate: %w", err)
	}

	// 11. Show heads
	fmt.Printf("Current heads: %v\n", obj.Heads())

	// 12. List all object IDs in the space
	allIDs, err := space.ListObjectIDs(ctx)
	if err != nil {
		return fmt.Errorf("list object IDs: %w", err)
	}
	fmt.Printf("Objects in space: %d\n", len(allIDs))

	fmt.Println("Done.")
	return nil
}
