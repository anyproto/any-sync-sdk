# any-sync-sdk

A minimal Go SDK for the [any-sync](https://github.com/anyproto/any-sync) peer-to-peer synchronization protocol.

Create spaces, push and receive changes through CRDT object trees, store key-value data, and subscribe to real-time sync events — all without dealing with the underlying protocol machinery.

## Installation

```bash
go get github.com/anyproto/any-sync-sdk
```

## Usage

```go
package main

import (
	"context"
	"fmt"

	syncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/client"
	"github.com/anyproto/any-sync-sdk/keys"
)

func main() {
	ctx := context.Background()

	// Generate identity keys
	signingKey, _, _ := keys.GenerateRandomKey()

	// Create the client
	c, _ := client.New(ctx, syncsdk.Config{
		SigningKey: signingKey,
		Network: syncsdk.NetworkConfig{
			ID:        "my-network",
			NetworkID: "...",
			Nodes: []syncsdk.NodeInfo{
				{PeerID: "12D3Koo...", Addresses: []string{"node1.example.com:443"}, Types: []string{"tree"}},
				{PeerID: "12D3Koo...", Addresses: []string{"coord.example.com:443"}, Types: []string{"coordinator"}},
			},
		},
		StoragePath: "/tmp/my-app",
	})
	defer c.Close(ctx)

	// Create a space
	space, _ := c.CreateSpace(ctx)
	fmt.Println("Space:", space.ID())

	// Create an object and add content
	obj, _ := space.CreateObject(ctx, syncsdk.WithChangeType("document"))
	obj.AddContent(ctx, []byte(`{"title": "Hello"}`))
	obj.AddContent(ctx, []byte(`{"title": "World"}`), syncsdk.WithSnapshot())

	// Read back the change history
	obj.Iterate(func(change syncsdk.ChangeInfo) bool {
		fmt.Printf("  %s: %s\n", change.ID, string(change.Data))
		return true
	})

	// Subscribe to remote updates
	unsub := obj.Subscribe(func(e syncsdk.Event) {
		fmt.Println("Remote update:", e.Heads)
	})
	defer unsub()
}
```

A complete runnable example is in [`example/main.go`](example/main.go).

## API overview

### Client

The entry point. Creates, derives, or opens spaces.

```go
c, err := client.New(ctx, cfg)

space, err := c.CreateSpace(ctx)             // New random space
space, err := c.DeriveSpace(ctx, "myapp.v1") // Deterministic from key + type
space, err := c.OpenSpace(ctx, spaceID)      // Existing space (lazy — no I/O until first use)

unsub := c.Subscribe(handler)
c.Close(ctx)
```

> Use `client.New()` from the `client` sub-package, not `syncsdk.New()`.

### Space

A container for objects and key-value data.

```go
obj, err := space.CreateObject(ctx, syncsdk.WithChangeType("doc"))
obj, err := space.GetObject(ctx, objectID)
obj, err := space.DeriveObject(ctx) // Deterministic, idempotent

ids, err := space.ListObjectIDs(ctx)
err := space.DeleteObject(ctx, objectID)

kv := space.KeyValue()
unsub := space.Subscribe(handler)
space.Close(ctx)
```

### Object

A CRDT object tree. Changes are signed, persisted locally, and broadcast to peers.

```go
info, err := obj.AddContent(ctx, data,
    syncsdk.WithDataType("text"),
    syncsdk.WithSnapshot(),
    syncsdk.WithEncryption(),
)

obj.Iterate(func(change syncsdk.ChangeInfo) bool {
    // Walk the DAG in causal order
    return true
})

heads := obj.Heads()
unsub := obj.Subscribe(handler)
obj.Close()
```

### KeyValue

Per-space encrypted key-value storage, synced across peers.

```go
kv := space.KeyValue()

kv.Set(ctx, "theme", []byte("dark"))
val, err := kv.Get(ctx, "theme")

kv.Iterate(ctx, func(key string, value []byte) bool {
    fmt.Printf("%s = %s\n", key, value)
    return true
})
```

### Events

Subscribe at any level (client, space, or object) to receive sync notifications.

```go
unsub := obj.Subscribe(func(e syncsdk.Event) {
    switch e.Type {
    case syncsdk.ObjectUpdated:  // Incremental change from a peer
    case syncsdk.ObjectRebuilt:  // Full rebuild (snapshot received)
    }
})
defer unsub()
```

| Event | Meaning |
|-------|---------|
| `ObjectUpdated` | Remote peer appended changes |
| `ObjectRebuilt` | Full DAG rebuild (e.g. snapshot received) |
| `SpaceConnected` | Space is actively syncing |
| `SpaceDisconnected` | Lost all peers |

## Configuration

```go
syncsdk.Config{
    SigningKey:   privKey,    // Required — Ed25519 key, signs all changes
    MasterKey:   masterKey,  // Optional — defaults to SigningKey
    PeerKey:     peerKey,    // Optional — auto-generated if nil
    Network:     networkCfg, // Node topology (tree nodes, coordinators, consensus)
    StoragePath: "/data/db", // Required — directory for local SQLite databases
}
```

Generate keys with `keys.GenerateRandomKey()` or bring your own `crypto.PrivKey` from any-sync.

Node types in `NetworkConfig.Nodes`:

| Type | Role |
|------|------|
| `tree` | Stores and syncs object trees |
| `coordinator` | Manages space membership and discovery |
| `consensus` | Provides ordering guarantees |
| `file` | File storage |

### Loading network config from YAML

Network topology is commonly distributed as a YAML file. Load it directly:

```go
network, err := syncsdk.NetworkConfigFromFile("network.yml")
network.ID = "production"
```

Or parse from bytes:

```go
network, err := syncsdk.NetworkConfigFromYAML(data)
```

Expected YAML format:

```yaml
networkId: N9DU6hLk...
nodes:
  - peerId: 12D3Koo...
    addresses:
      - node1.example.com:443
    types:
      - tree
```

## Design

**Lazy initialization.** `OpenSpace` returns instantly. The underlying connection and component graph are created on the first operation via `sync.Once`.

**Callback-based events.** Handlers are plain functions (`func(Event)`). No channels, no backpressure risk. Wrap with a channel if you need buffering.

**No exposed internals.** Users never import `any-sync/app` or deal with the component dependency graph. All wiring is behind `client.New()`.

**CRDT conflict resolution.** Object trees are append-only DAGs. Concurrent changes from multiple peers are merged automatically by the any-sync protocol.

## Development

```bash
go build ./...          # Build
go test ./...           # Run all tests (42 tests)
go test -race ./...     # With race detector
go test -short ./...    # Skip E2E tests
go vet ./...            # Static analysis
```

## Limitations

- **No mDNS peer discovery.** Local network peer discovery over mDNS is not yet implemented. You must provide node addresses explicitly in `NetworkConfig.Nodes`.

## License

See the [any-sync](https://github.com/anyproto/any-sync) repository for license details.
