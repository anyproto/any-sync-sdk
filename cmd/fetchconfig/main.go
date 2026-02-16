// fetchconfig connects to the coordinator and writes a fresh network config YAML.
//
// Usage: go run ./cmd/fetchconfig -config staging.yml -out staging.yml
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	syncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/client"
	"github.com/anyproto/any-sync-sdk/keys"
)

func main() {
	configPath := flag.String("config", "staging.yml", "path to existing network config YAML (for bootstrap)")
	outPath := flag.String("out", "", "output path (default: stdout)")
	flag.Parse()

	if err := run(*configPath, *outPath); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(configPath, outPath string) error {
	network, err := syncsdk.NetworkConfigFromFile(configPath)
	if err != nil {
		return fmt.Errorf("reading config %s: %w", configPath, err)
	}

	signingKey, _, err := keys.GenerateRandomKey()
	if err != nil {
		return fmt.Errorf("generating key: %w", err)
	}

	dir, err := os.MkdirTemp("", "fetchconfig-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	c, err := client.New(ctx, syncsdk.Config{
		SigningKey:   signingKey,
		Network:     network,
		StoragePath: dir,
	})
	if err != nil {
		return fmt.Errorf("creating client: %w", err)
	}
	defer c.Close(context.Background())

	fresh, err := c.NetworkConfig(ctx)
	if err != nil {
		return fmt.Errorf("fetching network config: %w", err)
	}

	data, err := syncsdk.NetworkConfigToYAML(fresh)
	if err != nil {
		return fmt.Errorf("marshaling YAML: %w", err)
	}

	if outPath == "" {
		_, err = os.Stdout.Write(data)
		return err
	}
	if err := os.WriteFile(outPath, data, 0644); err != nil {
		return fmt.Errorf("writing %s: %w", outPath, err)
	}
	fmt.Fprintf(os.Stderr, "wrote %s (%d nodes, networkId=%s)\n", outPath, len(fresh.Nodes), fresh.NetworkID)
	return nil
}
