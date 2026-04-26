// Example: walks through the full SDK surface — Open, create a space,
// define a user type with properties, create an object of that type,
// write property values at multiple scopes (base, account, device),
// write user data to a type-owned dataset, query, subscribe.
//
// Compiles against the current public interfaces. The internal layers
// are stubbed, so running this will currently return nil handles and
// panic on the first method call — its purpose today is to pressure-test
// the API shape from a caller's perspective.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/anyproto/any-sync/app/logger"

	anysyncsdk "github.com/anyproto/any-sync-sdk"
	"github.com/anyproto/any-sync-sdk/auth"
	"github.com/anyproto/any-sync-sdk/config"
	"github.com/anyproto/any-sync-sdk/space"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	provider, err := makeAuth(ctx)
	if err != nil {
		return fmt.Errorf("auth: %w", err)
	}

	cfg := config.Config{
		Storage: config.Storage{
			DataDir:  filepath.Join(os.TempDir(), "anysync-demo"),
			Topology: config.StorageShared,
		},
		Network: config.Network{
			NodeConfYAML: mustRead("network.yaml"),
		},
		Log: logger.Config{DefaultLevel: "info"},
	}

	sdk, err := anysyncsdk.Open(ctx, cfg, provider)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer sdk.Close()

	log.Printf("sdk up; account %s", sdk.Account().Id())

	sp, err := findOrCreateSpace(ctx, sdk.Spaces(), "My Notes")
	if err != nil {
		return err
	}
	log.Printf("space %s", sp.Id())

	typeId, titleProp, tagsProp, err := defineNotebookType(ctx, sp.Types())
	if err != nil {
		return fmt.Errorf("define type: %w", err)
	}
	_ = titleProp
	_ = tagsProp

	// Create an instance with the notebook type attached at birth.
	// InitialProperties seeds base-scope values in one step (saves a
	// separate SetBase roundtrip).
	objectId, err := sp.Objects().Create(ctx, space.CreateObjectOpts{
		Types: []string{typeId},
		InitialProperties: map[string]map[string]any{
			typeId: {
				"title": "Research",
				"tags":  []string{"personal"},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("create object: %w", err)
	}
	log.Printf("object %s", objectId)

	// Subscribe before further writes — catches our own writes and
	// any inbound sync events through the same channel.
	sub, err := sp.Subscribe(ctx,
		[]space.SubscribeTarget{{ObjectId: objectId}}, // all datasets on this object
		space.SubscribeOpts{IncludeOwnWrites: true},
	)
	if err != nil {
		return err
	}
	go pumpEvents(sub)

	// Account-scope override: rename the notebook just for this user
	// across their devices. Routes through the tech space under the
	// hood.
	if _, err := sp.Properties().SetAccount(ctx, objectId, typeId, map[string]any{
		"title": "My Research (account)",
	}); err != nil {
		return fmt.Errorf("set account: %w", err)
	}

	// Device-scope override: a local-only preference that does not
	// sync. No VersionId — device writes are a local DB operation.
	if err := sp.Properties().SetDevice(ctx, objectId, typeId, map[string]any{
		"sortOrder": "newest",
	}); err != nil {
		return fmt.Errorf("set device: %w", err)
	}

	// Read the computed record. Priority device > account > base
	// resolves which variant "wins" for each field.
	computed, err := sp.Properties().Get(ctx, objectId, space.PropertyReadOpts{})
	if err != nil {
		return fmt.Errorf("read properties: %w", err)
	}
	log.Printf("computed properties: %s", computed)

	// Read with variants visible — useful for a "why is this value the
	// way it is?" settings UI.
	full, err := sp.Properties().Get(ctx, objectId, space.PropertyReadOpts{
		IncludeVariants: true,
	})
	if err != nil {
		return fmt.Errorf("read properties w/ variants: %w", err)
	}
	log.Printf("full properties: %s", full)

	// Write user data into a type-owned dataset. In v1 datasets are
	// permissionless — "notes" here is free-form and its schema is
	// whatever the caller writes.
	ver, err := sp.Modify(ctx, space.ModifyBatch{
		ObjectId: objectId,
		Dataset:  "notes",
		Records: []space.RecordModify{{
			Upsert: true, // empty Id — derived from the DAG changeId
			Ops: []space.Op{
				{Type: space.OpSet, Value: map[string]any{
					"title": "First note",
					"body":  "hello world",
				}},
				{Type: space.OpAddToSet, Path: "tags", Value: "idea"},
			},
		}},
		TraceIds: []string{"demo-session"},
	})
	if err != nil {
		return fmt.Errorf("write note: %w", err)
	}
	log.Printf("wrote note at version %s", ver)

	// Query the notes back, ordered by creation time (newest first).
	notes, err := sp.Query(objectId, "notes").
		Filter(map[string]any{"tags": "idea"}).
		Sort("-_ver.id").
		Limit(10).
		All(ctx)
	if err != nil {
		return fmt.Errorf("query notes: %w", err)
	}
	log.Printf("found %d notes", len(notes))

	waitForSignal()
	return nil
}

// defineNotebookType creates (if needed) a "Notebook" user type with
// two properties: a required string "title" and a string-array "tags".
// Real apps would cache the typeId + propIds locally so they don't
// re-mint on every run.
func defineNotebookType(ctx context.Context, api space.TypesAPI) (typeId, titleProp, tagsProp string, err error) {
	typeId, err = api.Create(ctx, space.TypeCreateParams{
		Name:        "Notebook",
		Description: "A collection of notes",
	})
	if err != nil {
		return "", "", "", err
	}

	titleProp, err = api.AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Title",
		XKey: "title",
		Kind: space.PropertyKindString,
	})
	if err != nil {
		return "", "", "", err
	}

	tagsProp, err = api.AddProperty(ctx, typeId, space.PropertyDraft{
		Name: "Tags",
		XKey: "tags",
		Kind: space.PropertyKindArray,
		Items: &space.PropertyDraft{
			Kind: space.PropertyKindString,
		},
	})
	if err != nil {
		return "", "", "", err
	}

	return typeId, titleProp, tagsProp, nil
}

func makeAuth(_ context.Context) (auth.Provider, error) {
	// The example picks the wallet location — auth/ doesn't impose a
	// default. Optionally passkey-encrypted via ANY_SYNC_PASSKEY.
	// First run generates a fresh mnemonic + device key; subsequent
	// runs reuse them.
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	p, err := auth.NewFileProvider(auth.FileProviderConfig{
		Path:    filepath.Join(home, ".any", "wallet.key"),
		Passkey: os.Getenv("ANY_SYNC_PASSKEY"),
	})
	if err != nil {
		return nil, err
	}
	if p.Created() {
		log.Printf("wallet generated at %s — back up this mnemonic now:\n  %s",
			p.Path(), p.Mnemonic())
	}
	return p, nil
}

func findOrCreateSpace(ctx context.Context, svc space.Service, name string) (space.Space, error) {
	list, err := svc.List(ctx)
	if err != nil {
		return nil, err
	}
	for _, info := range list {
		if info.Name == name && info.Status == space.StatusActive {
			return svc.Get(ctx, info.Id)
		}
	}
	return svc.Create(ctx, space.CreateRequest{Name: name})
}

func pumpEvents(sub space.Subscription) {
	defer sub.Close()
	for ev := range sub.Events() {
		for _, rec := range ev.Records {
			log.Printf("event %s/%s %v %s", ev.ObjectId, ev.Dataset, rec.Type, rec.Id)
		}
	}
}

func waitForSignal() {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
}

func mustRead(path string) []byte {
	b, err := os.ReadFile(path)
	if err != nil {
		log.Fatalf("read %s: %v", path, err)
	}
	return b
}
