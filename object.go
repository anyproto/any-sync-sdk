package syncsdk

import (
	"context"

	"github.com/anyproto/any-sync-sdk/keys"
)

type AddOption func(*addOptions)

type addOptions struct {
	encrypt    bool
	isSnapshot bool
	dataType   string
}

func WithEncryption() AddOption {
	return func(o *addOptions) { o.encrypt = true }
}

func WithSnapshot() AddOption {
	return func(o *addOptions) { o.isSnapshot = true }
}

func WithDataType(t string) AddOption {
	return func(o *addOptions) { o.dataType = t }
}

type ChangeInfo struct {
	ID          string
	PreviousIDs []string
	Data        []byte
	DataType    string
	Identity    keys.PublicKey
	Timestamp   int64
	IsSnapshot  bool
	Version     string
	AddSeq      uint64 // Space-global monotonic sequence; assigned on storage insert
}

// ResolvedAddOptions holds the resolved values of AddOption functions.
// It is exported for use by internal packages.
type ResolvedAddOptions struct {
	Encrypt    bool
	IsSnapshot bool
	DataType   string
}

// ResolveAddOptions applies the given AddOption functions and returns the resolved values.
func ResolveAddOptions(opts []AddOption) ResolvedAddOptions {
	var o addOptions
	for _, opt := range opts {
		opt(&o)
	}
	return ResolvedAddOptions{
		Encrypt:    o.encrypt,
		IsSnapshot: o.isSnapshot,
		DataType:   o.dataType,
	}
}

type Object interface {
	ID() string
	SpaceID() string
	Heads() []string
	AddContent(ctx context.Context, data []byte, opts ...AddOption) (ChangeInfo, error)
	Iterate(visitor func(change ChangeInfo) bool) error
	IterateAfterAddSeq(addSeq uint64, visitor func(change ChangeInfo) bool) error
	Subscribe(handler Handler) (unsubscribe func())
	Close() error
}
