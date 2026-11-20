package anysyncx

import (
	"context"
	"sync"

	"github.com/anyproto/any-sync/app"
	"github.com/anyproto/any-sync/commonspace/object/keyvalue/keyvaluestorage"
	"github.com/anyproto/any-sync/commonspace/object/keyvalue/keyvaluestorage/innerstorage"
)

// KeyValueHandler receives applied key-value writes (local Set and
// remote SetRaw) for one space. Runs synchronously on any-sync's
// apply/broadcast path — hand real work off to your own goroutine.
type KeyValueHandler func(decryptor keyvaluestorage.Decryptor, kvs []innerstorage.KeyValue)

// kvDispatcher is the per-space keyvaluestorage.Indexer wired into
// commonspace.Deps. It fans applied writes out to the handlers
// registered on the App for that space. Errors never propagate —
// any-sync only logs Indexer errors anyway, so durability must come
// from a consumer-side reconcile, not from this hook.
type kvDispatcher struct {
	spaceId string
	owner   *App
}

func (d *kvDispatcher) Init(*app.App) error { return nil }
func (d *kvDispatcher) Name() string        { return keyvaluestorage.IndexerCName }

func (d *kvDispatcher) Index(decryptor keyvaluestorage.Decryptor, kvs ...innerstorage.KeyValue) error {
	d.owner.dispatchKeyValues(d.spaceId, decryptor, kvs)
	return nil
}

func (a *App) newKVDispatcher(spaceId string) keyvaluestorage.Indexer {
	return &kvDispatcher{spaceId: spaceId, owner: a}
}

// OnKeyValues registers a handler for a space's applied key-value
// writes. The returned cancel is idempotent. Writes applied while no
// handler is registered are not replayed — consumers reconcile from
// the store on startup (Iterate / GetAll).
func (a *App) OnKeyValues(spaceId string, h KeyValueHandler) (cancel func()) {
	a.kvMu.Lock()
	defer a.kvMu.Unlock()
	if a.kvHandlers == nil {
		a.kvHandlers = map[string][]*kvHandlerReg{}
	}
	reg := &kvHandlerReg{h: h}
	a.kvHandlers[spaceId] = append(a.kvHandlers[spaceId], reg)
	var once sync.Once
	return func() {
		once.Do(func() {
			a.kvMu.Lock()
			defer a.kvMu.Unlock()
			regs := a.kvHandlers[spaceId]
			for i, r := range regs {
				if r == reg {
					a.kvHandlers[spaceId] = append(regs[:i], regs[i+1:]...)
					break
				}
			}
		})
	}
}

type kvHandlerReg struct{ h KeyValueHandler }

func (a *App) dispatchKeyValues(spaceId string, decryptor keyvaluestorage.Decryptor, kvs []innerstorage.KeyValue) {
	a.kvMu.RLock()
	regs := append([]*kvHandlerReg(nil), a.kvHandlers[spaceId]...)
	a.kvMu.RUnlock()
	for _, r := range regs {
		r.h(decryptor, kvs)
	}
}

// KeyValueStore returns a space's default key-value store (loads the
// space if needed).
func (a *App) KeyValueStore(ctx context.Context, spaceId string) (keyvaluestorage.Storage, error) {
	handle, err := a.GetSpace(ctx, spaceId)
	if err != nil {
		return nil, err
	}
	return handle.Inner().KeyValue().DefaultStore(), nil
}
