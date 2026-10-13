package crdt

import (
	"context"

	anystore "github.com/anyproto/any-store/v2"

	"github.com/anyproto/any-sync-sdk/internal/schema"
)

// What a copy of this object's records has to honour: the dataset's
// effective declaration and id rule live on the controller, module and
// runtime datasets alike (a CompiledDataset carries no schema for a
// module-served one).

// OpenForRead is collectionForRead reporting why an open failed. A nil
// collection with a nil error means the dataset has no handler here or
// no writer materialised it yet — no records, not a failed read.
func (c *Controller) OpenForRead(ctx context.Context, dataset string) (anystore.Collection, error) {
	return c.openForRead(ctx, dataset)
}

// DatasetSchema is the normalized declaration registered for dataset,
// or false when the dataset has no handler here.
func (c *Controller) DatasetSchema(dataset string) (schema.Dataset, bool) {
	ds, ok := c.schemas[dataset]
	return ds, ok
}

// DerivesRecordIds reports whether the dataset's handler is the generic
// schema handler with IdAuto: it derives every record id from the
// change and refuses an explicit one.
func (c *Controller) DerivesRecordIds(dataset string) bool {
	sh, ok := c.handlers[dataset].(*SchemaHandler)
	return ok && sh.idRule == schema.IdAuto
}
