// Package eventbus owns the wire-shape Event types and the
// per-change BuildEvent projector. The per-Object AfterApply hook in
// internal/spaceobjects calls BuildEvent once per applied change and
// hands the result to internal/subscribe.Engine.OnApply.
//
// This package used to host a per-space Dispatcher with its own
// Subscribe / SubscribeProperties API. That surface has been retired
// in favour of internal/subscribe.Engine + space.Query.Subscribe,
// which provides windowed live queries (filter / sort / limit /
// snapshot / drift detection) instead of a raw event firehose.
package eventbus
