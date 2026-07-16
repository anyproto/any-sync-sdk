// Per-space client settings — the free-form `settings` subtree on a
// space-index row (FieldSettings). Account-private (owner-only tech
// ACL), synced across the account's devices, edited per key so devices
// touching different keys converge without clobbering each other.

package techspace

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/anyproto/any-store/v2/anyenc"

	"github.com/anyproto/any-sync-sdk/internal/crdt"
	"github.com/anyproto/any-sync-sdk/internal/object"
)

// Settings-patch sentinels — wrapped by SettingsOps validation errors
// so callers can classify with errors.Is.
var (
	ErrSettingsEmpty      = errors.New("techspace: settings patch is empty")
	ErrSettingsBadKey     = errors.New("techspace: invalid settings key")
	ErrSettingsBadValue   = errors.New("techspace: unsupported settings value")
	ErrSettingsKeyOverlap = errors.New("techspace: key in both set and unset")
)

// SettingsOps encodes a settings patch into per-path CRDT ops: one
// OpSet at ["settings", key] per set entry, one OpUnset per unset
// entry. Exported so the handler-level tests can apply the exact ops
// SetSettings emits without booting the full service.
//
// Validation (v1 keeps the shape deliberately narrow):
//   - at least one entry across set+unset;
//   - keys non-empty and dot-free (single-level keys under `settings`
//     — a dotted key would silently become a deeper path);
//   - no key in both set and unset (op order within one change would
//     otherwise decide the winner);
//   - values are scalars only: string, bool, or any Go numeric type
//     (encoded as anyenc float64, the wire's only number shape).
//
// Set keys are emitted in sorted order (map iteration is random;
// deterministic ops keep changes reproducible), unset keys in caller
// order.
func SettingsOps(a *anyenc.Arena, set map[string]any, unset []string) ([]crdt.Op, error) {
	if len(set) == 0 && len(unset) == 0 {
		return nil, ErrSettingsEmpty
	}
	validKey := func(k string) error {
		if k == "" {
			return fmt.Errorf("%w: empty", ErrSettingsBadKey)
		}
		if strings.ContainsRune(k, '.') {
			return fmt.Errorf("%w: %q contains '.' (single-level keys only)", ErrSettingsBadKey, k)
		}
		return nil
	}
	ops := make([]crdt.Op, 0, len(set)+len(unset))
	for _, k := range slices.Sorted(maps.Keys(set)) {
		if err := validKey(k); err != nil {
			return nil, err
		}
		v, err := encodeSettingsValue(a, set[k])
		if err != nil {
			return nil, fmt.Errorf("%w: key %q: %w", ErrSettingsBadValue, k, err)
		}
		ops = append(ops, crdt.Op{Type: crdt.OpSet, Path: []string{FieldSettings, k}, Payload: v})
	}
	for _, k := range unset {
		if err := validKey(k); err != nil {
			return nil, err
		}
		if _, both := set[k]; both {
			return nil, fmt.Errorf("%w: %q", ErrSettingsKeyOverlap, k)
		}
		ops = append(ops, crdt.Op{Type: crdt.OpUnset, Path: []string{FieldSettings, k}})
	}
	return ops, nil
}

// encodeSettingsValue maps a Go scalar onto its anyenc value. Numbers
// all collapse to float64 — anyenc's only number representation — so
// an int written here reads back as float64 (JSON semantics).
func encodeSettingsValue(a *anyenc.Arena, v any) (*anyenc.Value, error) {
	switch t := v.(type) {
	case string:
		return a.NewString(t), nil
	case bool:
		return a.NewBool(t), nil
	case int:
		return a.NewNumberFloat64(float64(t)), nil
	case int32:
		return a.NewNumberFloat64(float64(t)), nil
	case int64:
		return a.NewNumberFloat64(float64(t)), nil
	case uint:
		return a.NewNumberFloat64(float64(t)), nil
	case uint32:
		return a.NewNumberFloat64(float64(t)), nil
	case uint64:
		return a.NewNumberFloat64(float64(t)), nil
	case float32:
		return a.NewNumberFloat64(float64(t)), nil
	case float64:
		return a.NewNumberFloat64(t), nil
	}
	return nil, fmt.Errorf("type %T (string/bool/number only)", v)
}

// SetSettings applies a per-key patch to spaceId's `settings` subtree:
// $set each set entry at settings.<key>, $unset each unset key — all
// under one SYNCED change (LocalWrite → DAG → the account's other
// devices). No upsert: the row must already exist (a modify on an
// absent id is a silent no-op by CRDT rules; the spaceimpl wrapper
// guards with an existence check so callers get an error instead).
//
// Handler note: SpaceIndexHandler guards only `type` (pinned) and the
// status fields (terminal delete) — `settings` passes untouched, so a
// deleted row's settings stay editable by design (the tombstone row is
// still the account's record of the space).
func (s *Service) SetSettings(ctx context.Context, spaceId string, set map[string]any, unset []string) (object.WriteResult, error) {
	if !s.open.Load() {
		return object.WriteResult{}, errors.New("techspace: service not open")
	}
	if spaceId == "" {
		return object.WriteResult{}, errors.New("techspace: SetSettings: empty spaceId")
	}
	arena := &anyenc.Arena{}
	ops, err := SettingsOps(arena, set, unset)
	if err != nil {
		return object.WriteResult{}, err
	}
	obj, err := s.indexObj(ctx)
	if err != nil {
		return object.WriteResult{}, err
	}
	change := crdt.Change{
		Dataset:     SpaceIndexDataset,
		DataVersion: HandlerVersion,
		Records: []crdt.RecordChange{
			{Id: spaceId, Ops: ops},
		},
	}
	return obj.LocalWrite(ctx, change)
}
