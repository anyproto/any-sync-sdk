// Package anyencx holds small anyenc helpers shared across the SDK.
package anyencx

import (
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/anyenc/anyencutil"
)

// Clone deep-copies v off whatever buffer it currently lives in (a
// reused iterator/doc buffer, a shared arena) onto a fresh
// parser-owned arena via anyencutil.Value.FillCopy. nil in, nil out.
//
// Each call allocates one fresh anyencutil.Value (one Parser plus
// scratch buffer); the returned value points into that parser's
// arena, which GC keeps alive as long as any caller retains the
// value.
func Clone(v *anyenc.Value) *anyenc.Value {
	if v == nil {
		return nil
	}
	var w anyencutil.Value
	w.FillCopy(v)
	return w.Value
}

// StampSeconds reads a derived timestamp leaf as unix seconds, or 0 when
// it is absent or not a timestamp — the "unknown" value every caller of
// these stamps already expects.
//
// Stamps are datetime instants (unix millis). Rows materialized before
// that carried a plain epoch-seconds number and are read the old way
// until the re-index reaches them (docs/08-versioning.md), so both
// shapes resolve here.
func StampSeconds(v *anyenc.Value) int64 {
	if v == nil {
		return 0
	}
	switch v.Type() {
	case anyenc.TypeDateTime:
		ms, err := v.DateTimeMillis()
		if err != nil {
			return 0
		}
		return ms / 1000
	case anyenc.TypeNumber:
		return int64(v.GetFloat64())
	}
	return 0
}
