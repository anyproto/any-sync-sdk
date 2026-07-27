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
