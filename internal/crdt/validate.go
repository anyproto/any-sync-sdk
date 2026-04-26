package crdt

import (
	"errors"
	"fmt"
	"strings"

	"github.com/anyproto/any-store/anyenc"
)

// ErrInvalidPath is returned when an op references a malformed or reserved
// field path. Joined with ErrValidation so callers can detect both classes
// with errors.Is(err, ErrValidation).
var ErrInvalidPath = errors.New("crdt: invalid path")

// validateOpPaths checks that `op` targets a legal field path. Called
// during the pre-apply validation pass (alongside handler.Validate); a
// failure drops the entire change.
//
// Rules:
//   - Single-path ops (non-empty `Path`) must pass validatePath.
//   - Multi-field `$set`/`$unset` (empty `Path`) must have an object
//     payload; each key is a dotted path split on "." and each resulting
//     segment must pass validatePath's per-segment rules.
//   - `delete` has no path and is always allowed.
func validateOpPaths(op Op) error {
	switch op.Type {
	case OpDelete:
		return nil
	case OpSet, OpUnset:
		if len(op.Path) == 0 {
			// Multi-field form.
			if op.Payload == nil {
				return nil
			}
			if op.Payload.Type() != anyenc.TypeObject {
				return fmt.Errorf("%w: multi-field %s requires an object payload", ErrInvalidPath, op.Type)
			}
			obj, _ := op.Payload.Object()
			var firstErr error
			obj.Visit(func(k []byte, _ *anyenc.Value) {
				if firstErr != nil {
					return
				}
				segments := strings.Split(string(k), ".")
				if err := validatePath(segments); err != nil {
					firstErr = fmt.Errorf("%w: key %q: %s", ErrInvalidPath, string(k), err)
				}
			})
			return firstErr
		}
		return validatePath(op.Path)
	case OpAddToSet, OpPull, OpInc, OpIncGated:
		return validatePath(op.Path)
	}
	return nil
}

// validatePath enforces structural and reservation rules for a single op
// target path:
//
//   - Path must be non-empty.
//   - No element may be empty ("a..b" style).
//   - No element may contain "." (would silently shadow the dotted-path
//     parsing used by the multi-field form and mongo convention).
//   - The first element must not be `id` (immutable) or start with `_`
//     (reserved for protocol-owned fields: `_ver`, `_deletedAt`, and any
//     future system field).
func validatePath(path []string) error {
	if len(path) == 0 {
		return fmt.Errorf("%w: path is empty", ErrInvalidPath)
	}
	for i, elem := range path {
		if elem == "" {
			return fmt.Errorf("%w: path element %d is empty", ErrInvalidPath, i)
		}
		if strings.ContainsRune(elem, '.') {
			return fmt.Errorf("%w: path element %d %q contains '.'", ErrInvalidPath, i, elem)
		}
	}
	top := path[0]
	if top == IdField {
		return fmt.Errorf("%w: %q is immutable", ErrInvalidPath, top)
	}
	if strings.HasPrefix(top, "_") {
		return fmt.Errorf("%w: top-level field %q is reserved (underscore prefix is protocol-owned)", ErrInvalidPath, top)
	}
	return nil
}
