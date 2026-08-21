package schema

import (
	"errors"
	"fmt"
	"regexp"
)

// ErrDecl is the sentinel wrapped by dataset-declaration validation
// failures. Classify with errors.Is.
var ErrDecl = errors.New("schema: invalid dataset declaration")

// ValidateDatasetDecl checks a dataset declaration's well-formedness.
// Shared by the generic schema handler's constructor, external-type
// catalog validation, and the runtime dataset-def write preflight, so a
// declaration rejected here can neither register nor sync.
//
// Rules:
//   - field ids are non-empty, unique, and dot-free;
//   - Stamp forces ScopeDerived (zero scope is normalized by the
//     handler; an explicit conflicting scope is rejected);
//   - at most one field per stamp kind;
//   - Required is mutually exclusive with Stamp, and required fields
//     are synced-scope (they must ride the create change);
//   - stamped fields cannot also declare MutableBy (handler-written);
//   - MutableByAuthor anywhere or DeleteByAuthor requires a
//     StampCreator field — the authorship fact must live on the record
//     so apply-time checks read only ctx.Before;
//   - IdPattern must compile (RE2) when set; id constraints only make
//     sense under IdUser;
//   - a search text mapping, when present, names at least one field
//     key, with no empty or duplicate keys (mapped keys are NOT
//     required to be declared fields — the annotation stays opaque).
func ValidateDatasetDecl(ds Dataset) error {
	seen := make(map[string]struct{}, len(ds.Fields))
	stamps := make(map[Stamp]string, 3)
	var hasCreatorStamp, hasAuthorMutable bool
	for i := range ds.Fields {
		f := &ds.Fields[i]
		if f.Id == "" {
			return fmt.Errorf("%w: field %d has an empty id", ErrDecl, i)
		}
		for _, c := range f.Id {
			if c == '.' {
				return fmt.Errorf("%w: field %q: dots are not allowed in field ids", ErrDecl, f.Id)
			}
		}
		if _, dup := seen[f.Id]; dup {
			return fmt.Errorf("%w: duplicate field id %q", ErrDecl, f.Id)
		}
		seen[f.Id] = struct{}{}
		if f.Stamp != StampNone {
			if f.Scope != 0 && f.Scope != ScopeDerived {
				return fmt.Errorf("%w: field %q: stamp %s requires derived scope, declared %s", ErrDecl, f.Id, f.Stamp, f.Scope)
			}
			if prev, dup := stamps[f.Stamp]; dup {
				return fmt.Errorf("%w: stamp %s declared on both %q and %q", ErrDecl, f.Stamp, prev, f.Id)
			}
			stamps[f.Stamp] = f.Id
			if f.Required {
				return fmt.Errorf("%w: field %q: required is incompatible with a stamp", ErrDecl, f.Id)
			}
			if f.MutableBy != MutableNever {
				return fmt.Errorf("%w: field %q: a stamped field is handler-written, mutableBy is meaningless", ErrDecl, f.Id)
			}
			if f.Stamp == StampCreator {
				hasCreatorStamp = true
			}
		}
		if f.Required && f.Scope != 0 && f.Scope != ScopeSynced {
			return fmt.Errorf("%w: field %q: required fields must be synced-scope (declared %s)", ErrDecl, f.Id, f.Scope)
		}
		if f.MutableBy == MutableByAuthor {
			hasAuthorMutable = true
		}
	}
	if (hasAuthorMutable || ds.DeleteBy == DeleteByAuthor) && !hasCreatorStamp {
		return fmt.Errorf("%w: author-gated rules (mutableBy:author / deleteBy:author) require a creator-stamped field", ErrDecl)
	}
	if ds.IdRule == IdAuto && (ds.IdPattern != "" || ds.IdMaxLen != 0) {
		return fmt.Errorf("%w: id pattern/length constraints require idRule user", ErrDecl)
	}
	if ds.IdPattern != "" {
		if _, err := regexp.Compile("^(?:" + ds.IdPattern + ")$"); err != nil {
			return fmt.Errorf("%w: id pattern does not compile: %v", ErrDecl, err)
		}
	}
	if ds.IdMaxLen < 0 {
		return fmt.Errorf("%w: negative id max length", ErrDecl)
	}
	if ds.Search != nil && ds.Search.Text != nil {
		if err := ValidateSearchText(ds.Search.Text); err != nil {
			return err
		}
	}
	return nil
}

// ValidateSearchText checks a search text mapping's field keys: at
// least one, none empty, no duplicates. Shared by the declaration
// validator and the wire-leaf checks (the dataset-def handler and the
// PatchDataset preflight), so every entry path rejects the same forms.
func ValidateSearchText(keys []string) error {
	if len(keys) == 0 {
		return fmt.Errorf("%w: search.text must name at least one field key", ErrDecl)
	}
	seen := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		if k == "" {
			return fmt.Errorf("%w: search.text has an empty field key", ErrDecl)
		}
		if _, dup := seen[k]; dup {
			return fmt.Errorf("%w: search.text names %q twice", ErrDecl, k)
		}
		seen[k] = struct{}{}
	}
	return nil
}

// CompileIdPattern compiles the dataset's user-id constraint into a
// full-match regexp, applying defaults. Call after ValidateDatasetDecl;
// compile errors are impossible on a validated declaration.
func CompileIdPattern(ds Dataset) (*regexp.Regexp, int, error) {
	pat := ds.IdPattern
	if pat == "" {
		pat = DefaultIdPattern
	}
	maxLen := ds.IdMaxLen
	if maxLen == 0 {
		maxLen = DefaultIdMaxLen
	}
	re, err := regexp.Compile("^(?:" + pat + ")$")
	if err != nil {
		return nil, 0, fmt.Errorf("%w: id pattern does not compile: %v", ErrDecl, err)
	}
	return re, maxLen, nil
}
