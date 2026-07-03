// Package anyuri builds and parses any:// object URIs — the value
// convention for link-format properties (space.FormatLinks) and the
// canonical way to reference an object as a string.
//
// Two path forms exist:
//
//	any://<objectId>            — in-space reference (the property-value form)
//	any://<spaceId>/<objectId>  — global reference
//
// plus an optional #fragment for sub-object anchors (e.g. a chat turn).
// Property values of link-format properties use the one-segment,
// fragment-less form; consumers enforcing that stricter shape should
// check Parse's result rather than IsValid.
package anyuri

import (
	"errors"
	"fmt"
	"strings"
)

// Scheme is the URI scheme, without separators.
const Scheme = "any"

const prefix = Scheme + "://"

// ErrInvalid is wrapped by every parse failure; classify with errors.Is.
var ErrInvalid = errors.New("anyuri: invalid any:// URI")

// URI is a parsed any:// reference. SpaceId is empty for the in-space
// one-segment form; Fragment is empty when no #fragment was present.
type URI struct {
	SpaceId  string
	ObjectId string
	Fragment string
}

// Build returns the in-space reference "any://<objectId>" — the form
// used in link-format property values.
func Build(objectId string) string {
	return prefix + objectId
}

// BuildGlobal returns the global reference "any://<spaceId>/<objectId>".
func BuildGlobal(spaceId, objectId string) string {
	return prefix + spaceId + "/" + objectId
}

// String renders the URI back to its text form.
func (u URI) String() string {
	var b strings.Builder
	b.WriteString(prefix)
	if u.SpaceId != "" {
		b.WriteString(u.SpaceId)
		b.WriteByte('/')
	}
	b.WriteString(u.ObjectId)
	if u.Fragment != "" {
		b.WriteByte('#')
		b.WriteString(u.Fragment)
	}
	return b.String()
}

// Parse decodes both path forms plus an optional fragment. Errors wrap
// ErrInvalid.
func Parse(s string) (URI, error) {
	rest, ok := strings.CutPrefix(s, prefix)
	if !ok {
		return URI{}, fmt.Errorf("%w: missing %q prefix in %q", ErrInvalid, prefix, s)
	}
	var u URI
	rest, u.Fragment, _ = strings.Cut(rest, "#")
	first, second, hasSlash := strings.Cut(rest, "/")
	if hasSlash {
		u.SpaceId, u.ObjectId = first, second
		if u.SpaceId == "" {
			return URI{}, fmt.Errorf("%w: empty space id in %q", ErrInvalid, s)
		}
		if strings.ContainsRune(u.ObjectId, '/') {
			return URI{}, fmt.Errorf("%w: too many path segments in %q", ErrInvalid, s)
		}
	} else {
		u.ObjectId = first
	}
	if u.ObjectId == "" {
		return URI{}, fmt.Errorf("%w: empty object id in %q", ErrInvalid, s)
	}
	return u, nil
}

// IsValid reports whether s parses as an any:// URI in either form.
func IsValid(s string) bool {
	_, err := Parse(s)
	return err == nil
}
