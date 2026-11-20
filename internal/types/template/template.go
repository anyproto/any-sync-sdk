// Package template declares the built-in `template` collection: the
// marker of an object that is a template of its type.
//
// A template is an ordinary object of the type it is for — it holds
// that type's values and datasets like any member — filed under this
// collection. CreateObjectOpts.From copies an object; when the source
// is filed here the marker is not carried over and the new row is
// stamped `any.template` {id, version} (internal/types/any). Hidden,
// and registered by the SDK itself ahead of the consumer's collections
// (spaceobjects.NewStoreWithConfig), so a consumer registration under
// this id is refused at Open.
package template

import "github.com/anyproto/any-sync-sdk/handler"

const (
	// CollectionId is the collection's literal id and xKey.
	CollectionId = "template"
	Name         = "Template"
	Description  = "Marks a template: an object of its type whose values and datasets are what a copy starts from"

	// PropTitle is the label a template picker shows — not the
	// pre-filled name, which may be empty. PropVersion is the
	// template's version: the one a copy records under
	// any.template.version, bumped by the consumer when the template
	// changes.
	PropTitle   = "title"
	PropVersion = "version"
)

// Collection returns the registered declaration.
func Collection() handler.Collection {
	return handler.Collection{
		Id:          CollectionId,
		Name:        Name,
		Description: Description,
		Hidden:      true,
		Properties: []handler.PropertyDecl{
			{Id: PropTitle, Name: "Title", Kind: handler.PropertyKindString, XFormat: map[string]any{"type": "text"},
				Description: "Label shown where a template is picked; not the pre-filled name."},
			{Id: PropVersion, Name: "Version", Kind: handler.PropertyKindNumber,
				Description: "Template version; a copy records it under any.template.version."},
		},
	}
}
