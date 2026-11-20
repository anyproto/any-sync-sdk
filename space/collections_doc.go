package space

// Membership vocabulary on the objects row — the fields Objects().Get
// returns and queries filter on:
//
//	any.type        the object's one type id; "__type__" on a type
//	                object, "__collection__" on a collection object
//	any.collections the ids of the collections the object is filed under
//
// A filter `{"any.type": typeId}` returns the objects of that type and
// never the type object itself; `{"any.collections": collectionId}`
// the members of that collection and never the collection object.
const (
	// TypeMarker is the `any.type` value of a type object.
	TypeMarker = "__type__"
	// CollectionMarker is the `any.type` value of a collection object.
	CollectionMarker = "__collection__"
)
