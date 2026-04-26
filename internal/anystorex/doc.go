// Package anystorex holds the thin helpers over any-store that don't
// justify their own package: transaction savepoints, collection-naming
// conventions (objectId/datasetName), and small query builders shared
// between the CRDT apply engine and the store layer.
//
// Zero dependencies on other internal packages.
package anystorex
