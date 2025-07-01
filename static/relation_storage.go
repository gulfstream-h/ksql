package static

import (
	"errors"
	"ksql/schema"
	"ksql/shared"
	"sync"
)

// Current file describes global settings
// storage for all existing streams and topics

// The most useful fields are
// StreamSettings.Schema and TableSettings.Schema
// Current fields provides implicit description
// of holding and processable fields

type (
	RelationStorage[S shared.RelationSettings] struct {
		storage sync.Map
	}
)

// FindRelationFields returns the fields of a relation (stream or table) based on its name.
// It can be used for other DDL check-ups
func FindRelationFields(relationName string) (map[string]schema.SearchField, error) {
	streamSettings, exists := StreamsProjections.Get(relationName)
	if exists {
		return streamSettings.Schema.Map(), nil
	}

	tableSettings, exists := TablesProjections.Get(relationName)
	if exists {
		return tableSettings.Schema.Map(), nil
	}

	return nil, errors.New("cannot find relation fields")
}

var (
	ReflectionFlag bool

	StreamsProjections RelationStorage[shared.StreamSettings]
	TablesProjections  RelationStorage[shared.TableSettings]
)

// Get - returns cached settings value
// with reflection inside
func (rs *RelationStorage[S]) Get(name string) (S, bool) {
	var (
		s S
	)

	value, ok := rs.storage.Load(name)
	if !ok {
		return s, false
	}

	settings, ok := value.(S)
	if !ok {
		return s, false
	}

	return settings, true
}

// Set - cache some relation settings with schema inside
func (rs *RelationStorage[S]) Set(
	name string,
	settings S,
	responseSchema schema.LintedFields,
) {
	s := shared.Settings(settings)

	s.Schema = responseSchema
	rs.storage.Store(name, s)
}

// Also schemas are required for DDL representation
// And linter functionality in sql builder

// However in-memory relation storage is highly valuable for
// fast building of streams and topics, reducing propagation
// of fields to ksql client
