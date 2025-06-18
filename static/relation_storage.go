package static

import "sync"

// Current file describes global settings
// storage for all existing streams and topics

// The most useful fields are
// StreamSettings.Schema and TableSettings.Schema
// Current fields provides implicit description
// of holding and processable fields

var (
	ReflectionFlag bool

	StreamsProjections sync.Map

	TablesProjections sync.Map
)

// Also schemas are required for DDL representation
// And linter functionality in ksql.Builder & protocol.Unmarshaler

// However in-memory relation storage is highly valuable for
// fast building of streams and topics, reducing propagation
// of fields to ksql client
