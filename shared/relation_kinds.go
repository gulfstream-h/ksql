package shared

import (
	"context"
	"ksql/kinds"
	"ksql/schema"
)

// Settings - common structure
// that describes relation data.
// used in cache for fast schema access
type Settings struct {
	SourceTopic *string
	Partitions  *int
	Schema      schema.LintedFields
	Format      kinds.ValueFormat
}

// StreamSettings - describes the settings of stream
// it's not bound to any specific structure
// so can be easily called from any space
type StreamSettings Settings

// TableSettings - describes the settings of a table
// it's not bound to any specific structure
// so can be easily called from any space
type TableSettings Settings

// RelationSettings - is generic constraint
// it allows using both methods on certain struct
// both fields of structure without interface Getters
type RelationSettings interface {
	~struct {
		SourceTopic *string
		Partitions  *int
		Schema      schema.LintedFields
		Format      kinds.ValueFormat
	}
}

// Linter - initializer for reflection-mode settings
type Linter interface {
	InitLinter(context.Context) error
}

// Config - library entry point. Establish connection
// in network and set-ups reflection rules
type Config interface {
	Linter
	Configure(context.Context) error
}
