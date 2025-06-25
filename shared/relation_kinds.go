package shared

import (
	"context"
	"ksql/kinds"
	"ksql/schema"
)

type Settings struct {
	Name        string
	SourceTopic *string
	Partitions  *uint8
	Schema      schema.LintedFields
	Format      kinds.ValueFormat
	DeleteFunc  func(context.Context)
}

// StreamSettings - describes the settings of stream
// it's not bound to any specific structure
// so can be easily called from any space
type StreamSettings Settings

// TableSettings - describes the settings of a table
// it's not bound to any specific structure
// so can be easily called from any space
type TableSettings Settings

type RelationSettings interface {
	~struct {
		Name        string
		SourceTopic *string
		Partitions  *uint8
		Schema      schema.LintedFields
		Format      kinds.ValueFormat
		DeleteFunc  func(context.Context)
	}
}

type Linter interface {
	InitLinter(context.Context) error
}

type Config interface {
	Linter
	Configure(context.Context) error
}
