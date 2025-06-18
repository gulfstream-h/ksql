package shared

import (
	"context"
	"ksql/kinds"
	"reflect"
)

// StreamSettings - describes the settings of stream
// it's not bound to any specific structure
// so can be easily called from any space
type StreamSettings struct {
	Name        string
	SourceTopic *string
	Partitions  *uint8
	Schema      reflect.Type
	Format      kinds.ValueFormat
	DeleteFunc  func(context.Context)
}

// TableSettings - describes the settings of a table
// it's not bound to any specific structure
// so can be easily called from any space
type TableSettings struct {
	Name        string
	SourceTopic *string
	Partitions  *uint8
	Schema      reflect.Type
	Format      kinds.ValueFormat
	DeleteFunc  func(context.Context)
}

type Linter interface {
	InitLinter(context.Context) error
}

type Config interface {
	Linter
	Configure(context.Context) error
}
