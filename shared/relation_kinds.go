package shared

import (
	"context"
	"errors"
	"ksql/kinds"
	"ksql/schema"
	"strings"
)

type Settings struct {
	Name        string
	SourceTopic string
	Partitions  uint8
	Schema      schema.LintedFields
	Format      kinds.ValueFormat
	DeleteFunc  func(context.Context)
}

func (s *Settings) Validate() error {
	s.Name = strings.TrimSpace(s.Name)

	if len(s.Name) == 0 {
		return errors.New("invalid topic name")
	}

	s.SourceTopic = strings.TrimSpace(s.SourceTopic)
	//if len(s.SourceTopic) == 0 {
	// todo return nothing ?
	//}

	if s.Partitions < 1 {
		return errors.New("invalid partitions number")
	}
	return nil
}

// StreamSettings - describes the settings of stream
// it's not bound to any specific structure
// so can be easily called from any space
type StreamSettings = Settings

// TableSettings - describes the settings of a table
// it's not bound to any specific structure
// so can be easily called from any space
type TableSettings = Settings

type RelationSettings interface {
	~struct {
		Name        string
		SourceTopic string
		Partitions  uint8
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
