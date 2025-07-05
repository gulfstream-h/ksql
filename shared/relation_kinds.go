package shared

import (
	"context"
	"errors"
	"fmt"
	"ksql/internal/schema"
	"ksql/kinds"
	"strings"
)

// Settings - common structure
// that describes relation data.
// used in cache for fast schema access
type Settings struct {
	Name        string
	SourceTopic string
	Partitions  int
	Schema      schema.LintedFields
	Format      kinds.ValueFormat
}

// StreamSettings - describes the settings of stream
// it's not bound to any specific structure
// so can be easily called from any space
type StreamSettings = Settings

// Validate - primary checks settings
// to avoid malformed relation creation
func (s *Settings) Validate() error {
	s.Name = strings.TrimSpace(s.Name)

	if len(s.Name) == 0 {
		return errors.New("invalid stream name")
	}

	s.SourceTopic = strings.TrimSpace(s.SourceTopic)
	if len(s.SourceTopic) == 0 {
		return fmt.Errorf("souce topic cannot be blank")
	}

	return nil
}

// TableSettings - describes the settings of a table
// it's not bound to any specific structure
// so can be easily called from any space
type TableSettings = Settings

// Validate - primary checks settings
// to avoid malformed relation creation
//func (s *TableSettings) Validate() error {
//	s.Name = strings.TrimSpace(s.Name)
//
//	if len(s.Name) == 0 {
//		return errors.New("invalid topic name")
//	}
//
//	s.SourceTopic = strings.TrimSpace(s.SourceTopic)
//	if len(s.SourceTopic) == 0 {
//		return fmt.Errorf("source topic cannot be blank")
//	}
//
//	return nil
//}

// RelationSettings - is generic constraint
// it allows using both methods on certain struct
// both fields of structure without interface Getters
type RelationSettings interface {
	~struct {
		Name        string
		SourceTopic string
		Partitions  int
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
