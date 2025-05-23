package tables

import (
	"context"
	"errors"
	"ksql/kinds"
	"ksql/static"
	"reflect"
)

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

// Register - registers a full-functional table
// with the provided settings. Also it is bound to
// user provided generic scheme for select operations
func Register[S any](
	ctx context.Context,
	settings TableSettings) (
	*Table[S], error) {

	var (
		table *Table[S]
		err   error
	)

	table, err = GetTable[S](ctx, settings.Name, settings)
	if err != nil {
		if errors.Is(err, static.ErrTableDoesNotExist) {
			return CreateTable[S](ctx, settings.Name, settings)
		}
		return nil, err
	}

	return table, nil
}
