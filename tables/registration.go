package tables

import (
	"context"
	"errors"
	"ksql/schema"
	"reflect"
)

type TableSettings struct {
	Name        string
	SourceTopic *string
	Partitions  *uint8
	Schema      reflect.Type
	Format      schema.ValueFormat
	DeleteFunc  func(context.Context)
}

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
		if errors.Is(err, ErrTableDoesNotExist) {
			return CreateTable[S](ctx, settings.Name, settings)
		}
		return nil, err
	}

	return table, nil
}
