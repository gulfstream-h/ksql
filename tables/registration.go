package tables

import (
	"context"
	"ksql/schema"
)

type TableSettings struct {
	Name        string
	SourceTopic *string
	Partitions  *uint8
	Format      schema.ValueFormat
	DeleteFunc  func(context.Context)
}

func Register[S any](ctx context.Context, tableName string, settings *TableSettings) (*Table[S], error) {
	projection, err := GetTableProjection(ctx, tableName)
	if err != nil {
		if settings.SourceTopic != nil {
			stream, err := createTableRemotely[S](ctx, nil, settings.Name, *settings)
			if err != nil {
				return nil, err
			}

			return stream, nil
		}

		return nil, err
	}

	return &Table[S]{
		sourceTopic: projection.SourceTopic,
		partitions:  projection.Partitions,
		format:      projection.Format,
	}, nil
}
