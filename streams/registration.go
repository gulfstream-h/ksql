package streams

import (
	"context"
	"errors"
	"ksql/kinds"
	"ksql/static"
	"reflect"
)

type StreamSettings struct {
	Name         string
	SourceTopic  *string
	SourceStream *string
	SourceTable  *string
	Partitions   *uint8
	Schema       reflect.Type
	Format       kinds.ValueFormat
	DeleteFunc   func(context.Context)
}

func Register[S any](
	ctx context.Context,
	settings StreamSettings) (
	*Stream[S], error) {

	var (
		stream *Stream[S]
		err    error
	)

	stream, err = GetStream[S](ctx, settings.Name, settings)
	if err != nil {
		if errors.Is(err, static.ErrStreamDoesNotExist) {
			return CreateStream[S](ctx, settings.Name, settings)
		}
		return nil, err
	}

	return stream, nil
}
