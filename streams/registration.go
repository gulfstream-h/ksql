package streams

import (
	"context"
	"errors"
	"ksql/schema"
	"reflect"
)

type StreamSettings struct {
	Name         string
	SourceTopic  *string
	SourceStream *string
	Partitions   *uint8
	Schema       reflect.Type
	format       schema.ValueFormat
	DeleteFunc   func(context.Context)
}

var (
	streamsProjections = make(map[string]StreamSettings)
)

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
		if errors.Is(err, ErrStreamDoesNotExist) {
			return CreateStream[S](ctx, settings.Name, settings)
		}
		return nil, err
	}

	return stream, nil
}

func GetStreamProjection(
	streamName string) (StreamSettings, error) {

	settings, exists := streamsProjections[streamName]
	if !exists {
		return StreamSettings{}, ErrStreamDoesNotExist
	}

	return settings, nil
}
