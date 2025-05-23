package streams

import (
	"context"
	"errors"
	"ksql/kinds"
	"ksql/static"
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

// Register - registers a full-functional table
// with the provided settings. Also it is bound to
// user provided generic scheme for select operations
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
