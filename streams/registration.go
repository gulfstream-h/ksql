package streams

import (
	"context"
	"errors"
	"ksql/shared"
	"ksql/static"
)

// Register - registers a full-functional table
// with the provided settings. Also it is bound to
// user provided generic scheme for select operations
func Register[S any](
	ctx context.Context,
	settings shared.StreamSettings) (
	*Stream[S], error) {

	var (
		stream *Stream[S]
		err    error
	)

	stream, err = GetStream[S](ctx, settings.Name)
	if err != nil {
		if errors.Is(err, static.ErrStreamDoesNotExist) {
			return CreateStream[S](ctx, settings.Name, settings)
		}
		return nil, err
	}

	return stream, nil
}
