package streams

import (
	"context"
	"ksql/schema"
)

type StreamSettings struct {
	Name         string
	SourceTopic  *string
	SourceStream *string
	Partitions   *uint8
	format       schema.ValueFormat
	DeleteFunc   func(context.Context)
}

func Register[S any](ctx context.Context, settings StreamSettings) (*Stream[S], error) {
	projection, err := getStreamProjection(ctx, settings.Name)
	if err != nil {
		if settings.SourceTopic != nil {
			stream, err := createStreamRemotely[S](ctx, nil, settings.Name, settings)
			if err != nil {
				return nil, err
			}

			return stream, nil
		}

		return nil, err
	}

	return &Stream[S]{
		sourceTopic:  projection.SourceTopic,
		sourceStream: projection.SourceStream,
		partitions:   projection.Partitions,
		vf:           projection.format,
	}, nil
}
