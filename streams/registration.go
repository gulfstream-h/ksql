package streams

import (
	"context"
	"ksql/formats"
)

type StreamSettings struct {
	Name         string
	SourceTopic  *string
	SourceStream *string
	Partitions   *uint8
	format       formats.ValueFormat
	DeleteFunc   func(context.Context)
}

func RegisterStream[S any](ctx context.Context, settings StreamSettings) (*Stream[S], error) {
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
