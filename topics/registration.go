package topics

import (
	"context"
)

type TopicSettings struct {
	Partitions   int
	Replications int
}

func Register[T any](ctx context.Context, topicName string, makeSettings *TopicSettings) (*Topic[T], error) {
	settings, err := GetTopicProjection(ctx, topicName)
	if err != nil {
		if makeSettings != nil {
			topic, err := createTopicRemotely[T](ctx, nil, topicName, *settings)
			if err != nil {
				return nil, err
			}

			return topic, nil
		}
	}

	return &Topic[T]{
		Name:              topicName,
		Partitions:        settings.Partitions,
		ReplicationFactor: settings.Replications,
	}, nil
}
