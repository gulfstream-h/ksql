package topics

import (
	"context"
)

type TopicSettings struct {
	Partitions   int
	Replications int
}

func RegisterTopic[T any](ctx context.Context, topicName string, schema T) {
	var (
		topic  Topic
		exists bool
	)

	for _, topic = range GetTopics(ctx, nil) {
		if topic.Name == topicName {
			exists = true
			break
		}
	}

	if !exists {
		CreateTopic(ctx, nil, topicName, 1)
	}

}
