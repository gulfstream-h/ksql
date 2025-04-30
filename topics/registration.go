package topics

import (
	"context"
	"fmt"
	"ksql/streams"
)

type TopicSettings struct {
	Partitions   int
	Replications int
}

func RegisterTopic[T any](ctx context.Context, topicName string, makeSettings *TopicSettings) (*Topic[T], error) {
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

func Example() {
	ctx := context.Background()
	topicName := "example_topic"
	settings := &TopicSettings{
		Partitions:   3,
		Replications: 2,
	}

	topic, err := RegisterTopic[TopicSchemeDAO](ctx, topicName, settings)
	if err != nil {
		// Handle error
		return
	}

	streamSettings := topic.RegisterStream("example_stream")

	stream, err := streams.RegisterStream[StreamSchemeDAO](ctx, streamSettings)
	if err != nil {
		// Handle error
		return
	}

	streamChan, err := stream.SelectWithEmit(ctx, "SELECT * FROM example_stream EMIT CHANGES")
	if err != nil {
		// Handle error
		return
	}

	var a StreamSchemeDAO = <-streamChan
	fmt.Println(a.Name)

}

type TopicSchemeDAO struct {
	Name string
}

type StreamSchemeDAO struct {
	Name string
}
