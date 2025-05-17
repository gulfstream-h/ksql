package topics

import (
	"context"
	"ksql/constants"
	"ksql/streams"
	"ksql/tables"
	"time"
)

type (
	TopicSettings struct {
		Partitions   int
		Replications int
	}
)

var (
	topicProjections = make(
		map[string]TopicSettings,
	)
)

func RegisterTopic[S any](
	ctx context.Context,
	name string) *Topic[S] {

	topicSettings, err := GetTopicProjection(ctx, name)
	if err != nil {
		return nil
	}

	streamsList := streams.ListStreams(ctx)
	tablesList := tables.ListTables(ctx)

	streamsMap := map[string]struct{}{}
	tablesMap := map[string]struct{}{}

	for _, stream := range streamsList.Streams {
		streamsMap[stream.Name] = struct{}{}
	}

	for _, table := range tablesList.Tables {
		tablesMap[table.Name] = struct{}{}
	}

	return &Topic[S]{
		Name:              name,
		Partitions:        topicSettings.Partitions,
		ReplicationFactor: topicSettings.Replications,
		ChildObjects: ChildTopicObjects{
			Streams: streamsMap,
			Tables:  tablesMap,
		},
	}
}

func init() {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Duration(30)*time.Second)
	defer cancel()

	topics := ListTopics(ctx)

	for _, topic := range topics.Topics {
		topicProjections[topic.Name] = TopicSettings{}
	}
}

func GetTopicProjection(
	ctx context.Context,
	name string) (*TopicSettings, error) {

	topicSettings, exist := topicProjections[name]
	if !exist {
		return nil, constants.ErrTopicNotExist
	}

	return &topicSettings, nil
}
