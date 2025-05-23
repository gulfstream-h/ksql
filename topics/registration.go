package topics

import (
	"context"
	"ksql/static"
	"ksql/streams"
	"ksql/tables"
	"log"
	"time"
)

type (
	TopicSettings struct {
		Partitions   int
		Replications int
	}
)

var (
	// topicProjections stores all pre-fetched on init() settings from ksql-client
	topicProjections = make(
		map[string]TopicSettings,
	)
)

// RegisterTopic - by topic name, that func invokes a full code-instance of topic, inherited from settings
// full instance supports streams and tables creation with introspected from topic schema
func RegisterTopic[S any](
	ctx context.Context,
	name string) (*Topic[S], error) {

	topicSettings, err := GetTopicProjection(ctx, name)
	if err != nil {
		return nil, err
	}

	streamsList, err := streams.ListStreams(ctx)
	if err != nil {
		return nil, err
	}

	tablesList, err := tables.ListTables(ctx)
	if err != nil {
		return nil, err
	}

	streamsMap := map[string]struct{}{}
	tablesMap := map[string]struct{}{}

	for _, stream := range streamsList.Streams {
		if stream.Topic != name {
			continue
		}
		streamsMap[stream.Name] = struct{}{}
	}

	for _, table := range tablesList.Tables {
		if table.Topic != name {
			continue
		}
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
	}, nil
}

// init() - lists all topics existing in kafka
func init() {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		time.Duration(30)*time.Second)
	defer cancel()

	topics, err := ListTopics(ctx)
	if err != nil {
		log.Fatalf("cannot init ksql by listing topics: %w", err)
	}

	for _, topic := range topics.Topics {
		topicProjections[topic.Name] = TopicSettings{}
	}
}

// GetTopicProjection - safe and encapsulated wrapper for receiving in-memory
// settings of topic. Can be used in outer packages
func GetTopicProjection(
	ctx context.Context,
	name string) (*TopicSettings, error) {

	topicSettings, exist := topicProjections[name]
	if !exist {
		return nil, static.ErrTopicNotExist
	}

	return &topicSettings, nil
}
