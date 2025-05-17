package topics

import (
	"context"
	"ksql/constants"
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

func init() {
	// TODO list and save all topics
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
