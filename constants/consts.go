package constants

import (
	"errors"
	"ksql/kernel/protocol"
	"ksql/streams"
	"ksql/tables"
	"ksql/topics"
)

type (
	QueryPlan = protocol.KafkaSerializer

	TopicSettings  topics.TopicSettings
	StreamSettings streams.StreamSettings
	TableSettings  tables.TableSettings

	Topic[S any]  topics.Topic[S]
	Stream[S any] streams.Stream[S]
	Table[S any]  tables.Table[S]
)

var (
	StreamsProjections = make(
		map[string]StreamSettings,
	)

	TablesProjections = make(
		map[string]TableSettings,
	)
)

var (
	ErrTopicNotExist      = errors.New("topic doesn't exist")
	ErrStreamDoesNotExist = errors.New("stream does not exist")
	ErrTableDoesNotExist  = errors.New("table does not exist")
)
