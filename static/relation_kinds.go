package static

import (
	"ksql/kernel/protocol"
	"ksql/streams"
	"ksql/tables"
	"ksql/topics"
)

// Current file describes internal types
// That should be used to avoid import cycles
// Between project folders

type (
	QueryPlan = protocol.KafkaSerializer

	TopicSettings  topics.TopicSettings
	StreamSettings streams.StreamSettings
	TableSettings  tables.TableSettings

	Topic[S any]  topics.Topic[S]
	Stream[S any] streams.Stream[S]
	Table[S any]  tables.Table[S]
)

// In circumstances, that ksql provides functionality
// to create, as an example, topic from stream or stream from topic
// that static types can be useful for specific casting
