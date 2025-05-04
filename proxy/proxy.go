package proxy

import (
	"ksql/streams"
	"ksql/tables"
	"ksql/topics"
)

// CRUTCH for import cycle not allowed :))))

type (
	Topic[S any] topics.Topic[S]
)

func CreateTopicFromStream[S any](topicName string, stream *streams.Stream[S]) Topic[S] {
	return Topic[S](topics.Topic[S]{})
}

func CreateTopicFromTable[S any](topicName string, table *tables.Table[S]) Topic[S] {
	return Topic[S](topics.Topic[S]{})
}
