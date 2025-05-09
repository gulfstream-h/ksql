package proxy

import (
	"ksql/streams"
	"ksql/tables"
	"ksql/topics"
)

// CRUTCH for import cycle not allowed :))))

type (
	Topic[S any]  topics.Topic[S]
	Stream[S any] streams.Stream[S]
	Table[S any]  tables.Table[S]
)

func CreateTopicFromStream[S any](topicName string, stream *streams.Stream[S]) Topic[S] {
	return Topic[S](topics.Topic[S]{})
}

func CreateTopicFromTable[S any](topicName string, table *tables.Table[S]) Topic[S] {
	return Topic[S](topics.Topic[S]{})
}

func CreateStreamFromTable[S any](streamName string, table *tables.Table[S]) Stream[S] {
	return Stream[S](streams.Stream[S]{})
}

func CreateTableFromStream[S any](tableName string, stream *streams.Stream[S]) Table[S] {
	return Table[S](tables.Table[S]{})
}
