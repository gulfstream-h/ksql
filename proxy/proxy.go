package proxy

import (
	"ksql/streams"
	"ksql/tables"
	"ksql/topics"
)

func CreateTopicFromStream[S any](topicName string, table tables.Table[S]) *tables.Table[S] {
	return &tables.Table[S]{}
}

func CreateTopicFromTable[S any](topicName string, stream streams.Stream[S]) *topics.Topic[S] {
	return &topics.Topic[S]{}
}
