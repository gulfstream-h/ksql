package topics

import (
	"bytes"
	"context"
	jsoniter "github.com/json-iterator/go"
	"ksql/kernel/network"
	"ksql/kernel/protocol"
	"ksql/kernel/protocol/dao"
	"ksql/kernel/protocol/dto"
	"ksql/ksql"
	"ksql/streams"
	"ksql/tables"
	"net/http"
)

type Topic[S any] struct {
	Name              string
	Partitions        int
	ReplicationFactor int
	ChildObjects      ChildTopicObjects
}

type ChildTopicObjects struct {
	Streams map[string]struct{}
	Tables  map[string]struct{}
}

func ListTopics(ctx context.Context) dto.ShowTopics {
	query := []byte(
		protocol.KafkaSerializer{
			QueryAlgo: ksql.Query{
				Query: ksql.LIST,
				Ref:   ksql.TOPIC,
			}}.
			Query())

	var (
		pipeline = make(chan []byte)
	)

	req, err := http.NewRequest(
		"POST",
		"localhost:8080",
		bytes.NewReader(query))
	if err != nil {
		return dto.ShowTopics{}
	}

	req.Header.Set(
		"Content-Type",
		"application/json")

	go func() {
		network.Net.PerformRequest(
			req,
			&network.SingeHandler{
				MaxRPS:   100,
				Pipeline: pipeline,
			},
		)
	}()

	select {
	case <-ctx.Done():
		return dto.ShowTopics{}
	case val, ok := <-pipeline:
		if !ok {
			return dto.ShowTopics{}
		}

		var (
			topics dao.ShowTopics
		)

		if err = jsoniter.Unmarshal(val, &topics); err != nil {
			return dto.ShowTopics{}
		}

		return topics.DTO()
	}
}

func (t *Topic[S]) RegisterStream(streamName string) streams.StreamSettings {
	partitions := uint8(t.Partitions)

	streamSettings := streams.StreamSettings{
		Name:        streamName,
		SourceTopic: &t.Name,
		Partitions:  &partitions,
		DeleteFunc: func(ctx context.Context) {
			delete(t.ChildObjects.Streams, streamName)
		},
	}

	t.ChildObjects.Streams[streamName] = struct{}{}

	return streamSettings
}

func (t *Topic[S]) RegisterTable(tableName string) tables.TableSettings {
	partitions := uint8(t.Partitions)

	tableSettings := tables.TableSettings{
		Name:        tableName,
		SourceTopic: &t.Name,
		Partitions:  &partitions,
		DeleteFunc: func(ctx context.Context) {
			delete(t.ChildObjects.Tables, tableName)
		},
	}

	t.ChildObjects.Tables[tableName] = struct{}{}

	return tableSettings
}

func (t *Topic[S]) GetAllStreamAdapters() map[string]struct{} {
	copyMap := make(map[string]struct{}, len(t.ChildObjects.Streams))
	for k := range t.ChildObjects.Streams {
		copyMap[k] = struct{}{}
	}
	return copyMap
}

func (t *Topic[S]) GetAllTableAdapters() map[string]struct{} {
	copyMap := make(map[string]struct{}, len(t.ChildObjects.Tables))
	for k := range t.ChildObjects.Tables {
		copyMap[k] = struct{}{}
	}
	return copyMap
}
