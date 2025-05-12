package topics

import (
	"bytes"
	"context"
	"fmt"
	"ksql/kernel/network"
	"ksql/kernel/protocol"
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
	Streams map[string]streams.StreamSettings
	Tables  map[string]tables.TableSettings
}

func (t *Topic[S]) ListTopics(ctx context.Context) {
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
		return
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
		return
	case val, ok := <-pipeline:
		if !ok {
			return
		}

		fmt.Println(string(val))
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

	t.ChildObjects.Streams[streamName] = streamSettings

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

	t.ChildObjects.Tables[tableName] = tableSettings

	return tableSettings
}

func (t *Topic[S]) GetAllStreamAdapters() map[string]streams.StreamSettings {
	copyMap := make(map[string]streams.StreamSettings, len(t.ChildObjects.Streams))
	for k, v := range t.ChildObjects.Streams {
		copyMap[k] = v
	}
	return copyMap
}

func (t *Topic[S]) GetAllTableAdapters() map[string]tables.TableSettings {
	copyMap := make(map[string]tables.TableSettings, len(t.ChildObjects.Tables))
	for k, v := range t.ChildObjects.Tables {
		copyMap[k] = v
	}
	return copyMap
}
