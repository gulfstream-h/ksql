package topics

import (
	"context"
	"ksql/formats"
	"ksql/nets"
	"ksql/streams"
	"ksql/tables"
	"net"
	"strings"
	"sync"
)

type Topic struct {
	Name              string
	Partitions        int
	ReplicationFactor int
	ChildObjects      ChildTopicObjects
}

type ChildTopicObjects struct {
	Streams map[string]streams.StreamSettings
	Tables  map[string]tables.TableSettings
}

var (
	initialization sync.Once
	ExistingTopics = make(map[string]Topic)
)

func GetTopics(ctx context.Context, conn net.Conn) map[string]Topic {

	initialization.Do(func() {

	},
	)

	return ExistingTopics

	command := "GET TOPICS"
	response, err := nets.Perform(
		ctx,
		conn,
		conn.RemoteAddr().String(),
		len(command),
		command)
	if err != nil {
		return nil
	}

	topics := strings.Split(string(response), ",")

	var topicList []Topic
	for _, topic := range topics {
		topicList = append(topicList, Topic(topic))
	}

	return topicList
}

func CreateTopic(ctx context.Context, conn net.Conn, topicName string, partitions int) Topic {
	command := "CREATE TOPIC " + topicName + " PARTITIONS " + string(partitions)
	response, err := nets.Perform(
		ctx,
		conn,
		conn.RemoteAddr().String(),
		len(command),
		command)
	if err != nil {
		return Topic{}
	}

	println(string(response))

	return Topic{
		Name:       topicName,
		Partitions: 3,
	}
}

func (t Topic) GetBoundStreams() {
	type Data struct {
		Name string `ksql:"name"`
	}

	str, _ := streams.Create("data", Data{}, nil, nil, nil, formats.String)

	t.Streams = append(t.Streams, str)
}

func (t Topic) GetBoundTables() {
	type Data struct {
		Name string `ksql:"name"`
	}

	str, _ := streams.Create("data", Data{}, nil, nil, nil, formats.String)

	t.Tables = append(t.Tables, str)

	type NextData struct {
		Name string `ksql:"name"`
	}

	// tbl is any
	tbl := t.Tables[0]

	switch tbl.(type) {
	case *streams.Stream[Data]:
	case *streams.Stream[NextData]:
	}
}
