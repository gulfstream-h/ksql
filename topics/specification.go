package topics

import (
	"context"
	"errors"
	"ksql/kernel/network"
	"ksql/streams"
	"ksql/tables"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

type Topic[S any] struct {
	Name              string
	Partitions        int
	ReplicationFactor int
	RemoteSchema      *reflect.Type
	ChildObjects      ChildTopicObjects
}

type ChildTopicObjects struct {
	Streams map[string]streams.StreamSettings
	Tables  map[string]tables.TableSettings
}

var (
	onStart        sync.Once
	onStartErr     error
	existingTopics = make(map[string]*TopicSettings)
)

var (
	ErrStreamDoesNotExist = errors.New("stream does not exist")
	ErrTableDoesNotExist  = errors.New("table does not exist")
)

func getTopics(
	ctx context.Context,
	conn net.Conn) map[string]*TopicSettings {

	onStart.Do(func() {
		var (
			response []byte
			command  = "GET TOPICS"
		)

		response, onStartErr = network.Perform(
			ctx,
			conn,
			conn.RemoteAddr().String(),
			len(command),
			command)
		if onStartErr != nil {
			return
		}

		topics := strings.Split(string(response), ",")

		for _, topic := range topics {
			existingTopics[topic] = &TopicSettings{
				Partitions:   1,
				Replications: 1,
			}
		}
	},
	)

	return existingTopics
}

// GetTopicRemotely retrieves the topic information from the Kafka broker
// using the provided connection. That method should be used after in-code check-ups
// or in case of high-consistency requirements
func getTopicRemotely[S any](
	ctx context.Context,
	conn net.Conn,
	name string) (*Topic[S], error) {

	command := "GET TOPIC " + name
	response, err := network.Perform(
		ctx,
		conn,
		conn.RemoteAddr().String(),
		len(command),
		command)
	if err != nil {
		return nil, err
	}

	topic := strings.Split(string(response), ",")
	if len(topic) < 3 {
		return nil, errors.New("invalid topic format")
	}

	partitions, err := strconv.Atoi(topic[1])
	if err != nil {
		return nil, err
	}

	replicationFactor, err := strconv.Atoi(topic[2])
	if err != nil {
		return nil, err
	}

	return &Topic[S]{
		Partitions:        partitions,
		ReplicationFactor: replicationFactor,
	}, nil
}

// createTopicRemotely creates a topic with the given name and settings in all kafka
// brokers, replications.
// Current request is send to the leader broker, if in-code check-up haven't found current topic
// If the settings are empty, it won't create the topic
func createTopicRemotely[S interface{}](
	ctx context.Context,
	conn net.Conn,
	name string,
	settings TopicSettings) (*Topic[S], error) {

	command := "CREATE TOPIC " + name + " PARTITIONS " + strconv.Itoa(settings.Partitions)

	response, err := network.Perform(
		ctx,
		conn,
		conn.RemoteAddr().String(),
		len(command),
		command)
	if err != nil {
		return nil, err
	}

	return &Topic[S]{
		Name:              name,
		Partitions:        settings.Partitions,
		ReplicationFactor: settings.Replications,
		ChildObjects: ChildTopicObjects{
			Streams: make(map[string]streams.StreamSettings),
			Tables:  make(map[string]tables.TableSettings),
		},
	}, nil
}

func GetTopicProjection(ctx context.Context, name string) (*TopicSettings, error) {
	topicSettings, exist := existingTopics[name]
	if !exist {
		return nil, errors.New("topic does not exist")
	}
	return topicSettings, nil
}

func (t *Topic[S]) GetStreamAdapter() (*streams.StreamSettings, error) {
	stream, exists := t.ChildObjects.Streams[t.Name]
	if !exists {
		return &stream, ErrStreamDoesNotExist
	}
	return &stream, nil
}

func (t *Topic[S]) GetAllStreamAdapters() map[string]streams.StreamSettings {
	copyMap := make(map[string]streams.StreamSettings, len(t.ChildObjects.Streams))
	for k, v := range t.ChildObjects.Streams {
		copyMap[k] = v
	}
	return copyMap
}

func (t *Topic[S]) GetTableAdapter() (*tables.TableSettings, error) {
	table, exists := t.ChildObjects.Tables[t.Name]
	if !exists {
		return &table, ErrTableDoesNotExist
	}
	return &table, nil
}

func (t *Topic[S]) GetAllTableAdapters() map[string]tables.TableSettings {
	copyMap := make(map[string]tables.TableSettings, len(t.ChildObjects.Tables))
	for k, v := range t.ChildObjects.Tables {
		copyMap[k] = v
	}
	return copyMap
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
