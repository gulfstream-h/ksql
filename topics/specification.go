package topics

import (
	"context"
	"errors"
	"ksql/kernel/network"
	"ksql/streams"
	"ksql/tables"
	"net"
	"strconv"
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
	onStart        sync.Once
	onStartErr     error
	existingTopics = make(map[string]*Topic)
)

var (
	ErrCannotCreateTopicWithEmptySettings = errors.New("cannot create topic with empty settings")
	ErrStreamDoesNotExist                 = errors.New("stream does not exist")
	ErrTableDoesNotExist                  = errors.New("table does not exist")
)

func getTopics(
	ctx context.Context,
	conn net.Conn) map[string]*Topic {

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
			existingTopics[topic] = &Topic{
				Name:              topic,
				Partitions:        1,
				ReplicationFactor: 1,
				ChildObjects: ChildTopicObjects{
					Streams: make(map[string]streams.StreamSettings),
					Tables:  make(map[string]tables.TableSettings),
				},
			}
		}
	},
	)

	return existingTopics
}

func getOrCreateTopic(
	ctx context.Context,
	conn net.Conn,
	name string,
	settings *TopicSettings) (*Topic, error) {

	var (
		err error
	)

	topic, exists := existingTopics[name]
	if exists {
		return topic, nil
	}

	topic, err = getTopicRemotely(ctx, conn, name)
	if err == nil {
		existingTopics[name] = topic
		return topic, nil
	}

	if settings == nil {
		return nil, ErrCannotCreateTopicWithEmptySettings
	}

	return createTopicRemotely(ctx, conn, name, *settings)
}

// GetTopicRemotely retrieves the topic information from the Kafka broker
// using the provided connection. That method should be used after in-code check-ups
// or in case of high-consistency requirements
func getTopicRemotely(
	ctx context.Context,
	conn net.Conn,
	name string) (*Topic, error) {

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
		return nil, errors.New("Invalid topic format")
	}

	topicName := topic[0]
	partitions, err := strconv.Atoi(topic[1])
	if err != nil {
		return nil, err
	}

	replicationFactor, err := strconv.Atoi(topic[2])
	if err != nil {
		return nil, err
	}

	return &Topic{
		Name:              topicName,
		Partitions:        partitions,
		ReplicationFactor: replicationFactor,
		ChildObjects: ChildTopicObjects{
			Streams: make(map[string]streams.StreamSettings),
			Tables:  make(map[string]tables.TableSettings),
		},
	}, nil
}

// createTopicRemotely creates a topic with the given name and settings in all kafka
// brokers, replications.
// Current request is send to the leader broker, if in-code check-up haven't found current topic
// If the settings are empty, it won't create the topic
func createTopicRemotely(
	ctx context.Context,
	conn net.Conn,
	name string,
	settings TopicSettings) (*Topic, error) {

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

	if err = validateResponse(response); err != nil {
		return nil, err
	}

	return &Topic{
		Name:              name,
		Partitions:        settings.Partitions,
		ReplicationFactor: settings.Replications,
		ChildObjects: ChildTopicObjects{
			Streams: make(map[string]streams.StreamSettings),
			Tables:  make(map[string]tables.TableSettings),
		},
	}, nil
}

func (t *Topic) GetStreamAdapter(ctx context.Context) (*streams.StreamSettings, error) {
	stream, exists := t.ChildObjects.Streams[t.Name]
	if !exists {
		return &stream, ErrStreamDoesNotExist
	}
	return &stream, nil
}

func (t *Topic) GetAllStreamAdapters(ctx context.Context) map[string]streams.StreamSettings {
	copyMap := make(map[string]streams.StreamSettings, len(t.ChildObjects.Streams))
	for k, v := range t.ChildObjects.Streams {
		copyMap[k] = v
	}
	return copyMap
}

func (t *Topic) GetTableAdapter() (*tables.TableSettings, error) {
	table, exists := t.ChildObjects.Tables[t.Name]
	if !exists {
		return &table, ErrTableDoesNotExist
	}
	return &table, nil
}

func (t *Topic) GetAllTableAdapters(ctx context.Context) map[string]tables.TableSettings {
	copyMap := make(map[string]tables.TableSettings, len(t.ChildObjects.Tables))
	for k, v := range t.ChildObjects.Tables {
		copyMap[k] = v
	}
	return copyMap
}

func validateResponse(response []byte) error {
	if strings.Contains(string(response), "error") {
		return errors.New("error creating topic: " + string(response))
	}
	return nil
}
