package tables

import (
	"context"
	"errors"
	"github.com/fatih/structs"
	"ksql/kernel/network"
	"ksql/proxy"
	"ksql/schema"
	"net"
	"reflect"
)

type Table[S any] struct {
	sourceTopic  *string
	sourceStream *string
	partitions   *uint8
	remoteSchema *reflect.Type
	format       schema.ValueFormat
}

var (
	existingTables = make(map[string]TableSettings)
)

var (
	ErrTableDoesNotExist = errors.New("table does not exist")
)

func createTableRemotely[S any](ctx context.Context, conn net.Conn, tableName string, settings TableSettings) (*Table[S], error) {
	format, err := settings.Format.GetName()
	if err != nil {
		return nil, err
	}

	fields := structs.Fields(settings)

	command := "CREATE TABLE " + tableName + " (" + fields[0].Name() + " " + fields[0].Kind().String() + ")" +
		" WITH (" + *settings.SourceTopic + ", " + format + ")"

	response, err := network.Perform(
		ctx,
		conn,
		conn.RemoteAddr().String(),
		len(command),
		command)
	if err != nil {
		return nil, err
	}

	return &Table[S]{
		sourceTopic:  settings.SourceTopic,
		sourceStream: &settings.Name,
		partitions:   settings.Partitions,
		format:       settings.Format,
	}, nil
}

func getTableRemotely[S any](
	ctx context.Context,
	conn net.Conn,
	tableName string) (*Table[S], error) {

	var (
		command = "DESCRIBE " + tableName
	)

	response, err := network.Perform(
		ctx,
		conn,
		conn.RemoteAddr().String(),
		len(command),
		command)
	if err != nil {
		return nil, err
	}

	var (
		dst S
	)

	kinds := schema.Deserialize(response, &dst)

	table := &Table[S]{}

	if _, ok := kinds["topic"]; ok {
		srcTopic := "topic"
		table.sourceTopic = &srcTopic
	}

	if _, ok := kinds["stream"]; ok {
		srcStream := "stream"
		table.sourceStream = &srcStream
	}

	if _, ok := kinds["partitions"]; ok {
		partitions := uint8(1)
		table.partitions = &partitions
	}
	if _, ok := kinds["value_format"]; ok {
		vf := schema.Json
		table.format = vf
	}

	return table, nil
}

func GetTableProjection(ctx context.Context, name string) (*TableSettings, error) {
	tableSettings, exist := existingTables[name]
	if !exist {
		return nil, errors.New("table does not exist")
	}
	return &tableSettings, nil
}

func (s *Table[S]) ToTopic(topicName string) proxy.Topic[S] {
	return proxy.CreateTopicFromTable[S](topicName, s)
}

func (s *Table[S]) ToStream(streamName string) proxy.Stream[S] {
	return proxy.CreateStreamFromTable[S](streamName, s)
}
