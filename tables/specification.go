package tables

import (
	"context"
	"errors"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"ksql/kernel/network"
	"ksql/kernel/protocol"
	"ksql/kernel/protocol/dao"
	"ksql/kernel/protocol/dto"
	"ksql/kinds"
	"ksql/ksql"
	"ksql/schema"
	"ksql/static"
	"net/http"
	"reflect"
)

// Table - is full-functional type,
// providing all ksql-supported operations
// via referred to type functions calls
type Table[S any] struct {
	Name         string
	sourceTopic  *string
	partitions   *uint8
	remoteSchema *reflect.Type
	format       kinds.ValueFormat
}

// ListTables - responses with all tables list
// in the current ksqlDB instance. Also it reloads
// map of available projections
func ListTables(ctx context.Context) (
	dto.ShowTables, error) {

	query := protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.LIST,
			Ref:   ksql.TABLE,
		}}.
		Query()

	pipeline, err := network.Net.Perform(
		ctx,
		http.MethodPost,
		query,
		&network.ShortPolling{},
	)
	if err != nil {
		err = fmt.Errorf("cannot perform request: %w", err)
		return dto.ShowTables{}, err
	}

	select {
	case <-ctx.Done():
		return dto.ShowTables{}, ctx.Err()
	case val, ok := <-pipeline:
		if !ok {
			return dto.ShowTables{}, static.ErrMalformedResponse
		}

		var (
			tables dao.ShowTables
		)

		if err := jsoniter.Unmarshal(val, &tables); err != nil {
			err = errors.Join(static.ErrUnserializableResponse, err)
			return dto.ShowTables{}, err
		}

		return tables.DTO(), nil
	}
}

// Describe - responses with table description.
// Can be used for table schema and query by which
// it was created
func (s *Table[S]) Describe(ctx context.Context) (dto.RelationDescription, error) {
	query := protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.DESCRIBE,
			Name:  s.Name,
		},
	}.Query()

	pipeline, err := network.Net.Perform(
		ctx,
		http.MethodPost,
		query,
		&network.ShortPolling{},
	)
	if err != nil {
		err = fmt.Errorf("cannot perform request: %w", err)
		return dto.RelationDescription{}, err
	}

	select {
	case <-ctx.Done():
		return dto.RelationDescription{}, ctx.Err()
	case val, ok := <-pipeline:
		if !ok {
			return dto.RelationDescription{}, static.ErrMalformedResponse
		}

		var (
			describe dao.DescribeResponse
		)

		if err := jsoniter.Unmarshal(val, &describe); err != nil {
			err = errors.Join(static.ErrUnserializableResponse, err)
			return dto.RelationDescription{}, err
		}

		return describe.DTO(), nil
	}
}

// Drop - drops table from ksqlDB instance
// with parent topic. Also deletes projection from list
func (s *Table[S]) Drop(ctx context.Context) error {
	query := protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.DROP,
			Ref:   ksql.TABLE,
			Name:  s.Name,
		},
	}.Query()

	pipeline, err := network.Net.Perform(
		ctx,
		http.MethodPost,
		query,
		&network.ShortPolling{},
	)
	if err != nil {
		return fmt.Errorf("cannot perform request: %w", err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case val, ok := <-pipeline:
		if !ok {
			return static.ErrMalformedResponse
		}

		var (
			drop dao.DropInfo
		)

		if err := jsoniter.Unmarshal(val, &drop); err != nil {
			return fmt.Errorf("cannot unmarshal drop response: %w", err)
		}

		if drop.CommandStatus.Status != static.SUCCESS {
			return fmt.Errorf("cannot drop table: %s", drop.CommandStatus.Status)
		}

		return nil
	}
}

// GetTable - gets table from ksqlDB instance
// by receiving http description from settings
// current command return difference between
// struct tags and remote schema
func GetTable[S any](
	ctx context.Context,
	table string,
	settings TableSettings) (*Table[S], error) {

	var (
		s S
	)

	scheme := schema.SerializeProvidedStruct(s)

	tableInstance := &Table[S]{
		Name:         table,
		partitions:   settings.Partitions,
		remoteSchema: &scheme,
		format:       settings.Format,
	}
	desc, err := tableInstance.Describe(ctx)
	if err != nil {
		if errors.Is(err, static.ErrTableDoesNotExist) {
			return nil, err
		}
		return nil, fmt.Errorf("cannot describe table: %w", err)
	}

	var (
		responseSchema = map[string]string{}
	)

	for _, field := range desc.Fields {
		responseSchema[field.Name] = field.Kind
	}

	if responseSchema == nil {
		return nil, static.ErrTableDoesNotExist
	}

	remoteSchema := schema.SerializeRemoteSchema(responseSchema)
	matchMap, diffMap := schema.CompareStructs(scheme, remoteSchema)

	if len(matchMap) == 0 {
		fmt.Println(diffMap)
		return nil, errors.New("schemes doesnt match")
	}

	return tableInstance, nil
}

// CreateTable - creates table in ksqlDB instance
// after creating, user should call
// select or select with emit to get data from it
func CreateTable[S any](
	ctx context.Context,
	tableName string,
	settings TableSettings) (*Table[S], error) {

	var (
		s S
	)

	rmSchema := schema.SerializeProvidedStruct(s)

	query := protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.CREATE,
			Ref:   ksql.TABLE,
			Name:  tableName,
		},
		SchemaAlgo: schema.ParseStructToFields(
			tableName,
			rmSchema,
		),
		MetadataAlgo: ksql.With{
			Topic:       *settings.SourceTopic,
			ValueFormat: kinds.JSON.String(),
		},
	}.Query()

	pipeline, err := network.Net.Perform(
		ctx,
		http.MethodPost,
		query,
		&network.ShortPolling{},
	)
	if err != nil {
		return nil, fmt.Errorf("cannot perform request: %w", err)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case val, ok := <-pipeline:
		if !ok {
			return nil, static.ErrMalformedResponse
		}

		var (
			create dao.CreateRelationResponse
		)

		if err := jsoniter.Unmarshal(val, &create); err != nil {
			return nil, fmt.Errorf("cannot unmarshal create response: %w", err)
		}

		if len(create) < 1 {
			return nil, fmt.Errorf("unsuccessful response")
		}

		status := create[0]

		if status.CommandStatus.Status != static.SUCCESS {
			return nil, fmt.Errorf("unsuccesful respose. msg: %s", status.CommandStatus.Message)
		}

		return &Table[S]{
			sourceTopic:  settings.SourceTopic,
			partitions:   settings.Partitions,
			remoteSchema: &rmSchema,
			format:       settings.Format,
		}, nil
	}
}

// CreateTableAsSelect - creates table in ksqlDB instance
// with user built query
// after creating, user should call
// select or select with emit to get data from it
func CreateTableAsSelect[S any](
	ctx context.Context,
	tableName string,
	settings TableSettings,
	query static.QueryPlan) (*Table[S], error) {

	var (
		s S
	)

	scheme := schema.SerializeProvidedStruct(s)
	rmScheme := schema.SerializeFieldsToStruct(query.SchemaAlgo)

	matchMap, diffMap := schema.CompareStructs(scheme, rmScheme)

	if len(matchMap) == 0 {
		fmt.Println(diffMap)
		return nil, errors.New("schemes doesnt match")
	}

	q := protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.CREATE,
			Ref:   ksql.TABLE,
			Name:  tableName,
		},
		SchemaAlgo: query.SchemaAlgo,
		MetadataAlgo: ksql.With{
			Topic:       *settings.SourceTopic,
			ValueFormat: kinds.JSON.String(),
		},
		CTE: map[string]protocol.KafkaSerializer{
			"AS": query,
		},
	}.Query()

	pipeline, err := network.Net.Perform(
		ctx,
		http.MethodPost,
		q,
		&network.ShortPolling{},
	)
	if err != nil {
		return nil, fmt.Errorf("cannot perform request: %w", err)
	}

	select {
	case <-ctx.Done():
		return nil, errors.New("context is done")
	case val, ok := <-pipeline:
		if !ok {
			return nil, errors.New("drop response channel is closed")
		}

		var (
			create dao.CreateRelationResponse
		)

		if err := jsoniter.Unmarshal(val, &create); err != nil {
			return nil, fmt.Errorf("cannot unmarshal create response: %w", err)
		}

		if len(create) < 1 {
			return nil, fmt.Errorf("unsuccessful response")
		}

		status := create[0]

		if status.CommandStatus.Status != static.SUCCESS {
			return nil, fmt.Errorf("unsuccesful respose. msg: %s", status.CommandStatus.Message)
		}

		return &Table[S]{
			sourceTopic:  settings.SourceTopic,
			partitions:   settings.Partitions,
			remoteSchema: &scheme,
			format:       settings.Format,
		}, nil
	}
}

// SelectOnce - performs select query
// and return only one http answer
// channel is closed almost immediately
func (s *Table[S]) SelectOnce(
	ctx context.Context) (S, error) {

	var (
		value S
	)

	query := protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.SELECT,
			Ref:   ksql.TABLE,
			Name:  s.Name,
		},
		SchemaAlgo: schema.ParseStructToFields(
			s.Name,
			*s.remoteSchema,
		),
		MetadataAlgo: ksql.With{
			ValueFormat: kinds.JSON.String(),
		},
	}.Query()

	pipeline, err := network.Net.Perform(
		ctx,
		http.MethodPost,
		query,
		&network.ShortPolling{},
	)
	if err != nil {
		return value, fmt.Errorf("cannot perform request: %w", err)
	}

	select {
	case <-ctx.Done():
		return value, ctx.Err()
	case val, ok := <-pipeline:
		if !ok {
			return value, static.ErrMalformedResponse
		}

		if err := jsoniter.Unmarshal(val, &value); err != nil {
			return value, fmt.Errorf("cannot unmarshal select response: %w", err)
		}

		return value, nil
	}
}

// SelectWithEmit - performs
// select with emit request
// answer is received for every new record
// and propagated to channel
func (s *Table[S]) SelectWithEmit(
	ctx context.Context) (<-chan S, error) {

	var (
		value   S
		valuesC = make(chan S)
	)

	query := protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.SELECT,
			Ref:   ksql.TABLE,
			Name:  s.Name,
		},
		SchemaAlgo: schema.ParseStructToFields(
			s.Name,
			*s.remoteSchema,
		),
		MetadataAlgo: ksql.With{
			ValueFormat: kinds.JSON.String(),
		},
	}.Query()

	pipeline, err := network.Net.Perform(
		ctx,
		http.MethodPost,
		query,
		&network.ShortPolling{},
	)
	if err != nil {
		return nil, fmt.Errorf("cannot perform request: %w", err)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				close(valuesC)
				return
			case val, ok := <-pipeline:
				if !ok {
					close(valuesC)
					return
				}
				if err := jsoniter.Unmarshal(val, &value); err != nil {
					close(valuesC)
					return
				}

				valuesC <- value
			}
		}
	}()

	return valuesC, nil
}

// ToTopic - propagates table data to new topic
// table scheme is fully extended to new topic
func (s *Table[S]) ToTopic(topicName string) (topic static.Topic[S]) {
	topic.Name = topicName
	topic.Partitions = int(*s.partitions)

	return
}

// ToStream - propagates table data to new stream
// and shares schema with it
func (s *Table[S]) ToStream(streamName string) (stream static.Stream[S]) {
	static.StreamsProjections.Store(streamName, static.StreamSettings{
		Name:        streamName,
		SourceTopic: s.sourceTopic,
		Partitions:  s.partitions,
	})

	stream.Name = streamName

	return
}
