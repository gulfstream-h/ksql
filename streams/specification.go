package streams

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
	"reflect"
)

// Stream - is full-functional type,
// providing all ksql-supported operations
// via referred to type functions calls
type Stream[S any] struct {
	Name         string
	sourceTopic  *string
	sourceStream *string
	partitions   *uint8
	remoteSchema *reflect.Type
	format       kinds.ValueFormat
}

// ListStreams - responses with all streams list
// in the current ksqlDB instance. Also it reloads
// map of available projections
func ListStreams(ctx context.Context) (dto.ShowStreams, error) {
	query := protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.LIST,
			Ref:   ksql.STREAM,
		}}.
		Query()

	var (
		pipeline = make(chan []byte)
	)

	if err := network.Net.Perform(
		ctx,
		query,
		pipeline,
		&network.ShortPolling{},
	); err != nil {
		err = fmt.Errorf("cannot perform request: %w", err)
		return dto.ShowStreams{}, err
	}

	select {
	case <-ctx.Done():
		return dto.ShowStreams{}, ctx.Err()
	case val, ok := <-pipeline:
		if !ok {
			return dto.ShowStreams{}, static.ErrMalformedResponse
		}

		var (
			streams dao.ShowStreams
		)

		if err := jsoniter.Unmarshal(val, &streams); err != nil {
			err = errors.Join(static.ErrUnserializableResponse, err)
			return dto.ShowStreams{}, err
		}

		return streams.DTO(), nil
	}
}

// Describe - responses with stream description.
// Can be used for table schema and query by which
// it was created
func (s *Stream[S]) Describe(ctx context.Context) (dto.RelationDescription, error) {
	query := protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.DESCRIBE,
			Name:  s.Name,
		},
	}.Query()

	var (
		pipeline = make(chan []byte)
	)

	if err := network.Net.Perform(
		ctx,
		query,
		pipeline,
		&network.ShortPolling{},
	); err != nil {

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

// Drop - drops stream from ksqlDB instance
// with parent topic. Also deletes projection from list
func (s *Stream[S]) Drop(ctx context.Context) error {
	query := protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.DROP,
			Ref:   ksql.STREAM,
			Name:  s.Name,
		},
	}.Query()

	var (
		pipeline = make(chan []byte)
	)

	if err := network.Net.Perform(
		ctx,
		query,
		pipeline,
		&network.ShortPolling{},
	); err != nil {
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

		if drop.CommandStatus.Status != "SUCCESS" {
			return fmt.Errorf("cannot drop stream: %s", drop.CommandStatus.Status)
		}

		return nil
	}
}

// GetStream - gets table from ksqlDB instance
// by receiving http description from settings
// current command return difference between
// struct tags and remote schema
func GetStream[S any](
	ctx context.Context,
	stream string,
	settings StreamSettings) (*Stream[S], error) {

	var (
		s S
	)

	scheme := schema.SerializeProvidedStruct(s)

	streamInstance := &Stream[S]{
		Name:         stream,
		sourceStream: nil,
		partitions:   settings.Partitions,
		remoteSchema: &scheme,
		format:       settings.Format,
	}
	desc, err := streamInstance.Describe(ctx)
	if err != nil {
		if errors.Is(err, static.ErrStreamDoesNotExist) {
			return nil, err
		}
		return nil, fmt.Errorf("cannot get stream description: %w", err)
	}

	var (
		responseSchema = map[string]string{}
	)

	for _, field := range desc.Fields {
		responseSchema[field.Name] = field.Kind
	}

	if responseSchema == nil {
		return nil, static.ErrStreamDoesNotExist
	}

	remoteSchema := schema.SerializeRemoteSchema(responseSchema)
	matchMap, diffMap := schema.CompareStructs(scheme, remoteSchema)

	if len(matchMap) == 0 {
		fmt.Println(diffMap)
		return nil, errors.New("schemes doesnt match")
	}

	return streamInstance, nil
}

// CreateStream - creates stream in ksqlDB instance
// after creating, user should call
// select or select with emit to get data from it
func CreateStream[S any](
	ctx context.Context,
	streamName string,
	settings StreamSettings) (*Stream[S], error) {

	var (
		s S
	)

	rmSchema := schema.SerializeProvidedStruct(s)

	query := protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.CREATE,
			Ref:   ksql.STREAM,
			Name:  streamName,
		},
		SchemaAlgo: schema.DeserializeStructToFields(
			streamName,
			rmSchema,
		),
		MetadataAlgo: ksql.With{
			Topic:       *settings.SourceTopic,
			ValueFormat: kinds.JSON.String(),
		},
	}.Query()

	var (
		pipeline = make(chan []byte)
	)

	if err := network.Net.Perform(
		ctx,
		query,
		pipeline,
		&network.ShortPolling{},
	); err != nil {
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

		if status.CommandStatus.Status != "SUCCESSFUL" {
			return nil, fmt.Errorf("unsuccesful respose. msg: %s", status.CommandStatus.Message)
		}

		return &Stream[S]{
			sourceTopic:  settings.SourceTopic,
			sourceStream: &streamName,
			partitions:   settings.Partitions,
			remoteSchema: &rmSchema,
			format:       settings.Format,
		}, nil
	}
}

// CreateStreamAsSelect - creates table in ksqlDB instance
// with user built query
// after creating, user should call
// select or select with emit to get data from it
func CreateStreamAsSelect[S any](
	ctx context.Context,
	streamName string,
	settings StreamSettings,
	query static.QueryPlan) (*Stream[S], error) {

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
			Ref:   ksql.STREAM,
			Name:  streamName,
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

	var (
		pipeline = make(chan []byte)
	)

	if err := network.Net.Perform(
		ctx,
		q,
		pipeline,
		&network.ShortPolling{},
	); err != nil {
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

		if status.CommandStatus.Status != "SUCCESSFUL" {
			return nil, fmt.Errorf("unsuccesful respose. msg: %s", status.CommandStatus.Message)
		}

		return &Stream[S]{
			sourceTopic:  settings.SourceTopic,
			sourceStream: &streamName,
			partitions:   settings.Partitions,
			remoteSchema: &scheme,
			format:       settings.Format,
		}, nil
	}
}

// Insert - provides insertion to stream functionality
// written fields are defined by user
func (s *Stream[S]) Insert(
	ctx context.Context,
	fields map[string]string) error {

	scheme := *s.remoteSchema

	remoteProjection := schema.DeserializeStructToFieldsDictionary(
		s.Name,
		scheme,
	)

	var (
		searchFields []schema.SearchField
	)

	for key, value := range fields {
		field, ok := remoteProjection[key]
		if !ok {
			continue
		}

		field.Value = &value

		searchFields = append(searchFields, field)
	}

	query := protocol.KafkaSerializer{
		SchemaAlgo: searchFields,
		QueryAlgo: ksql.Query{
			Query: ksql.INSERT,
			Ref:   ksql.STREAM,
			Name:  s.Name,
		},
	}.Query()

	var (
		pipeline = make(chan []byte)
	)

	if err := network.Net.Perform(
		ctx,
		query,
		pipeline,
		network.ShortPolling{},
	); err != nil {

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
			insert dao.CreateRelationResponse
		)

		if err := jsoniter.Unmarshal(val, &insert); err != nil {
			return fmt.Errorf("cannot unmarshal insert response: %w", err)
		}

		if len(insert) == 0 {
			return nil
		}

		return errors.New("unpredictable error occurred while inserting")
	}

}

// InsertAs - provides insertion to stream functionality
// written fields are pre-fetched from select query, that
// is built by user
func (s *Stream[S]) InsertAs(
	ctx context.Context,
	query protocol.KafkaSerializer) error {

	scheme := *s.remoteSchema
	rmScheme := schema.SerializeFieldsToStruct(query.SchemaAlgo)

	matchMap, diffMap := schema.CompareStructs(scheme, rmScheme)

	if len(matchMap) == 0 {
		fmt.Println(diffMap)
		return errors.New("schemes doesnt match")
	}

	q := protocol.KafkaSerializer{
		SchemaAlgo: query.SchemaAlgo,
		QueryAlgo: ksql.Query{
			Query: ksql.INSERT,
			Ref:   ksql.STREAM,
			Name:  s.Name,
		},
		CTE: map[string]protocol.KafkaSerializer{
			"AS": query,
		},
	}.Query()

	var (
		pipeline = make(chan []byte)
	)

	if err := network.Net.Perform(
		ctx,
		q,
		pipeline,
		network.ShortPolling{},
	); err != nil {

	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case val, ok := <-pipeline:
		if !ok {
			return errors.New("insert response channel is closed")
		}

		var (
			insert dao.CreateRelationResponse
		)

		if err := jsoniter.Unmarshal(val, &insert); err != nil {
			return fmt.Errorf("cannot unmarshal insert response: %w", err)
		}

		if len(insert) == 0 {
			return nil
		}

		return errors.New("unpredictable error occurred while inserting")
	}
}

// SelectOnce - performs select query
// and return only one http answer
// channel is closed almost immediately
func (s *Stream[S]) SelectOnce(
	ctx context.Context) (S, error) {

	var (
		value S
	)

	query := protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.SELECT,
			Ref:   ksql.STREAM,
			Name:  s.Name,
		},
		SchemaAlgo: schema.DeserializeStructToFields(
			s.Name,
			*s.remoteSchema,
		),
		MetadataAlgo: ksql.With{
			ValueFormat: kinds.JSON.String(),
		},
	}.Query()

	var (
		pipeline = make(chan []byte)
	)

	if err := network.Net.Perform(
		ctx,
		query,
		pipeline,
		&network.ShortPolling{},
	); err != nil {

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
func (s *Stream[S]) SelectWithEmit(
	ctx context.Context) (<-chan S, error) {

	var (
		value   S
		valuesC = make(chan S)
	)

	query := protocol.KafkaSerializer{
		QueryAlgo: ksql.Query{
			Query: ksql.SELECT,
			Ref:   ksql.STREAM,
			Name:  s.Name,
		},
		SchemaAlgo: schema.DeserializeStructToFields(
			s.Name,
			*s.remoteSchema,
		),
		MetadataAlgo: ksql.With{
			ValueFormat: kinds.JSON.String(),
		},
	}.Query()

	var (
		pipeline = make(chan []byte)
	)

	if err := network.Net.Perform(
		ctx,
		query,
		pipeline,
		&network.LongPolling{},
	); err != nil {
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

// ToTopic - propagates stream data to new topic
// stream scheme is fully extended to new topic
func (s *Stream[S]) ToTopic(topicName string) (topic static.Topic[S]) {
	topic.Name = topicName
	topic.Partitions = int(*s.partitions)

	return
}

// ToTable - propagates stream data to new table
// and shares schema with it
func (s *Stream[S]) ToTable(tableName string) (table static.Table[S]) {
	static.StreamsProjections[tableName] = static.StreamSettings{
		Name:        tableName,
		SourceTopic: s.sourceTopic,
		Partitions:  s.partitions,
	}

	table.Name = tableName

	return
}
