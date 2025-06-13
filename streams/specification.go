package streams

import (
	"context"
	"errors"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"ksql/kernel/network"
	"ksql/kernel/protocol/dao"
	"ksql/kernel/protocol/dto"
	"ksql/kinds"
	"ksql/ksql"
	"ksql/schema"
	"ksql/shared"
	"ksql/static"

	"ksql/util"
	"net/http"
	"reflect"
)

// Stream - is full-functional type,
// providing all ksql-supported operations
// via referred to type functions calls
type Stream[S any] struct {
	Name         string
	partitions   *uint8
	remoteSchema *reflect.Type
	format       kinds.ValueFormat
}

// ListStreams - responses with all streams list
// in the current ksqlDB instance. Also it reloads
// map of available projections
func ListStreams(ctx context.Context) (dto.ShowStreams, error) {

	query := util.MustTrue(ksql.List(ksql.STREAM).Expression)

	pipeline, err := network.Net.Perform(
		ctx,
		http.MethodPost,
		query,
		&network.ShortPolling{},
	)
	if err != nil {
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

	query := util.MustTrue(ksql.Describe(ksql.STREAM, s.Name).Expression)

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

// Drop - drops stream from ksqlDB instance
// with parent topic. Also deletes projection from list
func (s *Stream[S]) Drop(ctx context.Context) error {

	query := util.MustTrue(ksql.Drop(ksql.STREAM, s.Name).Expression)

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
	settings shared.StreamSettings) (*Stream[S], error) {

	var (
		s S
	)

	scheme := schema.SerializeProvidedStruct(s)

	streamInstance := &Stream[S]{
		Name:         stream,
		partitions:   settings.Partitions,
		remoteSchema: &scheme,
		format:       settings.Format,
	}
	desc, err := streamInstance.Describe(ctx)
	if err != nil {
		if errors.Is(err, static.ErrStreamDoesNotExist) || len(desc.Fields) == 0 {
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
	settings shared.StreamSettings,
) (*Stream[S], error) {

	var (
		s S
	)

	rmSchema := schema.SerializeProvidedStruct(s)
	searchFields := schema.ParseStructToFields(streamName, rmSchema)

	metadata := ksql.Metadata{
		Topic:       *settings.SourceTopic,
		ValueFormat: kinds.JSON.String(),
		Partitions:  int(*settings.Partitions),
	}

	query, _ := ksql.Create(ksql.STREAM, streamName).
		SchemaFields(searchFields...).
		With(metadata).
		Expression()

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

		return &Stream[S]{
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
	settings shared.StreamSettings,
	selectBuilder ksql.SelectBuilder) (*Stream[S], error) {

	var (
		s S
	)

	scheme := schema.SerializeProvidedStruct(s)
	rmScheme := schema.SerializeFieldsToStruct(selectBuilder.SchemaFields())

	matchMap, diffMap := schema.CompareStructs(scheme, rmScheme)

	if len(matchMap) == 0 {
		fmt.Println(diffMap)
		return nil, errors.New("schemes doesnt match")
	}

	query, ok := ksql.Create(ksql.STREAM, streamName).
		AsSelect(selectBuilder).
		With(ksql.Metadata{
			Topic:       *settings.SourceTopic,
			ValueFormat: kinds.JSON.String(),
		}).
		Expression()

	if !ok {
		return nil, errors.New("cannot build create query")
	}

	pipeline, err := network.Net.Perform(
		ctx,
		http.MethodPost,
		query,
		&network.ShortPolling{},
	)
	if err != nil {
		err = fmt.Errorf("cannot perform request: %w", err)
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

		return &Stream[S]{
			partitions:   settings.Partitions,
			remoteSchema: &scheme,
			format:       settings.Format,
		}, nil
	}
}

/*
  TODO:
    - Replace fields to any ?
*/

// Insert - provides insertion to stream functionality
// written fields are defined by user
func (s *Stream[S]) Insert(
	ctx context.Context,
	fields ksql.Rows,
) error {

	scheme := *s.remoteSchema

	remoteProjection := schema.ParseStructToFieldsDictionary(
		s.Name,
		scheme,
	)

	var (
		searchFields []schema.SearchField
	)

	// TODO:
	// 	if no key presented in fields - return error?
	for key, value := range fields {
		field, ok := remoteProjection[key]
		if !ok {
			continue
		}

		*field.Value = util.Serialize(value)

		searchFields = append(searchFields, field)
	}

	query, ok := ksql.Insert(ksql.STREAM, s.Name).Rows(fields).Expression()
	if !ok {
		return errors.New("cannot build insert query")
	}

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
	selectQuery ksql.SelectBuilder,
) error {

	scheme := *s.remoteSchema
	rmScheme := schema.SerializeFieldsToStruct(selectQuery.SchemaFields())

	matchMap, diffMap := schema.CompareStructs(scheme, rmScheme)

	if len(matchMap) == 0 {
		fmt.Println(diffMap)
		return errors.New("schemes doesnt match")
	}

	query, ok := ksql.Insert(ksql.STREAM, s.Name).AsSelect(selectQuery).Expression()
	if !ok {
		return errors.New("cannot build insert query")
	}

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

	query, ok := ksql.SelectAsStruct(*s.remoteSchema).
		From(s.Name).
		WithMeta(ksql.Metadata{ValueFormat: kinds.JSON.String()}).
		Expression()

	if !ok {
		return value, errors.New("cannot build select query")
	}

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
func (s *Stream[S]) SelectWithEmit(
	ctx context.Context) (<-chan S, error) {

	var (
		value   S
		valuesC = make(chan S)
	)

	query, ok := ksql.SelectAsStruct(*s.remoteSchema).
		From(s.Name).
		WithMeta(ksql.Metadata{ValueFormat: kinds.JSON.String()}).
		Expression()

	if !ok {
		return nil, errors.New("cannot build select query")
	}

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
