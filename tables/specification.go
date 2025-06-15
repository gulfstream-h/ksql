package tables

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
	"log/slog"
	"net/http"
	"reflect"
	"strings"
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
	dto.ShowTables, error,
) {

	query := util.MustTrue(ksql.List(ksql.TABLE).Expression)

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
			tables []dao.ShowTables
		)

		if err = jsoniter.Unmarshal(val, &tables); err != nil {
			err = errors.Join(static.ErrUnserializableResponse, err)
			return dto.ShowTables{}, err
		}

		if len(tables) == 0 {
			return dto.ShowTables{}, errors.New("no tables have been found")
		}

		return tables[0].DTO(), nil
	}
}

// Describe - responses with table description.
// Can be used for table schema and query by which
// it was created
func Describe(ctx context.Context, stream string) (dto.RelationDescription, error) {
	query := util.MustTrue(ksql.Describe(ksql.TABLE, stream).Expression)

	pipeline, err := network.Net.Perform(
		ctx,
		http.MethodPost,
		query,
		&network.ShortPolling{},
	)
	if err != nil {
		fmt.Println(err)
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
			describe []dao.DescribeResponse
		)

		fmt.Println(string(val))

		if strings.Contains(string(val), "Could not find STREAM/TABLE") {
			return dto.RelationDescription{}, static.ErrTableDoesNotExist
		}

		if err = jsoniter.Unmarshal(val, &describe); err != nil {
			err = errors.Join(static.ErrUnserializableResponse, err)
			return dto.RelationDescription{}, err
		}

		if len(describe) == 0 {
			return dto.RelationDescription{}, errors.New("table not found")
		}

		return describe[0].DTO(), nil
	}
}

// Drop - drops table from ksqlDB instance
// with parent topic. Also deletes projection from list
func Drop(ctx context.Context, name string) error {
	query := util.MustTrue(ksql.Drop(ksql.TABLE, name).Expression)

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
			drop []dao.DropInfo
		)

		if err = jsoniter.Unmarshal(val, &drop); err != nil {
			return fmt.Errorf("cannot unmarshal drop response: %w", err)
		}

		if drop[0].CommandStatus.Status != static.SUCCESS {
			return fmt.Errorf("cannot drop table: %s", drop[0].CommandStatus.Status)
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
	table string) (*Table[S], error) {

	var (
		s S
	)

	scheme := schema.SerializeProvidedStruct(s)

	tableInstance := &Table[S]{
		Name:         table,
		remoteSchema: &scheme,
	}
	desc, err := Describe(ctx, table)
	if err != nil {
		if errors.Is(err, static.ErrTableDoesNotExist) {
			return nil, err
		}
		return nil, fmt.Errorf("cannot describe table: %w", err)
	}

	var (
		responseSchema = make(map[string]string)
	)

	for _, field := range desc.Fields {
		responseSchema[field.Name] = field.Kind
	}

	remoteSchema := schema.SerializeRemoteSchema(responseSchema)
	matchMap, diffMap := schema.CompareStructs(scheme, remoteSchema)

	if len(diffMap) != 0 {
		slog.Info("match", "fields", matchMap)
		slog.Info("diff", "fields", diffMap)
		return nil, errors.New("schemes doesnt match")
	}

	slog.Info("new struct serialized", "fields", matchMap)

	return tableInstance, nil
}

// CreateTable - creates table in ksqlDB instance
// after creating, user should call
// select or select with emit to get data from it
func CreateTable[S any](
	ctx context.Context,
	tableName string,
	settings shared.TableSettings) (*Table[S], error) {

	var (
		s S
	)

	rmSchema := schema.SerializeProvidedStruct(s)
	searchFields := schema.ParseStructToFields(tableName, rmSchema)

	metadata := ksql.Metadata{
		Topic:       *settings.SourceTopic,
		ValueFormat: kinds.JSON.String(),
	}

	query, ok := ksql.Create(ksql.TABLE, tableName).
		SchemaFields(searchFields...).
		With(metadata).
		Expression()
	if !ok {
		return nil, errors.New("cannot build query for table creation")
	}

	fmt.Println(query)

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
			create []dao.CreateRelationResponse
		)

		fmt.Println(string(val))

		if err = jsoniter.Unmarshal(val, &create); err != nil {
			return nil, fmt.Errorf("cannot unmarshal create response: %w", err)
		}

		if len(create) < 1 {
			return nil, fmt.Errorf("unsuccessful response")
		}

		status := create[0]

		if status.CommandStatus.Status != static.SUCCESS {
			return nil, fmt.Errorf("unsuccesful respose. msg: %s", status.CommandStatus.Message)
		}

		query = fmt.Sprintf("CREATE TABLE QUERYABLE_%s AS SELECT * FROM %s;", tableName, tableName)

		pipeline, err = network.Net.Perform(ctx, http.MethodPost, query, network.ShortPolling{})
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
				create []dao.CreateRelationResponse
			)

			fmt.Println(string(val))

			if err = jsoniter.Unmarshal(val, &create); err != nil {
				return nil, fmt.Errorf("cannot unmarshal create response: %w", err)
			}

			if len(create) < 1 {
				return nil, fmt.Errorf("unsuccessful response")
			}

			status := create[0]

			if status.CommandStatus.Status != static.SUCCESS {
				return nil, fmt.Errorf("unsuccesful respose. msg: %s", status.CommandStatus.Message)
			}
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
	settings shared.TableSettings,
	selectQuery ksql.SelectBuilder,
) (*Table[S], error) {

	var (
		s S
	)

	scheme := schema.SerializeProvidedStruct(s)
	rmScheme := schema.SerializeFieldsToStruct(selectQuery.SchemaFields())

	matchMap, diffMap := schema.CompareStructs(scheme, rmScheme)

	if len(matchMap) == 0 {
		fmt.Println(diffMap)
		return nil, errors.New("schemes doesnt match")
	}

	meta := ksql.Metadata{
		Topic:       *settings.SourceTopic,
		ValueFormat: kinds.JSON.String(),
	}

	query, ok := ksql.Create(ksql.TABLE, tableName).
		AsSelect(selectQuery).
		With(meta).
		Expression()

	if !ok {
		return nil, errors.New("cannot build query for table creation")
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

	select {
	case <-ctx.Done():
		return nil, errors.New("context is done")
	case val, ok := <-pipeline:
		if !ok {
			return nil, errors.New("drop response channel is closed")
		}

		var (
			create []dao.CreateRelationResponse
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
	ctx context.Context,
) (S, error) {

	var (
		value S
	)

	meta := ksql.Metadata{ValueFormat: kinds.JSON.String()}

	query, ok := ksql.Create(ksql.TABLE, s.Name).
		SchemaFromStruct(*s.remoteSchema).
		With(meta).
		Expression()

	if !ok {
		return value, errors.New("cannot build query for table creation")
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
func (s *Table[S]) SelectWithEmit(
	ctx context.Context,
) (<-chan S, error) {

	var (
		value   S
		valuesC = make(chan S)
	)

	query, ok := ksql.SelectAsStruct("QUERYABLE_"+s.Name, *s.remoteSchema).
		From(s.Name).
		WithMeta(ksql.Metadata{ValueFormat: kinds.JSON.String()}).
		Expression()
	if !ok {
		return nil, errors.New("cannot build query for table creation")
	}

	query = "SELECT VERSION, UPDATED_AT FROM QUERYABLE_seeker_table;"

	pipeline, err := network.Net.PerformSelect(
		ctx,
		http.MethodPost,
		query,
		&network.LongPolling{},
	)
	if err != nil {
		return nil, fmt.Errorf("cannot perform request: %w", err)
	}

	go func() {
		var (
			iter    = 0
			headers dao.Header
		)

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

				if iter == 0 {
					str := val[1 : len(val)-1]

					fmt.Println(string(val))

					if err = jsoniter.Unmarshal(str, &headers); err != nil {
						panic(err)
						return
					}

					iter++
					continue
				}

				fmt.Println(headers.Header.Schema)

				var (
					row dao.Row
				)
				fmt.Println(string(val))

				if err = jsoniter.Unmarshal(val[:len(val)-1], &row); err != nil {
					fmt.Println(err)
					return
				}

				mappa, err := schema.ParseHeadersAndValues(headers.Header.Schema, row.Row.Columns)
				if err != nil {
					panic(err)
				}

				fmt.Println(mappa)

				t := reflect.ValueOf(&value)
				if t.Kind() != reflect.Ptr || t.Elem().Kind() != reflect.Struct {
					panic("value must be a pointer to a struct")
				}
				t = t.Elem()
				tt := t.Type()

				for k, v := range mappa {
					for i := 0; i < t.NumField(); i++ {
						structField := tt.Field(i)
						fieldVal := t.Field(i)

						if strings.EqualFold(structField.Tag.Get("ksql"), k) {
							if fieldVal.CanSet() && v != nil {
								val := reflect.ValueOf(v)
								if val.Type().ConvertibleTo(fieldVal.Type()) {
									fieldVal.Set(val.Convert(fieldVal.Type()))
								}
							}
							break
						}
					}
				}

				valuesC <- value
			}
		}
	}()

	return valuesC, nil
}
