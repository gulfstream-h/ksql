package tables

import (
	"context"
	"errors"
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"ksql/consts"
	"ksql/kernel/network"
	"ksql/kernel/protocol/dao"
	"ksql/kernel/protocol/dto"
	"ksql/kinds"
	"ksql/ksql"
	"ksql/schema"
	"ksql/schema/netparse"
	"ksql/schema/report"
	"ksql/shared"
	"ksql/static"
	"ksql/util"
	"log/slog"
	"net/http"
	"strings"
)

// Table - is full-functional type,
// providing all ksql-supported operations
// via referred to type functions calls
type Table[S any] struct {
	Name         string
	sourceTopic  *string
	partitions   *uint8
	remoteSchema schema.LintedFields
	format       kinds.ValueFormat
}

// ListTables - responses with all tables list
// in the current ksqlDB instance. Also it reloads
// map of available projections
func ListTables(ctx context.Context) (
	dto.ShowTables, error,
) {

	query := util.MustNoError(ksql.List(ksql.TABLE).Expression)

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
	query := util.MustNoError(ksql.Describe(ksql.TABLE, stream).Expression)

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
			describe []dao.DescribeResponse
		)

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
	query := util.MustNoError(ksql.Drop(ksql.TABLE, name).Expression)

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

		if len(drop) == 0 {
			return errors.New("cannot drop stream")
		}

		if drop[0].CommandStatus.Status != consts.SUCCESS {
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

	scheme, err := schema.NativeStructRepresentation(s)
	if err != nil {
		return nil, err
	}

	tableInstance := &Table[S]{
		Name:         table,
		remoteSchema: scheme,
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

	remoteSchema := schema.RemoteFieldsRepresentation(table, responseSchema)
	if err = remoteSchema.CompareWithFields(scheme.Array()); err != nil {
		return nil, fmt.Errorf("reflection error %w", err)
	}

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

	rmSchema, err := schema.NativeStructRepresentation(s)
	if err != nil {
		return nil, err
	}

	metadata := ksql.Metadata{
		Topic:       *settings.SourceTopic,
		ValueFormat: kinds.JSON.String(),
	}

	query, err := ksql.Create(ksql.TABLE, tableName).
		SchemaFields(rmSchema.Array()...).
		With(metadata).
		Expression()

	if err != nil {
		return nil, fmt.Errorf("build create query: %w", err)
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
		return nil, ctx.Err()
	case val, ok := <-pipeline:
		if !ok {
			return nil, static.ErrMalformedResponse
		}

		var (
			create []dao.CreateRelationResponse
		)

		if err = jsoniter.Unmarshal(val, &create); err != nil {
			return nil, fmt.Errorf("cannot unmarshal create response: %w", err)
		}

		if len(create) < 1 {
			return nil, fmt.Errorf("unsuccessful response")
		}

		status := create[0]

		if status.CommandStatus.Status != consts.SUCCESS {
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

			if err = jsoniter.Unmarshal(val, &create); err != nil {
				return nil, fmt.Errorf("cannot unmarshal create response: %w", err)
			}

			if len(create) < 1 {
				return nil, fmt.Errorf("unsuccessful response")
			}

			status := create[0]

			if status.CommandStatus.Status != consts.SUCCESS {
				return nil, fmt.Errorf("unsuccesful respose. msg: %s", status.CommandStatus.Message)
			}
		}

		return &Table[S]{
			Name:         tableName,
			sourceTopic:  settings.SourceTopic,
			partitions:   settings.Partitions,
			remoteSchema: rmSchema,
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
	selectBuilder ksql.SelectBuilder,
) (*Table[S], error) {

	var (
		s S
	)

	if selectBuilder == nil {
		return nil, errors.New("select builder cannot be nil")
	}

	fields := selectBuilder.Returns()

	if len(fields.Map()) == 0 {
		return nil, errors.New("select builder must return at least one field")
	}

	if static.ReflectionFlag {
		err := report.ReflectionReportNative(s, fields)
		if err != nil {
			return nil, fmt.Errorf("reflection report native: %w", err)
		}

		for relName, rel := range selectBuilder.RelationReport() {
			err = report.ReflectionReportRemote(relName, rel.Map())
			if err != nil {
				return nil, fmt.Errorf("reflection report remote: %w", err)
			}
		}
	}

	meta := ksql.Metadata{
		Topic:       *settings.SourceTopic,
		ValueFormat: kinds.JSON.String(),
	}

	query, err := ksql.Create(ksql.TABLE, tableName).
		AsSelect(selectBuilder).
		With(meta).
		Expression()

	if err != nil {
		return nil, fmt.Errorf("build create query: %w", err)
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

		if status.CommandStatus.Status != consts.SUCCESS {
			return nil, fmt.Errorf("unsuccesful respose. msg: %s", status.CommandStatus.Message)
		}

		return &Table[S]{
			sourceTopic:  settings.SourceTopic,
			partitions:   settings.Partitions,
			remoteSchema: fields,
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

	query, err := ksql.Create(ksql.TABLE, s.Name).
		SchemaFromRemoteStruct(s.remoteSchema).
		With(meta).
		Expression()

	if err != nil {
		return value, fmt.Errorf("build select query: %w", err)
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

	query, err := ksql.SelectAsStruct("QUERYABLE_"+s.Name, s.remoteSchema).
		From(s.Name, 0).
		WithMeta(ksql.Metadata{ValueFormat: kinds.JSON.String()}).
		Expression()
	if err != nil {
		return nil, fmt.Errorf("build select query: %w", err)
	}

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

					if err = jsoniter.Unmarshal(str, &headers); err != nil {
						return
					}

					iter++
					continue
				}

				var (
					row dao.Row
				)

				if err = jsoniter.Unmarshal(val[:len(val)-1], &row); err != nil {
					return
				}

				value, err = netparse.ParseNetResponse[S](headers, row)
				if err != nil {
					slog.Error(
						"parse net response",
						slog.String("error", err.Error()),
						slog.Any("headers", headers),
						slog.Any("row", row),
					)
					return
				}

				valuesC <- value
			}
		}
	}()

	return valuesC, nil
}
