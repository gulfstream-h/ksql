package migrations

import (
	"context"
	"errors"
	"ksql/kernel/network"
	"ksql/kinds"
	"ksql/shared"
	"ksql/static"
	"ksql/streams"
	"ksql/tables"
	"log/slog"
	"net/http"
	"time"
)

const (
	systemStreamName = "seeker_stream"
	systemTableName  = "seeker_table"
)

type ksqlController struct {
	host   string
	stream *streams.Stream[migrationRelation]
	table  *tables.Table[migrationRelation]
}

type (
	migrationRelation struct {
		Version   string `ksql:"version,primary"`
		UpdatedAt string `ksql:"updated_at"`
	}
)

func newKsqlController(host string) controller {
	return &ksqlController{
		host: host,
	}
}

func (ctrl *ksqlController) createSystemRelations(
	ctx context.Context) (*tables.Table[migrationRelation], error) {

	var (
		topic      = "migrations"
		partitions = uint8(1)
	)

	settings := shared.StreamSettings{
		Format:      kinds.JSON,
		SourceTopic: &topic,
		Partitions:  &partitions,
	}

	migStream, err := streams.CreateStream[migrationRelation](
		ctx,
		systemStreamName,
		settings)
	if err != nil {
		return nil, err
	}

	ctrl.stream = migStream

	migTable, err := tables.CreateTable[migrationRelation](ctx, systemTableName, shared.TableSettings{
		SourceTopic: &topic,
		Format:      kinds.JSON,
	})
	if err != nil {
		return nil, err
	}

	if err = migStream.Insert(ctx, map[string]any{
		"version":    time.Time{}.Format(time.RFC3339),
		"updated_at": time.Time{}.Format(time.RFC3339),
	}); err != nil {
		slog.Debug("cannot insert default values to migration stream")

		return nil, err
	}

	return migTable, nil
}

func (k *ksqlController) GetLatestVersion(ctx context.Context) (time.Time, error) {
	migrationTable, err := tables.GetTable[migrationRelation](
		ctx,
		systemTableName,
	)

	if errors.Is(err, static.ErrTableDoesNotExist) {
		slog.Debug("migration table doesnt exist")
		migrationTable, err = k.createSystemRelations(ctx)
		return time.Time{}, err
	}

	if err != nil {
		return time.Time{}, err
	}

	k.table = migrationTable

	message, err := migrationTable.SelectWithEmit(ctx)
	if err != nil {
		return time.Time{}, err
	}

	msg := <-message

	v, err := time.Parse(time.RFC3339, msg.UpdatedAt)
	if err != nil {
		return time.Time{}, err
	}

	return v, nil
}

func (k *ksqlController) UpgradeWithMigration(
	ctx context.Context,
	version time.Time,
	query string) error {

	stream, err := streams.GetStream[migrationRelation](ctx, systemStreamName)
	if err != nil {
		slog.Debug("cannot get migration stream",
			"error", err.Error())

		return ErrMigrationServiceNotAvailable
	}

	if _, err = network.Net.Perform(
		ctx,
		http.MethodPost,
		query, network.ShortPolling{},
	); err != nil {
		return errors.Join(ErrMigrationServiceNotAvailable, err)
	}

	fields := map[string]any{
		"version":    version.Format(time.RFC3339),
		"updated_at": time.Now().Format(time.RFC3339),
	}

	if err = stream.Insert(ctx, fields); err != nil {
		return errors.Join(ErrMigrationServiceNotAvailable, err)
	}

	return nil
}
