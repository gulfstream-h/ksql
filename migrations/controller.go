package migrations

import (
	"context"
	"errors"
	"ksql/kernel/network"
	"ksql/kinds"
	"ksql/static"
	"ksql/streams"
	"ksql/tables"
	"net/http"
	"os"
	"time"
)

const (
	systemStreamName = "seeker-stream"
	systemTableName  = "seeker-table"
)

type ksqlController struct {
	host   string
	stream *streams.Stream[migrationRelation]
	table  *tables.Table[migrationRelation]
}

type (
	migrationRelation struct {
		Version   time.Time `ksql:"version"`
		UpdatedAt time.Time `ksql:"updated_at"`
	}
)

func newKsqlController() controller {
	return &ksqlController{
		host: os.Getenv("KSQL_HOST"),
	}
}

func (ctrl *ksqlController) createSystemRelations(
	ctx context.Context) (*tables.Table[migrationRelation], error) {

	settings := streams.StreamSettings{
		Format: kinds.JSON,
	}

	migStream, err := streams.CreateStream[migrationRelation](
		ctx,
		systemStreamName,
		settings)
	if err != nil {
		return nil, err
	}

	migTable, err := migStream.ToTable(systemTableName)
	if err != nil {
		return nil, err
	}

	return migTable.Cast(), nil
}

func (k *ksqlController) GetLatestVersion(ctx context.Context) (time.Time, error) {
	migrationTable, err := tables.GetTable[migrationRelation](
		ctx,
		systemTableName,
		tables.TableSettings{},
	)

	if errors.Is(err, static.ErrTableDoesNotExist) {
		migrationTable, err = k.createSystemRelations(ctx)
	}

	if err != nil {
		return time.Time{}, err
	}

	k.table = migrationTable

	message, err := migrationTable.SelectOnce(ctx)
	if err != nil {
		return time.Time{}, err
	}

	return message.Version, nil
}

func (k *ksqlController) UpgradeWithMigration(
	ctx context.Context,
	version time.Time,
	query string) error {

	if k.stream == nil {
		return ErrMigrationServiceNotAvailable
	}

	if _, err := network.Net.Perform(
		ctx,
		http.MethodPost,
		query, network.ShortPolling{},
	); err != nil {
		return errors.Join(ErrMigrationServiceNotAvailable, err)
	}

	fields := map[string]string{
		"version":    version.Format(time.RFC3339),
		"updated_at": time.Now().Format(time.RFC3339),
	}

	if err := k.stream.Insert(ctx, fields); err != nil {
		return errors.Join(ErrMigrationServiceNotAvailable, err)
	}

	return nil
}
