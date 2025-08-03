package migrations

import (
	"context"
	"errors"
	libErrors "github.com/gulfstream-h/ksql/errors"
	"github.com/gulfstream-h/ksql/internal/kernel/network"
	"github.com/gulfstream-h/ksql/kinds"
	"github.com/gulfstream-h/ksql/shared"
	"github.com/gulfstream-h/ksql/streams"
	"log/slog"
	"net/http"
	"time"
)

const (
	systemStreamName = "seeker_stream" //ksql-system-stream name
)

// ksqlController - essence that manages
// iteration with embedded ksql system stream
type ksqlController struct {
	host   string
	stream *streams.Stream[migrationRelation]
}

type (
	migrationRelation struct {
		Version   string `ksql:"VERSION"`
		UpdatedAt string `ksql:"UPDATED_AT"`
	}
)

func newKsqlController(host string) controller {
	return &ksqlController{
		host: host,
	}
}

func (ctrl *ksqlController) createSystemRelations(
	ctx context.Context) (*streams.Stream[migrationRelation], error) {

	var (
		topic      = "migrations"
		partitions = 1
	)

	settings := shared.StreamSettings{
		Name:        systemStreamName,
		ValueFormat: kinds.JSON,
		SourceTopic: topic,
		Partitions:  partitions,
	}

	migStream, err := streams.CreateStream[migrationRelation](
		ctx,
		systemStreamName,
		settings)
	if err != nil {
		return nil, err
	}

	if err = migStream.InsertRow(ctx, map[string]any{
		"VERSION":    time.Time{}.Format(time.RFC3339),
		"UPDATED_AT": time.Time{}.Format(time.RFC3339),
	}); err != nil {
		slog.Debug("cannot insert default values to migration stream")

		return nil, err
	}

	return migStream, nil
}

func (k *ksqlController) GetLatestVersion(ctx context.Context) (time.Time, error) {
	migrationStream, err := streams.GetStream[migrationRelation](
		ctx,
		systemStreamName,
	)

	if errors.Is(err, libErrors.ErrStreamDoesNotExist) {
		slog.Debug("migration table doesnt exist")
		migrationStream, err = k.createSystemRelations(ctx)
		return time.Time{}, err
	}

	if err != nil {
		return time.Time{}, err
	}

	k.stream = migrationStream

	msg, err := migrationStream.SelectOnce(ctx)
	if err != nil {
		return time.Time{}, err
	}

	slog.Info("selected", "version", msg)

	v, err := time.Parse(time.RFC3339, msg.Version)
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

	resp, err := network.Net.Perform(
		ctx,
		http.MethodPost,
		query, network.ShortPolling{},
	)
	if err != nil {
		return errors.Join(ErrMigrationServiceNotAvailable, err)
	}

	for v := range resp {
		slog.Info("mig resp", "formatted", string(v))
	}

	fields := map[string]any{
		"VERSION":    version.Format(time.RFC3339),
		"UPDATED_AT": time.Now().Format(time.RFC3339),
	}

	if err = stream.InsertRow(ctx, fields); err != nil {
		return errors.Join(ErrMigrationServiceNotAvailable, err)
	}

	return nil
}
