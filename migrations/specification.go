package migrations

import (
	"context"
	"errors"
)

type (
	Migrator interface {
		Migrate(context.Context) error
	}

	controller interface {
		GetLatestVersion(
			context.Context,
		) (int, error)

		UpgradeWithMigration(
			ctx context.Context,
			version int,
			query string) error
	}
)

var (
	ErrMigrationServiceNotAvailable = errors.New("migration service is not available")
	ErrMalformedMigrationFile       = errors.New("malformed migration file")
)

var (
	_ Migrator   = new(migrator)
	_ controller = new(ksqlController)
)
