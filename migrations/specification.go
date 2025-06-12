package migrations

import (
	"context"
	"errors"
	"time"
)

type (
	Migrator interface {
		Up(string) error
		Down(string) error
		AutoMigrate(context.Context) error
	}

	controller interface {
		GetLatestVersion(
			context.Context,
		) (time.Time, error)

		UpgradeWithMigration(
			ctx context.Context,
			version time.Time,
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
