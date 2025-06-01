package migrations

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
)

type migrator struct {
	ctrl            controller
	reflectionCheck bool
	migrationPath   string
}

func New(migrationPath string) Migrator {
	return &migrator{
		migrationPath: migrationPath,
		ctrl:          newKsqlController(),
	}
}

func (m *migrator) Migrate(ctx context.Context) error {
	currentVersion, err := m.ctrl.GetLatestVersion(ctx)
	if err != nil {
		return ErrMigrationServiceNotAvailable
	}

	files, err := os.ReadDir(m.migrationPath)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		filenameSegments := strings.Split(file.Name(), "_")
		if len(filenameSegments) != 2 {
			return ErrMalformedMigrationFile
		}

		version, err := strconv.Atoi(filenameSegments[0])
		if err != nil {
			return errors.Join(ErrMalformedMigrationFile, err)
		}

		if version <= currentVersion {
			continue
		}

		query, err := m.ReadQuery(file.Name())
		if err != nil {
			return err
		}

		if err = m.ctrl.UpgradeWithMigration(
			ctx,
			version,
			query,
		); err != nil {
			return err
		}
	}

	return nil
}

func (m *migrator) ReadQuery(fileName string) (string, error) {
	file, err := os.ReadFile(m.migrationPath + "/" + fileName)
	if err != nil {
		return "", err
	}

	partialQuery, found := strings.CutPrefix(string(file), "-- +seeker Up")
	if !found {
		return "", errors.Join(ErrMalformedMigrationFile, errors.New("missing migration prefix"))
	}

	query, found := strings.CutSuffix(partialQuery, "-- +seeker Down")
	if !found {
		return "", errors.Join(ErrMalformedMigrationFile, errors.New("missing migration suffix"))
	}

	return query, nil
}
