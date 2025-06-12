package migrations

import (
	"context"
	"errors"
	"os"
	"strings"
	"time"
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

func (m *migrator) AutoMigrate(ctx context.Context) error {
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

		version, err := time.Parse(time.RFC3339, filenameSegments[0])
		if err != nil {
			return errors.Join(ErrMalformedMigrationFile, err)
		}

		if version.Before(currentVersion) {
			continue
		}

		query, err := m.ReadUpQuery(file.Name())
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

func (m *migrator) Up(filename string) error {
	currentVersion, err := m.ctrl.GetLatestVersion(context.TODO())
	if err != nil {
		return ErrMigrationServiceNotAvailable
	}

	filenameSegments := strings.Split(filename, "_")
	if len(filenameSegments) != 2 {
		return ErrMalformedMigrationFile
	}

	version, err := time.Parse(time.RFC3339, filenameSegments[0])
	if err != nil {
		return errors.Join(ErrMalformedMigrationFile, err)
	}

	if version.Before(currentVersion) {
		return errors.New("cannot up migration, cuz current version is ahead")
	}

	query, err := m.ReadUpQuery(filename)
	if err != nil {
		return err
	}

	if err = m.ctrl.UpgradeWithMigration(context.TODO(), version, query); err != nil {
		return err
	}

	return nil
}

func (m *migrator) Down(filename string) error {
	currentVersion, err := m.ctrl.GetLatestVersion(context.TODO())
	if err != nil {
		return ErrMigrationServiceNotAvailable
	}

	filenameSegments := strings.Split(filename, "_")
	if len(filenameSegments) != 2 {
		return ErrMalformedMigrationFile
	}

	version, err := time.Parse(time.RFC3339, filenameSegments[0])
	if err != nil {
		return errors.Join(ErrMalformedMigrationFile, err)
	}

	if version != currentVersion {
		return errors.New("cannot down migration, cuz current version is not equal to invoked")
	}

	query, err := m.ReadUpQuery(filename)
	if err != nil {
		return err
	}

	if err = m.ctrl.UpgradeWithMigration(context.TODO(), version, query); err != nil {
		return err
	}

	return nil
}

func (m *migrator) ReadUpQuery(fileName string) (string, error) {
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

func (m *migrator) ReadDownQuery(fileName string) (string, error) {
	file, err := os.ReadFile(m.migrationPath + "/" + fileName)
	if err != nil {
		return "", err
	}

	query, found := strings.CutPrefix(string(file), "-- +seeker Down")
	if !found {
		return "", errors.Join(ErrMalformedMigrationFile, errors.New("missing migration prefix"))
	}

	return query, nil
}
