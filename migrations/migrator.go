package migrations

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type migrationPath string

// migrator - orchestrate migration actions
type migrator struct {
	ctrl            controller
	reflectionCheck bool
	migrationPath   string
}

// New - creates new migration orchestrator
func New(host string, migrationPath migrationPath) Migrator {
	return &migrator{
		migrationPath: string(migrationPath),
		ctrl:          newKsqlController(host),
	}
}

// GenPath - returns function for building migration absolute path
func GenPath() func(relPath string) (migrationPath, error) {
	return func(relPath string) (migrationPath, error) {
		absPath, err := filepath.Abs(relPath)
		return migrationPath(absPath), err
	}
}

// AutoMigrate - iterates through all existing migrations,
// skipping already applied and executing Up migration till
// the newest version
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
		if len(filenameSegments) < 2 {
			return ErrMalformedMigrationFile
		}

		versionInt, err := strconv.Atoi(filenameSegments[0])
		if err != nil {
			return errors.Join(ErrMalformedMigrationFile, err)
		}

		version := time.Unix(int64(versionInt), 0)

		if version.Before(currentVersion) {
			continue
		}

		query, err := m.ReadUpQuery(file.Name(), m.migrationPath)
		if err != nil {
			return err
		}

		query = strings.Replace(query, "\n", "", -1)

		slog.Info("query", "parsed", query)

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

// Up - executes single up query from passed file
func (m *migrator) Up(filename string) error {
	currentVersion, err := m.ctrl.GetLatestVersion(context.TODO())
	if err != nil {
		slog.Debug("cannot get actual version")
		return err
	}

	slog.Info("current version", "formatted", currentVersion)

	filenameSegments := strings.Split(filename, "_")
	if len(filenameSegments) < 2 {
		slog.Debug("cannot split filename")
		return err
	}

	versionInt, err := strconv.Atoi(filenameSegments[0])
	if err != nil {
		slog.Debug("cannot convert version to time")
		return errors.Join(ErrMalformedMigrationFile, err)
	}

	version := time.Unix(int64(versionInt), 0)

	slog.Info("version", "formatted", version)

	if version.Before(currentVersion) {
		return errors.New("cannot up migration, cuz current version is ahead")
	}

	if version == currentVersion {
		return errors.New("cannot up migration, cuz current version is already applied")
	}

	query, err := m.ReadUpQuery(filename, "./")
	if err != nil {
		return err
	}

	query = strings.Replace(query, "\n", "", -1)

	slog.Info("query", "parsed", query)

	if err = m.ctrl.UpgradeWithMigration(context.TODO(), version, query); err != nil {
		return err
	}

	return nil
}

// Down - executes single down query from passed file
func (m *migrator) Down(filename string) error {
	currentVersion, err := m.ctrl.GetLatestVersion(context.TODO())
	if err != nil {
		return ErrMigrationServiceNotAvailable
	}

	filenameSegments := strings.Split(filename, "_")
	if len(filenameSegments) < 2 {
		return ErrMalformedMigrationFile
	}

	versionInt, err := strconv.Atoi(filenameSegments[0])
	if err != nil {
		return errors.Join(ErrMalformedMigrationFile, err)
	}

	version := time.Unix(int64(versionInt), 0)

	if version != currentVersion {
		return errors.New("cannot down migration, cuz current version is not equal to invoked")
	}

	query, err := m.ReadDownQuery(filename)
	if err != nil {
		return err
	}

	query = strings.Replace(query, "\n", "", -1)

	slog.Info("query", "parsed", query)

	lastVersion := m.FindPrecedingMigration(int64(versionInt))

	if err = m.ctrl.UpgradeWithMigration(context.TODO(), lastVersion, query); err != nil {
		return err
	}

	return nil
}

// ReadUpQuery - parses migration file and copies up-command
func (m *migrator) ReadUpQuery(fileName string, path string) (string, error) {
	file, err := os.ReadFile(path + fileName)
	if err != nil {
		return "", err
	}

	partialQuery, found := strings.CutPrefix(string(file), "-- +seeker Up")
	if !found {
		return "", errors.Join(ErrMalformedMigrationFile, errors.New("missing migration prefix"))
	}

	query, _, found := strings.Cut(partialQuery, "-- +seeker Down")
	if !found {
		return "", errors.Join(ErrMalformedMigrationFile, errors.New("missing migration suffix"))
	}

	return query, nil
}

// ReadDownQuery - parses migration file and copies down-command
func (m *migrator) ReadDownQuery(fileName string) (string, error) {
	file, err := os.ReadFile("./" + fileName)
	if err != nil {
		return "", err
	}

	_, query, found := strings.Cut(string(file), "-- +seeker Down")
	if !found {
		return "", errors.Join(ErrMalformedMigrationFile, errors.New("missing migration prefix"))
	}

	return query, nil
}

// FindPrecedingMigration - searches for previous migrations
// to downshift versions to previous and save previous version
func (m *migrator) FindPrecedingMigration(currentVersion int64) time.Time {
	directories, err := os.ReadDir(".")
	if err != nil {
		return time.Time{}
	}

	var (
		lastVersion int64 = math.MinInt64
	)

	for _, dir := range directories {
		metaLabels := strings.Split(dir.Name(), "_")
		if len(metaLabels) < 2 {
			return time.Time{}
		}

		version, err := strconv.ParseInt(metaLabels[0], 10, 64)
		if err != nil {
			return time.Time{}
		}

		if lastVersion < version &&
			version != currentVersion {
			lastVersion = version
		}
	}

	return time.Unix(lastVersion, 0)
}
