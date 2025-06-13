package tables

import (
	"context"
	"errors"
	"ksql/shared"
	"ksql/static"
)

// Register - registers a full-functional table
// with the provided settings. Also it is bound to
// user provided generic scheme for select operations
func Register[S any](
	ctx context.Context,
	settings shared.TableSettings) (
	*Table[S], error) {

	var (
		table *Table[S]
		err   error
	)

	table, err = GetTable[S](ctx, settings.Name, settings)
	if err != nil {
		if errors.Is(err, static.ErrTableDoesNotExist) {
			return CreateTable[S](ctx, settings.Name, settings)
		}
		return nil, err
	}

	return table, nil
}
