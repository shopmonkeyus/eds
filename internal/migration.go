package internal

import (
	"context"

	"github.com/shopmonkeyus/go-common/logger"
)

// DriverMigration is an interface that Drivers implement when they support schema migrations.
type DriverMigration interface {

	// MigrateNewTable is called when a new table is detected with the appropriate information for the driver to perform the migration.
	MigrateNewTable(ctx context.Context, logger logger.Logger, schema *Schema) error

	// MigrateNewColumns is called when one or more new columns are detected with the appropriate information for the driver to perform the migration.
	MigrateNewColumns(ctx context.Context, logger logger.Logger, schema *Schema, columns []string) error
}
