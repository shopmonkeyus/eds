package postgresql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/importer"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

const maxBytesSizeInsert = 5_000_000

type postgresqlDriver struct {
	ctx          context.Context
	logger       logger.Logger
	db           *sql.DB
	registry     internal.SchemaRegistry
	waitGroup    sync.WaitGroup
	once         sync.Once
	pending      strings.Builder
	count        int
	executor     func(string) error
	importConfig internal.ImporterConfig
	size         int
	dbname       string
	dbschema     internal.DatabaseSchema
}

var _ internal.Driver = (*postgresqlDriver)(nil)
var _ internal.DriverLifecycle = (*postgresqlDriver)(nil)
var _ internal.Importer = (*postgresqlDriver)(nil)
var _ internal.DriverHelp = (*postgresqlDriver)(nil)
var _ internal.DriverMigration = (*postgresqlDriver)(nil)

func (p *postgresqlDriver) refreshSchema(ctx context.Context, db *sql.DB, failIfEmpty bool) error {
	if p.dbname == "" {
		dbname, err := util.GetCurrentDatabase(ctx, db, "current_database()")
		if err != nil {
			return fmt.Errorf("error getting current database name: %w", err)
		}
		p.dbname = dbname
	}
	schema, err := util.BuildDBSchemaFromInfoSchema(ctx, p.logger, db, "table_catalog", p.dbname, failIfEmpty)
	if err != nil {
		return fmt.Errorf("error building database schema: %w", err)
	}
	p.dbschema = schema
	return nil
}

func (p *postgresqlDriver) connectToDB(ctx context.Context, url string) (*sql.DB, error) {
	urlstr, err := GetConnectionStringFromURL(url)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("postgres", urlstr)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection: %w", err)
	}
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pctx); err != nil {
		db.Close()
		return nil, err
	}

	if err := p.refreshSchema(ctx, db, false); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

// Start the driver. This is called once at the beginning of the driver's lifecycle.
func (p *postgresqlDriver) Start(config internal.DriverConfig) error {
	p.logger = config.Logger.WithPrefix("[postgres]")
	db, err := p.connectToDB(config.Context, config.URL)
	if err != nil {
		return err
	}
	p.registry = config.SchemaRegistry
	p.db = db
	p.ctx = config.Context
	return nil
}

// Stop the driver. This is called once at the end of the driver's lifecycle.
func (p *postgresqlDriver) Stop() error {
	p.logger.Debug("stopping")
	p.once.Do(func() {
		p.logger.Debug("waiting on waitgroup")
		p.waitGroup.Wait()
		p.logger.Debug("completed waitgroup")
		if p.db != nil {
			p.logger.Debug("closing db")
			p.db.Close()
			p.db = nil
			p.logger.Debug("closed db")
		}
	})
	p.logger.Debug("stopped")
	return nil
}

// MaxBatchSize returns the maximum number of events that can be processed in a single call to Process and when Flush should be called.
// Return -1 to indicate that there is no limit.
func (p *postgresqlDriver) MaxBatchSize() int {
	return -1
}

// Process a single event. It returns a bool indicating whether Flush should be called. If an error is returned, the driver will NAK the event.
func (p *postgresqlDriver) Process(logger logger.Logger, event internal.DBChangeEvent) (bool, error) {
	logger.Trace("processing event: %s", event.String())
	p.waitGroup.Add(1)
	defer p.waitGroup.Done()
	_, version, err := p.registry.GetTableVersion(event.Table)
	if err != nil {
		return false, fmt.Errorf("unable to get table version for table: %s: %w", event.Table, err)
	}
	schema, err := p.registry.GetSchema(event.Table, version)
	if err != nil {
		return false, fmt.Errorf("unable to get schema for table: %s (%s). %w", event.Table, version, err)
	}
	sql, err := toSQL(event, schema)
	if err != nil {
		return false, err
	}
	logger.Trace("sql: %s", sql)
	if _, err := p.pending.WriteString(sql); err != nil {
		return false, fmt.Errorf("error writing sql to pending buffer: %w", err)
	}
	p.count++
	return false, nil
}

// Flush is called to commit any pending events. It should return an error if the flush fails. If the flush fails, the driver will NAK all pending events.
func (p *postgresqlDriver) Flush(logger logger.Logger) error {
	logger.Debug("flush")
	p.waitGroup.Add(1)
	defer p.waitGroup.Done()
	if p.count > 0 {
		tx, err := p.db.BeginTx(p.ctx, nil)
		if err != nil {
			return fmt.Errorf("unable to start transaction: %w", err)
		}
		var success bool
		defer func() {
			if !success {
				tx.Rollback()
			}
		}()
		if _, err := tx.ExecContext(p.ctx, p.pending.String()); err != nil {
			logger.Error("offending sql: %s", p.pending.String())
			return fmt.Errorf("unable to execute sql: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("unable to commit transaction: %w", err)
		}
		success = true
		logger.Debug("flushed %d records", p.count)
	}
	p.pending.Reset()
	p.count = 0
	return nil
}

// CreateDatasource allows the handler to create the datasource before importing data.
func (p *postgresqlDriver) CreateDatasource(schema internal.SchemaMap) error {
	// create all the tables
	for _, table := range p.importConfig.Tables {
		data := schema[table]
		p.logger.Debug("creating table %s", table)
		if err := p.executor(createSQL(data)); err != nil {
			return fmt.Errorf("error creating table: %s. %w", table, err)
		}
		p.logger.Debug("created table %s", table)
	}
	return nil
}

// ImportEvent allows the handler to process the event.
func (p *postgresqlDriver) ImportEvent(event internal.DBChangeEvent, data *internal.Schema) error {
	object, err := event.GetObject()
	if err != nil {
		return err
	}
	sql := toSQLFromObject("INSERT", data, event.Table, object, nil)
	p.pending.WriteString(sql)
	p.count++
	p.size += len(sql)
	if p.size >= maxBytesSizeInsert || p.importConfig.Single {
		if err := p.executor(p.pending.String()); err != nil {
			p.logger.Error("offending sql: %s", p.pending.String())
			return fmt.Errorf("unable to execute sql: %w", err)
		}
		p.pending.Reset()
		p.size = 0
	}
	return nil
}

// ImportCompleted is called when all events have been processed.
func (p *postgresqlDriver) ImportCompleted() error {
	if p.size > 0 {
		if err := p.executor(p.pending.String()); err != nil {
			p.logger.Error("offending sql: %s", p.pending.String())
			return fmt.Errorf("unable to execute sql: %w", err)
		}
	}
	return nil
}

// Import is called to import data from the source.
func (p *postgresqlDriver) Import(config internal.ImporterConfig) error {
	p.logger = config.Logger.WithPrefix("[postgres]")
	db, err := p.connectToDB(config.Context, config.URL)
	if err != nil {
		return err
	}
	defer db.Close()

	p.registry = config.SchemaRegistry
	p.importConfig = config
	p.executor = util.SQLExecuter(config.Context, p.logger, db, config.DryRun)
	p.pending = strings.Builder{}
	p.count = 0
	p.size = 0

	return importer.Run(p.logger, config, p)
}

// Name is a unique name for the driver.
func (p *postgresqlDriver) Name() string {
	return "PostgreSQL"
}

// Description is the description of the driver.
func (p *postgresqlDriver) Description() string {
	return "Supports streaming EDS messages to a PostgreSQL database."
}

// ExampleURL should return an example URL for configuring the driver.
func (p *postgresqlDriver) ExampleURL() string {
	return "postgres://localhost:5432/database"
}

// Help should return a detailed help documentation for the driver.
func (p *postgresqlDriver) Help() string {
	var help strings.Builder
	help.WriteString(util.GenerateHelpSection("Schema", "The database will match the public schema from the Shopmonkey transactional database.\n"))
	return help.String()
}

func (p *postgresqlDriver) Aliases() []string {
	return []string{"postgresql"}
}

// Test is called to test the drivers connectivity with the configured url. It should return an error if the test fails or nil if the test passes.
func (p *postgresqlDriver) Test(ctx context.Context, logger logger.Logger, url string) error {
	p.logger = logger.WithPrefix("[postgres]")
	db, err := p.connectToDB(ctx, url)
	if err != nil {
		return err
	}
	return db.Close()
}

// Configuration returns the configuration fields for the driver.
func (p *postgresqlDriver) Configuration() []internal.DriverField {
	return internal.NewDatabaseConfiguration(5432)
}

// Validate validates the configuration and returns an error if the configuration is invalid or a valid url if the configuration is valid.
func (p *postgresqlDriver) Validate(values map[string]any) (string, []internal.FieldError) {
	return internal.URLFromDatabaseConfiguration("postgres", 5432, values), nil
}

// MigrateNewTable is called when a new table is detected with the appropriate information for the driver to perform the migration.
func (p *postgresqlDriver) MigrateNewTable(ctx context.Context, logger logger.Logger, schema *internal.Schema) error {
	p.waitGroup.Add(1)
	defer p.waitGroup.Done()
	if _, ok := p.dbschema[schema.Table]; ok {
		logger.Info("table already exists for: %s, dropping and recreating...", schema.Table)
		if err := util.DropTable(ctx, logger, p.db, quoteIdentifier(schema.Table)); err != nil {
			return err
		}
	}
	sql := createSQL(schema)
	logger.Trace("migrate new table: %s", sql)
	if _, err := p.db.ExecContext(ctx, sql); err != nil {
		return err
	}
	return p.refreshSchema(ctx, p.db, true)
}

// MigrateNewColumns is called when one or more new columns are detected with the appropriate information for the driver to perform the migration.
func (p *postgresqlDriver) MigrateNewColumns(ctx context.Context, logger logger.Logger, schema *internal.Schema, columns []string) error {
	p.waitGroup.Add(1)
	defer p.waitGroup.Done()
	sqls := addNewColumnsSQL(logger, columns, schema, p.dbschema)
	for _, sql := range sqls {
		logger.Trace("migrating new columns: %s", sql)
		_, err := p.db.ExecContext(ctx, sql)
		if err != nil {
			return err
		}
		logger.Debug("migrated new columns: %s", sql)
	}
	return p.refreshSchema(ctx, p.db, true)
}

func init() {
	internal.RegisterDriver("postgres", &postgresqlDriver{})
	internal.RegisterImporter("postgres", &postgresqlDriver{})
}
