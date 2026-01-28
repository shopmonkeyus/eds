package sqlserver

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/microsoft/go-mssqldb"
	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/importer"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

const maxBytesSizeInsert = 5_000_000
const maxBatchSize = 500

type sqlserverDriver struct {
	ctx          context.Context
	logger       logger.Logger
	db           *sql.DB
	registry     internal.SchemaRegistry
	waitGroup    sync.WaitGroup
	once         sync.Once
	pending      strings.Builder
	count        int
	importConfig internal.ImporterConfig
	executor     func(string) error
	size         int
	dbname       string
	dbschema     internal.DatabaseSchema
}

var _ internal.Driver = (*sqlserverDriver)(nil)
var _ internal.DriverLifecycle = (*sqlserverDriver)(nil)
var _ internal.Importer = (*sqlserverDriver)(nil)
var _ internal.DriverHelp = (*sqlserverDriver)(nil)
var _ importer.Handler = (*sqlserverDriver)(nil)
var _ internal.DriverMigration = (*sqlserverDriver)(nil)

func (p *sqlserverDriver) refreshSchema(ctx context.Context, db *sql.DB, failIfEmpty bool) error {
	if p.dbname == "" {
		dbname, err := util.QuerySingleValue(ctx, db, "DB_NAME()")
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

func (p *sqlserverDriver) connectToDB(ctx context.Context, urlstr string) (*sql.DB, error) {
	dsn, err := ParseURLToDSN(urlstr)
	if err != nil {
		return nil, fmt.Errorf("error parsing url: %w", err)
	}
	db, err := sql.Open("sqlserver", dsn)
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
func (p *sqlserverDriver) Start(config internal.DriverConfig) error {
	p.logger = config.Logger.WithPrefix("[sqlserver]")
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
func (p *sqlserverDriver) Stop() error {
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
func (p *sqlserverDriver) MaxBatchSize() int {
	return maxBatchSize
}

// Process a single event. It returns a bool indicating whether Flush should be called. If an error is returned, the driver will NAK the event.
func (p *sqlserverDriver) Process(logger logger.Logger, event internal.DBChangeEvent) (bool, error) {
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
func (p *sqlserverDriver) Flush(logger logger.Logger) error {
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
	}
	p.pending.Reset()
	p.count = 0
	return nil
}

// CreateDatasource allows the handler to create the datasource before importing data.
func (p *sqlserverDriver) CreateDatasource(schema internal.SchemaMap) error {
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
func (p *sqlserverDriver) ImportEvent(event internal.DBChangeEvent, schema *internal.Schema) error {
	object, err := event.GetObject()
	if err != nil {
		return err
	}
	sql := toSQLFromObject(schema, event.Table, object, nil)
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
func (p *sqlserverDriver) ImportCompleted() error {
	if p.size > 0 {
		if err := p.executor(p.pending.String()); err != nil {
			p.logger.Error("offending sql: %s", p.pending.String())
			return fmt.Errorf("unable to execute sql: %w", err)
		}
	}
	return nil
}

// Import is called to import data from the source.
func (p *sqlserverDriver) Import(config internal.ImporterConfig) error {
	p.logger = config.Logger.WithPrefix("[sqlserver]")
	db, err := p.connectToDB(config.Context, config.URL)
	if err != nil {
		return err
	}
	defer db.Close()

	p.importConfig = config
	p.registry = config.SchemaRegistry
	p.executor = util.SQLExecuter(config.Context, p.logger, db, config.DryRun)
	p.pending = strings.Builder{}
	p.count = 0
	p.size = 0

	return importer.Run(p.logger, config, p)
}

func (p *sqlserverDriver) Name() string {
	return "Microsoft SQL Server"
}

// Description is the description of the driver.
func (p *sqlserverDriver) Description() string {
	return "Supports streaming EDS messages to a Microsoft SQL Server database."
}

// ExampleURL should return an example URL for configuring the driver.
func (p *sqlserverDriver) ExampleURL() string {
	return "sqlserver://user:password@localhost:1433/database"
}

// Help should return a detailed help documentation for the driver.
func (p *sqlserverDriver) Help() string {
	var help strings.Builder
	help.WriteString(util.GenerateHelpSection("Schema", "The database will match the public schema from the Shopmonkey transactional database.\n"))
	return help.String()
}

func (p *sqlserverDriver) Aliases() []string {
	return []string{"mssql"}
}

// Test is called to test the drivers connectivity with the configured url. It should return an error if the test fails or nil if the test passes.
func (p *sqlserverDriver) Test(ctx context.Context, logger logger.Logger, url string) error {
	p.logger = logger.WithPrefix("[sqlserver]")
	db, err := p.connectToDB(ctx, url)
	if err != nil {
		return err
	}
	return db.Close()
}

// Configuration returns the configuration fields for the driver.
func (p *sqlserverDriver) Configuration() []internal.DriverField {
	return internal.NewDatabaseConfiguration(1433)
}

// Validate validates the configuration and returns an error if the configuration is invalid or a valid url if the configuration is valid.
func (p *sqlserverDriver) Validate(values map[string]any) (string, []internal.FieldError) {
	return internal.URLFromDatabaseConfiguration("sqlserver", 1433, values), nil
}

// MigrateNewTable is called when a new table is detected with the appropriate information for the driver to perform the migration.
func (p *sqlserverDriver) MigrateNewTable(ctx context.Context, logger logger.Logger, schema *internal.Schema) error {
	p.waitGroup.Add(1)
	defer p.waitGroup.Done()
	if _, ok := p.dbschema[schema.Table]; ok {
		logger.Info("table already exists for: %s, dropping and recreating...", schema.Table)
		if err := util.DropTable(ctx, logger, p.db, quoteIdentifier(schema.Table, true)); err != nil {
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
func (p *sqlserverDriver) MigrateNewColumns(ctx context.Context, logger logger.Logger, schema *internal.Schema, columns []string) error {
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

func (p *sqlserverDriver) GetDestinationSchema(ctx context.Context, logger logger.Logger) internal.DatabaseSchema {
	return p.dbschema
}

func init() {
	internal.RegisterDriver("sqlserver", &sqlserverDriver{})
	internal.RegisterImporter("sqlserver", &sqlserverDriver{})
}
