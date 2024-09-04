package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/importer"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

const maxBytesSizeInsert = 5_000_000

type mysqlDriver struct {
	ctx          context.Context
	logger       logger.Logger
	registry     internal.SchemaRegistry
	db           *sql.DB
	waitGroup    sync.WaitGroup
	once         sync.Once
	pending      strings.Builder
	count        int
	executor     func(string) error
	importConfig internal.ImporterConfig
	size         int
}

var _ internal.Driver = (*mysqlDriver)(nil)
var _ internal.DriverLifecycle = (*mysqlDriver)(nil)
var _ internal.Importer = (*mysqlDriver)(nil)
var _ internal.DriverHelp = (*mysqlDriver)(nil)
var _ importer.Handler = (*mysqlDriver)(nil)

func (p *mysqlDriver) connectToDB(ctx context.Context, urlstr string) (*sql.DB, error) {
	dsn, err := parseURLToDSN(urlstr)
	if err != nil {
		return nil, fmt.Errorf("error parsing url: %w", err)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// Start the driver. This is called once at the beginning of the driver's lifecycle.
func (p *mysqlDriver) Start(config internal.DriverConfig) error {
	db, err := p.connectToDB(config.Context, config.URL)
	if err != nil {
		return err
	}
	p.logger = config.Logger.WithPrefix("[mysql]")
	p.registry = config.SchemaRegistry
	p.db = db
	p.ctx = config.Context
	return nil
}

// Stop the driver. This is called once at the end of the driver's lifecycle.
func (p *mysqlDriver) Stop() error {
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
func (p *mysqlDriver) MaxBatchSize() int {
	return -1
}

// Process a single event. It returns a bool indicating whether Flush should be called. If an error is returned, the driver will NAK the event.
func (p *mysqlDriver) Process(logger logger.Logger, event internal.DBChangeEvent) (bool, error) {
	logger.Trace("processing event: %s", event.String())
	p.waitGroup.Add(1)
	defer p.waitGroup.Done()
	schema, err := p.registry.GetSchema(event.Table, event.ModelVersion)
	if err != nil {
		return false, fmt.Errorf("unable to get schema for table: %s (%s). %w", event.Table, event.ModelVersion, err)
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
func (p *mysqlDriver) Flush(logger logger.Logger) error {
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
func (p *mysqlDriver) CreateDatasource(schema internal.SchemaMap) error {
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
func (p *mysqlDriver) ImportEvent(event internal.DBChangeEvent, data *internal.Schema) error {
	sql, err := toSQLFromObject("INSERT", data, event.Table, event, nil)
	if err != nil {
		return err
	}
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
func (p *mysqlDriver) ImportCompleted() error {
	if p.size > 0 {
		if err := p.executor(p.pending.String()); err != nil {
			p.logger.Error("offending sql: %s", p.pending.String())
			return fmt.Errorf("unable to execute sql: %w", err)
		}
	}
	return nil
}

// Import is called to import data from the source.
func (p *mysqlDriver) Import(config internal.ImporterConfig) error {
	db, err := p.connectToDB(config.Context, config.URL)
	if err != nil {
		return err
	}
	defer db.Close()

	p.registry = config.SchemaRegistry
	p.importConfig = config
	p.logger = config.Logger.WithPrefix("[mysql]")
	p.executor = util.SQLExecuter(config.Context, p.logger, db, config.DryRun)
	p.pending = strings.Builder{}
	p.count = 0
	p.size = 0

	return importer.Run(p.logger, config, p)
}

// Name is a unique name for the driver.
func (p *mysqlDriver) Name() string {
	return "MySQL"
}

// Description is the description of the driver.
func (p *mysqlDriver) Description() string {
	return "Supports streaming EDS messages to a MySQL database."
}

// ExampleURL should return an example URL for configuring the driver.
func (p *mysqlDriver) ExampleURL() string {
	return "mysql://user:password@localhost:3306/database"
}

// Help should return a detailed help documentation for the driver.
func (p *mysqlDriver) Help() string {
	var help strings.Builder
	help.WriteString(util.GenerateHelpSection("Schema", "The database will match the public schema from the Shopmonkey transactional database.\n"))
	return help.String()
}

// Test is called to test the drivers connectivity with the configured url. It should return an error if the test fails or nil if the test passes.
func (p *mysqlDriver) Test(ctx context.Context, logger logger.Logger, url string) error {
	db, err := p.connectToDB(ctx, url)
	if err != nil {
		return err
	}
	return db.Close()
}

// Configuration returns the configuration fields for the driver.
func (p *mysqlDriver) Configuration() []internal.DriverField {
	return internal.NewDatabaseConfiguration(3306)
}

// Validate validates the configuration and returns an error if the configuration is invalid or a valid url if the configuration is valid.
func (p *mysqlDriver) Validate(values map[string]any) (string, []internal.FieldError) {
	return internal.URLFromDatabaseConfiguration("mysql", 3306, values), nil
}

// MigrateNewTable is called when a new table is detected with the appropriate information for the driver to perform the migration.
func (p *mysqlDriver) MigrateNewTable(ctx context.Context, logger logger.Logger, schema *internal.Schema) error {
	p.waitGroup.Add(1)
	defer p.waitGroup.Done()
	sql := createSQL(schema)
	logger.Trace("migrate new table: %s", sql)
	_, err := p.db.ExecContext(ctx, sql)
	return err
}

// MigrateNewColumns is called when one or more new columns are detected with the appropriate information for the driver to perform the migration.
func (p *mysqlDriver) MigrateNewColumns(ctx context.Context, logger logger.Logger, schema *internal.Schema, columns []string) error {
	p.waitGroup.Add(1)
	defer p.waitGroup.Done()
	sql := addNewColumnsSQL(columns, schema)
	logger.Trace("migrate new columns: %s", sql)
	_, err := p.db.ExecContext(ctx, sql)
	return err
}

func init() {
	internal.RegisterDriver("mysql", &mysqlDriver{})
	internal.RegisterImporter("mysql", &mysqlDriver{})
}
