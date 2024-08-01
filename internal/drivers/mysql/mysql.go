package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

const maxBytesSizeInsert = 5_000_000

type mysqlDriver struct {
	ctx       context.Context
	logger    logger.Logger
	db        *sql.DB
	schema    internal.SchemaMap
	waitGroup sync.WaitGroup
	once      sync.Once
	pending   strings.Builder
	count     int
}

var _ internal.Driver = (*mysqlDriver)(nil)
var _ internal.DriverLifecycle = (*mysqlDriver)(nil)
var _ internal.Importer = (*mysqlDriver)(nil)
var _ internal.DriverHelp = (*mysqlDriver)(nil)

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
		return nil, fmt.Errorf("unable to ping db: %w", err)
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
	schema, err := config.SchemaRegistry.GetLatestSchema()
	if err != nil {
		p.db.Close()
		return fmt.Errorf("unable to get schema: %w", err)
	}
	p.schema = schema
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
	sql, err := toSQL(event, p.schema)
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
func (p *mysqlDriver) Flush() error {
	p.logger.Debug("flush")
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
			p.logger.Trace("offending sql: %s", p.pending.String())
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

// Import is called to import data from the source.
func (p *mysqlDriver) Import(config internal.ImporterConfig) error {
	db, err := p.connectToDB(config.Context, config.URL)
	if err != nil {
		return err
	}
	defer db.Close()

	schema, err := config.SchemaRegistry.GetLatestSchema()
	if err != nil {
		return err
	}

	logger := config.Logger.WithPrefix("[mysql]")
	started := time.Now()
	executeSQL := util.SQLExecuter(config.Context, logger, db, config.DryRun)

	// create all the tables
	for _, table := range config.Tables {
		data := schema[table]
		logger.Debug("creating table %s", table)
		if err := executeSQL(createSQL(data)); err != nil {
			return fmt.Errorf("error creating table: %s. %w", table, err)
		}
		logger.Debug("created table %s", table)
	}

	files, err := util.ListDir(config.DataDir)
	if err != nil {
		return fmt.Errorf("unable to list dir: %w", err)
	}

	var total int

	// NOTE: these files should automatically be sorted by the filesystem
	// so we need to do them in order and not in parallel
	for _, file := range files {
		table, _, ok := util.ParseCRDBExportFile(file)
		if !ok {
			logger.Debug("skipping file: %s", file)
			continue
		}
		if !util.SliceContains(config.Tables, table) {
			continue
		}
		data := schema[table]
		if data == nil {
			return fmt.Errorf("unexpected table (%s) not found in schema but in import directory: %s", table, file)
		}
		logger.Debug("processing file: %s, table: %s", file, table)
		dec, err := util.NewNDJSONDecoder(file)
		if err != nil {
			return fmt.Errorf("unable to create JSON decoder for %s: %w", file, err)
		}
		defer dec.Close()
		var count int
		var size int
		var pending strings.Builder
		tstarted := time.Now()
		for dec.More() {
			var obj map[string]interface{}
			if err := dec.Decode(&obj); err != nil {
				return fmt.Errorf("unable to decode JSON: %w", err)
			}
			sql := toSQLFromObject("INSERT", data, table, obj, nil)
			pending.WriteString(sql)
			count++
			size += len(sql)
			if size >= maxBytesSizeInsert || config.Single {
				if err := executeSQL(pending.String()); err != nil {
					logger.Trace("offending sql: %s", pending.String())
					return fmt.Errorf("unable to execute %s sql: %w", table, err)
				}
				pending.Reset()
				size = 0
			}
		}
		if size > 0 {
			if err := executeSQL(pending.String()); err != nil {
				logger.Trace("offending sql: %s", pending.String())
				return fmt.Errorf("unable to execute %s sql: %w", table, err)
			}
		}
		dec.Close()
		total += count
		logger.Debug("imported %d %s records in %s", count, table, time.Since(tstarted))
	}

	logger.Info("imported %d records from %d files in %s", total, len(files), time.Since(started))

	return nil
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

func init() {
	var driver mysqlDriver
	internal.RegisterDriver("mysql", &driver)
	internal.RegisterImporter("mysql", &driver)
}
