package postgresql

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

const (
	maxBytesSizeInsert = 1_000_000
)

type postgresqlProcessor struct {
	ctx       context.Context
	logger    logger.Logger
	db        *sql.DB
	schema    internal.SchemaMap
	waitGroup sync.WaitGroup
	once      sync.Once
	pending   strings.Builder
	count     int
}

var _ internal.Processor = (*postgresqlProcessor)(nil)
var _ internal.ProcessorLifecycle = (*postgresqlProcessor)(nil)
var _ internal.Importer = (*postgresqlProcessor)(nil)

func (p *postgresqlProcessor) connectToDB(url string) (*sql.DB, error) {
	db, err := sql.Open("postgres", url)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("unable to ping db: %w", err)
	}
	return db, nil
}

// Start the processor. This is called once at the beginning of the processor's lifecycle.
func (p *postgresqlProcessor) Start(config internal.ProcessorConfig) error {
	db, err := p.connectToDB(config.URL)
	if err != nil {
		return err
	}
	p.logger = config.Logger.WithPrefix("[postgresql]")
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

// Stop the processor. This is called once at the end of the processor's lifecycle.
func (p *postgresqlProcessor) Stop() error {
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
func (p *postgresqlProcessor) MaxBatchSize() int {
	return -1
}

// Process a single events. It returns a bool indicating whether Flush should be called. If an error is returned, the processor will NAK the event.
func (p *postgresqlProcessor) Process(event internal.DBChangeEvent) (bool, error) {
	p.logger.Trace("processing event: %s", event)
	p.waitGroup.Add(1)
	defer p.waitGroup.Done()
	sql, err := toSQL(event, p.schema)
	if err != nil {
		return false, err
	}
	p.logger.Trace("sql: %s", sql)
	if _, err := p.pending.WriteString(sql); err != nil {
		return false, fmt.Errorf("error writing sql to pending buffer: %w", err)
	}
	p.count++
	return false, nil
}

// Flush is called to commit any pending events. It should return an error if the flush fails. If the flush fails, the processor will NAK all pending events.
func (p *postgresqlProcessor) Flush() error {
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
func (p *postgresqlProcessor) Import(config internal.ImporterConfig) error {
	db, err := p.connectToDB(config.URL)
	if err != nil {
		return err
	}
	defer db.Close()

	schema, err := config.SchemaRegistry.GetLatestSchema()
	if err != nil {
		return err
	}

	logger := config.Logger.WithPrefix("[postgres]")
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
		if !strings.HasSuffix(file, ".ndjson.gz") {
			continue
		}
		// [0]                               [1]             [2][3] [4]      [5] [6]
		// 202407131650522808024600000000000-c7274317e9a4a9cb-1-651-00000000-user-2.ndjson.gz
		filename := filepath.Base(file)
		parts := strings.Split(filename, "-")
		table := parts[5]
		data := schema[table]
		if data == nil {
			return fmt.Errorf("unexpected table (%s) not found in schema but in import directory: %s", table, filename)
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
			if size >= maxBytesSizeInsert {
				if err := executeSQL(pending.String()); err != nil {
					return fmt.Errorf("unable to execute %s sql: %w", table, err)
				}
				pending.Reset()
				size = 0
			}
		}
		if size > 0 {
			if err := executeSQL(pending.String()); err != nil {
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

func init() {
	var processor postgresqlProcessor
	internal.RegisterProcessor("postgresql", &processor)
	internal.RegisterImporter("postgresql", &processor)
}
