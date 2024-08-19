package snowflake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
	sf "github.com/snowflakedb/gosnowflake"
	"golang.org/x/sync/semaphore"
)

const maxBatchSize = 200

type snowflakeDriver struct {
	config    internal.DriverConfig
	logger    logger.Logger
	db        *sql.DB
	schema    internal.SchemaMap
	waitGroup sync.WaitGroup
	once      sync.Once
	ctx       context.Context
	batcher   *util.Batcher
	locker    sync.Mutex
	sessionID string
}

var _ internal.Driver = (*snowflakeDriver)(nil)
var _ internal.DriverLifecycle = (*snowflakeDriver)(nil)
var _ internal.Importer = (*snowflakeDriver)(nil)
var _ internal.DriverSessionHandler = (*snowflakeDriver)(nil)
var _ internal.DriverHelp = (*snowflakeDriver)(nil)

func (p *snowflakeDriver) SetSessionID(sessionID string) {
	if sessionID != "" {
		p.sessionID = sessionID
		p.ctx = sf.WithRequestID(p.config.Context, sf.ParseUUID(sessionID))
	}
}

func (p *snowflakeDriver) connectToDB(ctx context.Context, url string) (*sql.DB, error) {
	url, err := getConnectionStringFromURL(url)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("snowflake", url)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection: %w", err)
	}
	row := db.QueryRowContext(ctx, "SELECT 1")
	if err := row.Err(); err != nil {
		db.Close()
		return nil, fmt.Errorf("unable to ping db: %w", err)
	}
	return db, nil
}

// Start the driver. This is called once at the beginning of the driver's lifecycle.
func (p *snowflakeDriver) Start(config internal.DriverConfig) error {
	p.config = config
	p.ctx = config.Context
	p.logger = config.Logger.WithPrefix("[snowflake]")
	schema, err := p.config.SchemaRegistry.GetLatestSchema()
	if err != nil {
		return fmt.Errorf("unable to get schema: %w", err)
	}
	p.schema = schema
	db, err := p.connectToDB(config.Context, config.URL)
	if err != nil {
		return fmt.Errorf("unable to create connection: %w", err)
	}
	p.db = db
	p.batcher = util.NewBatcher()
	p.logger.Debug("started")
	return nil
}

// Stop the driver. This is called once at the end of the driver's lifecycle.
func (p *snowflakeDriver) Stop() error {
	p.logger.Debug("stopping")
	p.once.Do(func() {
		p.logger.Debug("waiting on flush")
		p.Flush(p.logger) // make sure we flush
		p.logger.Debug("completed flush")
		p.logger.Debug("waiting on waitgroup")
		p.waitGroup.Wait()
		p.logger.Debug("completed waitgroup")
		p.locker.Lock()
		defer p.locker.Unlock()
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
func (p *snowflakeDriver) MaxBatchSize() int {
	return maxBatchSize
}

// Process a single event. It returns a bool indicating whether Flush should be called. If an error is returned, the driver will NAK the event.
func (p *snowflakeDriver) Process(logger logger.Logger, event internal.DBChangeEvent) (bool, error) {
	logger.Trace("processing event: %s", event.String())
	p.waitGroup.Add(1)
	defer p.waitGroup.Done()
	if _, ok := p.schema[event.Table]; !ok {
		// NOTE: remove this once we have schema evolution reimplemented
		logger.Warn("skipping event: %s because table was not found in schema: %s", event.String(), event.Table)
		return false, nil
	}
	object, err := event.GetObject()
	if err != nil {
		return false, fmt.Errorf("error getting json object: %w", err)
	}
	p.batcher.Add(event.Table, event.GetPrimaryKey(), event.Operation, event.Diff, object, &event)
	return false, nil
}

var sequence int64

// Flush is called to commit any pending events. It should return an error if the flush fails. If the flush fails, the driver will NAK all pending events.
func (p *snowflakeDriver) Flush(logger logger.Logger) error {
	p.locker.Lock()
	defer p.locker.Unlock()
	if p.db == nil {
		return internal.ErrDriverStopped
	}
	p.waitGroup.Add(1)
	defer p.waitGroup.Done()
	records := p.batcher.Records()
	count := len(records)
	p.batcher.Clear()
	if count > 0 {
		logger.Debug("flush: %d / %d", count, sequence+1)
		sequence++
		tag := fmt.Sprintf("eds-server-%s/%d/%d", p.sessionID, sequence, count)
		ctx := sf.WithQueryTag(context.Background(), tag)
		var query strings.Builder
		var statementCount int
		var cachekeys []string
		var deletekeys []string
		for i, record := range records {
			var force bool
			var key string
			switch record.Operation {
			case "INSERT":
				key = fmt.Sprintf("snowflake:%s:%s", record.Table, record.Id)
				ok, _, err := p.config.Tracker.GetKey(key)
				if err != nil {
					return fmt.Errorf("error getting cache key %s from tracker: %w", key, err)
				}
				force = ok
				if force {
					logger.Trace("forcing delete before insert because we've seen an insert for %s/%s", record.Table, record.Id)
				}
			case "UPDATE":
				// slight optimization to skip records that just have an updatedDate and nothing else
				justUpdatedDate := len(record.Diff) == 1 && record.Diff[0] == "updatedDate"
				noUpdates := len(record.Diff) == 0
				if justUpdatedDate || noUpdates {
					logger.Trace("skipping update because only updatedDate changed for %s/%s", record.Table, record.Id)
					continue
				}
			case "DELETE":
				key = fmt.Sprintf("snowflake:%s:%s", record.Table, record.Id)
				deletekeys = append(deletekeys, key)
			}
			sql, c := toSQL(record, p.schema, force)
			statementCount += c
			logger.Trace("adding %d to %s sql (%d/%d): %s", c, tag, i+1, count, strings.TrimRight(sql, "\n"))
			query.WriteString(sql)
			if key != "" {
				cachekeys = append(cachekeys, key)
			}
		}
		if statementCount > 0 {
			execCTX, err := sf.WithMultiStatement(ctx, statementCount)
			if err != nil {
				return fmt.Errorf("error creating exec context: %w", err)
			}
			ts := time.Now()
			logger.Trace("executing query (%s/%d)", tag, statementCount)
			if _, err := p.db.ExecContext(execCTX, query.String()); err != nil {
				return fmt.Errorf("unable to run query: %s: %w", query.String(), err)
			}
			logger.Trace("executed query (%s/%d) in %v", tag, statementCount, time.Since(ts))
		}
		if len(cachekeys) > 0 {
			// cache keys seen for the past 24 hours ... might want to make it configurable at some point but this is good enough for now
			if err := p.config.Tracker.SetKeys(cachekeys, tag, time.Hour*24); err != nil {
				return fmt.Errorf("error setting cache keys: %s in tracker: %w", cachekeys, err)
			}
		}
		if len(deletekeys) > 0 {
			if err := p.config.Tracker.DeleteKey(deletekeys...); err != nil {
				return fmt.Errorf("error deleting cache keys from tracker: %w", err)
			}
		}
	}
	return nil
}

// Import is called to import data from the source.
func (p *snowflakeDriver) Import(config internal.ImporterConfig) error {
	db, err := p.connectToDB(config.Context, config.URL)
	if err != nil {
		return err
	}
	defer db.Close()

	schema, err := config.SchemaRegistry.GetLatestSchema()
	if err != nil {
		return err
	}

	logger := config.Logger.WithPrefix("[snowflake]")
	executeSQL := util.SQLExecuter(config.Context, logger, db, config.DryRun)
	logger.Info("loading data into database")

	// create all the tables
	for _, table := range config.Tables {
		data := schema[table]
		logger.Debug("creating table %s", table)
		if err := executeSQL(createSQL(data)); err != nil {
			return fmt.Errorf("error creating table: %s. %w", table, err)
		}
		logger.Debug("created table %s", table)
	}

	if config.SchemaOnly {
		return nil
	}

	// create a stage
	stageName := "eds_import_" + config.JobID
	logger.Debug("creating stage %s", stageName)
	if err := executeSQL("CREATE STAGE " + stageName); err != nil {
		return fmt.Errorf("error creating stage: %s", err)
	}
	logger.Debug("stage %s created", stageName)

	started := time.Now()

	parallel := config.MaxParallel
	if parallel <= 0 {
		parallel = 1
	} else if parallel > 99 {
		parallel = 99
	}

	// upload files
	fileURI := util.ToFileURI(config.DataDir, "*.ndjson.gz")
	if err := executeSQL(fmt.Sprintf(`PUT '%s' @%s PARALLEL=%d SOURCE_COMPRESSION=gzip`, fileURI, stageName, parallel)); err != nil {
		return fmt.Errorf("error uploading files: %s", err)
	}
	logger.Debug("files uploaded in %v", time.Since(started))

	// import the data
	var wg sync.WaitGroup
	errorChannel := make(chan error, len(config.Tables))

	var sem = semaphore.NewWeighted(int64(4))

	for _, table := range config.Tables {
		wg.Add(1)
		go func(table string) {
			defer util.RecoverPanic(logger)
			defer func() {
				sem.Release(1)
				wg.Done()
			}()
			sem.Acquire(config.Context, 1)
			if err := executeSQL(fmt.Sprintf(`COPY INTO %s FROM @%s MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = true COMPRESSION = 'GZIP') PATTERN='.*-%s-.*'`, util.QuoteIdentifier(table), stageName, table)); err != nil {
				logger.Trace("error importing data: %s", err)
				errorChannel <- fmt.Errorf("error importing %s data: %s", table, err)
			}
		}(table)
	}
	logger.Debug("waiting for all tables to import")
	wg.Wait()
	logger.Debug("all tables completed in %v", time.Since(started))

	errs := make([]error, 0)
done:
	for {
		select {
		case err := <-errorChannel:
			logger.Error("%s", err)
			errs = append(errs, err)
		default:
			break done
		}
	}
	close(errorChannel)

	if err := executeSQL("DROP STAGE " + stageName); err != nil {
		return fmt.Errorf("error dropping stage: %s", err)
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// Name is a unique name for the driver.
func (p *snowflakeDriver) Name() string {
	return "Snowflake"
}

// Description is the description of the driver.
func (p *snowflakeDriver) Description() string {
	return "Supports streaming EDS messages to a Snowflake database."
}

// ExampleURL should return an example URL for configuring the driver.
func (p *snowflakeDriver) ExampleURL() string {
	return "snowflake://user:password@host/database"
}

// Help should return a detailed help documentation for the driver.
func (p *snowflakeDriver) Help() string {
	var help strings.Builder
	help.WriteString(util.GenerateHelpSection("Schema", "The database will match the public schema from the Shopmonkey transactional database.\n"))
	return help.String()
}

// Test is called to test the drivers connectivity with the configured url. It should return an error if the test fails or nil if the test passes.
func (p *snowflakeDriver) Test(ctx context.Context, logger logger.Logger, url string) error {
	db, err := p.connectToDB(ctx, url)
	if err != nil {
		return err
	}
	return db.Close()
}

// Configuration returns the configuration fields for the driver.
func (p *snowflakeDriver) Configuration() []internal.DriverField {
	return internal.NewDatabaseConfiguration(-1)
}

// Validate validates the configuration and returns an error if the configuration is invalid or a valid url if the configuration is valid.
func (p *snowflakeDriver) Validate(values map[string]any) (string, []internal.FieldError) {
	return internal.URLFromDatabaseConfiguration("snowflake", -1, values), nil
}

func init() {
	var driver snowflakeDriver
	internal.RegisterDriver("snowflake", &driver)
	internal.RegisterImporter("snowflake", &driver)
}
