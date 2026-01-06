package snowflake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
	sf "github.com/snowflakedb/gosnowflake"
	"golang.org/x/sync/semaphore"
)

const maxBatchSize = 200

type snowflakeDriver struct {
	config                  internal.DriverConfig
	ctx                     context.Context
	logger                  logger.Logger
	db                      *sql.DB
	registry                internal.SchemaRegistry
	waitGroup               sync.WaitGroup
	once                    sync.Once
	batcher                 *util.Batcher
	locker                  sync.Mutex
	sessionID               string
	dbname                  string
	dbschema                internal.DatabaseSchema
	updateStrategy          string
	profilingInfo           DriverProfilingInfo
	bulkRecordModifications []func([]*util.Record) []*util.Record
	connectToDBFunc         func(ctx context.Context, url string) (*sql.DB, error)
}

var _ internal.Driver = (*snowflakeDriver)(nil)
var _ internal.DriverLifecycle = (*snowflakeDriver)(nil)
var _ internal.Importer = (*snowflakeDriver)(nil)
var _ internal.DriverSessionHandler = (*snowflakeDriver)(nil)
var _ internal.DriverHelp = (*snowflakeDriver)(nil)
var _ internal.DriverMigration = (*snowflakeDriver)(nil)

var uuidRegexp = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)

func (p *snowflakeDriver) SetSessionID(sessionID string) {
	if sessionID != "" && uuidRegexp.MatchString(sessionID) {
		p.sessionID = sessionID
		p.ctx = sf.WithRequestID(p.config.Context, sf.ParseUUID(sessionID))
	}
}

func (p *snowflakeDriver) RefreshSchema(ctx context.Context, db *sql.DB, failIfEmpty bool) error {
	if p.dbname == "" {
		dbname, err := util.GetCurrentDatabase(ctx, db, "CURRENT_DATABASE()")
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

func (p *snowflakeDriver) connectToDB(ctx context.Context, url string) (*sql.DB, error) {
	url, err := GetConnectionStringFromURL(url)
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
		return nil, err
	}

	if err := p.RefreshSchema(ctx, db, false); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

type DriverProfilingInfo struct {
	recordsProcessedCount    int
	lastExecutionDuration    time.Duration
	averageExecutionDuration time.Duration
}

func (p *DriverProfilingInfo) String() string {
	return fmt.Sprintf("records processed: %d, last batch duration: %v, average row execution duration: %v", p.recordsProcessedCount, p.lastExecutionDuration, p.averageExecutionDuration)
}

func (p *snowflakeDriver) addProfilingRecord(duration time.Duration, recordsProcessed int) {
	p.profilingInfo.lastExecutionDuration = duration
	oldCount := p.profilingInfo.recordsProcessedCount
	newCount := oldCount + recordsProcessed
	oldAverage := p.profilingInfo.averageExecutionDuration
	newAverage := time.Duration((oldAverage.Nanoseconds()*int64(oldCount) + duration.Nanoseconds()) / int64(newCount))
	p.profilingInfo.averageExecutionDuration = newAverage
	p.profilingInfo.recordsProcessedCount = newCount
}

// Used to allow embedding in temporary snowflake_keypair driver
func (p *snowflakeDriver) getConnectFunc() func(context.Context, string) (*sql.DB, error) {
	if p.connectToDBFunc != nil {
		return p.connectToDBFunc
	}
	return p.connectToDB
}

// Start the driver. This is called once at the beginning of the driver's lifecycle.
func (p *snowflakeDriver) Start(config internal.DriverConfig) error {
	p.config = config
	p.ctx = config.Context
	p.logger = config.Logger.WithPrefix("[snowflake]")
	p.registry = config.SchemaRegistry
	p.updateStrategy = config.UpdateStrategy
	p.bulkRecordModifications = snowflakeBulkRecordModifications
	db, err := p.getConnectFunc()(config.Context, config.URL)
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
	p.batcher.Add(&event)
	return false, nil
}

var sequence int64

var snowflakeBulkRecordModifications = []func([]*util.Record) []*util.Record{
	util.SortRecordsByMVCCTimestamp,
	util.CombineRecordsWithSamePrimaryKey,
}

func (p *snowflakeDriver) runBulkRecordModifications(records []*util.Record) []*util.Record {
	for i := range p.bulkRecordModifications {
		records = p.bulkRecordModifications[i](records)
	}
	return records
}

// Flush is called to commit any pending events. It should return an error if the flush fails. If the flush fails, the driver will NAK all pending events.
func (p *snowflakeDriver) Flush(logger logger.Logger) error {
	logger.Debug("Flush method called") // Add this line to see if Flush is being called
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
		records = p.runBulkRecordModifications(records)
		tag := fmt.Sprintf("eds-%s/%d/%d", p.sessionID, sequence, count)
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
				justUpdatedMeta := len(record.Diff) == 2 && slices.Contains(record.Diff, "updatedDate") && slices.Contains(record.Diff, "meta")
				noUpdates := len(record.Diff) == 0
				if justUpdatedDate || noUpdates || justUpdatedMeta {
					logger.Trace("skipping update because only either updatedDate, meta and updatedDate, or nothing changed for %s/%s", record.Table, record.Id)
					continue
				}
			case "DELETE":
				key = fmt.Sprintf("snowflake:%s:%s", record.Table, record.Id)
				deletekeys = append(deletekeys, key)
			}
			_, version, err := p.registry.GetTableVersion(record.Table)
			if err != nil {
				return fmt.Errorf("unable to get table version for table: %s: %w", record.Table, err)
			}
			schema, err := p.registry.GetSchema(record.Table, version)
			if err != nil {
				return fmt.Errorf("unable to get schema for table: %s (%s). %w", record.Table, version, err)
			}
			sql, c := toSQL(record, schema, force, p.updateStrategy)
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
			res, err := p.db.ExecContext(execCTX, query.String())
			if err != nil {
				return fmt.Errorf("unable to run query: %s: %w", query.String(), err)
			}
			rows, _ := res.RowsAffected()
			executionDuration := time.Since(ts)
			p.addProfilingRecord(executionDuration, count)
			// TODO: Diagnose incorrect statement count:
			// TODO: The current after update strategy resulted in duplicate pks, so we're seeing two rows updated sometimes.
			// TODO: Need to fix this. Possibly need to change inserts to merges for all strategies to avoid missing inserts
			if rows != int64(statementCount) {
				logger.Warn("executed query (%s/%d/%d) in %v (expected %d rows, was %d)", tag, statementCount, rows, executionDuration, statementCount, rows)
			} else {
				logger.Trace("executed query (%s/%d/%d) in %v", tag, statementCount, rows, executionDuration)
			}
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
	logger.Debug("profiling info for session %s: %s", p.sessionID, p.profilingInfo.String())
	return nil
}

// Import is called to import data from the source.
func (p *snowflakeDriver) Import(config internal.ImporterConfig) error {
	p.logger = config.Logger.WithPrefix("[snowflake]")
	db, err := p.getConnectFunc()(config.Context, config.URL)
	if err != nil {
		return err
	}
	defer db.Close()

	p.registry = config.SchemaRegistry
	executeSQL := util.SQLExecuter(config.Context, p.logger, db, config.DryRun)
	p.logger.Info("loading data into database")

	schema, err := p.registry.GetLatestSchema()
	if err != nil {
		return fmt.Errorf("unable to get latest schema: %w", err)
	}

	// create all the tables
	for _, table := range config.Tables {
		data := schema[table]
		p.logger.Debug("creating table %s", table)
		if err := executeSQL(createSQL(data)); err != nil {
			return fmt.Errorf("error creating table: %s. %w", table, err)
		}
		p.logger.Debug("created table %s", table)
	}

	if config.SchemaOnly {
		return nil
	}

	jobId := config.JobID
	if jobId == "" {
		jobId = util.Hash(time.Now().UnixNano()) // this can happen if we're not running in a job
	}

	// create a stage
	stageName := "eds_import_" + jobId
	p.logger.Debug("creating stage %s", stageName)
	if err := executeSQL("CREATE STAGE " + stageName); err != nil {
		return fmt.Errorf("error creating stage: %s", err)
	}
	p.logger.Debug("stage %s created", stageName)

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
	p.logger.Debug("files uploaded in %v", time.Since(started))

	// import the data
	var wg sync.WaitGroup
	errorChannel := make(chan error, len(config.Tables))

	var sem = semaphore.NewWeighted(int64(4))

	for _, table := range config.Tables {
		wg.Add(1)
		go func(table string) {
			defer util.RecoverPanic(p.logger)
			defer func() {
				sem.Release(1)
				wg.Done()
			}()
			sem.Acquire(config.Context, 1)
			if err := executeSQL(fmt.Sprintf(`COPY INTO %s FROM @%s MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = true COMPRESSION = 'GZIP') PATTERN='.*-%s-.*'`, util.QuoteIdentifier(table), stageName, table)); err != nil {
				p.logger.Trace("error importing data: %s", err)
				errorChannel <- fmt.Errorf("error importing %s data: %s", table, err)
			}
		}(table)
	}
	p.logger.Debug("waiting for all tables to import")
	wg.Wait()
	p.logger.Debug("all tables completed in %v", time.Since(started))

	errs := make([]error, 0)
done:
	for {
		select {
		case err := <-errorChannel:
			p.logger.Error("%s", err)
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
	return "Snowflake [DEPRECATED]"
}

// Description is the description of the driver.
func (p *snowflakeDriver) Description() string {
	return "This driver is provided for legacy support of Snowflake username/password authentication. New Snowflake connections should use the Snowflake Key Pair driver."
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
	p.logger = logger.WithPrefix("[snowflake]")
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

// MigrateNewTable is called when a new table is detected with the appropriate information for the driver to perform the migration.
func (p *snowflakeDriver) MigrateNewTable(ctx context.Context, logger logger.Logger, schema *internal.Schema) error {
	p.waitGroup.Add(1)
	defer p.waitGroup.Done()
	if _, ok := p.dbschema[schema.Table]; ok {
		logger.Info("table already exists for: %s, dropping and recreating...", schema.Table)
		if err := util.DropTable(ctx, logger, p.db, util.QuoteIdentifier(schema.Table)); err != nil {
			return err
		}
		delCount, err := p.config.Tracker.DeleteKeysWithPrefix("snowflake:" + schema.Table + ":")
		if err != nil {
			return fmt.Errorf("error deleting cache keys on table truncate: %w", err)
		}
		logger.Debug("deleted %d cache keys for table %s", delCount, schema.Table)
	}
	sql := createSQL(schema)
	logger.Trace("migrate new table: %s", sql)
	if _, err := p.db.ExecContext(ctx, sql); err != nil {
		return err
	}
	return p.RefreshSchema(ctx, p.db, true)
}

// MigrateNewColumns is called when one or more new columns are detected with the appropriate information for the driver to perform the migration.
func (p *snowflakeDriver) MigrateNewColumns(ctx context.Context, logger logger.Logger, schema *internal.Schema, columns []string) error {
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
	return p.RefreshSchema(ctx, p.db, true)
}

func (p *snowflakeDriver) GetDestinationSchema(ctx context.Context, logger logger.Logger) internal.DatabaseSchema {
	return p.dbschema
}

func init() {
	internal.RegisterDriver("snowflake", &snowflakeDriver{})
	internal.RegisterImporter("snowflake", &snowflakeDriver{})
}
