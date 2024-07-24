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

type snowflakeProcessor struct {
	config    internal.ProcessorConfig
	logger    logger.Logger
	pending   strings.Builder
	count     int
	db        *sql.DB
	schema    internal.SchemaMap
	waitGroup sync.WaitGroup
	once      sync.Once
	ctx       context.Context
	sessionID string
}

var _ internal.Processor = (*snowflakeProcessor)(nil)
var _ internal.ProcessorLifecycle = (*snowflakeProcessor)(nil)
var _ internal.Importer = (*snowflakeProcessor)(nil)
var _ internal.ProcessorSessionHandler = (*snowflakeProcessor)(nil)

func (p *snowflakeProcessor) SetSessionID(sessionID string) {
	if sessionID != "" {
		p.sessionID = sessionID
		p.ctx = sf.WithRequestID(p.config.Context, sf.ParseUUID(sessionID))
	}
}

func (p *snowflakeProcessor) connectToDB(ctx context.Context, url string) (*sql.DB, error) {
	url, err := getConnectionStringFromURL(url)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("snowflake", url)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("unable to ping db: %w", err)
	}
	return db, nil
}

// Start the processor. This is called once at the beginning of the processor's lifecycle.
func (p *snowflakeProcessor) Start(config internal.ProcessorConfig) error {
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
	p.logger.Debug("started")
	return nil
}

// Stop the processor. This is called once at the end of the processor's lifecycle.
func (p *snowflakeProcessor) Stop() error {
	p.logger.Debug("stopping")
	p.once.Do(func() {
		p.logger.Debug("waiting on waitgroup")
		p.waitGroup.Wait()
		p.logger.Debug("completed waitgroup")
		p.Flush() // make sure we flush
		if p.db != nil {
			p.logger.Debug("closing db")
			p.db.Close()
			p.db = nil
			p.count = 0
			p.logger.Debug("closed db")
		}
	})
	p.logger.Debug("stopped")
	return nil
}

// MaxBatchSize returns the maximum number of events that can be processed in a single call to Process and when Flush should be called.
func (p *snowflakeProcessor) MaxBatchSize() int {
	return -1
}

// Process a single event. It returns a bool indicating whether Flush should be called. If an error is returned, the processor will NAK the event.
func (p *snowflakeProcessor) Process(event internal.DBChangeEvent) (bool, error) {
	p.logger.Trace("processing event: %s", event.String())
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

var queryCount int64

// Flush is called to commit any pending events. It should return an error if the flush fails. If the flush fails, the processor will NAK all pending events.
func (p *snowflakeProcessor) Flush() error {
	p.logger.Debug("flush: %v", p.count)
	p.waitGroup.Add(1)
	defer p.waitGroup.Done()
	if p.count > 0 {
		queryCount++
		ctx := sf.WithQueryTag(context.Background(), fmt.Sprintf("eds-%s/%d", p.sessionID, queryCount))
		execCTX, err := sf.WithMultiStatement(ctx, p.count) // for the transaction below
		if err != nil {
			return fmt.Errorf("error creating exec context: %w", err)
		}
		if _, err := p.db.ExecContext(execCTX, p.pending.String()); err != nil {
			return fmt.Errorf("unable to run query: %s: %w", p.pending.String(), err)
		}
	}
	p.pending.Reset()
	p.count = 0
	return nil
}

// Import is called to import data from the source.
func (p *snowflakeProcessor) Import(config internal.ImporterConfig) error {
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

// Description is the description of the processor.
func (p *snowflakeProcessor) Description() string {
	return "Supports streaming EDS messages to a Snowflake database."
}

// ExampleURL should return an example URL for configuring the processor.
func (p *snowflakeProcessor) ExampleURL() string {
	return "snowflake://user:password@host/database"
}

// Help should return a detailed help documentation for the processor.
func (p *snowflakeProcessor) Help() string {
	var help strings.Builder
	help.WriteString(util.GenerateHelpSection("Schema", "The database will match the public schema from the Shopmonkey transactional database.\n"))
	return help.String()
}

func init() {
	var processor snowflakeProcessor
	internal.RegisterProcessor("snowflake", &processor)
	internal.RegisterImporter("snowflake", &processor)
}
