package cmd

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	glogger "github.com/shopmonkeyus/go-common/logger"
	"github.com/spf13/cobra"
)

func connect2DB(ctx context.Context, url string) (*sql.DB, error) {
	db, err := sql.Open("snowflake", url)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection: %s", err.Error())
	}
	row := db.QueryRowContext(ctx, "SELECT 1")
	if err := row.Err(); err != nil {
		return nil, fmt.Errorf("unable to query db: %s", err.Error())
	}
	return db, nil
}

func migrateDB(ctx context.Context, db *sql.DB) error {
	// create tables and or truncate tables

	return nil
}

func runImport(ctx context.Context, log glogger.Logger, db *sql.DB, jobID string, dataDir string, dryRun bool) error {

	executeSQL := func(sql string) error {
		if dryRun {
			log.Info("🚨 Dry run: would have run: %s", sql)
			return nil
		}
		log.Debug("executing: %s", sql)
		if _, err := db.ExecContext(ctx, sql); err != nil {
			return err
		}
		return nil
	}

	// create a stage
	// CREATE TEMP STAGE eds_import_6271949614cbe915929e0e0f;
	if err := executeSQL("CREATE TEMP STAGE eds_import_" + jobID); err != nil {
		return fmt.Errorf("error creating stage: %s", err)
	}

	// upload files
	tables := []string{"user"} //FIXME
	for _, table := range tables {
		// put 'file:///Users/robindiddams/work/eds-server/dist/6271949614cbe915929e0e0f/user.json.gz' @eds_import_6271949614cbe915929e0e0f SOURCE_COMPRESSION=gzip;
		if err := executeSQL(fmt.Sprintf(`PUT 'file://%s/%s/*.json.gz' @eds_import_%s/%s SOURCE_COMPRESSION=gzip`, dataDir, table, jobID, table)); err != nil {
			return fmt.Errorf("error uploading files: %s", err)
		}
	}

	// import the data

	// 	COPY INTO user_data
	// FROM @eds_import_6271949614cbe915929e0e0f/user.json.gz
	// FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = 'GZIP');
	// TODO!

	return nil
}

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "import data from your shopmonkey instance to your external database",
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		dryRun, _ := cmd.Flags().GetBool("dry-run")

		log := newLogger(cmd, glogger.LevelDebug)
		log.Info("🚀 Starting import")
		if dryRun {
			log.Info("🚨 Dry run enabled")
		}

		db, err := connect2DB(ctx, mustFlagString(cmd, "db-url", true))
		if err != nil {
			log.Error("error connecting to db: %s", err)
			return
		}

		defer db.Close()

		jobID := string(time.Now().Unix()) // todo: generate a unique job id

		// create a new job from the api

		// get the urls from the api

		// download the files
		dir, err := os.MkdirTemp("", "eds-import")
		if err != nil {
			log.Error("error creating temp dir: %s", err)
			return
		}
		defer os.RemoveAll(dir)

		// migrate the db
		if err := migrateDB(ctx, db); err != nil {
			log.Error("error migrating db: %s", err)
			return
		}

		if err := runImport(ctx, log, db, jobID, dir, dryRun); err != nil {
			log.Error("error running import: %s", err)
			return
		}

		log.Info("👋 Bye")
	},
}

func init() {
	rootCmd.AddCommand(importCmd)
	importCmd.Flags().Bool("dry-run", false, "only simulate loading but don't actually make db changes")
	importCmd.Flags().String("db-url", "", "snowflake connection string")
}
