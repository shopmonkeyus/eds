package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
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

func shouldSkip(table string, only []string, except []string) bool {
	if len(only) > 0 && !sliceContains(only, table) {
		return true
	}
	if len(except) > 0 && sliceContains(except, table) {
		return true
	}
	return false
}

type property struct {
	Type     string `json:"type"`
	Format   string `json:"format"`
	Nullable bool   `json:"nullable"`
}

type schema struct {
	Properties  map[string]property `json:"properties"`
	Required    []string            `json:"required"`
	PrimaryKeys []string            `json:"primaryKeys"`
	Table       string              `json:"table"`
}

func propTypeToSQLType(propType string, format string) string {
	switch propType {
	case "string":
		if format == "date-time" {
			return "TIMESTAMP_NTZ"
		}
		return "STRING"
	case "integer":
		return "INTEGER"
	case "number":
		return "FLOAT"
	case "boolean":
		return "BOOLEAN"
	case "object":
		return "STRING"
	case "array":
		return "VARIANT"
	default:
		return "STRING"
	}
}

func sliceContains(slice []string, val string) bool {
	for _, s := range slice {
		if s == val {
			return true
		}
	}
	return false
}

func quoteIdentifier(name string) string {
	return `"` + name + `"`
}

func (s schema) createSQL() string {
	var sql strings.Builder
	sql.WriteString("CREATE OR REPLACE TABLE ")
	sql.WriteString(quoteIdentifier((s.Table)))
	sql.WriteString(" (\n")
	for name, prop := range s.Properties {
		sql.WriteString("\t")
		sql.WriteString(quoteIdentifier(name))
		sql.WriteString(" ")
		sql.WriteString(propTypeToSQLType(prop.Type, prop.Format))
		if sliceContains(s.Required, name) && !prop.Nullable {
			sql.WriteString(" NOT NULL")
		}
		sql.WriteString(",\n")
	}
	if len(s.PrimaryKeys) > 0 {
		sql.WriteString("\tPRIMARY KEY (")
		for i, pk := range s.PrimaryKeys {
			sql.WriteString(quoteIdentifier(pk))
			if i < len(s.PrimaryKeys)-1 {
				sql.WriteString(", ")
			}
		}
		sql.WriteString(")")
	}
	sql.WriteString("\n);\n")
	return sql.String()
}

func loadSchema(apiURL string) (map[string]schema, error) {
	resp, err := http.Get(apiURL + "/v3/schema")
	if err != nil {
		return nil, fmt.Errorf("error fetching schema: %s", err)
	}
	defer resp.Body.Close()
	tables := make(map[string]schema)
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&tables); err != nil {
		return nil, fmt.Errorf("error decoding schema: %s", err)
	}
	return tables, nil
}

func sqlExecuter(ctx context.Context, log glogger.Logger, db *sql.DB, dryRun bool) func(sql string) error {
	return func(sql string) error {
		if dryRun {
			log.Info("%s", sql)
			return nil
		}
		log.Debug("executing: %s", sql)
		if _, err := db.ExecContext(ctx, sql); err != nil {
			return err
		}
		return nil
	}
}

func migrateDB(ctx context.Context, log glogger.Logger, db *sql.DB, tables map[string]schema, only []string, dryRun bool) error {
	executeSQL := sqlExecuter(ctx, log, db, dryRun)
	for _, schema := range tables {
		if shouldSkip(schema.Table, only, nil) {
			continue
		}
		if err := executeSQL(schema.createSQL()); err != nil {
			return fmt.Errorf("error creating table: %s. %w", schema.Table, err)
		}
	}
	return nil
}

func runImport(ctx context.Context, log glogger.Logger, db *sql.DB, tables map[string]schema, only []string, jobID string, dataDir string, dryRun bool) error {

	executeSQL := sqlExecuter(ctx, log, db, dryRun)

	stageName := "eds_import_" + jobID
	log.Debug("creating stage %s", stageName)
	// create a stage
	// CREATE TEMP STAGE eds_import_6271949614cbe915929e0e0f;
	if err := executeSQL("CREATE TEMP STAGE " + stageName); err != nil {
		return fmt.Errorf("error creating stage: %s", err)
	}

	// upload files
	for _, schema := range tables {
		if shouldSkip(schema.Table, only, nil) {
			continue
		}
		// put 'file:///Users/robindiddams/work/eds-server/dist/6271949614cbe915929e0e0f/user.json.gz' @eds_import_6271949614cbe915929e0e0f SOURCE_COMPRESSION=gzip;
		if err := executeSQL(fmt.Sprintf(`PUT 'file://%s/%s/*.json.gz' @%s/%s SOURCE_COMPRESSION=gzip`, dataDir, schema.Table, stageName, schema.Table)); err != nil {
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
		apiURL, _ := cmd.Flags().GetString("api-url")
		only, _ := cmd.Flags().GetStringSlice("only")

		log := newLogger(cmd, glogger.LevelDebug)
		log.Info("🚀 Starting import")
		if dryRun {
			log.Info("🚨 Dry run enabled")
		}

		schema, err := loadSchema(apiURL)
		if err != nil {
			log.Error("error loading schema: %s", err)
			os.Exit(1)
		}

		db, err := connect2DB(ctx, mustFlagString(cmd, "db-url", true))
		if err != nil {
			log.Error("error connecting to db: %s", err)
			os.Exit(1)
		}

		defer db.Close()

		jobID := fmt.Sprintf("%d", time.Now().Unix()) // todo: generate a unique job id

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
		if err := migrateDB(ctx, log, db, schema, only, dryRun); err != nil {
			log.Error("error migrating db: %s", err)
			return
		}

		if err := runImport(ctx, log, db, schema, only, jobID, dir, dryRun); err != nil {
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
	importCmd.Flags().String("api-url", "https://api.shopmonkey.cloud", "url to shopmonkey api")
	importCmd.Flags().StringSlice("only", nil, "only import these tables")
}
