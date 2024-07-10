package cmd

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/shopmonkeyus/eds-server/internal/util"
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

// generic api response
type apiResponse[T any] struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Data    T      `json:"data"`
}

func decodeShopmonkeyAPIResponse[T any](resp *http.Response) (*T, error) {
	var apiResp apiResponse[T]
	dec := json.NewDecoder(resp.Body)
	if err := dec.Decode(&apiResp); err != nil {
		return nil, fmt.Errorf("error decoding response: %s", err)
	}
	if !apiResp.Success {
		return nil, fmt.Errorf("api error: %s", apiResp.Message)
	}
	return &apiResp.Data, nil
}

type exportJobCreateResponse struct {
	JobID string `json:"jobId"`
}

type exportJobCreateRequest struct {
	CompanyIDs  []string `json:"companyIds,omitempty"`
	LocationIDs []string `json:"locationIds,omitempty"`
	Tables      []string `json:"tables,omitempty"`
}

func createExportJob(ctx context.Context, apiURL string, apiKey string, filters exportJobCreateRequest) (string, error) {
	// encode the request
	body, err := json.Marshal(filters)
	if err != nil {
		return "", fmt.Errorf("error encoding request: %s", err)
	}
	req, err := http.NewRequestWithContext(ctx, "POST", apiURL+"/v3/export/bulk", bytes.NewBuffer(body))
	if err != nil {
		return "", fmt.Errorf("error creating request: %s", err)
	}
	req.Header = http.Header{
		"Authorization": {"Bearer " + apiKey},
		"Content-Type":  {"application/json"},
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("error fetching schema: %s", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		buf, _ := io.ReadAll(resp.Body)
		fmt.Println(string(buf))
	}

	job, err := decodeShopmonkeyAPIResponse[exportJobCreateResponse](resp)
	if err != nil {
		return "", fmt.Errorf("error decoding response: %s", err)
	}
	return job.JobID, nil
}

type exportJobTableData struct {
	Error  string   `json:"error"`
	Status string   `json:"status"`
	URLs   []string `json:"urls"`
}

type exportJobResponse struct {
	Completed bool                          `json:"completed"`
	Tables    map[string]exportJobTableData `json:"tables"`
}

func checkExportJob(ctx context.Context, apiURL string, apiKey string, jobID string) (*exportJobResponse, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", apiURL+"/v3/export/bulk/"+jobID, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request: %s", err)
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error fetching schema: %s", err)
	}
	defer resp.Body.Close()

	job, err := decodeShopmonkeyAPIResponse[exportJobResponse](resp)
	if err != nil {
		return nil, fmt.Errorf("error decoding response: %s", err)
	}
	return job, nil
}

func pollUntilComplete(ctx context.Context, log glogger.Logger, apiURL string, apiKey string, jobID string) (exportJobResponse, error) {
	for {
		log.Info("checking job status")
		job, err := checkExportJob(ctx, apiURL, apiKey, jobID)
		if err != nil {
			return exportJobResponse{}, err
		}
		log.Debug("job status: %s", util.MustJSONStringify(job, false))
		if job.Completed && len(job.Tables) > 0 {
			return *job, nil
		}
		// TODO: check for errors!
		time.Sleep(5 * time.Second)
	}
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
		if err := executeSQL(fmt.Sprintf(`PUT 'file://%s/%s/*.ndjson.gz' @%s/%s SOURCE_COMPRESSION=gzip`, dataDir, schema.Table, stageName, schema.Table)); err != nil {
			return fmt.Errorf("error uploading files: %s", err)
		}
	}

	// import the data
	for _, schema := range tables {
		if shouldSkip(schema.Table, only, nil) {
			continue
		}
		if err := executeSQL(fmt.Sprintf(`COPY INTO %s FROM @%s/%s FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = 'GZIP')`, schema.Table, stageName, schema.Table)); err != nil {
			return fmt.Errorf("error importing data: %s", err)
		}
	}
	return nil
}

func downloadFile(log glogger.Logger, dir string, fullURL string) error {
	parsedURL, err := url.Parse(fullURL)
	if err != nil {
		return fmt.Errorf("error parsing url: %s", err)
	}
	baseFileName := filepath.Base(parsedURL.Path)
	// download the file
	resp, err := http.Get(fullURL)
	if err != nil {
		return fmt.Errorf("error fetching data: %s", err)
	}
	defer resp.Body.Close()
	filename := fmt.Sprintf("%s/%s", dir, baseFileName)
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("error creating file: %s", err)
	}
	if _, err := io.Copy(file, resp.Body); err != nil {
		return fmt.Errorf("error writing file: %s", err)
	}
	log.Debug("downloaded file %s", filename)
	return nil
}

type donwloadPacket struct {
	table string
	url   string
}

func bulkDownloadData(log glogger.Logger, data map[string]exportJobTableData, dir string) error {
	var downloads []donwloadPacket
	for table, tableData := range data {
		tableDir := filepath.Join(dir, table)
		if err := os.MkdirAll(tableDir, 0755); err != nil {
			return fmt.Errorf("error creating dir: %s", err)
		}
		for _, fullURL := range tableData.URLs {
			downloads = append(downloads, donwloadPacket{table, fullURL})
		}
	}
	if len(downloads) == 0 {
		log.Info("no files to download")
		return nil
	}

	log.Info("downloading %d files", len(downloads))
	concurrency := 10
	downloadChan := make(chan donwloadPacket, len(downloads))
	var downloadWG sync.WaitGroup
	errors := make(chan error, concurrency)

	// start the download workers
	for i := 0; i < concurrency; i++ {
		downloadWG.Add(1)
		go func() {
			defer downloadWG.Done()
			for packet := range downloadChan {
				if err := downloadFile(log, filepath.Join(dir, packet.table), packet.url); err != nil {
					errors <- fmt.Errorf("error downloading file: %s", err)
					return
				}
			}
		}()
	}

	// send the downloads
	for _, packet := range downloads {
		downloadChan <- packet
	}
	close(downloadChan)

	// check for errors
	select {
	case err := <-errors:
		return err
	default:
	}
	// wait for the downloads to finish
	downloadWG.Wait()

	return nil
}

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "import data from your shopmonkey instance to your external database",
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		log := newLogger(cmd, glogger.LevelDebug)

		dryRun, _ := cmd.Flags().GetBool("dry-run")
		apiURL, _ := cmd.Flags().GetString("api-url")
		apiKey, _ := cmd.Flags().GetString("api-key")
		only, _ := cmd.Flags().GetStringSlice("only")

		log.Info("🚀 Starting import")
		if dryRun {
			log.Info("🚨 Dry run enabled")
		}

		if cmd.Flags().Changed("api-url") {
			log.Info("using alternative API url: %s", apiURL)
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

		// create a new job from the api
		jobID, err := createExportJob(ctx, apiURL, apiKey, exportJobCreateRequest{
			Tables: only,
		})
		if err != nil {
			log.Error("error creating export job: %s", err)
			return
		}

		log.Info("created job: %s", jobID)

		// poll until the job is complete
		job, err := pollUntilComplete(ctx, log, apiURL, apiKey, jobID)
		if err != nil {
			log.Error("error polling job: %s", err)
			return
		}

		// download the files
		dir, err := os.MkdirTemp("", "eds-import")
		if err != nil {
			log.Error("error creating temp dir: %s", err)
			return
		}
		defer os.RemoveAll(dir)

		// get the urls from the api
		if err := bulkDownloadData(log, job.Tables, dir); err != nil {
			log.Error("error downloading files: %s", err)
			return
		}
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
	importCmd.Flags().String("api-key", os.Getenv("SM_APIKEY"), "shopmonkey api key")
	importCmd.Flags().StringSlice("only", nil, "only import these tables")
}
