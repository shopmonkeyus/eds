package cmd

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	glog "log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/huh"
	"github.com/shopmonkeyus/eds-server/internal/util"
	glogger "github.com/shopmonkeyus/go-common/logger"
	csys "github.com/shopmonkeyus/go-common/sys"
	"github.com/spf13/cobra"
)

func connect2DB(ctx context.Context, url string) (*sql.DB, error) {
	db, err := sql.Open("snowflake", url)
	if err != nil {
		return nil, fmt.Errorf("unable to create connection: %s", err.Error())
	}
	row := db.QueryRowContext(ctx, "SELECT 1")
	if err := row.Err(); err != nil {
		return nil, fmt.Errorf("unable to ping db: %s", err.Error())
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

var skipFields = map[string]bool{
	"meta": true,
}

func (s schema) createSQL() string {
	var sql strings.Builder
	sql.WriteString("CREATE OR REPLACE TABLE ")
	sql.WriteString(quoteIdentifier((s.Table)))
	sql.WriteString(" (\n")
	for name, prop := range s.Properties {
		if skipFields[name] {
			continue
		}
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
		return "", fmt.Errorf("API error: %s (status code=%d)", string(buf), resp.StatusCode)
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

func (e *exportJobResponse) GetProgress() float64 {
	var completed, total int
	for _, data := range e.Tables {
		total++
		if data.Status == "Completed" {
			completed++
		}
	}
	if total == 0 {
		return 0
	}
	return float64(completed) / float64(total)
}

func (e *exportJobResponse) String() string {
	var tablesAndStatus []string
	var pending, completed, failed int
	for table, data := range e.Tables {
		tablesAndStatus = append(tablesAndStatus, fmt.Sprintf("%s: %s", table, data.Status))
		switch data.Status {
		case "Pending":
			pending++
		case "Completed":
			completed++
		case "Failed":
			failed++
		}
	}
	slices.Sort(tablesAndStatus)
	return fmt.Sprintf("%s\n %d/%d", strings.Join(tablesAndStatus, "\n"), completed, len(e.Tables))
}

func checkExportJob(ctx context.Context, apiURL string, apiKey string, jobID string) (*exportJobResponse, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", apiURL+"/v3/export/bulk/"+jobID, nil)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, nil
		}
		return nil, fmt.Errorf("error creating request: %s", err)
	}
	req.Header = http.Header{
		"Authorization": {"Bearer " + apiKey},
		"Accept":        {"application/json"},
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error fetching schema: %s", err)
	}
	defer resp.Body.Close()
	job, err := decodeShopmonkeyAPIResponse[exportJobResponse](resp)
	if err != nil {
		return nil, fmt.Errorf("error decoding response: %s", err)
	}
	for table, data := range job.Tables {
		if data.Status == "Failed" {
			return job, fmt.Errorf("error exporting table %s: %s", table, data.Error)
		}
	}
	return job, nil
}

func pollUntilComplete(ctx context.Context, apiURL string, apiKey string, jobID string, progressbar *util.ProgressBar) (exportJobResponse, error) {
	for {
		progressbar.SetMessage("Checking for Export Status (" + jobID + ")")
		job, err := checkExportJob(ctx, apiURL, apiKey, jobID)
		if err != nil {
			return exportJobResponse{}, err
		}
		if job == nil {
			return exportJobResponse{}, nil // cancelled
		}
		progressbar.SetProgress(job.GetProgress())
		if job.Completed {
			return *job, nil
		}
		progressbar.SetMessage("Waiting for Export to Complete")
		select {
		case <-ctx.Done():
			return exportJobResponse{}, nil
		case <-time.After(time.Second * 5):
		}
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

func migrateDB(ctx context.Context, log glogger.Logger, db *sql.DB, tables map[string]schema, only []string, dryRun bool, progressbar *util.ProgressBar) error {
	executeSQL := sqlExecuter(ctx, log, db, dryRun)
	total := len(tables)
	var i int
	for table, schema := range tables {
		progressbar.SetProgress(float64(i) / float64(total))
		i++
		if shouldSkip(schema.Table, only, nil) {
			continue
		}
		progressbar.SetMessage("Creating Schema ... " + table)
		if err := executeSQL(schema.createSQL()); err != nil {
			return fmt.Errorf("error creating table: %s. %w", schema.Table, err)
		}
		progressbar.SetMessage("Created Schema ... " + table)
	}
	return nil
}

func runImport(ctx context.Context, log glogger.Logger, db *sql.DB, tables []string, jobID string, dataDir string, dryRun bool, parallel int, progressbar *util.ProgressBar) error {
	executeSQL := sqlExecuter(ctx, log, db, dryRun)
	stageName := "eds_import_" + jobID
	log.Debug("creating stage %s", stageName)

	// create a stage
	if err := executeSQL("CREATE TEMP STAGE " + stageName); err != nil {
		return fmt.Errorf("error creating stage: %s", err)
	}

	// upload files
	progressbar.SetMessage("Uploading files...")
	if err := executeSQL(fmt.Sprintf(`PUT 'file://%s/*.ndjson.gz' @%s PARALLEL=%d SOURCE_COMPRESSION=gzip`, dataDir, stageName, parallel)); err != nil {
		return fmt.Errorf("error uploading files: %s", err)
	}

	// import the data
	for i, table := range tables {
		progressbar.SetMessage("Loading ... " + table)
		if err := executeSQL(fmt.Sprintf(`COPY INTO %s FROM @%s MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE FILE_FORMAT = (TYPE = 'JSON' STRIP_OUTER_ARRAY = true COMPRESSION = 'GZIP') PATTERN='.*-%s-.*'`, quoteIdentifier(table), stageName, table)); err != nil {
			return fmt.Errorf("error importing data: %s", err)
		}
		progressbar.SetProgress(float64(i) / float64(len(tables)))
	}
	return nil
}

func downloadFile(log glogger.Logger, dir string, fullURL string) error {
	parsedURL, err := url.Parse(fullURL)
	if err != nil {
		return fmt.Errorf("error parsing url: %s", err)
	}
	baseFileName := filepath.Base(parsedURL.Path)
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
	defer file.Close()
	if _, err := io.Copy(file, resp.Body); err != nil {
		return fmt.Errorf("error writing file: %s", err)
	}
	log.Debug("downloaded file %s", filename)
	return nil
}

func bulkDownloadData(log glogger.Logger, data map[string]exportJobTableData, dir string, progressbar *util.ProgressBar) ([]string, error) {
	var downloads []string
	var tablesWithData []string
	for table, tableData := range data {
		if len(tableData.URLs) > 0 {
			tablesWithData = append(tablesWithData, table)
		} else {
			log.Debug("no data for table %s", table)
		}
		downloads = append(downloads, tableData.URLs...)
	}
	if len(downloads) == 0 {
		log.Debug("no files to download")
		return nil, nil
	}

	concurrency := 10
	downloadChan := make(chan string, len(downloads))
	var downloadWG sync.WaitGroup
	errors := make(chan error, concurrency)
	total := float64(len(downloads))

	var completed int32

	// start the download workers
	for i := 0; i < concurrency; i++ {
		downloadWG.Add(1)
		go func() {
			defer downloadWG.Done()
			for url := range downloadChan {
				if err := downloadFile(log, dir, url); err != nil {
					errors <- fmt.Errorf("error downloading file: %s", err)
					return
				}
				val := atomic.AddInt32(&completed, 1)
				progressbar.SetProgress(float64(val) / total)
			}
		}()
	}

	// send the downloads
	for _, packet := range downloads {
		downloadChan <- packet
	}
	close(downloadChan)

	// wait for the downloads to finish
	downloadWG.Wait()

	// check for errors
	select {
	case err := <-errors:
		return nil, err
	default:
	}

	return tablesWithData, nil
}

func isCancelled(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "import data from your shopmonkey instance to your external database",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		confirmed, _ := cmd.Flags().GetBool("confirm")
		dbUrl := mustFlagString(cmd, "db-url", true)
		dryRun, _ := cmd.Flags().GetBool("dry-run")
		enableDebug, _ := cmd.Flags().GetBool("debug")
		parallel, _ := cmd.Flags().GetInt("parallel")
		if parallel <= 0 {
			parallel = 1
		} else if parallel > 99 {
			parallel = 99
		}
		glog.SetFlags(0)
		var log glogger.Logger
		if enableDebug {
			log = newLogger(glogger.LevelTrace)
		} else {
			log = newLogger(glogger.LevelInfo)
		}

		if !dryRun && !confirmed {
			parts := strings.Split(dbUrl, "/")
			dbName := parts[len(parts)-2]
			schemaName := parts[len(parts)-1]

			form := huh.NewForm(
				huh.NewGroup(
					huh.NewNote().
						Title("\n🚨 WARNING 🚨"),
					huh.NewConfirm().
						Title(fmt.Sprintf("YOU ARE ABOUT TO DELETE EVERYTHING IN %s/%s", dbName, schemaName)).
						Affirmative("Confirm").
						Negative("Cancel").
						Value(&confirmed),
				),
			)
			custom := huh.ThemeBase()
			form.WithTheme(custom)

			if err := form.Run(); err != nil {
				if !errors.Is(err, huh.ErrUserAborted) {
					log.Error("error running form: %s", err)
					log.Info("You may use --confirm to skip this prompt")
					os.Exit(1)
				}
			}
			if !confirmed {
				os.Exit(0)
			}
		}

		started := time.Now()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			select {
			case <-ctx.Done():
				return
			case <-csys.CreateShutdownChannel():
				cancel()
				return
			}
		}()

		apiURL, _ := cmd.Flags().GetString("api-url")
		apiKey, _ := cmd.Flags().GetString("api-key")
		only, _ := cmd.Flags().GetStringSlice("only")
		jobID, _ := cmd.Flags().GetString("job-id")

		companyIds, _ := cmd.Flags().GetStringSlice("companyIds")
		locationIds, _ := cmd.Flags().GetStringSlice("locationIds")

		if cmd.Flags().Changed("api-url") {
			log.Info("using alternative API url: %s", apiURL)
		}
		var err error

		db, err := connect2DB(ctx, dbUrl)
		if err != nil {
			log.Error("error connecting to db: %s", err)
			os.Exit(1)
		}
		defer db.Close()

		var schema map[string]schema

		util.RunTaskWithSpinner("Loading schema...", func() {
			schema, err = loadSchema(apiURL)
			if err != nil {
				log.Error("error loading schema: %s", err)
				os.Exit(1)
			}
		})

		if dryRun {
			log.Info("🚨 Dry run enabled")
		}

		if jobID == "" {
			// create a new job from the api
			util.RunTaskWithSpinner("Requesting Export...", func() {
				jobID, err = createExportJob(ctx, apiURL, apiKey, exportJobCreateRequest{
					Tables:      only,
					CompanyIDs:  companyIds,
					LocationIDs: locationIds,
				})
				if err != nil {
					log.Error("error creating export job: %s", err)
					os.Exit(1)
				}
			})
			log.Trace("created job: %s", jobID)
		}

		var job exportJobResponse

		util.RunWithProgress(ctx, cancel, func(progressbar *util.ProgressBar) {
			// poll until the job is complete
			progressbar.SetMessage("Waiting for Export to Complete...")
			job, err = pollUntilComplete(ctx, apiURL, apiKey, jobID, progressbar)
			if err != nil && !isCancelled(ctx) {
				log.Error("error polling job: %s", err)
				os.Exit(1)
			}
		})

		if isCancelled(ctx) {
			return
		}

		util.RunWithProgress(ctx, cancel, func(progressbar *util.ProgressBar) {
			// migrate the db
			progressbar.SetMessage("Preparing Database Tables...")
			if err := migrateDB(ctx, log, db, schema, only, dryRun, progressbar); err != nil {
				log.Error("error migrating db: %s", err)
				os.Exit(1)
			}
		})

		if isCancelled(ctx) {
			return
		}

		// download the files
		dir, err := os.MkdirTemp("", "eds-import")
		if err != nil {
			log.Error("error creating temp dir: %s", err)
			os.Exit(1)
		}
		success := true
		defer func() {
			if success {
				os.RemoveAll(dir)
			}
		}()

		var tables []string

		util.RunWithProgress(ctx, cancel, func(progressbar *util.ProgressBar) {
			// get the urls from the api
			progressbar.SetMessage("Downloading export data...")
			tables, err = bulkDownloadData(log, job.Tables, dir, progressbar)
			if err != nil {
				log.Error("error downloading files: %s", err)
				success = false
				os.Exit(1)
			}
		})

		if isCancelled(ctx) {
			return
		}

		util.RunWithProgress(ctx, cancel, func(progressbar *util.ProgressBar) {
			progressbar.SetMessage("Importing data...")
			if err := runImport(ctx, log, db, tables, jobID, dir, dryRun, parallel, progressbar); err != nil {
				log.Error("error running import: %s", err)
				success = false
				os.Exit(1)
			}
		})

		log.Info("👋 Loaded %d tables in %v", len(tables), time.Since(started))
	},
}

func init() {
	rootCmd.AddCommand(importCmd)
	importCmd.Flags().Bool("dry-run", false, "only simulate loading but don't actually make db changes")
	importCmd.Flags().String("db-url", "", "Snowflake Database connection string")
	importCmd.Flags().String("api-url", "https://api.shopmonkey.cloud", "url to shopmonkey api")
	importCmd.Flags().String("api-key", os.Getenv("SM_APIKEY"), "shopmonkey api key")
	importCmd.Flags().String("job-id", "", "resume an existing job")
	importCmd.Flags().Bool("confirm", false, "skip the confirmation prompt")
	importCmd.Flags().StringSlice("only", nil, "only import these tables")
	importCmd.Flags().StringSlice("companyIds", nil, "only import these company ids")
	importCmd.Flags().StringSlice("locationIds", nil, "only import these location ids")
	importCmd.Flags().Bool("debug", false, "turn on debug logging")
	importCmd.Flags().Int("parallel", 4, "the number of parallel upload tasks")
}
