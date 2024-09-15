package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/huh"
	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/registry"
	"github.com/shopmonkeyus/eds/internal/tracker"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
	"github.com/shopmonkeyus/go-common/sys"
	"github.com/spf13/cobra"
)

// generic api response
type apiResponse[T any] struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Data    T      `json:"data"`
}

func decodeAPIResponse[T any](resp *http.Response) (*T, error) {
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
	TimeOffset  *int64   `json:"timeOffset,omitempty"`
	CompanyIDs  []string `json:"companyIds,omitempty"`
	LocationIDs []string `json:"locationIds,omitempty"`
	Tables      []string `json:"tables,omitempty"`
}

type errorResponse struct {
	Message string `json:"message"`
}

func (e *errorResponse) Parse(buf []byte, statusCode int, context string, requestId string) error {
	var requestIdTag string
	if requestId != "" {
		requestIdTag = fmt.Sprintf("(requestId=%s)", requestId)
	}
	if err := json.Unmarshal(buf, e); err == nil {
		return fmt.Errorf("%s: %s %s", context, e.Message, requestIdTag)
	}
	return fmt.Errorf("%s: %s (status code=%d) %s", context, string(buf), statusCode, requestIdTag)
}

func handleAPIError(resp *http.Response, context string) error {
	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("%s: error reading response: %w", context, err)
	}
	var errResponse errorResponse
	return errResponse.Parse(buf, resp.StatusCode, context, getRequestID(resp))
}

func createExportJob(ctx context.Context, logger logger.Logger, apiURL string, apiKey string, filters exportJobCreateRequest) (string, error) {
	// encode the request
	body, err := json.Marshal(filters)
	if err != nil {
		return "", fmt.Errorf("error encoding request: %s", err)
	}
	req, err := http.NewRequestWithContext(ctx, "POST", apiURL+"/v3/export/bulk", bytes.NewBuffer(body))
	if err != nil {
		return "", fmt.Errorf("error creating request: %s", err)
	}
	setHTTPHeader(req, apiKey)
	retry := util.NewHTTPRetry(req, util.WithLogger(logger))
	resp, err := retry.Do()
	if err != nil {
		return "", fmt.Errorf("error creating bulk export job: %s", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", handleAPIError(resp, "import")
	}
	job, err := decodeAPIResponse[exportJobCreateResponse](resp)
	if err != nil {
		return "", fmt.Errorf("error decoding response: %s", err)
	}
	return job.JobID, nil
}

type exportJobTableData struct {
	Error  string   `json:"error"`
	Status string   `json:"status"`
	URLs   []string `json:"urls"`
	Cursor string   `json:"cursor"`
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
	var pending, completed, failed int
	for _, data := range e.Tables {
		switch data.Status {
		case "Pending":
			pending++
		case "Completed":
			completed++
		case "Failed":
			failed++
		}
	}
	var percent float64
	if completed > 0 {
		percent = 100 * float64(completed) / float64(len(e.Tables))
	}
	return fmt.Sprintf("%d/%d (%.2f%%)", completed, len(e.Tables), percent)
}

func checkExportJob(ctx context.Context, logger logger.Logger, apiURL string, apiKey string, jobID string) (*exportJobResponse, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", apiURL+"/v3/export/bulk/"+jobID, nil)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, nil
		}
		return nil, fmt.Errorf("error creating request: %s", err)
	}
	setHTTPHeader(req, apiKey)
	retry := util.NewHTTPRetry(req, util.WithLogger(logger))
	resp, err := retry.Do()
	if err != nil {
		return nil, fmt.Errorf("error fetching bulk export status: %s", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, handleAPIError(resp, "import")
	}
	job, err := decodeAPIResponse[exportJobResponse](resp)
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

func pollUntilComplete(ctx context.Context, logger logger.Logger, apiURL string, apiKey string, jobID string) (exportJobResponse, error) {
	var lastPrinted time.Time
	for {
		var showProgress bool
		if lastPrinted.IsZero() || time.Since(lastPrinted) > time.Minute {
			logger.Info("Checking for Export Status (" + jobID + ")")
			lastPrinted = time.Now()
			showProgress = true
		}
		job, err := checkExportJob(ctx, logger, apiURL, apiKey, jobID)
		if err != nil {
			return exportJobResponse{}, err
		}
		if job == nil {
			return exportJobResponse{}, nil // cancelled
		}
		if job.Completed {
			logger.Info("Export Progress: %s", job.String())
			return *job, nil
		}
		logger.Debug("Waiting for Export to Complete: %s", job.String())
		if showProgress {
			logger.Info("Export Progress: %s", job.String())
		}
		select {
		case <-ctx.Done():
			return exportJobResponse{}, nil
		case <-time.After(time.Second * 5):
		}
	}
}

func downloadFile(log logger.Logger, dir string, parsedURL *url.URL) (int64, error) {
	baseFileName := filepath.Base(parsedURL.Path)
	resp, err := http.Get(parsedURL.String())
	if err != nil {
		return 0, fmt.Errorf("error fetching data: %s", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		buf, _ := io.ReadAll(resp.Body)
		log.Trace("error fetching data: %s, (url: %s)\n%s", resp.Status, parsedURL.String(), buf)
		return 0, fmt.Errorf("error fetching data: %s", resp.Status)
	}
	filename := filepath.Join(dir, baseFileName)
	file, err := os.Create(filename)
	if err != nil {
		return 0, fmt.Errorf("error creating file: %s", err)
	}
	defer file.Close()
	bytes, err := io.Copy(file, resp.Body)
	if err != nil {
		return 0, fmt.Errorf("error writing file: %s", err)
	}
	log.Debug("downloaded file %s (%d bytes)", filename, bytes)
	return bytes, nil
}

type TableExportInfo struct {
	Table     string
	Timestamp time.Time
}

const trackerTableExportKey = "table-export"

func bulkDownloadData(log logger.Logger, data map[string]exportJobTableData, dir string) ([]TableExportInfo, error) {
	var downloads []*url.URL
	started := time.Now()
	var tables []TableExportInfo
	for table, tableData := range data {
		if len(tableData.URLs) == 0 {
			log.Debug("no data for table %s, setting timestamp: %v", table, tableData.Cursor)
			tv, err := strconv.ParseInt(tableData.Cursor, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("error parsing timestamp value: %s. %w", tableData.Cursor, err)
			}
			tables = append(tables, TableExportInfo{
				Table:     table,
				Timestamp: time.UnixMicro(tv / 1000),
			})
			continue
		}
		var finalTimestamp time.Time

		for _, fullURL := range tableData.URLs {
			parsedURL, err := url.Parse(fullURL)
			if err != nil {
				return nil, fmt.Errorf("error parsing url: %s", err)
			}
			_, timestamp, ok := util.ParseCRDBExportFile(parsedURL.Path)
			if !ok {
				return nil, fmt.Errorf("unrecognized file path: %s", filepath.Base(parsedURL.Path))
			}
			if timestamp.After(finalTimestamp) {
				finalTimestamp = timestamp
			}
			downloads = append(downloads, parsedURL)
		}
		tables = append(tables, TableExportInfo{
			Table:     table,
			Timestamp: finalTimestamp,
		})
	}
	if len(downloads) == 0 {
		log.Debug("no files to download")
		return nil, nil
	}

	concurrency := 10
	downloadChan := make(chan *url.URL, len(downloads))
	var downloadWG sync.WaitGroup
	errors := make(chan error, concurrency)
	total := float64(len(downloads))

	var completed int32
	var downloadBytes int64

	// start the download workers
	for i := 0; i < concurrency; i++ {
		downloadWG.Add(1)
		go func() {
			defer util.RecoverPanic(log)
			defer downloadWG.Done()
			for url := range downloadChan {
				size, err := downloadFile(log, dir, url)
				if err != nil {
					errors <- fmt.Errorf("error downloading file: %s", err)
					return
				}
				atomic.AddInt64(&downloadBytes, size)
				val := atomic.AddInt32(&completed, 1)
				log.Debug("download completed: %d/%d (%.2f%%)", val, int(total), 100*(float64(val)/total))
			}
		}()
	}

	// send the downloads
	for _, downloadURL := range downloads {
		downloadChan <- downloadURL
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

	log.Info("Downloaded %d files (%d bytes) in %v", len(downloads), downloadBytes, time.Since(started))

	return tables, nil
}

func isCancelled(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func tableNames(tableData []TableExportInfo) []string {
	var tables []string
	for _, table := range tableData {
		tables = append(tables, table.Table)
	}
	return tables
}

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import data from your Shopmonkey instance to your system",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		logger := newLogger(cmd)
		logger = logger.WithPrefix("[import]")

		noconfirm, _ := cmd.Flags().GetBool("no-confirm")
		driverUrl := mustFlagString(cmd, "url", true)
		dryRun, _ := cmd.Flags().GetBool("dry-run")
		parallel := mustFlagInt(cmd, "parallel", false)
		apiURL := mustFlagString(cmd, "api-url", true)
		apiKey := mustFlagString(cmd, "api-key", true)
		jobID := mustFlagString(cmd, "job-id", false)
		single, _ := cmd.Flags().GetBool("single")
		dir := mustFlagString(cmd, "dir", false)
		schemaOnly := mustFlagBool(cmd, "schema-only", false)
		validateOnly := mustFlagBool(cmd, "validate-only", false)
		timeOffset := mustFlagString(cmd, "timeOffset", false)
		noDelete := mustFlagBool(cmd, "no-delete", false)
		var timeOffsetUnixMilli *int64

		if timeOffset != "" {
			tv, err := time.Parse(time.RFC3339, timeOffset)
			if err != nil {
				logger.Fatal("error parsing time offset: %s", err)
			}
			t := tv.UnixMilli()
			timeOffsetUnixMilli = &t
		}

		defer util.RecoverPanic(logger)

		dataDir := getDataDir(cmd, logger)

		if dryRun {
			logger.Info("🚨 Dry run enabled")
		}

		started := time.Now()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go func() {
			defer util.RecoverPanic(logger)
			select {
			case <-ctx.Done():
				return
			case <-sys.CreateShutdownChannel():
				cancel()
				return
			}
		}()

		only, _ := cmd.Flags().GetStringSlice("only")
		companyIds, _ := cmd.Flags().GetStringSlice("companyIds")
		locationIds, _ := cmd.Flags().GetStringSlice("locationIds")

		if cmd.Flags().Changed("api-url") {
			logger.Info("using alternative API url: %s", apiURL)
		} else {
			url, err := util.GetAPIURLFromJWT(apiKey)
			if err != nil {
				logger.Fatal("invalid API key. %s", err)
			}
			apiURL = url
		}

		var err error

		theTracker, err := tracker.NewTracker(tracker.TrackerConfig{
			Context: ctx,
			Logger:  logger,
			Dir:     dataDir,
		})
		if err != nil {
			logger.Fatal("error creating tracker: %s", err)
		}
		defer theTracker.Close()

		// check to see if there's a schema validator and if so load it
		validator, err := loadSchemaValidator(cmd)
		if err != nil {
			logger.Fatal("error loading validator: %s", err)
		}

		// load the schema from the api fresh
		registry, err := registry.NewAPIRegistry(ctx, logger, apiURL, theTracker)
		if err != nil {
			logger.Fatal("error creating registry: %s", err)
		}

		defer registry.Close()

		// create the driver for testing the connection
		driver, err := internal.NewDriverForImport(ctx, logger, driverUrl, registry, theTracker, dataDir)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(3) // this means the test failed
		}
		timedCtx, timedCancel := context.WithTimeout(ctx, 15*time.Second)
		if err := driver.Test(timedCtx, logger, driverUrl); err != nil {
			fmt.Println(err.Error())
			os.Exit(3) // this means the test failed
		}
		timedCancel()
		logger.Debug("driver test successful")
		// NOTE: we don't stop the driver here since we need it for the importer

		if validateOnly {
			os.Exit(0)
		}

		// create a new importer for loading the data using the provider
		importer, err := internal.NewImporter(ctx, logger, driverUrl, registry)
		if err != nil {
			logger.Fatal("error creating importer: %s", err)
		}

		var skipDeleteConfirm bool

		// check to see if the importer supports delete
		if importerHelp, ok := importer.(internal.ImporterHelp); ok {
			skipDeleteConfirm = !importerHelp.SupportsDelete()
		}

		if !dryRun && !noconfirm && !skipDeleteConfirm && !schemaOnly && !noDelete {

			meta, err := internal.GetDriverMetadataForURL(driverUrl)
			if err != nil {
				logger.Fatal("error getting driver metadata: %s", err)
			}

			var confirmed bool
			form := huh.NewForm(
				huh.NewGroup(
					huh.NewNote().
						Title("\n🚨 WARNING 🚨"),
					huh.NewConfirm().
						Title(fmt.Sprintf("YOU ARE ABOUT TO DELETE EVERYTHING IN %s", meta.Name)).
						Affirmative("Confirm").
						Negative("Cancel").
						Value(&confirmed),
				),
			)
			custom := huh.ThemeBase()
			form.WithTheme(custom)

			if err := form.Run(); err != nil {
				if !errors.Is(err, huh.ErrUserAborted) {
					logger.Error("error running form: %s", err)
					logger.Info("You may use --confirm to skip this prompt")
					os.Exit(1)
				}
			}
			if !confirmed {
				os.Exit(0)
			}
		}

		noCleanup, _ := cmd.Flags().GetBool("no-cleanup")
		var tables []string

		// if we pass in a directory, dont delete it
		if dir != "" {
			noCleanup = true
		}

		var success bool
		var tableExportInfo []TableExportInfo

		defer func() {
			defer util.RecoverPanic(logger)
			logger.Trace("exit success: %v", success)
			var filesRemoved bool
			if success {
				if !noCleanup {
					os.RemoveAll(dir)
					filesRemoved = true
				}
				if err := theTracker.SetKey(trackerTableExportKey, util.JSONStringify(tableExportInfo), 0); err != nil {
					logger.Error("error saving table export data to tracker: %s", err)
				}
				theTracker.Close()
				logger.Trace("tracker closed")
			}
			if !filesRemoved && dir != "" {
				logger.Info("downloaded files saved to: %s", dir)
			}
			if !success {
				os.Exit(1)
			}
		}()
		defer util.RecoverPanic(logger) // panic recover needs to happen after the defer above

		if dir == "" {
			if !schemaOnly {
				if jobID == "" {
					logger.Info("Requesting Export...")
					jobID, err = createExportJob(ctx, logger, apiURL, apiKey, exportJobCreateRequest{
						Tables:      only,
						CompanyIDs:  companyIds,
						LocationIDs: locationIds,
						TimeOffset:  timeOffsetUnixMilli,
					})
					if err != nil {
						logger.Fatal("error creating export job: %s", err)
					}
					logger.Trace("created job: %s", jobID)
				}

				logger.Info("Waiting for Export to Complete...")
				job, err := pollUntilComplete(ctx, logger, apiURL, apiKey, jobID)
				if err != nil && !isCancelled(ctx) {
					logger.Fatal("error polling job: %s", err)
				}

				if isCancelled(ctx) {
					return
				}

				// download the files
				dir, err = os.MkdirTemp(dataDir, "import-"+jobID+"-*")
				if err != nil {
					logger.Fatal("error creating temp dir: %s", err)
				}
				logger.Trace("temp dir created: %s", dir)

				logger.Info("Downloading export data...")
				tableData, err := bulkDownloadData(logger, job.Tables, dir)
				if err != nil {
					logger.Fatal("error downloading files: %s", err)
				}

				if isCancelled(ctx) {
					return
				}
				tableExportInfo = tableData
				tables = tableNames(tableData)
			} else {
				logger.Debug("schema only, skipping download")
				// we need to manually create all the tables in this specific case since we are --schema-only
				schema, err := registry.GetLatestSchema()
				if err != nil {
					logger.Fatal("error getting latest schema: %s", err)
				}
				time := time.Now()
				var tableData []TableExportInfo
				for _, data := range schema {
					tableData = append(tableData, TableExportInfo{
						Table:     data.Table,
						Timestamp: time,
					})
				}
				tables = tableNames(tableData)
				tableExportInfo = tableData
			}
		} else {
			tableData, err := loadTableExportInfo(theTracker)
			if err != nil {
				logger.Fatal("%s", err)
			}
			// if we don't have any previous data before we've specified a directory, we need to
			// determine the downloaded files from the directory and use that to filter tables
			if tableData != nil {
				tableExportInfo = tableData
				tables = tableNames(tableData)
				logger.Debug("reloading tables (%s) from %s", strings.Join(tables, ","), dir)
			} else {
				files, err := util.ListDir(dir)
				if err != nil {
					logger.Fatal("unable to list files in directory: %s", err)
				}
				for _, file := range files {
					table, _, ok := util.ParseCRDBExportFile(file)
					if ok && !util.SliceContains(tables, table) {
						tables = append(tables, table)
					}
				}
			}
		}

		if len(only) > 0 {
			var filtered []string
			for _, table := range tables {
				if util.SliceContains(only, table) {
					filtered = append(filtered, table)
				}
			}
			tables = filtered
		}

		logger.Info("Importing data to tables %s", strings.Join(tables, ", "))
		if err := importer.Import(internal.ImporterConfig{
			Context:         ctx,
			URL:             driverUrl,
			Logger:          logger,
			SchemaRegistry:  registry,
			MaxParallel:     parallel,
			JobID:           jobID,
			DataDir:         dir,
			DryRun:          dryRun,
			Tables:          tables,
			Single:          single,
			SchemaValidator: validator,
			SchemaOnly:      schemaOnly,
			NoDelete:        noDelete,
		}); err != nil {
			logger.Error("error running import: %s", err)
			return
		}

		// if the driver supports migration, set the table versions that we just upgraded
		if _, ok := driver.(internal.DriverMigration); ok {
			latest, err := registry.GetLatestSchema()
			if err != nil {
				logger.Error("error getting latest schema: %s", err)
				return
			}

			for _, info := range tableExportInfo {
				if util.SliceContains(tables, info.Table) {
					data := latest[info.Table]
					if err := registry.SetTableVersion(info.Table, data.ModelVersion); err != nil {
						logger.Error("error setting table %s version: %s. %s", info.Table, data.ModelVersion, err)
						return
					}
					logger.Trace("set table %s version to %s", info.Table, data.ModelVersion)
				}
			}
		} else {
			logger.Trace("driver does not support migration, skip setting table versions")
		}

		success = true
		logger.Info("👋 Loaded %d tables in %v", len(tables), time.Since(started))
	},
}

func init() {
	rootCmd.AddCommand(importCmd)

	// normal flags
	importCmd.Flags().String("url", "", "driver connection string")
	importCmd.Flags().String("api-key", os.Getenv("SM_APIKEY"), "shopmonkey api key")

	// helpful flags
	importCmd.Flags().String("job-id", "", "resume an existing job")
	importCmd.Flags().Bool("dry-run", false, "only simulate loading but don't actually make changes")
	importCmd.Flags().Bool("no-confirm", false, "skip the confirmation prompt")
	importCmd.Flags().Bool("no-cleanup", false, "skip removing the temp directory")
	importCmd.Flags().Bool("no-delete", false, "skip dropping tables and recreating them")
	importCmd.Flags().String("dir", "", "restart reading files from this existing import directory instead of downloading again")
	importCmd.Flags().Bool("schema-only", false, "run the schema creation only, skipping the data import")
	importCmd.Flags().Bool("validate-only", false, "run the validation only, skipping the data import")
	importCmd.Flags().String("timeOffset", "", "timestamp in RFC3339 format to export data with records updated after this time")

	// tuning and testing flags
	importCmd.Flags().Int("parallel", 4, "the number of parallel upload tasks (if supported by driver)")
	importCmd.Flags().Bool("single", false, "run one insert at a time instead of batching")
	importCmd.Flags().StringSlice("only", nil, "only import these tables")
	importCmd.Flags().StringSlice("companyIds", nil, "only import these company ids")
	importCmd.Flags().StringSlice("locationIds", nil, "only import these location ids")

	// internal flags
	importCmd.Flags().String("api-url", "https://api.shopmonkey.cloud", "url to shopmonkey api")
	importCmd.Flags().MarkHidden("api-url")
}
