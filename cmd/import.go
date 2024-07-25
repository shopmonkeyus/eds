package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/huh"
	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/consumer"
	"github.com/shopmonkeyus/eds-server/internal/registry"
	"github.com/shopmonkeyus/eds-server/internal/util"
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
	CompanyIDs  []string `json:"companyIds,omitempty"`
	LocationIDs []string `json:"locationIds,omitempty"`
	Tables      []string `json:"tables,omitempty"`
}

func shouldRetryError(err error) bool {
	msg := err.Error()
	if strings.Contains(msg, "connection refused") || strings.Contains(msg, "connection reset") {
		return true
	}
	return false
}

func shouldRetryStatus(statusCode int) bool {
	switch statusCode {
	case http.StatusInternalServerError, http.StatusServiceUnavailable, http.StatusGatewayTimeout, http.StatusTooManyRequests:
		return true
	default:
		return false
	}
}

func backoffRetry(retryCount int) time.Duration {
	return time.Duration(rand.Intn(1000*retryCount)) * time.Millisecond
}

func createExportJob(ctx context.Context, apiURL string, apiKey string, filters exportJobCreateRequest) (string, error) {
	// encode the request
	body, err := json.Marshal(filters)
	if err != nil {
		return "", fmt.Errorf("error encoding request: %s", err)
	}
	var retryCount int
	started := time.Now()
	for time.Since(started) < time.Minute {
		req, err := http.NewRequestWithContext(ctx, "POST", apiURL+"/v3/export/bulk", bytes.NewBuffer(body))
		if err != nil {
			return "", fmt.Errorf("error creating request: %s", err)
		}
		setHTTPHeader(req, apiKey)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			if shouldRetryError(err) {
				retryCount++
				backoffRetry(retryCount)
				continue
			}
			return "", fmt.Errorf("error creating bulk export job: %s", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			buf, _ := io.ReadAll(resp.Body)
			if shouldRetryStatus(resp.StatusCode) {
				retryCount++
				backoffRetry(retryCount)
				continue
			}
			return "", fmt.Errorf("API error: %s (status code=%d)", string(buf), resp.StatusCode)
		}
		job, err := decodeAPIResponse[exportJobCreateResponse](resp)
		if err != nil {
			return "", fmt.Errorf("error decoding response: %s", err)
		}
		return job.JobID, nil
	}
	return "", fmt.Errorf("error creating export job: too many retries")
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

func checkExportJob(ctx context.Context, apiURL string, apiKey string, jobID string) (*exportJobResponse, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", apiURL+"/v3/export/bulk/"+jobID, nil)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, nil
		}
		return nil, fmt.Errorf("error creating request: %s", err)
	}
	setHTTPHeader(req, apiKey)
	var retryCount int
	started := time.Now()
	for time.Since(started) < time.Minute*5 {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			if shouldRetryError(err) {
				retryCount++
				backoffRetry(retryCount)
				continue
			}
			return nil, fmt.Errorf("error fetching bulk export status: %s", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			buf, _ := io.ReadAll(resp.Body)
			if shouldRetryStatus(resp.StatusCode) {
				retryCount++
				backoffRetry(retryCount)
				continue
			}
			return nil, fmt.Errorf("API error: %s (status code=%d)", string(buf), resp.StatusCode)
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
	return nil, fmt.Errorf("error checking status of export job: too many retries")
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
		job, err := checkExportJob(ctx, apiURL, apiKey, jobID)
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

func bulkDownloadData(log logger.Logger, data map[string]exportJobTableData, dir string) ([]TableExportInfo, error) {
	var downloads []*url.URL
	started := time.Now()
	var tables []TableExportInfo
	for table, tableData := range data {
		if len(tableData.URLs) == 0 {
			log.Debug("no data for table %s", table)
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

func createEDSSession(ctx context.Context, logger logger.Logger, serverURL string, apiURL string, apiKey string, suffix string) error {
	logger.Info("Creating EDS Checkpoint...")
	// TODO: just get the cred instead of starting a session?
	session, err := sendStart(logger, apiURL, apiKey)
	if err != nil {
		return fmt.Errorf("error creating EDS session: %s", err)
	}
	logger.Trace("EDS session created: %s", session.SessionId)
	if session.Credential == nil {
		return fmt.Errorf("no credential found in session")
	}
	credsFile := filepath.Join(os.TempDir(), fmt.Sprintf("eds-%s.creds", session.SessionId))
	if err := writeCredsToFile(*session.Credential, credsFile); err != nil {
		logger.Fatal("failed to write creds to file: %s", err)
	}
	logger.Trace("credential saved to: %s", credsFile)
	defer os.Remove(credsFile)
	consumerContext, cancel := context.WithCancel(ctx)
	defer cancel() // cancel the consumer context to close the nats connections etc
	con, err := consumer.CreateConsumer(consumer.ConsumerConfig{
		Logger:      logger,
		Context:     consumerContext,
		URL:         serverURL,
		Credentials: credsFile,
		Suffix:      suffix,
	})
	if err != nil {
		return fmt.Errorf("error creating consumer: %s", err)
	}
	logger.Trace("consumer created: %s", con.Name())
	// stop the session
	if _, err := sendEnd(logger, apiURL, apiKey, session.SessionId, false); err != nil {
		return fmt.Errorf("error ending session: %s", err)
	}
	logger.Trace("EDS session ended: %s", session.SessionId)
	return nil
}

func tableNames(tableData []TableExportInfo) []string {
	var tables []string
	for _, table := range tableData {
		tables = append(tables, table.Table)
	}
	return tables
}

func loadTablesJSON(fp string) ([]TableExportInfo, error) {
	to, err := os.Open(fp)
	if err != nil {
		return nil, fmt.Errorf("couldn't open temp tables file at %s: %w", fp, err)
	}
	dec := json.NewDecoder(to)
	tables := make([]TableExportInfo, 0)
	if err := dec.Decode(&tables); err != nil {
		return nil, fmt.Errorf("error decoding tables: %w", err)
	}
	return tables, nil
}

var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import data from your Shopmonkey instance to your system",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		noconfirm, _ := cmd.Flags().GetBool("no-confirm")
		providerUrl := mustFlagString(cmd, "url", true)
		dryRun, _ := cmd.Flags().GetBool("dry-run")
		parallel := mustFlagInt(cmd, "parallel", false)
		apiURL := mustFlagString(cmd, "api-url", true)
		apiKey := mustFlagString(cmd, "api-key", true)
		jobID := mustFlagString(cmd, "job-id", false)
		schemaFile := mustFlagString(cmd, "schema", false)
		single, _ := cmd.Flags().GetBool("single")
		dir := mustFlagString(cmd, "dir", false)

		logger, closer := newLogger(cmd)
		defer closer()
		logger = logger.WithPrefix("[import]")

		if !dryRun && !noconfirm {

			u, err := url.Parse(providerUrl)
			if err != nil {
				logger.Fatal("error parsing url: %s", err)
			}

			// present a nicer looking provider
			name := u.Scheme
			if u.Path != "" {
				name = name + ":" + u.Path[1:]
			}
			var confirmed bool
			form := huh.NewForm(
				huh.NewGroup(
					huh.NewNote().
						Title("\n🚨 WARNING 🚨"),
					huh.NewConfirm().
						Title(fmt.Sprintf("YOU ARE ABOUT TO DELETE EVERYTHING IN %s", name)).
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

		// create eds session
		noEDSSession, _ := cmd.Flags().GetBool("no-eds-session")
		if !noEDSSession {
			serverURL := mustFlagString(cmd, "server", true)
			suffix := mustFlagString(cmd, "consumer-suffix", false)
			if err := createEDSSession(ctx, logger, serverURL, apiURL, apiKey, suffix); err != nil {
				logger.Error("error creating EDS session: %s", err)
				logger.Info("If you do not intend to run EDS server after the import then you may use --no-eds-session to skip this")
				os.Exit(1)
			}
		} else {
			logger.Warn("Skipping EDS session, this may cause data loss if you run EDS after the import")
		}

		only, _ := cmd.Flags().GetStringSlice("only")
		companyIds, _ := cmd.Flags().GetStringSlice("companyIds")
		locationIds, _ := cmd.Flags().GetStringSlice("locationIds")

		if cmd.Flags().Changed("api-url") {
			logger.Info("using alternative API url: %s", apiURL)
		}
		var err error

		// load the schema from the api fresh
		registry, err := registry.NewAPIRegistry(apiURL)
		if err != nil {
			logger.Fatal("error creating registry: %s", err)
		}

		// save the new schema file
		if err := registry.Save(schemaFile); err != nil {
			logger.Fatal("error saving schema: %s", err)
		}

		// create a new importer for loading the data using the provider
		importer, err := internal.NewImporter(ctx, logger, providerUrl, registry)
		if err != nil {
			logger.Fatal("error creating importer: %s", err)
		}

		noCleanup, _ := cmd.Flags().GetBool("no-cleanup")
		var tables []string

		// if we pass in a directory, dont delete it
		if dir != "" {
			noCleanup = true
		}

		var success bool
		defer func() {
			logger.Trace("exit success: %v", success)
			var filesRemoved bool
			if success {
				if _, err := sys.CopyFile(filepath.Join(dir, "tables.json"), "tables.json"); err != nil {
					logger.Error("error copying tables.json: %s", err)
				} else {
					logger.Info("tables.json saved to: %s", "tables.json")
				}
				if !noCleanup {
					os.RemoveAll(dir)
					filesRemoved = true
				}
			}
			if !filesRemoved {
				logger.Info("downloaded files saved to: %s", dir)
			}
			if !success {
				os.Exit(1)
			}
		}()

		if dir == "" {
			if jobID == "" {
				logger.Info("Requesting Export...")
				jobID, err = createExportJob(ctx, apiURL, apiKey, exportJobCreateRequest{
					Tables:      only,
					CompanyIDs:  companyIds,
					LocationIDs: locationIds,
				})
				if err != nil {
					logger.Error("error creating export job: %s", err)
					return
				}
				logger.Trace("created job: %s", jobID)
			}

			logger.Info("Waiting for Export to Complete...")
			job, err := pollUntilComplete(ctx, logger, apiURL, apiKey, jobID)
			if err != nil && !isCancelled(ctx) {
				logger.Error("error polling job: %s", err)
				return
			}

			if isCancelled(ctx) {
				return
			}

			// download the files
			dir, err = os.MkdirTemp("", "eds-import-"+jobID+"-*")
			if err != nil {
				logger.Error("error creating temp dir: %s", err)
				return
			}
			logger.Trace("temp dir created: %s", dir)

			logger.Info("Downloading export data...")
			tableData, err := bulkDownloadData(logger, job.Tables, dir)
			if err != nil {
				logger.Error("error downloading files: %s", err)
				return
			}

			if isCancelled(ctx) {
				return
			}
			to, err := os.Create(filepath.Join(dir, "tables.json"))
			if err != nil {
				logger.Error("couldn't open temp tables file: %s", err)
				return
			}
			enc := json.NewEncoder(to)
			if err := enc.Encode(tableData); err != nil {
				logger.Error("error encoding tables: %s", err)
				return
			}
			to.Close()
			tables = tableNames(tableData)
		} else {
			fp := filepath.Join(dir, "tables.json")
			tableData, err := loadTablesJSON(fp)
			if err != nil {
				logger.Error("error loading tables: %s", err)
				return
			}
			tables = tableNames(tableData)
			logger.Debug("reloading tables (%s) from %s", strings.Join(tables, ","), dir)
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
			Context:        ctx,
			URL:            providerUrl,
			Logger:         logger,
			SchemaRegistry: registry,
			MaxParallel:    parallel,
			JobID:          jobID,
			DataDir:        dir,
			DryRun:         dryRun,
			Tables:         tables,
			Single:         single,
		}); err != nil {
			logger.Error("error running import: %s", err)
			return
		}
		success = true
		logger.Info("👋 Loaded %d tables in %v", len(tables), time.Since(started))
	},
}

func init() {
	rootCmd.AddCommand(importCmd)
	importCmd.Flags().Bool("dry-run", false, "only simulate loading but don't actually changes")
	importCmd.Flags().String("url", "", "provider connection string")
	importCmd.Flags().String("api-url", "https://api.shopmonkey.cloud", "url to shopmonkey api")
	importCmd.Flags().String("api-key", os.Getenv("SM_APIKEY"), "shopmonkey api key")
	importCmd.Flags().String("job-id", "", "resume an existing job")
	importCmd.Flags().Bool("no-confirm", false, "skip the confirmation prompt")
	importCmd.Flags().Bool("no-eds-session", false, "skip creating an EDS session")
	importCmd.Flags().String("server", "nats://connect.nats.shopmonkey.pub", "the nats server url, could be multiple comma separated")
	importCmd.Flags().String("consumer-suffix", "", "a suffix to use for the consumer group name")
	importCmd.Flags().StringSlice("only", nil, "only import these tables")
	importCmd.Flags().StringSlice("companyIds", nil, "only import these company ids")
	importCmd.Flags().StringSlice("locationIds", nil, "only import these location ids")
	importCmd.Flags().Int("parallel", 4, "the number of parallel upload tasks")
	importCmd.Flags().String("schema", "schema.json", "the schema file to output")
	importCmd.Flags().Bool("no-cleanup", false, "skip removing the temp directory")
	importCmd.Flags().String("dir", "", "restart reading files from this existing import directory instead of downloading again")
	importCmd.Flags().Bool("single", false, "run one insert at a time instead of batching")
}
