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
	"github.com/shopmonkeyus/eds-server/internal/registry"
	"github.com/shopmonkeyus/go-common/logger"
	csys "github.com/shopmonkeyus/go-common/sys"
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
		req.Header = http.Header{
			"Authorization": {"Bearer " + apiKey},
			"Content-Type":  {"application/json"},
		}
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
	req.Header = http.Header{
		"Authorization": {"Bearer " + apiKey},
		"Accept":        {"application/json"},
	}
	var retryCount int
	started := time.Now()
	for time.Since(started) < time.Minute {
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

func downloadFile(log logger.Logger, dir string, fullURL string) error {
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
	if resp.StatusCode != http.StatusOK {
		buf, _ := io.ReadAll(resp.Body)
		log.Trace("error fetching data: %s, (url: %s)\n%s", resp.Status, fullURL, buf)
		return fmt.Errorf("error fetching data: %s", resp.Status)
	}
	filename := filepath.Join(dir, baseFileName)
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

func bulkDownloadData(log logger.Logger, data map[string]exportJobTableData, dir string) ([]string, error) {
	var downloads []string
	var tablesWithData []string
	started := time.Now()
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
				log.Debug("download completed: %d/%d (%.2f%%)", val, int(total), 100*(float64(val)/total))
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

	log.Info("Downloaded %d files in %v", len(downloads), time.Since(started))

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
	Short: "import data from your Shopmonkey instance to your system",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		noconfirmed, _ := cmd.Flags().GetBool("no-confirm")
		providerUrl := mustFlagString(cmd, "url", true)
		dryRun, _ := cmd.Flags().GetBool("dry-run")
		parallel := mustFlagInt(cmd, "parallel", false)
		apiURL := mustFlagString(cmd, "api-url", true)
		apiKey := mustFlagString(cmd, "api-key", true)
		jobID := mustFlagString(cmd, "job-id", false)
		schemaFile := mustFlagString(cmd, "schema", false)

		logger, closer := newLogger(cmd)
		defer closer()
		logger = logger.WithPrefix("[import]")

		if !dryRun && !noconfirmed {

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
			select {
			case <-ctx.Done():
				return
			case <-csys.CreateShutdownChannel():
				cancel()
				return
			}
		}()

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

		if jobID == "" {
			logger.Info("Requesting Export...")
			jobID, err = createExportJob(ctx, apiURL, apiKey, exportJobCreateRequest{
				Tables:      only,
				CompanyIDs:  companyIds,
				LocationIDs: locationIds,
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
		dir, err := os.MkdirTemp("", "eds-import")
		if err != nil {
			logger.Fatal("error creating temp dir: %s", err)
		}
		logger.Trace("temp dir created: %s", dir)
		success := true
		defer func() {
			if success {
				os.RemoveAll(dir)
			}
		}()

		logger.Info("Downloading export data...")
		tables, err := bulkDownloadData(logger, job.Tables, dir)
		if err != nil {
			success = false
			logger.Fatal("error downloading files: %s", err)
		}

		if isCancelled(ctx) {
			return
		}

		logger.Info("Importing data...")
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
		}); err != nil {
			success = false
			logger.Fatal("error running import: %s", err)
		}

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
	importCmd.Flags().StringSlice("only", nil, "only import these tables")
	importCmd.Flags().StringSlice("companyIds", nil, "only import these company ids")
	importCmd.Flags().StringSlice("locationIds", nil, "only import these location ids")
	importCmd.Flags().Int("parallel", 4, "the number of parallel upload tasks")
	importCmd.Flags().String("schema", "schema.json", "the schema file to output")
}
