package cmd

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fatih/color"
	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/api"
	"github.com/shopmonkeyus/eds/internal/consumer"
	"github.com/shopmonkeyus/eds/internal/notification"
	"github.com/shopmonkeyus/eds/internal/upgrade"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/shopmonkeyus/go-common/command"
	"github.com/shopmonkeyus/go-common/logger"
	cstr "github.com/shopmonkeyus/go-common/string"
	"github.com/shopmonkeyus/go-common/sys"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var Version string                // set in main
var ShopmonkeyPublicPGPKey string // set in main

const maxFailures = 5

func writeCredsToFile(data string, filename string) error {
	buf, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return fmt.Errorf("failed to decode base64: %w", err)
	}
	if err := os.WriteFile(filename, buf, 0600); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}
	return nil
}

var errAlreadyRunning = errors.New("already running")

func sendStart(logger logger.Logger, apiURL string, apiKey string, driverUrl string, edsServerId string, companyIds []string) (*api.EdsSession, error) {
	var body api.SessionStart
	ipaddress, err := util.GetLocalIP()
	if err != nil {
		return nil, fmt.Errorf("failed to get local IP: %w", err)
	}
	machineid, err := util.GetMachineId()
	if err != nil {
		return nil, fmt.Errorf("failed to get machine ID: %w", err)
	}
	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("failed to get hostname: %w", err)
	}
	osinfo, err := util.GetSystemInfo()
	if err != nil {
		return nil, fmt.Errorf("failed to get system info: %w", err)
	}
	body.MachineId = machineid
	body.IPAddress = ipaddress
	body.Hostname = hostname
	body.Version = Version
	body.OsInfo = osinfo
	body.ServerID = edsServerId
	body.CompanyIDs = companyIds

	if driverUrl != "" {
		driverMetadata, err := internal.GetDriverMetadataForURL(driverUrl)
		if err != nil {
			return nil, fmt.Errorf("failed to get driver metadata: %w", err)
		}
		if driverMetadata == nil {
			return nil, fmt.Errorf("invalid driver URL: %s", driverUrl)
		}
		var driverInfo api.DriverMeta
		driverInfo.Description = driverMetadata.Description
		driverInfo.Name = driverMetadata.Name
		driverInfo.ID = driverMetadata.Scheme
		driverInfo.URL, err = util.MaskURL(driverUrl)
		if err != nil {
			return nil, fmt.Errorf("failed to mask driver URL: %w", err)
		}
		body.Driver = &driverInfo
	}

	logger.Trace("sending session start: %s", util.JSONStringify(body))

	req, err := http.NewRequest("POST", apiURL+"/v3/eds/internal", bytes.NewBuffer([]byte(util.JSONStringify(body))))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}
	setHTTPHeader(req, apiKey)
	retry := util.NewHTTPRetry(req, util.WithLogger(logger))
	resp, err := retry.Do()
	if err != nil {
		return nil, fmt.Errorf("failed to send session start: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusConflict {
			return nil, errAlreadyRunning
		}
		return nil, handleAPIError(resp, "session start")
	}
	var sessionResp api.SessionStartResponse
	if err := json.NewDecoder(resp.Body).Decode(&sessionResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}
	if !sessionResp.Success {
		return nil, fmt.Errorf("failed to start session: %s", sessionResp.Message)
	}
	logger.Trace("session %s started successfully", sessionResp.Data.SessionId)
	return &sessionResp.Data, nil
}

func sendEnd(logger logger.Logger, apiURL string, apiKey string, sessionId string, errored bool) (*api.SessionEndURLs, error) {
	var body api.SessionEnd
	body.Errored = errored
	req, err := http.NewRequest("POST", apiURL+"/v3/eds/internal/"+sessionId, bytes.NewBuffer([]byte(util.JSONStringify(body))))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}
	setHTTPHeader(req, apiKey)
	retry := util.NewHTTPRetry(req, util.WithLogger(logger))
	resp, err := retry.Do()
	if err != nil {
		return nil, fmt.Errorf("failed to send session end: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, handleAPIError(resp, "session end")
	}
	var s api.SessionEndResponse
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}
	if !s.Success {
		return nil, fmt.Errorf("failed to end session: %s", s.Message)
	}
	logger.Trace("session %s ended successfully: %s", sessionId, s.Data.URL)
	return &s.Data, nil
}

func uploadFile(logger logger.Logger, url string, logFileBundle string) error {
	of, err := os.Open(logFileBundle)
	if err != nil {
		return fmt.Errorf("failed to open log file bundle: %w", err)
	}
	defer of.Close()
	req, err := http.NewRequest("PUT", url, of)
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}
	setHTTPHeader(req, "")
	req.Header.Set("Content-Type", "application/x-tgz")
	retry := util.NewHTTPRetry(req, util.WithLogger(logger))
	resp, err := retry.Do()
	if err != nil {
		return fmt.Errorf("failed to upload logs: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return handleAPIError(resp, "upload logs")
	}
	logger.Trace("logs uploaded successfully: %s", logFileBundle)
	return nil
}

func uploadLogFile(logger logger.Logger, uploadURL string, logFile string) (string, error) {
	logger.Debug("uploading logfile: %s", logFile)
	// gzip the log file, do this first in case we get an error!
	if err := util.GzipFile(logFile); err != nil {
		return "", fmt.Errorf("failed to compress log file: %w", err)
	}
	compressedLogFile := logFile + ".gz"
	defer os.Remove(compressedLogFile)

	if err := uploadFile(logger, uploadURL, compressedLogFile); err != nil {
		return "", fmt.Errorf("failed to upload logs to %s: %s", uploadURL, err)
	}
	parsedURL, err := url.Parse(uploadURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse upload URL: %w", err)
	}
	return parsedURL.Path, nil
}

func getRemainingLog(logsDir string) (string, error) {
	files, err := util.ListDir(logsDir)
	if err != nil {
		return "", fmt.Errorf("failed to list directory: %w", err)
	}
	if len(files) == 0 {
		return "", nil
	}
	sort.Strings(files)

	var last string
	for _, file := range files {
		if !strings.HasSuffix(file, ".log") {
			continue
		}
		last = file
	}
	return last, nil
}

func sendEndAndUpload(logger logger.Logger, apiurl string, apikey string, sessionId string, errored bool, logfile string, stderrFile string) (string, error) {
	logger.Info("uploading logs for session: %s", sessionId)
	urls, err := sendEnd(logger, apiurl, apikey, sessionId, errored)
	if err != nil {
		return "", fmt.Errorf("failed to send session end: %w", err)
	}
	var logFileStoragePath string
	if logfile != "" {
		logFileStoragePath, err = uploadLogFile(logger, urls.URL, logfile)
		if err != nil {
			return "", fmt.Errorf("failed to upload logs: %w", err)
		}
	}
	if urls.ErrorURL != "" && stderrFile != "" {
		if _, err := uploadLogFile(logger, urls.ErrorURL, stderrFile); err != nil {
			return "", fmt.Errorf("failed to upload error logs: %w", err)
		}
	}
	logger.Trace("logs uploaded successfully for session: %s", sessionId)
	if errored {
		logger.Info("error log files saved to %s for session: %s", logfile, sessionId)
	}
	return logFileStoragePath, nil
}

func getLogUploadURL(logger logger.Logger, apiURL string, apiKey string, sessionId string) (string, error) {
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/v3/eds/internal/%s/log", apiURL, sessionId), strings.NewReader("{}"))
	if err != nil {
		return "", fmt.Errorf("failed to create HTTP request: %w", err)
	}
	setHTTPHeader(req, apiKey)
	retry := util.NewHTTPRetry(req, util.WithLogger(logger))
	resp, err := retry.Do()
	if err != nil {
		return "", fmt.Errorf("failed to get log upload url: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", handleAPIError(resp, "log upload url")
	}
	var s api.SessionEndResponse
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}
	if !s.Success {
		return "", fmt.Errorf("failed to get log upload url: %s", s.Message)
	}
	logger.Trace("session %s log url received: %s", sessionId, s.Data.URL)
	return s.Data.URL, nil
}

var serverIgnoreFlags = map[string]bool{
	"--api-url":        true,
	"--api-key":        true,
	"--eds-id":         true,
	"--silent":         true,
	"--port":           true,
	"--health-port":    true,
	"--renew-interval": true,
	"--wrapper":        true,
	"--parent":         true,
	"--url":            true,
	"--server":         true,
	"--keep-logs":      true,
	"--no-restart":     true,
}

func collectCommandArgs() []string {
	var skipping bool
	var _args []string
	for _, arg := range os.Args[2:] {
		if skipping {
			skipping = false
			continue
		}
		tok := strings.Split(arg, "=")
		_arg := tok[0]
		if serverIgnoreFlags[_arg] {
			skipping = true
			continue
		}
		_args = append(_args, arg)
	}
	return _args
}

// runWrapperLoop will run the main parent process which will fork the child process (server)
// which acts as a control mechanism for the fork process (which has all the real logic).
// this loop is only responsible for waiting for a restart signal and then restarting the child process.
func runWrapperLoop(logger logger.Logger) {
	logger = logger.WithPrefix("[wrapper]")
	logger.Trace("running the wrapper loop")
	port, err := util.GetFreePort()
	if err != nil {
		logger.Fatal("failed to get free port: %s", err)
	}

	parentProcess := os.Args[0] // start of with the original process

	logger.Trace("parent process port is: %d", port)

	exitCode := -1
	restart := make(chan bool, 1)
	exited := make(chan bool, 1)
	var waitGroup sync.WaitGroup
	var inUpgrade bool

	httphandler := &http.ServeMux{}
	httpsrv := &http.Server{
		Addr:           fmt.Sprintf("127.0.0.1:%d", port), // only bind to localhost so we don't expose externally
		Handler:        httphandler,
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   1 * time.Second,
		IdleTimeout:    5 * time.Minute,
		MaxHeaderBytes: 1 << 20,
	}
	httphandler.HandleFunc("/restart", func(w http.ResponseWriter, r *http.Request) {
		logger.Debug("received restart request")
		inUpgrade = true
		// NOTE: we do this is a separate goroutine so we can respond to the request w/o blocking
		// since we will need to shutdown the child process
		go func() {
			restart <- true
		}()
		w.WriteHeader(http.StatusAccepted)
	})

	go func() {
		defer util.RecoverPanic(logger)
		if err := httpsrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("failed to start wrapper server: %s", err)
		}
	}()

	args := os.Args[1:]
	args = append(args, "--wrapper")
	args = append(args, fmt.Sprintf("--parent=%d", port))

	shutdownHTTP := func() {
		defer util.RecoverPanic(logger)
		logger.Debug("initiating HTTP server shutdown")
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelShutdown()
		if err := httpsrv.Shutdown(shutdownCtx); err != nil {
			logger.Error("HTTP server shutdown error: %s", err)
		}
		logger.Debug("HTTP server successfully shutdown")
	}

	var failures int
	var completed bool

	for failures < maxFailures && !completed {
		logger.Trace("starting process: %s %s", parentProcess, strings.Join(util.MaskArguments(args), " "))
		cmd := exec.Command(parentProcess, args...)
		cmd.Stderr = os.Stderr
		cmd.Stdout = os.Stdout
		cmd.Stdin = os.Stdin
		if err := cmd.Start(); err != nil {
			logger.Fatal("failed to start: %s", err)
		}
		waitGroup.Add(1)
		go func() {
			defer util.RecoverPanic(logger)
			defer waitGroup.Done()
			if err := cmd.Wait(); err != nil {
				logger.Debug("wrapped process exited: %s", err)
			}
			exitCode = cmd.ProcessState.ExitCode()
			logger.Trace("wrapped process exited with exit code: %d", exitCode)
			if !completed {
				exited <- true
			}
		}()
		select {
		case <-restart:
			logger.Trace("restarting process")
			cmd.Process.Signal(syscall.SIGINT)
			if err := cmd.Wait(); err != nil {
				logger.Error("failed to wait for process: %s", err)
			}
		case <-sys.CreateShutdownChannel():
			logger.Trace("SIGINT received")
			cmd.Process.Signal(syscall.SIGINT)
			exitCode = 0
			completed = true
		case <-exited:
			logger.Trace("exit received: %d", exitCode)
			if inUpgrade && exitCode != 0 {
				inUpgrade = false
				failures++
				parentProcess = os.Args[0] // reset to the original process since the upgrade failed
				logger.Trace("upgrade failed, resetting to original process and restarting")
				continue
			}
			if exitCode == 0 || exitCode == 1 {
				completed = true
			} else {
				failures++
				time.Sleep(time.Second * time.Duration(failures))
			}
		}
	}

	logger.Trace("waiting for child processes to exit")
	waitGroup.Wait()
	logger.Trace("child process exited")
	shutdownHTTP()
	logger.Trace("exit with exit code: %d", exitCode)
	os.Exit(exitCode)
}

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Run the server",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		logger := newLogger(cmd)
		logger = logger.WithPrefix("[server]")
		defer util.RecoverPanic(logger)
		apiurl := mustFlagString(cmd, "api-url", false)

		// NOTE: do these before bothering with the wrapper since they are required to run
		edsServerId := viper.GetString("server_id")
		if edsServerId == "" {
			yellow := color.New(color.FgYellow, color.Bold).SprintFunc()
			fmt.Println(yellow("Welcome to Shopmonkey EDS!\n"))
			whiteBold := color.New(color.FgWhite, color.Bold).SprintFunc()
			for {
				fmt.Print(whiteBold("Enter one-time enrollment code: "))
				var code string
				if _, err := fmt.Scanln(&code); err != nil {
					if err.Error() == "unexpected newline" {
						os.Exit(1)
					}
					logger.Fatal("failed to read enrollment code: %s", err)
					return
				}
				code = strings.TrimSpace(code)
				if code == "" {
					os.Exit(1)
				}
				args := []string{"enroll", code, "--silent"}
				if cmd.Flags().Changed("api-url") {
					args = append(args, "--api-url", apiurl)
				}
				cmd := exec.Command(util.GetExecutable(), args...)
				cmd.Stderr = os.Stderr
				cmd.Stdout = os.Stdout
				cmd.Run()
				if cmd.ProcessState.ExitCode() == 0 {
					initConfig()
					edsServerId = viper.GetString("server_id")
					break
				}
				fmt.Println()
			}
		}
		dataDir := getDataDir(cmd, logger)
		driverURL := viper.GetString("url")
		server := mustFlagString(cmd, "server", false)
		apikey := viper.GetString("token")
		if apikey == "" {
			logger.Fatal("API key not found. Make sure you run %s before continuing.", getCommandExample("enroll", "[CODE]"))
		}
		keepLogs := viper.GetBool("keep_logs")
		verbose := mustFlagBool(cmd, "verbose", false)
		noRestart := mustFlagBool(cmd, "no-restart", false)

		// run the wrapper to handle the rest of the code from inside the wrapper
		wrapper := mustFlagBool(cmd, "wrapper", false)
		if !wrapper {
			runWrapperLoop(logger)
			return
		}

		logger.Trace("using parameter api token %s", cstr.Mask(apikey))
		logger.Info("server id: %s", edsServerId)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		apiurl = strings.TrimSuffix(apiurl, "/") // remove trailing slash

		if cmd.Flags().Changed("api-url") {
			logger.Info("using alternative API url: %s", apiurl)
		} else {
			url, err := util.GetAPIURLFromJWT(apikey)
			if err != nil {
				logger.Fatal("invalid API key. %s", err)
			}
			apiurl = url
			logger.Debug("using API url: %s", apiurl)
		}

		var credsFile string
		var sessionDir string

		// must be in a defer to make sure we pick up credsFile variable
		defer func() {
			// make sure we remove the temporary credential
			os.Remove(credsFile)
			// if this is the last file in the directory, go ahead and remove it too
			if files, _ := os.ReadDir(sessionDir); len(files) == 0 && !keepLogs {
				os.RemoveAll(sessionDir)
			}
		}()

		port := mustFlagInt(cmd, "port", true)
		oldHealthPort := mustFlagInt(cmd, "health-port", false)
		if oldHealthPort > 0 {
			port = oldHealthPort // allow it for now for backwards compatibility but eventually remove it
		}
		parentPort := mustFlagInt(cmd, "parent", true)

		_args := collectCommandArgs()
		_args = append(_args, "--port", fmt.Sprintf("%d", port))
		_args = append(_args, "--data-dir", dataDir)
		_args = append(_args, "--server", server)
		_args = append(_args, "--api-url", apiurl)

		var sessionId string
		configured := driverURL != ""
		configureChannel := make(chan bool, 1)

		processCallback := func(p *os.Process) {
			logger.Debug("fork process started with pid: %d", p.Pid)
		}

		restart := func() {
			if configured {
				logger.Info("need to restart")
				resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/control/restart", port))
				if err != nil {
					logger.Error("restart failed: %s", err)
					return
				}
				logger.Debug("restart response: %d", resp.StatusCode)
			}
		}

		shutdown := func(msg string, deleted bool) {
			if configured {
				logger.Info("shutdown requested: %s", msg)
				resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/control/shutdown", port))
				if err != nil {
					logger.Fatal("shutdown failed: %s", err)
					return
				}
				logger.Debug("shutdown response: %d", resp.StatusCode)
				if deleted {
					logger.Info("shutdown successful")
					viper.Set("server_id", "")
					if err := viper.WriteConfig(); err != nil {
						logger.Error("failed to write config: %s", err)
					}
					logger.Debug("server id removed from config")
				}
			}
		}

		pause := func() error {
			if configured {
				logger.Info("server pause requested")
				resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/control/pause", port))
				if err != nil {
					logger.Error("pause failed: %s", err)
					return err
				}
				logger.Debug("pause response: %d", resp.StatusCode)
				if resp.StatusCode == http.StatusOK {
					logger.Info("server paused")
				} else {
					logger.Error("pause failed %d", resp.StatusCode)
				}
			}
			return nil
		}

		unpause := func() error {
			if configured {
				logger.Info("server unpause requested")
				resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/control/unpause", port))
				if err != nil {
					logger.Error("unpause failed: %s", err)
					return err
				}
				logger.Debug("unpause response: %d", resp.StatusCode)
				if resp.StatusCode == http.StatusOK {
					logger.Info("server unpaused")
				} else {
					logger.Error("unpause failed %s", resp.StatusCode)
				}
			}
			return nil
		}

		upgrade := func(version string) notification.UpgradeResponse {
			versionWithoutV := strings.TrimPrefix(version, "v")
			logger.Info("server upgrade requested to version: %s", versionWithoutV)
			if util.IsRunningInsideDocker() {
				return notification.UpgradeResponse{
					Success:   false,
					Message:   "upgrade is not supported inside a virtualized container system",
					SessionID: sessionId,
					Version:   version,
				}
			}
			pause()
			fn := filepath.Join(dataDir, "eds-"+versionWithoutV)
			c := exec.Command(os.Args[0], "download", version, fn, fmt.Sprintf("--verbose=%v", verbose))
			c.Stdout = os.Stdout
			c.Stderr = os.Stderr
			if err := c.Run(); err != nil {
				logger.Error("upgrade failed: %s", err)
				unpause()
				return notification.UpgradeResponse{
					Success:   false,
					Message:   fmt.Sprintf("failed to download version %s: %s", versionWithoutV, err),
					SessionID: sessionId,
					Version:   versionWithoutV,
				}
			}
			c = exec.Command(fn, "version")
			var out strings.Builder
			c.Stdout = &out
			c.Stderr = os.Stderr
			if err := c.Run(); err != nil {
				logger.Error("upgrade failed checking version: %s", err)
				unpause()
				return notification.UpgradeResponse{
					Success:   false,
					Message:   fmt.Sprintf("upgrade failed checking version: %s", err),
					SessionID: sessionId,
					Version:   versionWithoutV,
				}
			}
			newversion := strings.TrimSpace(out.String())
			if newversion != versionWithoutV {
				logger.Error("upgrade failed checking version: %s, was: %s", versionWithoutV, newversion)
				unpause()
				return notification.UpgradeResponse{
					Success:   false,
					Message:   fmt.Sprintf("upgrade failed checking version: %s, was: %s", versionWithoutV, newversion),
					SessionID: sessionId,
					Version:   versionWithoutV,
				}
			}

			exec := util.GetExecutable() // current running executable path

			if err := upgrade.Apply(exec, fn); err != nil {
				if rerr := upgrade.RollbackError(err); rerr != nil {
					logger.Fatal("failed to apply upgrade: %s", rerr)
				} else {
					logger.Error("failed to apply upgrade: %s", err)
					unpause()
					return notification.UpgradeResponse{
						Success:   false,
						Message:   fmt.Sprintf("failed to rename old binary: %s", err),
						SessionID: sessionId,
						Version:   versionWithoutV,
					}
				}
			}

			// if we get here our new binary is in place and we can restart
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/restart", parentPort))
			if err != nil {
				logger.Fatal("restart failed: %s", err)
				return notification.UpgradeResponse{
					Success:   false,
					Message:   fmt.Sprintf("upgrade failed. tried restarting: %s", err),
					SessionID: sessionId,
					Version:   versionWithoutV,
				}
			} else {
				logger.Debug("restart response: %d", resp.StatusCode)
				return notification.UpgradeResponse{
					Success:   true,
					SessionID: sessionId,
					Version:   versionWithoutV,
				}
			}
		}

		var logsLock sync.Mutex
		sendLogs := func() *notification.SendLogsResponse {
			logger.Info("server logfile requested")
			logsLock.Lock()
			defer logsLock.Unlock()
			if sessionId == "" {
				logger.Error("no session ID to rotate logs")
				return nil
			}
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/control/logfile", port))
			if err != nil {
				logger.Error("logfile failed: %s", err)
				return nil
			}
			logger.Debug("logfile response: %d", resp.StatusCode)
			if resp.StatusCode != http.StatusOK {
				logger.Error("logfile failed: %d", resp.StatusCode)
				return nil
			}
			defer resp.Body.Close()

			buf, err := io.ReadAll(resp.Body)
			if err != nil {
				logger.Error("failed to read body: %s", err)
				return nil
			}
			logFile := string(buf)
			uploadURL, err := getLogUploadURL(logger, apiurl, apikey, sessionId)
			if err != nil {
				logger.Error("failed to get upload URL: %s", err)
				return nil
			}
			path, err := uploadLogFile(logger, uploadURL, logFile)
			if err != nil {
				logger.Error("failed to upload logfile: %s", err)
				return nil
			}

			// fork will be done writing to the file, so we can remove it
			if !keepLogs {
				logger.Debug("removing old logfile: %s", logFile)
				os.Remove(logFile)
			}

			return &notification.SendLogsResponse{
				Path:      path,
				SessionID: sessionId,
			}
		}

		runImport := func(ctx context.Context, url string, schemaOnly bool, validateOnly bool) (bool, bool, *string, *string) {
			importargs := []string{"--url", url, "--api-key", apikey, "--no-confirm", "--data-dir", dataDir}
			if schemaOnly {
				importargs = append(importargs, "--schema-only")
			}
			if validateOnly {
				importargs = append(importargs, "--validate-only", "--silent")
			} else {
				importargs = append(importargs, fmt.Sprintf("--verbose=%v", verbose))
			}
			if validateOnly {
				logger.Info("configuring the driver, one moment please...")
			} else {
				logger.Info("running import process ... (duration will vary based on the amount of data being exported)")
			}
			result, err := command.Fork(command.ForkArgs{
				Context:          ctx,
				Log:              logger,
				Command:          "import",
				Args:             importargs,
				LogFilenameLabel: "import",
				SaveLogs:         true,
				ForwardInterrupt: true,
				WriteToStd:       false,
				Dir:              sessionDir,
			})
			if err != nil && result == nil {
				s := "Error importing data. Please contact support for assistance."
				return true, false, &s, nil
			} else {
				ec := result.ProcessState.ExitCode()
				logger.Debug("import exit code: %d, last log line: %s", ec, result.LastErrorLines)
				if noRestart {
					os.Exit(ec)
				}
				switch ec {
				case 0:
					return true, true, nil, nil
				case exitCodeIncorrectUsage:
					var msg string
					tok := strings.Split(strings.TrimRight(result.LastErrorLines, "\n"), "\n")
					if len(tok) > 1 {
						msg = tok[len(tok)-1]
					} else {
						msg = strings.TrimSpace(result.LastErrorLines)
					}
					return false, false, &msg, nil // this means the url is invalid
				default:
					var uploadLogPath string
					uploadURL, err := getLogUploadURL(logger, apiurl, apikey, sessionId)
					if err != nil {
						logger.Error("failed to get upload URL: %s", err)
					} else {
						logFile := filepath.Join(sessionDir, "import_stdout.txt")
						if fi, err := os.Stat(logFile); err == nil && fi.Size() > 0 {
							p, err := uploadLogFile(logger, uploadURL, logFile)
							if err != nil {
								logger.Error("failed to upload stdout logfile: %s", err)
							} else {
								uploadLogPath = p
							}
						}
						logFile = filepath.Join(sessionDir, "import_stderr.txt")
						if fi, err := os.Stat(logFile); err == nil && fi.Size() > 0 {
							if _, err := uploadLogFile(logger, uploadURL, logFile); err != nil {
								logger.Error("failed to upload stderr logfile: %s", err)
							}
						}
					}
					s := "Error importing data. See the error logs for more details or contact support for further assistance."
					return true, false, &s, &uploadLogPath
				}
			}
		}

		configure := func(config *notification.ConfigureRequest) *notification.ConfigureResponse {
			logger.Trace("received driver configuration. url: %s", cstr.Mask(config.URL))
			success, validated, msg, uploadLogPath := runImport(ctx, config.URL, false, true)
			var maskedURL *string
			if success && validated {
				viper.Set("url", config.URL)
				if err := viper.WriteConfig(); err != nil {
					logger.Error("failed to write config: %s", err)
				}
				logger.Info("driver configured successfully, waiting for import action...")
				driverURL = config.URL
				if masked, err := util.MaskURL(config.URL); err != nil {
					logger.Warn("could not mask URL, will not display in app: %s", err)
				} else {
					maskedURL = &masked
				}
				if !configured {
					// restart the server
					restart()
				}
			}

			return &notification.ConfigureResponse{
				SessionID: sessionId,
				Success:   validated,
				LogPath:   uploadLogPath,
				MaskedURL: maskedURL,
				Message:   msg,
				Backfill:  config.Backfill,
			}
		}

		importaction := func(req *notification.ImportRequest) *notification.ImportResponse {
			logger.Trace("received import action")
			pause() // pause the consumer, if any, from processing any data while we are importing
			success, _, msg, uploadLogPath := runImport(ctx, driverURL, !req.Backfill, false)
			if !configured {
				logger.Trace("driver configured")
				configureChannel <- true
			} else {
				restart() // once we have finished the import, restart the server to pick up the new timestamps, etc
			}
			return &notification.ImportResponse{SessionID: sessionId, Success: success, Message: msg, LogPath: uploadLogPath}
		}

		driverconfig := func() *notification.DriverConfigResponse {
			config := internal.GetDriverConfigurations()
			return &notification.DriverConfigResponse{
				SessionID: sessionId,
				Drivers:   config,
			}
		}

		validate := func(driver string, values map[string]any) *notification.ValidateResponse {
			url, fielderrs, err := internal.Validate(driver, values)
			var msg string
			if err != nil {
				msg = err.Error()
			}
			return &notification.ValidateResponse{
				Success:     err == nil && url != "",
				SessionID:   sessionId,
				FieldErrors: fielderrs,
				URL:         url,
				Message:     msg,
			}
		}

		natsurl := mustFlagString(cmd, "server", true)
		if strings.Contains(apiurl, "localhost") {
			natsurl = "nats://localhost:4222"
		}

		// create a notification consumer that will listen for notification actions and handle them here
		notificationConsumer := notification.New(logger, natsurl, notification.NotificationHandler{
			Restart:      restart,
			Shutdown:     shutdown,
			Pause:        pause,
			Unpause:      unpause,
			Upgrade:      upgrade,
			SendLogs:     sendLogs,
			Configure:    configure,
			Import:       importaction,
			DriverConfig: driverconfig,
			Validate:     validate,
		})

		// setup tickers
		duration, _ := cmd.Flags().GetDuration("renew-interval")
		logger.Trace("will renew session every %v", duration)
		renewTicker := time.NewTicker(duration)
		logSenderTicker := time.NewTicker(time.Hour)
		defer renewTicker.Stop()
		defer logSenderTicker.Stop()
		go func() {
			defer util.RecoverPanic(logger)
			for {
				select {
				case <-logSenderTicker.C:
					// ask the notification consumer to send the logs so it can report the success/failure
					notificationConsumer.CallSendLogs()
				case <-renewTicker.C:
					restart()
				}
			}
		}()

		companyIds, _ := cmd.Flags().GetStringSlice("companyIds")

		// main loop
		var failures int
		for {
			if failures >= maxFailures {
				logger.Fatal("too many failures after %d attempts, exiting", failures)
			}
			session, err := sendStart(logger, apiurl, apikey, driverURL, edsServerId, companyIds)
			if err != nil {
				if errors.Is(err, errAlreadyRunning) {
					logger.Info("another eds server is already running for this server (id: %s). Retrying in 5 seconds", edsServerId)
					time.Sleep(time.Second * 5)
					continue
				}
				logger.Fatal("failed to send session start: %s", err)
			}
			logger.Trace("session started: %s", util.JSONStringify(session))
			sessionId = session.SessionId
			sessionDir = filepath.Join(dataDir, sessionId)
			if err := os.MkdirAll(sessionDir, 0700); err != nil {
				logger.Fatal("failed to create session directory: %s", err)
			}
			if session.Credential == nil {
				logger.Fatal("no credential found in session")
			}
			// write credential to file
			credsFile = filepath.Join(sessionDir, "nats.creds")
			if err := writeCredsToFile(*session.Credential, credsFile); err != nil {
				logger.Fatal("failed to write creds to file: %s", err)
			}
			logger.Trace("creds written to %s", credsFile)
			if err := notificationConsumer.Start(credsFile); err != nil {
				if strings.Contains(err.Error(), "error connecting to NATS") {
					logger.Trace("error from nats: %s", err)
					logger.Trace("nats not available, retrying in 5 seconds")
					time.Sleep(time.Second * 5)
					continue
				}
			}
			if !configured {
				logger.Info("Return to HQ and continue with configuring your server.")
				select {
				case <-configureChannel:
					configured = true
				case <-ctx.Done():
					return
				case <-sys.CreateShutdownChannel():
					cancel()
					return
				}
			}
			sessionLogsDir := filepath.Join(sessionDir, "logs")
			args := append(_args,
				"--creds", credsFile,
				"--logs-dir", sessionLogsDir,
				"--url", driverURL,
				"--server", natsurl,
			)
			result, err := command.Fork(command.ForkArgs{
				Log:              logger,
				Command:          "fork",
				Args:             args,
				LogFilenameLabel: "server",
				SaveLogs:         true,
				LogFileSink:      true,
				WriteToStd:       true,
				ForwardInterrupt: true,
				ProcessCallback:  processCallback,
				Dir:              sessionDir,
			})
			if err != nil && result == nil {
				logger.Error("failed to fork: %s", err)
				failures++
			} else {
				ec := result.ProcessState.ExitCode()
				if noRestart {
					logger.Trace("no restart, stopping consumer on %v", ec)
					notificationConsumer.Stop()
					os.Exit(ec)
				}
				if ec != exitCodeIncorrectUsage {
					logFile, err := getRemainingLog(sessionLogsDir)
					if err != nil {
						logger.Error("failed to get remaining log: %s", err)
					}
					logsLock.Lock()
					logPath, err := sendEndAndUpload(logger, apiurl, apikey, session.SessionId, ec != 0 && ec != exitCodeRestart, logFile, filepath.Join(sessionDir, "server_stderr.txt"))
					logsLock.Unlock()
					if err != nil {
						logger.Error("failed to send end and upload logs: %s", err)
					} else {
						if err := notificationConsumer.PublishSendLogsResponse(&notification.SendLogsResponse{Path: logPath, SessionID: sessionId}); err != nil {
							logger.Error("failed to publish send logs response: %s", err)
						}
					}
				}
				if ec == exitCodeNatsDisconnected {
					logger.Info("nats disconnected, retrying in 5 seconds")
					time.Sleep(time.Second * 5)
					continue
				}
				if ec == 0 {
					// on success, remove the logs
					if !keepLogs {
						os.RemoveAll(sessionDir)
					}
					break
				}
				// if a "normal" exit code, just exit and remove the logs
				if ec == exitCodeIncorrectUsage || ec == 1 && (strings.Contains(result.LastErrorLines, "error: required flag") || strings.Contains(result.LastErrorLines, "Global Flags")) {
					notificationConsumer.Stop()
					os.Exit(ec)
				}
				if ec == exitCodeRestart {
					logger.Info("server shut down as part of restart (code = %d)", ec)
				} else {
					failures++
					logger.Error("server exited with code %d", ec)
				}
			}
			notificationConsumer.Stop()
		}
	},
}

var serverHelpCmd = &cobra.Command{
	Use:   "help [driver]",
	Short: "Get help for the server operations",
	Args:  cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		yellow := color.New(color.FgYellow, color.Bold).SprintFunc()
		cyan := color.New(color.FgCyan).SprintFunc()
		black := color.New(color.FgBlack, color.Bold).SprintFunc()
		whiteBold := color.New(color.FgWhite, color.Bold).SprintFunc()
		white := color.New(color.FgWhite).SprintFunc()
		green := color.New(color.FgGreen).SprintFunc()
		blue := color.New(color.FgBlue, color.Bold).SprintFunc()
		driverMetadata := internal.GetDriverMetadata()
		fmt.Println()
		fmt.Printf("%s\n", blue("Shopmonkey Enterprise Data Streaming (EDS) Server"))
		fmt.Printf("%s\n", black("version: "+Version))
		fmt.Println()
		if len(args) == 0 {
			fmt.Println("This server version supports the following integrations:")
			fmt.Println()
			if len(driverMetadata) == 0 {
				fmt.Println("No integrations found.")
				fmt.Println()
				os.Exit(1)
			}
			for _, metadata := range driverMetadata {
				fmt.Printf("%s\n", yellow(metadata.Name))
				fmt.Printf("%s\n", whiteBold(metadata.Description))
				fmt.Printf("%s: %s\n", black("Example url"), cyan(metadata.ExampleURL))
				fmt.Println()
			}
			fmt.Println()
			fmt.Println(whiteBold("Example usage:"))
			fmt.Println()
			c := filepath.Base(os.Args[0])
			if Version == "dev" {
				c = "go run . "
			}
			fmt.Printf(" $ %s\n", green(c+" server --url "+driverMetadata[0].ExampleURL+" --api-key $TOKEN --creds /path/to/creds.json"))
			fmt.Println()
			fmt.Println(black("To get a full list of options, pass in the --help flag."))
			fmt.Println()
			fmt.Println(black("To get more detailed help for a specific integration run: " + c + "server help [name]"))
			fmt.Println()
		} else {
			var metadata *internal.DriverMetadata
			for _, driver := range driverMetadata {
				if driver.Name == args[0] {
					metadata = &driver
					break
				}
			}
			if metadata == nil {
				fmt.Printf("No integration named %s is not found.\n", yellow(args[0]))
				fmt.Println()
				os.Exit(1)
			}
			fmt.Printf("%s\n", yellow(metadata.Name))
			fmt.Printf("%s\n", white(metadata.Description))
			fmt.Printf("%s: %s\n", black("Example url"), cyan(metadata.ExampleURL))
			fmt.Println()
			fmt.Println(metadata.Help)
			fmt.Println()
		}
	},
}

func init() {
	rootCmd.AddCommand(serverCmd)
	serverCmd.AddCommand(serverHelpCmd)

	// NOTE: sync these with forkCmd
	serverCmd.Flags().String("url", "", "driver connection string")
	viper.BindPFlag("url", serverCmd.Flags().Lookup("url"))
	serverCmd.Flags().String("api-key", os.Getenv("SM_APIKEY"), "shopmonkey API key")
	viper.BindPFlag("token", serverCmd.Flags().Lookup("api-key"))

	serverCmd.Flags().Int("port", getOSInt("PORT", 8080), "the port to listen for health checks, metrics etc")
	serverCmd.Flags().String("eds-id", "", "the EDS server ID")
	viper.BindPFlag("server_id", serverCmd.Flags().Lookup("eds-id"))
	serverCmd.Flags().StringSlice("companyIds", nil, "restrict to a specific company ID or multiple, if not set will use all")
	serverCmd.Flags().MarkHidden("companyIds") // not intended for production use
	serverCmd.Flags().Bool("keep-logs", false, "keep logs after the server exits instead of deleting them")
	viper.BindPFlag("keep_logs", serverCmd.Flags().Lookup("keep-logs"))

	// deprecated but left for backwards compatibility
	serverCmd.Flags().Int("health-port", 0, "the port to listen for health checks")
	serverCmd.Flags().MarkDeprecated("health-port", "use --port instead")

	// internal use only
	serverCmd.Flags().String("api-url", "https://api.shopmonkey.cloud", "url to shopmonkey api")
	serverCmd.Flags().MarkHidden("api-url")
	serverCmd.Flags().String("server", "nats://connect.nats.shopmonkey.pub", "the nats server url, could be multiple comma separated")
	serverCmd.Flags().MarkHidden("server")
	serverCmd.Flags().String("consumer-suffix", "", "suffix which is appended to the nats consumer group name")
	serverCmd.Flags().MarkHidden("consumer-suffix")
	serverCmd.Flags().Int("maxAckPending", defaultMaxAckPending, "the number of max ack pending messages")
	serverCmd.Flags().MarkHidden("maxAckPending")
	serverCmd.Flags().Int("maxPendingBuffer", defaultMaxPendingBuffer, "the maximum number of messages to pull from nats to buffer")
	serverCmd.Flags().MarkHidden("maxPendingBuffer")
	serverCmd.Flags().Duration("minPendingLatency", consumer.DefaultMinPendingLatency, "the minimum accumulation period before flushing (0 uses default)")
	serverCmd.Flags().MarkHidden("minPendingLatency")
	serverCmd.Flags().Duration("maxPendingLatency", consumer.DefaultMaxPendingLatency, "the maximum accumulation period before flushing (0 uses default)")
	serverCmd.Flags().MarkHidden("maxPendingLatency")
	serverCmd.Flags().Bool("restart", false, "restart the consumer from the beginning (only works on new consumers)")
	serverCmd.Flags().MarkHidden("restart")
	serverCmd.Flags().Duration("renew-interval", time.Hour*24, "the interval to renew the session")
	serverCmd.Flags().MarkHidden("renew-interval")
	serverCmd.Flags().Bool("wrapper", false, "running in wrapper mode")
	serverCmd.Flags().MarkHidden("wrapper")
	serverCmd.Flags().Int("parent", -1, "the parent pid")
	serverCmd.Flags().MarkHidden("parent")
	serverCmd.Flags().Bool("no-restart", false, "do not restart the server if it exits")
	serverCmd.Flags().MarkHidden("no-restart")
}
