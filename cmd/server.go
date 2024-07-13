package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/shopmonkeyus/eds-server/internal/util"
	"github.com/shopmonkeyus/go-common/command"
	"github.com/shopmonkeyus/go-common/logger"
	"github.com/spf13/cobra"
)

var Version string // set in main

const maxFailures = 5

type sessionStart struct {
	Version   string `json:"version"`
	Hostname  string `json:"hostname"`
	IPAddress string `json:"ipAddress"`
	MachineId string `json:"machineId"`
	OsInfo    any    `json:"osinfo"`
}

type sessionStartResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Data    struct {
		SessionId string `json:"sessionId"`
	} `json:"data"`
}

type sessionEnd struct {
	Errored bool `json:"errored"`
}

type sessionEndResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Data    struct {
		URL string `json:"url"`
	} `json:"data"`
}

func sendStart(logger logger.Logger, apiURL string, apiKey string) (string, error) {
	var body sessionStart
	ipaddress, err := util.GetLocalIP()
	if err != nil {
		return "", fmt.Errorf("failed to get local IP: %w", err)
	}
	machineid, err := util.GetMachineId()
	if err != nil {
		return "", fmt.Errorf("failed to get machine ID: %w", err)
	}
	hostname, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("failed to get hostname: %w", err)
	}
	osinfo, err := util.GetSystemInfo()
	if err != nil {
		return "", fmt.Errorf("failed to get system info: %w", err)
	}
	body.MachineId = machineid
	body.IPAddress = ipaddress
	body.Hostname = hostname
	body.Version = Version
	body.OsInfo = osinfo
	req, err := http.NewRequest("POST", apiURL+"/v3/eds", bytes.NewBuffer([]byte(util.JSONStringify(body))))
	if err != nil {
		return "", fmt.Errorf("failed to create HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send session start: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		buf, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("failed to send session start. status code=%d. %s", resp.StatusCode, string(buf))
	}
	var s sessionStartResponse
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}
	if !s.Success {
		return "", fmt.Errorf("failed to start session: %s", s.Message)
	}
	logger.Trace("session %s started successfully", s.Data.SessionId)
	return s.Data.SessionId, nil
}

func sendEnd(logger logger.Logger, apiURL string, apiKey string, sessionId string, errored bool) (string, error) {
	var body sessionEnd
	body.Errored = errored
	req, err := http.NewRequest("POST", apiURL+"/v3/eds/"+sessionId, bytes.NewBuffer([]byte(util.JSONStringify(body))))
	if err != nil {
		return "", fmt.Errorf("failed to create HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send session end: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		buf, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("failed to send session end. status code=%d. %s", resp.StatusCode, string(buf))
	}
	var s sessionEndResponse
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}
	if !s.Success {
		return "", fmt.Errorf("failed to end session: %s", s.Message)
	}
	logger.Trace("session %s ended successfully: %s", sessionId, s.Data.URL)
	return s.Data.URL, nil
}

func uploadLogs(logger logger.Logger, url string, logFileBundle string) error {
	of, err := os.Open(logFileBundle)
	if err != nil {
		return fmt.Errorf("failed to open log file bundle: %w", err)
	}
	defer of.Close()
	req, err := http.NewRequest("PUT", url, of)
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-tgz")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to upload logs: %w", err)
	}
	defer resp.Body.Close()
	buf, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to upload logs. status code=%d. %s", resp.StatusCode, string(buf))
	}
	logger.Trace("logs uploaded successfully: %s", logFileBundle)
	return nil
}

func sendEndAndUpload(logger logger.Logger, apiurl string, apikey string, sessionId string, errored bool, logfile string) {
	logger.Info("uploading logs for session: %s", sessionId)
	if !errored {
		defer os.Remove(logfile)
	}
	url, err := sendEnd(logger, apiurl, apikey, sessionId, errored)
	if err != nil {
		logger.Fatal("failed to send session end: %s", err)
	}
	if err := uploadLogs(logger, url, logfile); err != nil {
		logger.Fatal("failed to upload logs: %s", err)
	}
	logger.Trace("logs uploaded successfully for session: %s", sessionId)
	if errored {
		logger.Info("error log files saved to %s", logfile)
	}
}

var serverIgnoreFlags = map[string]bool{
	"--api-url": true,
	"--api-key": true,
	"--silent":  true,
}

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Run the server",
	Run: func(cmd *cobra.Command, args []string) {
		logger, closer := newLogger(cmd)
		closer()

		prefix := mustFlagString(cmd, "consumer-prefix", false)
		if prefix != "" {
			logger.Fatal("consumer-prefix is deprecated, use --consumer-suffix instead")
		}

		apiurl := mustFlagString(cmd, "api-url", true)
		apikey := mustFlagString(cmd, "api-key", true)

		var skipping bool
		var _args []string
		for _, arg := range os.Args[2:] {
			if skipping {
				skipping = false
				continue
			}
			if serverIgnoreFlags[arg] {
				skipping = true
				continue
			}
			_args = append(_args, arg)
		}

		// main loop
		var failures int
		for {
			if failures >= maxFailures {
				logger.Fatal("too many failures after %d attempts, exiting", failures)
			}
			sessionId, err := sendStart(logger, apiurl, apikey)
			if err != nil {
				logger.Fatal("failed to send session start: %s", err)
			}
			result, err := command.Fork(command.ForkArgs{
				Log:              logger,
				Command:          "fork",
				Args:             _args,
				LogFilenameLabel: "server",
				SaveLogs:         true,
				WriteToStd:       true,
				ForwardInterrupt: true,
				LogFileSink:      true,
			})
			if err != nil && result == nil {
				logger.Error("failed to fork: %s", err)
				failures++
			} else {
				ec := result.ProcessState.ExitCode()
				sendEndAndUpload(logger, apiurl, apikey, sessionId, ec != 0, result.LogFileBundle)
				if ec == 0 {
					break
				}
				// if a "normal" exit code, just exit and remove the logs
				if ec == 2 || ec == 1 && (strings.Contains(result.LastErrorLines, "error: required flag") || strings.Contains(result.LastErrorLines, "Global Flags")) {
					os.Exit(ec)
				}
				failures++
				logger.Error("server exited with code %d", ec)
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(serverCmd)
	// NOTE: sync these with forkCmd
	serverCmd.Flags().String("consumer-prefix", "", "deprecated - use --consumer-suffix instead")
	serverCmd.Flags().String("consumer-suffix", "", "a suffix to use for the consumer group name")
	serverCmd.Flags().String("creds", "", "the server credentials file provided by Shopmonkey")
	serverCmd.Flags().String("server", "nats://connect.nats.shopmonkey.pub", "the nats server url, could be multiple comma separated")
	serverCmd.Flags().String("db-url", "", "database connection string")
	serverCmd.Flags().String("api-url", "https://api.shopmonkey.cloud", "url to shopmonkey api")
	serverCmd.Flags().String("api-key", os.Getenv("SM_APIKEY"), "shopmonkey API key")
	serverCmd.Flags().String("schema", "schema.json", "the shopmonkey schema file")
	serverCmd.Flags().Int("replicas", 1, "the number of consumer replicas")
}
