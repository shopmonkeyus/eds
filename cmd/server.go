package cmd

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/fatih/color"
	"github.com/nats-io/nats.go"
	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/consumer"
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

type edsSession struct {
	SessionId  string  `json:"sessionId"`
	Credential *string `json:"credential"`
}

type sessionStartResponse struct {
	Success bool       `json:"success"`
	Message string     `json:"message"`
	Data    edsSession `json:"data"`
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

type sessionRenewResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	Data    struct {
		Credential *string `json:"credential"`
	} `json:"data"`
}

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

func sendStart(logger logger.Logger, apiURL string, apiKey string) (*edsSession, error) {
	var body sessionStart
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
	req, err := http.NewRequest("POST", apiURL+"/v3/eds", bytes.NewBuffer([]byte(util.JSONStringify(body))))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}
	setHTTPHeader(req, apiKey)
	retry := util.NewHTTPRetry(req)
	resp, err := retry.Do()
	if err != nil {
		return nil, fmt.Errorf("failed to send session start: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		buf, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to send session start. status code=%d. %s", resp.StatusCode, string(buf))
	}
	var sessionResp sessionStartResponse
	if err := json.NewDecoder(resp.Body).Decode(&sessionResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}
	if !sessionResp.Success {
		return nil, fmt.Errorf("failed to start session: %s", sessionResp.Message)
	}
	logger.Trace("session %s started successfully", sessionResp.Data.SessionId)
	return &sessionResp.Data, nil
}

func sendEnd(logger logger.Logger, apiURL string, apiKey string, sessionId string, errored bool) (string, error) {
	var body sessionEnd
	body.Errored = errored
	req, err := http.NewRequest("POST", apiURL+"/v3/eds/"+sessionId, bytes.NewBuffer([]byte(util.JSONStringify(body))))
	if err != nil {
		return "", fmt.Errorf("failed to create HTTP request: %w", err)
	}
	setHTTPHeader(req, apiKey)
	retry := util.NewHTTPRetry(req)
	resp, err := retry.Do()
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

func sendRenew(logger logger.Logger, apiURL string, apiKey string, sessionId string) (*string, error) {
	req, err := http.NewRequest("POST", apiURL+"/v3/eds/renew/"+sessionId, strings.NewReader(util.JSONStringify(map[string]any{})))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}
	setHTTPHeader(req, apiKey)
	retry := util.NewHTTPRetry(req)
	resp, err := retry.Do()
	if err != nil {
		return nil, fmt.Errorf("failed to send renew end: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		buf, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to send session end. status code=%d. %s", resp.StatusCode, string(buf))
	}
	var s sessionRenewResponse
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}
	if !s.Success {
		return nil, fmt.Errorf("failed to renew session: %s", s.Message)
	}
	logger.Trace("session %s renew successfully", sessionId)
	return s.Data.Credential, nil
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
	setHTTPHeader(req, "")
	req.Header.Set("Content-Type", "application/x-tgz")
	retry := util.NewHTTPRetry(req)
	resp, err := retry.Do()
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
		logger.Info("error log files saved to %s for session: %s", logfile, sessionId)
	}
}

func runHealthCheckServer(logger logger.Logger, port int, fwdport int) {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/", fwdport))
		if err != nil {
			logger.Error("health check failed: %s", err)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if resp.StatusCode != http.StatusOK {
			logger.Error("health check failed: %d", resp.StatusCode)
		}
		w.WriteHeader(resp.StatusCode)
	})
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/metrics", fwdport))
		if err != nil {
			logger.Error("metric failed: %s", err)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(resp.StatusCode)
		w.Header().Set("Content-Type", resp.Header.Get("Content-Type"))
		io.Copy(w, resp.Body)
		resp.Body.Close()
	})
	go func() {
		defer util.RecoverPanic(logger)
		if err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil); err != nil && err != http.ErrServerClosed {
			logger.Fatal("failed to start health check server: %s", err)
		}
	}()
}

type notificationConsumer struct {
	nc      *nats.Conn
	sub     *nats.Subscription
	logger  logger.Logger
	natsurl string

	Notifications chan *Notification
}

func (c *notificationConsumer) Start(sessionId string, credsFile string) error {
	var err error
	c.nc, _, err = consumer.NewNatsConnection(c.logger, c.natsurl, credsFile)
	if err != nil {
		return fmt.Errorf("failed to create nats connection: %w", err)
	}
	c.sub, err = c.nc.Subscribe(fmt.Sprintf("eds.notify.%s.>", sessionId), c.callback)
	if err != nil {
		return fmt.Errorf("failed to subscribe to eds.notify: %w", err)
	}
	return nil
}

type Notification struct {
	Action string `json:"action"`
	Data   any    `json:"data,omitempty"`
}

func (n *Notification) String() string {
	return util.JSONStringify(n)
}

func (c *notificationConsumer) callback(m *nats.Msg) {
	var notification Notification
	if err := util.DecodeNatsMsg(m, &notification); err != nil {
		c.logger.Error("failed to decode notification message: %s", err)
		return
	}
	c.logger.Trace("received message: %s", notification.String())
	c.Notifications <- &notification
}

func (c *notificationConsumer) Stop() {
	if c.sub != nil {
		if err := c.sub.Unsubscribe(); err != nil {
			c.logger.Error("failed to unsubscribe from nats: %s", err)
		}
		c.sub = nil
	}
	if c.nc != nil {
		c.nc.Close()
		c.nc = nil
	}
}

func (c *notificationConsumer) Restart(sessionId string, credsFile string) error {
	c.Stop()
	return c.Start(sessionId, credsFile)
}

func newNotificationConsumer(logger logger.Logger, natsurl string) *notificationConsumer {
	return &notificationConsumer{
		logger:        logger,
		natsurl:       natsurl,
		Notifications: make(chan *Notification),
	}
}

var serverIgnoreFlags = map[string]bool{
	"--api-url":        true,
	"--api-key":        true,
	"--silent":         true,
	"--port":           true,
	"--health-port":    true,
	"--renew-interval": true,
}

var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Run the server",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		logger, closer := newLogger(cmd)
		closer()

		logger = logger.WithPrefix("[server]")

		defer util.RecoverPanic(logger)

		apiurl := mustFlagString(cmd, "api-url", true)
		apikey := mustFlagString(cmd, "api-key", true)
		var credsFile string

		// must be in a defer to make sure we pick up credsFile variable
		defer func() {
			os.Remove(credsFile) // make sure we remove the temporary credential
		}()

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

		// setup health check server
		fwdPort, err := util.GetFreePort()
		if err != nil {
			logger.Fatal("failed to get free port: %s", err)
		}
		healthPort := mustFlagInt(cmd, "port", true)
		oldHealthPort := mustFlagInt(cmd, "health-port", false)
		if oldHealthPort > 0 {
			healthPort = oldHealthPort // allow it for now for backwards compatibility but eventually remove it
		}
		runHealthCheckServer(logger, healthPort, fwdPort)
		_args = append(_args, "--port", fmt.Sprintf("%d", fwdPort))

		dataDir := mustFlagString(cmd, "data-dir", true)
		dataDir, _ = filepath.Abs(filepath.Clean(dataDir))

		if !util.Exists(dataDir) {
			logger.Fatal("data directory %s does not exist. please create the directory and retry again.", dataDir)
		}
		if ok, err := util.IsDirWritable(dataDir); !ok {
			logger.Fatal("%s", err)
		}

		logger.Debug("using data directory: %s", dataDir)

		_args = append(_args, "--data-dir", dataDir)

		var currentProcess *os.Process
		var sessionId string

		processCallback := func(p *os.Process) {
			currentProcess = p
			logger.Debug("fork process started with pid: %d", p.Pid)
		}

		restart := func() {
			if currentProcess != nil {
				logger.Info("need to restart child process to reload nats credentials")
				currentProcess.Signal(syscall.SIGHUP) // tell the child to restart
			} else {
				logger.Fatal("no child process to signal on credentials renew")
			}
		}

		renew := func() {
			creds, err := sendRenew(logger, apiurl, apikey, sessionId)
			if err != nil {
				logger.Fatal("failed to renew session: %s", err)
			}
			if creds != nil {
				if err := writeCredsToFile(*creds, credsFile); err != nil {
					logger.Fatal("failed to write creds to file: %s", err)
				}
				restart()
			} else {
				logger.Trace("no new credentials to renew")
			}
		}

		natsurl := mustFlagString(cmd, "server", true)
		notificationConsumer := newNotificationConsumer(logger, natsurl)

		go func() {
			defer util.RecoverPanic(logger)
			duration, _ := cmd.Flags().GetDuration("renew-interval")
			logger.Trace("will renew session every %v", duration)
			ticker := time.NewTicker(duration)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					renew()
				case notification := <-notificationConsumer.Notifications:
					switch notification.Action {
					case "restart":
						logger.Info("received restart notification")
						restart()
					case "renew":
						logger.Info("received renew notification")
						renew()
					default:
						logger.Warn("unhandled notification: %s", notification.String())
					}
				}
			}
		}()

		// main loop
		var failures int
		for {
			if failures >= maxFailures {
				logger.Fatal("too many failures after %d attempts, exiting", failures)
			}
			session, err := sendStart(logger, apiurl, apikey)
			if err != nil {
				logger.Fatal("failed to send session start: %s", err)
			}
			logger.Trace("session started: %s", util.JSONStringify(session))
			sessionId = session.SessionId
			if credsFile == "" && session.Credential != nil {
				// write credential to file
				credsFile = filepath.Join(dataDir, fmt.Sprintf("eds-%s.creds", session.SessionId))
				if err := writeCredsToFile(*session.Credential, credsFile); err != nil {
					logger.Fatal("failed to write creds to file: %s", err)
				}
				logger.Trace("creds written to %s", credsFile)
			} else {
				logger.Trace("using creds file: %s", credsFile)
			}
			if err := notificationConsumer.Start(sessionId, credsFile); err != nil {
				logger.Fatal("failed to start notification consumer: %s", err)
			}
			args := append(_args, "--creds", credsFile)
			result, err := command.Fork(command.ForkArgs{
				Log:              logger,
				Command:          "fork",
				Args:             args,
				LogFilenameLabel: "server",
				SaveLogs:         true,
				WriteToStd:       true,
				ForwardInterrupt: true,
				LogFileSink:      true,
				ProcessCallback:  processCallback,
			})
			currentProcess = nil
			notificationConsumer.Stop()
			if err != nil && result == nil {
				logger.Error("failed to fork: %s", err)
				failures++
			} else {
				ec := result.ProcessState.ExitCode()
				if ec != 3 {
					sendEndAndUpload(logger, apiurl, apikey, session.SessionId, ec != 0, result.LogFileBundle)
				}
				if ec == 0 {
					break
				}
				// if a "normal" exit code, just exit and remove the logs
				if ec == 3 || ec == 1 && (strings.Contains(result.LastErrorLines, "error: required flag") || strings.Contains(result.LastErrorLines, "Global Flags")) {
					os.Exit(ec)
				}
				failures++
				logger.Error("server exited with code %d", ec)
			}
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
		black := color.New(color.FgBlack).SprintFunc()
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
			for _, metadata := range driverMetadata {
				fmt.Printf("%-25s%s\n", yellow(metadata.Name), whiteBold(metadata.Description))
				fmt.Printf("%14s%s: %s\n", "", black("Example url"), cyan(metadata.ExampleURL))
				fmt.Println()
			}
			fmt.Println()
			fmt.Println("Example usage:")
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

	cwd, err := os.Getwd()
	if err != nil {
		fmt.Println("couldn't get current working directory: ", err)
		os.Exit(1)
	}

	// NOTE: sync these with forkCmd
	serverCmd.Flags().String("url", "", "provider connection string")
	serverCmd.Flags().String("api-key", os.Getenv("SM_APIKEY"), "shopmonkey API key")
	serverCmd.Flags().Int("port", getOSInt("PORT", 8080), "the port to listen for health checks, metrics etc")
	serverCmd.Flags().String("data-dir", cwd, "the data directory for storing logs and other data")

	// deprecated but left for backwards compatibility
	serverCmd.Flags().Int("health-port", 0, "the port to listen for health checks")
	serverCmd.Flags().MarkDeprecated("health-port", "use --port instead")

	// internal use only
	serverCmd.Flags().String("schema", "schema.json", "the shopmonkey schema file")
	serverCmd.Flags().MarkHidden("schema")
	serverCmd.Flags().String("tables", "tables.json", "the shopmonkey tables file")
	serverCmd.Flags().MarkHidden("tables")
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
	serverCmd.Flags().Bool("restart", false, "restart the consumer from the beginning (only works on new consumers)")
	serverCmd.Flags().MarkHidden("restart")
	serverCmd.Flags().Duration("renew-interval", time.Hour*24, "the interval to renew the session")
	serverCmd.Flags().MarkHidden("renew-interval")
}
