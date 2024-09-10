package cmd

import (
	"encoding/json"
	"fmt"
	glog "log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/tracker"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var dataDir string

func mustFlagString(cmd *cobra.Command, name string, required bool) string {
	val, err := cmd.Flags().GetString(name)
	if err != nil {
		fmt.Printf("error: %s\n", err)
		os.Exit(3)
	}
	if required && val == "" {
		fmt.Printf("error: required flag --%s missing\n", name)
		os.Exit(3)
	}
	return val
}

func initConfig() {
	cfgFile := filepath.Join(dataDir, "config.toml")
	if dataDir != "" && util.Exists(cfgFile) {
		viper.SetConfigFile(cfgFile)
		if err := viper.ReadInConfig(); err != nil && !util.SliceContains(os.Args, "enroll") {
			fmt.Printf("error loading %s: %s\n", cfgFile, err)
			os.Exit(3)
		}
	}
}

func mustFlagInt(cmd *cobra.Command, name string, required bool) int {
	val, err := cmd.Flags().GetInt(name)
	if err != nil {
		fmt.Printf("error: %s\n", err)
		os.Exit(3)
	}
	if required && val <= 0 {
		fmt.Printf("error: required flag --%s missing\n", name)
		os.Exit(3)
	}
	return val
}

func mustFlagBool(cmd *cobra.Command, name string, required bool) bool {
	if cmd.Flags().Changed(name) {
		val, err := cmd.Flags().GetBool(name)
		if err != nil {
			fmt.Printf("error: %s\n", err)
			os.Exit(3)
		}
		return val
	}
	if required {
		fmt.Printf("error: required flag --%s missing\n", name)
		os.Exit(3)
	}
	return false
}

func getOSInt(name string, def int) int {
	val, ok := os.LookupEnv(name)
	if !ok {
		return def
	}
	i, err := strconv.Atoi(val)
	if err != nil {
		return def
	}
	return i
}

type logFileSink struct {
	logDir string
	lock   sync.Mutex
	f      *os.File
}

func (s *logFileSink) Write(buf []byte) (int, error) {
	if s == nil {
		return 0, nil
	}
	s.lock.Lock()
	defer s.lock.Unlock()

	n, err := s.f.Write(buf)
	if err != nil {
		return n, err
	}
	l, err := s.f.WriteString("\n")
	if err != nil {
		return l, err
	}
	return n + l, nil
}

func (s *logFileSink) Close() error {
	if s == nil {
		return nil
	}
	return s.f.Close()
}

// Rotate creates a new log file and closes the old one
// returns the old file name
func (s *logFileSink) Rotate() (string, error) {
	if s == nil {
		return "", fmt.Errorf("sink not initialized")
	}
	var old string
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.f != nil {
		if err := s.Close(); err != nil {
			return "", err
		}
		old = s.f.Name()
	}
	if err := os.MkdirAll(s.logDir, 0755); err != nil {
		return "", err
	}
	f, err := os.Create(filepath.Join(s.logDir, fmt.Sprintf("eds-%d.log", time.Now().UnixMilli())))
	if err != nil {
		return "", err
	}
	s.f = f
	return old, nil
}

func newLogFileSink(dir string) (*logFileSink, error) {
	sink := logFileSink{
		logDir: dir,
	}
	if _, err := sink.Rotate(); err != nil {
		return nil, fmt.Errorf("error creating log file: %s", err)
	}
	return &sink, nil
}

type CloseFunc func()

func newLogger(cmd *cobra.Command) logger.Logger {
	ts, _ := cmd.Flags().GetBool("timestamp")
	if !ts {
		glog.SetFlags(0)
	}
	glog.SetOutput(os.Stdout)
	silent, _ := cmd.Flags().GetBool("silent")
	var log logger.Logger
	if silent {
		log = logger.NewConsoleLogger(logger.LevelError)
	} else {
		verbose, _ := cmd.Flags().GetBool("verbose")
		if verbose {
			log = logger.NewConsoleLogger(logger.LevelTrace)
		} else {
			log = logger.NewConsoleLogger(logger.LevelInfo)
		}
	}

	return log
}

func newLoggerWithSink(log logger.Logger, sink logger.Sink) logger.Logger {
	if sink != nil {
		return logger.NewMultiLogger(log, logger.NewJSONLoggerWithSink(sink, logger.LevelTrace))
	}
	return log
}

func setHTTPHeader(req *http.Request, apiKey string) {
	req.Header = http.Header{
		"Content-Type": {"application/json"},
		"User-Agent":   {"Shopmonkey EDS Server/" + Version},
	}
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
}

func getRequestID(resp *http.Response) string {
	return resp.Header.Get("X-Request-Id")
}

func getDataDir(cmd *cobra.Command, logger logger.Logger) string {
	dataDir := mustFlagString(cmd, "data-dir", true)
	dataDir, _ = filepath.Abs(filepath.Clean(dataDir))

	if !util.Exists(dataDir) {
		if ok, err := util.IsDirWritable(filepath.Dir(dataDir)); !ok {
			logger.Fatal("%s", err)
		}
		if err := os.MkdirAll(dataDir, 0700); err != nil {
			logger.Fatal("mkdir: %s", err)
		}
		logger.Debug("making data directory: %s", dataDir)
	} else {
		if ok, err := util.IsDirWritable(dataDir); !ok {
			logger.Fatal("%s", err)
		}
	}

	logger.Debug("using data directory: %s", dataDir)
	return dataDir
}

func loadTableExportInfo(theTracker *tracker.Tracker) ([]TableExportInfo, error) {
	found, val, err := theTracker.GetKey(trackerTableExportKey)
	if err != nil {
		return nil, fmt.Errorf("error loading table export data from tracker: %w", err)
	}
	if !found {
		return nil, nil
	}
	var tableData []TableExportInfo
	if err := json.Unmarshal([]byte(val), &tableData); err != nil {
		return nil, fmt.Errorf("error decoding table export data: %w", err)
	}
	return tableData, nil
}

func loadSchemaValidator(cmd *cobra.Command) (internal.SchemaValidator, error) {
	schemaDir := mustFlagString(cmd, "schema-validator", false)
	if schemaDir == "" {
		return nil, nil
	}
	return util.NewSchemaValidator(schemaDir)
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:  "eds",
	Long: "Shopmonkey Enterprise Data Streaming server (EDS) \nFor detailed information, see: https://shopmonkey.dev/eds \nand https://github.com/shopmonkeyus/eds",
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func getExecutable() string {
	ex, err := os.Executable()
	if err != nil {
		ex = os.Args[0]
	}
	return ex
}

func getCommandExample(command string, args ...string) string {
	ex := getExecutable()
	var cmd string
	if strings.Contains(ex, "go-build") {
		// If we are running in a go build environment, we need to tell the user how to start the server
		cmd = fmt.Sprintf("go run . %s %s", command, strings.Join(args, " "))
	} else {
		// if in the current directory, we should add a period for *nix systems otherwise show the full path
		cwd, _ := os.Getwd()
		exc := ex
		if strings.HasPrefix(ex, cwd) && runtime.GOOS != "windows" {
			exc = "./" + strings.TrimPrefix(ex, cwd+"/")
		}
		cmd = fmt.Sprintf("%s %s %s", exc, command, strings.Join(args, " "))
	}
	return "`" + strings.TrimSpace(cmd) + "`"
}

func init() {
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Println("couldn't get current working directory: ", err)
		os.Exit(1)
	}
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().Bool("verbose", false, "turn on verbose logging")
	rootCmd.PersistentFlags().Bool("silent", false, "turn off all logging")
	rootCmd.PersistentFlags().Bool("timestamp", false, "turn on timestamps in logs")
	rootCmd.PersistentFlags().String("log-file-sink", "", "the log file sink to use")
	rootCmd.PersistentFlags().MarkHidden("log-file-sink")
	rootCmd.PersistentFlags().String("schema-validator", "", "the schema validator directory to use")
	rootCmd.PersistentFlags().StringVar(&dataDir, "data-dir", filepath.Join(cwd, "data"), "the data directory for storing state, logs, and other data")
}
