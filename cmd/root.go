package cmd

import (
	"fmt"
	glog "log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"

	"github.com/shopmonkeyus/go-common/logger"
	"github.com/spf13/cobra"

	// Register all drivers
	_ "github.com/shopmonkeyus/eds-server/internal/drivers/eventhub"
	_ "github.com/shopmonkeyus/eds-server/internal/drivers/kafka"
	_ "github.com/shopmonkeyus/eds-server/internal/drivers/mysql"
	_ "github.com/shopmonkeyus/eds-server/internal/drivers/postgresql"
	_ "github.com/shopmonkeyus/eds-server/internal/drivers/s3"
	_ "github.com/shopmonkeyus/eds-server/internal/drivers/snowflake"
	"github.com/shopmonkeyus/eds-server/internal/util"
)

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
	f *os.File
}

func (s *logFileSink) Write(buf []byte) (int, error) {
	return s.f.Write(buf)
}

func (s *logFileSink) Close() error {
	return s.f.Close()
}

func newLogFileSync(file string) (*logFileSink, error) {
	of, err := os.Create(file)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}
	return &logFileSink{f: of}, nil
}

type CloseFunc func()

func newLogger(cmd *cobra.Command) (logger.Logger, CloseFunc) {
	ts, _ := cmd.Flags().GetBool("timestamp")
	if !ts {
		glog.SetFlags(0)
	}
	glog.SetOutput(os.Stdout)
	sink, _ := cmd.Flags().GetString("log-file-sink")
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
	if sink != "" {
		log.Trace("using log file sink: %s", sink)
		logSync, err := newLogFileSync(sink)
		if err != nil {
			log.Error("failed to open log file: %s. %s", sink, err)
			os.Exit(3)
		}
		return logger.NewMultiLogger(log, logger.NewJSONLoggerWithSink(logSync, logger.LevelTrace)), func() {
			logSync.Close()
		}
	}
	return log, func() {}
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

func getDataDir(cmd *cobra.Command, logger logger.Logger) string {
	dataDir := mustFlagString(cmd, "data-dir", true)
	dataDir, _ = filepath.Abs(filepath.Clean(dataDir))

	if !util.Exists(dataDir) {
		logger.Fatal("data directory %s does not exist. please create the directory and retry again.", dataDir)
	}
	if ok, err := util.IsDirWritable(dataDir); !ok {
		logger.Fatal("%s", err)
	}

	logger.Debug("using data directory: %s", dataDir)
	return dataDir
}

func getSchemaAndTableFiles(datadir string) (string, string) {
	// assume these are default in the same directory as the data-dir
	schemaFile := filepath.Join(datadir, "schema.json")
	tablesFile := filepath.Join(datadir, "tables.json")
	return schemaFile, tablesFile
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:  "eds-server",
	Long: "Shopmonkey Enterprise Data Streaming server (EDS) \nFor detailed information, see: https://shopmonkey.dev/eds \nand https://github.com/shopmonkeyus/eds-server",
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().Bool("verbose", false, "turn on verbose logging")
	rootCmd.PersistentFlags().Bool("silent", false, "turn off all logging")
	rootCmd.PersistentFlags().Bool("timestamp", false, "turn on timestamps in logs")
	rootCmd.PersistentFlags().String("log-file-sink", "", "the log file sink to use")
	rootCmd.PersistentFlags().MarkHidden("log-file-sink")
}
