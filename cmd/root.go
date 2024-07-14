package cmd

import (
	"fmt"
	glog "log"
	"os"

	"github.com/shopmonkeyus/go-common/logger"
	"github.com/spf13/cobra"

	_ "github.com/shopmonkeyus/eds-server/internal/processors/kafka"
	_ "github.com/shopmonkeyus/eds-server/internal/processors/mysql"
	_ "github.com/shopmonkeyus/eds-server/internal/processors/postgresql"
	_ "github.com/shopmonkeyus/eds-server/internal/processors/s3"
	_ "github.com/shopmonkeyus/eds-server/internal/processors/snowflake"
)

func mustFlagString(cmd *cobra.Command, name string, required bool) string {
	val, err := cmd.Flags().GetString(name)
	if err != nil {
		fmt.Printf("error: %s\n", err)
		os.Exit(2)
	}
	if required && val == "" {
		fmt.Printf("error: required flag --%s missing\n", name)
		os.Exit(2)
	}
	return val
}

func mustFlagInt(cmd *cobra.Command, name string, required bool) int {
	val, err := cmd.Flags().GetInt(name)
	if err != nil {
		fmt.Printf("error: %s\n", err)
		os.Exit(2)
	}
	if required && val <= 0 {
		fmt.Printf("error: required flag --%s missing\n", name)
		os.Exit(2)
	}
	return val
}

type logFileSink struct {
	f *os.File
}

func (s *logFileSink) Write(buf []byte) error {
	_, err := s.f.Write(buf)
	return err
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
	glog.SetFlags(0)
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
			os.Exit(2)
		}
		return log.WithSink(logSync, logger.LevelTrace), func() { logSync.Close() }
	}
	return log, func() {}
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:  "eds-server",
	Long: "Shopmonkey Enterprise Data Streaming server \nFor detailed information, see: https://shopmonkey.dev/eds \nand https://github.com/shopmonkeyus/eds-server/blob/main/README.md ",
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
	rootCmd.PersistentFlags().String("log-file-sink", "", "the log file sink to use")
	rootCmd.PersistentFlags().MarkHidden("log-file-sink")
}
