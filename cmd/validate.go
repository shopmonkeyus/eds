package cmd

import (
	"io"
	"os"
	"path"
	"regexp"
	"time"

	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/go-datamodel/datatypes"
	v3 "github.com/shopmonkeyus/go-datamodel/v3"
	"github.com/spf13/cobra"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var filePattern = regexp.MustCompile(`^(\w+)_\d+_\w+\.json(\.gz)?$`)

func processFile(logger internal.Logger, name string, db *gorm.DB) {
	fn := path.Base(name)
	if !filePattern.MatchString(fn) {
		return
	}
	tok := filePattern.FindStringSubmatch(fn)
	model := tok[1]
	logger.Info("file: %s, model: %s", name, model)
	f, err := os.Open(name)
	if err != nil {
		logger.Error("error opening file: %s. %s", name, err)
		os.Exit(1)
	}
	buf, err := io.ReadAll(f)
	f.Close()
	if err != nil {
		logger.Error("error reading file: %s. %s", name, err)
		os.Exit(1)
	}
	object, err := v3.NewFromChangeEvent(model, buf, path.Ext(fn) == ".gz")
	if err != nil {
		logger.Error("error deserializing file: %s. %s", name, err)
		os.Exit(1)
	}
	intf := object.(datatypes.ChangeEventHelper)
	logger.Info("loaded: %s with object: %v", fn, object)
	if db != nil {
		started := time.Now()
		if err := db.Clauses(clause.OnConflict{
			UpdateAll: true,
		}).Create(intf.GetAfter()).Error; err != nil {
			logger.Error("file %s upsert error for change event: %s. %s", name, object, err)
			os.Exit(1)
		} else {
			logger.Trace("upserted db record for msgid: %s, took %v", model, time.Since(started))
		}
	}
}

var validateCmd = &cobra.Command{
	Use:   "validate [file_or_dir]",
	Short: "validate dump files and optionally load them into the db",
	Long: `Validate dump files and optionally load them into the db
	
You can validate files from a dump directory:
	
	go run . validate ./dump

You can validate a specific file by path:

	go run . validate ./dump/customer_location_connection_1672613089535087000_9c53baa609e55158.json.gz

You can validate and also load into the db:

	go run . validate --driver postgres --dsn "host=localhost user=root dbname=test port=26257 sslmode=disable TimeZone=US/Central" ./dump/customer_location_connection_1672613089535087000_9c53baa609e55158.json.gz
`,
	Run: func(cmd *cobra.Command, args []string) {
		logger := newLogger(cmd)
		dir := args[0]
		glogger := internal.NewGormLogAdapter(logger)
		driver := mustFlagString(cmd, "driver", false)
		var db *gorm.DB
		if driver != "" {
			db = loadDatabase(cmd, &gorm.Config{Logger: glogger, DryRun: mustFlagBool(cmd, "dry-run", false)})
		}
		ls, _ := os.Lstat(dir)
		if ls.IsDir() {
			files, err := os.ReadDir(dir)
			if err != nil {
				logger.Error("error reading files from: %s. %s", dir, err)
				os.Exit(1)
			}
			for _, file := range files {
				name := path.Join(dir, file.Name())
				processFile(logger, name, db)
			}
		} else {
			processFile(logger, dir, db)
		}
	},
}

func init() {
	rootCmd.AddCommand(validateCmd)
	validateCmd.Flags().Bool("dry-run", false, "only simulate loading but don't actually make db changes")
}
