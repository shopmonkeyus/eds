package file

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

type fileDriver struct {
	config internal.DriverConfig
	logger logger.Logger
	dir    string
}

var _ internal.Driver = (*fileDriver)(nil)
var _ internal.DriverLifecycle = (*fileDriver)(nil)
var _ internal.DriverHelp = (*fileDriver)(nil)
var _ internal.Importer = (*fileDriver)(nil)

func (p *fileDriver) GetPathFromURL(urlString string) (string, error) {
	u, err := url.Parse(urlString)
	if err != nil {
		return "", fmt.Errorf("unable to parse url: %w", err)
	}

	if u.Path == "" {
		return "", fmt.Errorf("path is required in url which should be the directory to store files")
	} else {
		if u.Path[0:1] == "/" {
			p.dir = u.Path
		} else {
			p.dir, err = filepath.Abs(p.dir)
			if err != nil {
				return "", fmt.Errorf("unable to get absolute path for %s: %w", p.dir, err)
			}
		}
		if !util.Exists(p.dir) {
			if err := os.MkdirAll(p.dir, 0755); err != nil {
				return "", fmt.Errorf("unable to create directory: %w", err)
			}
		}
	}
	return p.dir, nil
}

// Start the driver. This is called once at the beginning of the driver's lifecycle.
func (p *fileDriver) Start(pc internal.DriverConfig) error {
	p.config = pc
	p.logger = pc.Logger.WithPrefix("[file]")
	if _, err := p.GetPathFromURL(pc.URL); err != nil {
		return err
	}
	return nil
}

// Stop the driver. This is called once at the end of the driver's lifecycle.
func (p *fileDriver) Stop() error {
	return nil
}

// MaxBatchSize returns the maximum number of events that can be processed in a single call to Process and when Flush should be called.
// Return -1 to indicate that there is no limit.
func (p *fileDriver) MaxBatchSize() int {
	return -1
}

func (p *fileDriver) getFileName(table string, id string) string {
	return fmt.Sprintf("%s/%d-%s.json", table, time.Now().Unix(), id)
}

func (p *fileDriver) writeEvent(logger logger.Logger, event internal.DBChangeEvent, dryRun bool) error {
	key := p.getFileName(event.Table, event.ID)
	buf := []byte(util.JSONStringify(event))
	fp := filepath.Join(p.dir, key)
	if !dryRun {
		dir := filepath.Dir(fp)
		if !util.Exists(dir) {
			if err := os.MkdirAll(dir, 0755); err != nil {
				return fmt.Errorf("unable to create directory: %w", err)
			}
		}
		if err := os.WriteFile(fp, buf, 0644); err != nil {
			return fmt.Errorf("unable to write file: %w", err)
		}
		logger.Trace("stored %s", fp)
	} else {
		logger.Trace("would have stored %s", fp)
	}
	return nil
}

// Process a single event. It returns a bool indicating whether Flush should be called. If an error is returned, the driver will NAK the event.
func (p *fileDriver) Process(logger logger.Logger, event internal.DBChangeEvent) (bool, error) {
	if err := p.writeEvent(logger, event, false); err != nil {
		return false, err
	}
	return false, nil
}

// Flush is called to commit any pending events. It should return an error if the flush fails. If the flush fails, the driver will NAK all pending events.
func (p *fileDriver) Flush() error {
	return nil
}

// Name is a unique name for the driver.
func (p *fileDriver) Name() string {
	return "File"
}

// Description is the description of the driver.
func (p *fileDriver) Description() string {
	return "Supports streaming EDS messages to local filesystem directory."
}

// ExampleURL should return an example URL for configuring the driver.
func (p *fileDriver) ExampleURL() string {
	return "file://folder"
}

// Help should return a detailed help documentation for the driver.
func (p *fileDriver) Help() string {
	var help strings.Builder
	help.WriteString("Provide a directory in the URL path to store events into this folder.\n")
	return help.String()
}

func (p *fileDriver) Import(config internal.ImporterConfig) error {
	logger := config.Logger.WithPrefix("[file]")
	if _, err := p.GetPathFromURL(config.URL); err != nil {
		return err
	}
	files, err := util.ListDir(config.DataDir)
	if err != nil {
		return fmt.Errorf("unable to list files in directory: %w", err)
	}
	schema, err := config.SchemaRegistry.GetLatestSchema()
	if err != nil {
		return fmt.Errorf("unable to get schema: %w", err)
	}
	started := time.Now()
	var total int
	for _, file := range files {
		table, _, ok := util.ParseCRDBExportFile(file)
		if !ok {
			logger.Debug("skipping file: %s", file)
			continue
		}
		if !util.SliceContains(config.Tables, table) {
			continue
		}
		data := schema[table]
		if data == nil {
			return fmt.Errorf("unexpected table (%s) not found in schema but in import directory: %s", table, file)
		}
		logger.Debug("processing file: %s, table: %s", file, table)
		dec, err := util.NewNDJSONDecoder(file)
		if err != nil {
			return fmt.Errorf("unable to create JSON decoder for %s: %w", file, err)
		}
		defer dec.Close()
		var count int
		tstarted := time.Now()
		for dec.More() {
			var event internal.DBChangeEvent
			if err := dec.Decode(&event); err != nil {
				return fmt.Errorf("unable to decode JSON: %w", err)
			}
			count++
			if err := p.writeEvent(logger, event, config.DryRun); err != nil {
				return err
			}
		}
		if err := dec.Close(); err != nil {
			return err
		}
		total += count
		logger.Debug("imported %d %s records in %s", count, table, time.Since(tstarted))
	}

	logger.Info("imported %d records from %d files in %s", total, len(files), time.Since(started))
	return nil
}

func init() {
	internal.RegisterDriver("file", &fileDriver{})
	internal.RegisterImporter("file", &fileDriver{})
}
