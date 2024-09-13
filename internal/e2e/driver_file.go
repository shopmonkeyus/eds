//go:build e2e
// +build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/util"
	"github.com/shopmonkeyus/go-common/logger"
)

type driverFileTest struct {
}

var _ e2eTest = (*driverFileTest)(nil)

func (d *driverFileTest) Name() string {
	return "file"
}

func (d *driverFileTest) URL(dir string) string {
	return util.ToFileURI(dir, "export")
}

func (d *driverFileTest) Validate(logger logger.Logger, dir string, url string, event internal.DBChangeEvent) error {
	fn := filepath.Join(dir, "export", event.Table, fmt.Sprintf("%d-%s.json", time.UnixMilli(event.Timestamp).Unix(), event.GetPrimaryKey()))
	buf, err := os.ReadFile(fn)
	if err != nil {
		return fmt.Errorf("error reading file %s: %w", fn, err)
	}
	var event2 internal.DBChangeEvent
	if err := json.Unmarshal(buf, &event2); err != nil {
		return fmt.Errorf("error unmarshalling event: %w", err)
	}
	return dbchangeEventMatches(event, event2)
}

func init() {
	registerTest(&driverFileTest{})
}
