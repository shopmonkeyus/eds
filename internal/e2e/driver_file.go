//go:build e2e
// +build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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

func (d *driverFileTest) Test(logger logger.Logger, dir string, nc *nats.Conn, js jetstream.JetStream, url string) error {
	return runTest(logger, nc, js, func(event internal.DBChangeEvent) internal.DBChangeEvent {
		fn := filepath.Join(dir, "export", event.Table, fmt.Sprintf("%d-%s.json", time.UnixMilli(event.Timestamp).Unix(), event.Key[0]))
		buf, err := os.ReadFile(fn)
		if err != nil {
			panic(err)
		}
		var event2 internal.DBChangeEvent
		if err := json.Unmarshal(buf, &event2); err != nil {
			panic(err)
		}
		return event2
	})
}

func init() {
	registerTest(&driverFileTest{})
}
