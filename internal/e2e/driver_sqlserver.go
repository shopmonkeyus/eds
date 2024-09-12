//go:build e2e
// +build e2e

package e2e

import (
	"fmt"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/go-common/logger"
)

type driverSQLServerTest struct {
}

var _ e2eTest = (*driverSQLServerTest)(nil)

func (d *driverSQLServerTest) Name() string {
	return "sqlserver"
}

func (d *driverSQLServerTest) URL(dir string) string {
	return fmt.Sprintf("sqlserver://sa:%s@127.0.0.1:1433/master", dbpass)
}

func (d *driverSQLServerTest) Test(logger logger.Logger, dir string, nc *nats.Conn, js jetstream.JetStream, url string) error {
	return runTest(logger, nc, js, func(event internal.DBChangeEvent) internal.DBChangeEvent {
		return validateSQLEvent(logger, event, "sqlserver", fmt.Sprintf("sqlserver://sa:%s@127.0.0.1:1433?&database=master&encrypt=disable", dbpass), func(table string) string {
			return fmt.Sprintf("[%s]", table)
		})
	})
}

func init() {
	registerTest(&driverSQLServerTest{})
}
