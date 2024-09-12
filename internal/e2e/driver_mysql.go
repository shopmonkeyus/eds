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

type driverMySQLTest struct {
}

var _ e2eTest = (*driverMySQLTest)(nil)

func (d *driverMySQLTest) Name() string {
	return "mysql"
}

func (d *driverMySQLTest) URL(dir string) string {
	return fmt.Sprintf("mysql://%s:%s@127.0.0.1:13306/%s", dbuser, dbpass, dbname)
}

func (d *driverMySQLTest) Test(logger logger.Logger, dir string, nc *nats.Conn, js jetstream.JetStream, url string) error {
	return runTest(logger, nc, js, func(event internal.DBChangeEvent) internal.DBChangeEvent {
		return validateSQLEvent(logger, event, "mysql", fmt.Sprintf("%s:%s@tcp(127.0.0.1:13306)/%s", dbuser, dbpass, dbname), func(table string) string {
			return fmt.Sprintf("`%s`", table)
		})
	})
}

func init() {
	registerTest(&driverMySQLTest{})
}
