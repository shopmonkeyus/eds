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

type driverPostgresTest struct {
}

var _ e2eTest = (*driverPostgresTest)(nil)

func (d *driverPostgresTest) Name() string {
	return "postgres"
}

func (d *driverPostgresTest) URL(dir string) string {
	return fmt.Sprintf("postgres://%s:%s@127.0.0.1:15432/%s?sslmode=disable", dbuser, dbpass, dbname)
}

func (d *driverPostgresTest) Test(logger logger.Logger, dir string, nc *nats.Conn, js jetstream.JetStream, url string) error {
	return runTest(logger, nc, js, func(event internal.DBChangeEvent) internal.DBChangeEvent {
		return validateSQLEvent(logger, event, "postgres", url, func(table string) string {
			return fmt.Sprintf(`"%s"`, table)
		}, "$1")
	})
}

func init() {
	registerTest(&driverPostgresTest{})
}
