//go:build e2e
// +build e2e

package e2e

import (
	"fmt"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/drivers/postgresql"
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

func (d *driverPostgresTest) QuoteTable(table string) string {
	return fmt.Sprintf(`"%s"`, table)
}

func (d *driverPostgresTest) QuoteColumn(column string) string {
	return fmt.Sprintf(`%s`, column)
}

func (d *driverPostgresTest) QuoteValue(value string) string {
	return fmt.Sprintf(`'%s'`, value)
}

func (d *driverPostgresTest) TestInsert(logger logger.Logger, dir string, url string, event internal.DBChangeEvent) error {
	connStr, err := postgresql.GetConnectionStringFromURL(url)
	if err != nil {
		return fmt.Errorf("error parsing url: %w", err)
	}
	return validateSQLEvent(logger, event, "postgres", connStr, d)
}

func init() {
	registerTest(&driverPostgresTest{})
}
