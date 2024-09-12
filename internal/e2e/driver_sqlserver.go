//go:build e2e
// +build e2e

package e2e

import (
	"fmt"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/drivers/sqlserver"
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

func (d *driverSQLServerTest) QuoteTable(table string) string {
	return fmt.Sprintf(`[%s]`, table)
}

func (d *driverSQLServerTest) QuoteColumn(column string) string {
	return fmt.Sprintf(`[%s]`, column)
}

func (d *driverSQLServerTest) QuoteValue(value string) string {
	return fmt.Sprintf(`%s`, value)
}

func (d *driverSQLServerTest) TestInsert(logger logger.Logger, dir string, url string, event internal.DBChangeEvent) error {
	dsn, err := sqlserver.ParseURLToDSN(url)
	if err != nil {
		return fmt.Errorf("error parsing url: %w", err)
	}
	return validateSQLEvent(logger, event, "sqlserver", dsn, d)
}

func init() {
	registerTest(&driverSQLServerTest{})
}
