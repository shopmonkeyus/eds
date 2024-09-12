//go:build e2e
// +build e2e

package e2e

import (
	"fmt"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/drivers/mysql"
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

func (d *driverMySQLTest) QuoteTable(table string) string {
	return fmt.Sprintf("`%s`", table)
}

func (d *driverMySQLTest) QuoteColumn(column string) string {
	return fmt.Sprintf(`%s`, column)
}

func (d *driverMySQLTest) QuoteValue(value string) string {
	return fmt.Sprintf(`'%s'`, value)
}

func (d *driverMySQLTest) TestInsert(logger logger.Logger, dir string, url string, event internal.DBChangeEvent) error {
	dsn, err := mysql.ParseURLToDSN(url)
	if err != nil {
		return fmt.Errorf("error parsing url: %w", err)
	}
	return validateSQLEvent(logger, event, "mysql", dsn, d)
}

func init() {
	registerTest(&driverMySQLTest{})
}
