//go:build e2e
// +build e2e

package e2e

import (
	"fmt"
	"os"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/drivers/snowflake"
	"github.com/shopmonkeyus/go-common/logger"
)

var (
	snowflakeUser     = "EDS_E2E_TEST"
	snowflakePassword = os.Getenv("SM_SNOWFLAKE_PASSWORD")
	snowflakeDB       = "EDS_E2E"
	snowflakeHost     = "TFLXCJY-LU41015"
)

type driverSnowflakeTest struct {
}

var _ e2eTest = (*driverSnowflakeTest)(nil)
var _ e2eTestDisabled = (*driverSnowflakeTest)(nil)

func (d *driverSnowflakeTest) Disabled() bool {
	return snowflakePassword == ""
}

func (d *driverSnowflakeTest) Name() string {
	return "snowflake"
}

func (d *driverSnowflakeTest) URL(dir string) string {
	return fmt.Sprintf("snowflake://%s:%s@%s/%s/PUBLIC", snowflakeUser, snowflakePassword, snowflakeHost, snowflakeDB)
}

func (d *driverSnowflakeTest) QuoteTable(table string) string {
	return fmt.Sprintf(`"%s"`, table)
}

func (d *driverSnowflakeTest) QuoteColumn(column string) string {
	return fmt.Sprintf(`"%s"`, column)
}

func (d *driverSnowflakeTest) QuoteValue(value string) string {
	return fmt.Sprintf(`'%s'`, value)
}

func (d *driverSnowflakeTest) Validate(logger logger.Logger, dir string, url string, event internal.DBChangeEvent) error {
	connStr, err := snowflake.GetConnectionStringFromURL(url)
	if err != nil {
		return fmt.Errorf("error getting connection string: %w", err)
	}
	return validateSQLEvent(logger, event, "snowflake", connStr, d)
}

func init() {
	registerTest(&driverSnowflakeTest{})
}
