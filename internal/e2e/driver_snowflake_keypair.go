//go:build e2e
// +build e2e

package e2e

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/shopmonkeyus/eds/internal"
	"github.com/shopmonkeyus/eds/internal/drivers/snowflake"
	"github.com/shopmonkeyus/go-common/logger"
)

var snowflakeUserKeypair = "EDS_E2E_TEST_KEYPAIR"
var snowflakeKeypairSecretKey = os.Getenv("SNOWFLAKE_SECRET_ACCESS_KEY")

type driverSnowflakeKeypairTest struct {
	driverSnowflakeTest
	db *sql.DB
}

var _ e2eTest = (*driverSnowflakeKeypairTest)(nil)
var _ e2eTestDisabled = (*driverSnowflakeKeypairTest)(nil)
var _ io.Closer = (*driverSnowflakeKeypairTest)(nil)

func (d *driverSnowflakeKeypairTest) Close() error {
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

func (d *driverSnowflakeKeypairTest) Disabled() bool {
	return snowflakeKeypairSecretKey == ""
}

func (d *driverSnowflakeKeypairTest) Name() string {
	return "snowflake-keypair"
}

func (d *driverSnowflakeKeypairTest) URL(dir string) string {
	return fmt.Sprintf("snowflake-keypair://%s@%s/%s/PUBLIC?secret-key=SNOWFLAKE_SECRET_ACCESS_KEY",
		snowflakeUserKeypair, snowflakeHost, snowflakeDB)
}

func (d *driverSnowflakeKeypairTest) Validate(logger logger.Logger, dir string, url string, event internal.DBChangeEvent) error {
	if d.db == nil {
		db, err := snowflake.OpenSnowflakeWithKeyPair(snowflakeKeypairSecretKey, snowflakeUserKeypair, snowflakeHost, snowflakeDB, snowflakeSchema)
		if err != nil {
			return fmt.Errorf("error connecting with keypair: %w", err)
		}
		d.db = db
	}
	time.Sleep(3 * time.Second) // snowflake needs a long time for writes to complete

	return validateSQLEventWithDB(logger, event, d.db, d)
}

func init() {
	registerTest(&driverSnowflakeKeypairTest{})
}
