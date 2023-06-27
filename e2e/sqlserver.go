//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/types"
	"github.com/shopmonkeyus/go-common/logger"
	v3 "github.com/shopmonkeyus/go-datamodel/v3"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
)

type sqlserverProviderRunner struct {
	logger            logger.Logger
	dockerContainerID string
}

var _ TestProviderRunner = (*sqlserverProviderRunner)(nil)

func NewSQLServerTestProviderRunner(logger logger.Logger) TestProviderRunner {
	return &sqlserverProviderRunner{logger, ""}
}

func (r *sqlserverProviderRunner) Start() error {
	id, err := runDockerContainer("mcr.microsoft.com/azure-sql-edge", []string{}, []int{11433, 1433}, "MSSQL_SA_PASSWORD=3dg3Y0urB3ts", "ACCEPT_EULA=1", " MSSQL_PID=Developer")
	if err != nil {
		return err
	}
	r.dockerContainerID = id
	started := time.Now()
	for {
		var stdout bytes.Buffer
		cmd := exec.Command("docker", "logs", id)
		cmd.Stdout = &stdout
		if err := cmd.Run(); err != nil {
			if time.Since(started) > time.Second*10 {
				return fmt.Errorf("timed out waiting for sqlserver to be ready")
			}
			time.Sleep(time.Second)
			continue
		}
		if strings.Contains(stdout.String(), "Launchpad is connecting to mssql") {
			r.logger.Info("sqlserver is ready")
			break
		}
		time.Sleep(time.Second)
	}
	return nil
}

func (r *sqlserverProviderRunner) Stop() error {
	if r.dockerContainerID != "" {
		err := runDockerDestroy(r.dockerContainerID)
		r.dockerContainerID = ""
		return err
	}
	return nil
}

func (r *sqlserverProviderRunner) URL() string {
	return "sqlserver://sa:3dg3Y0urB3ts@localhost:11433?database=master"
}

// Validate that the object was committed to storage
func (r *sqlserverProviderRunner) Validate(object types.ChangeEventPayload) error {
	glogger := internal.NewGormLogAdapter(r.logger)
	db, err := gorm.Open(sqlserver.Open(r.URL()), &gorm.Config{Logger: glogger})
	if err != nil {
		return fmt.Errorf("error connecting to sql server: %s", err)
	}
	var result any
	for i, k := range v3.TableNames {
		if k == object.GetTable() {
			result = v3.ModelInstances[i]
			break
		}
	}
	if err := db.Raw(fmt.Sprintf(`SELECT * FROM "%s" WHERE "id" = '%s'`, object.GetTable(), object.GetKey()[0])).Scan(&result).Error; err != nil {
		return fmt.Errorf("error querying table: %s", err)
	}
	sqlraw, _ := json.Marshal(result)
	buf1, _ := json.Marshal(object.GetAfter())
	if err := compareJSON(sqlraw, buf1); err != nil {
		return err
	}
	return nil
}
