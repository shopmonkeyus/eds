//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"time"

	"github.com/shopmonkeyus/eds-server/internal/types"
	"github.com/shopmonkeyus/go-common/logger"
)

type postgresProviderRunner struct {
	logger            logger.Logger
	dockerContainerID string
}

var _ TestProviderRunner = (*postgresProviderRunner)(nil)

func NewPostgresTestProviderRunner(logger logger.Logger) TestProviderRunner {
	return &postgresProviderRunner{logger, ""}
}

func (r *postgresProviderRunner) Start() error {
	id, err := runDockerContainer("postgres", []string{}, []int{15432, 5432}, "POSTGRES_PASSWORD=password", "POSTGRES_DB=test", "POSTGRES_HOST_AUTH_METHOD=trust")
	if err != nil {
		return err
	}
	r.dockerContainerID = id
	started := time.Now()
	for {
		cmd := exec.Command("docker", "exec", id, "psql", "-U", "postgres", "-c", "GRANT ALL PRIVILEGES ON DATABASE test to postgres;")
		if err := cmd.Run(); err != nil {
			if time.Since(started) > time.Second*10 {
				return fmt.Errorf("timed out waiting for postgres to be ready")
			}
			time.Sleep(time.Second)
			continue
		}
		r.logger.Trace("granted database permission to test db")
		break
	}
	return nil
}

func (r *postgresProviderRunner) Stop() error {
	if r.dockerContainerID != "" {
		err := runDockerDestroy(r.dockerContainerID)
		r.dockerContainerID = ""
		return err
	}
	return nil
}

func (r *postgresProviderRunner) URL() string {
	return "postgresql://postgres@127.0.0.1:15432/test?sslmode=disable"
}

var fixupDates = regexp.MustCompile(`"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(\.\d{2,}\+\d{2,}:\d{2,})"`)

func (r *postgresProviderRunner) fixSQLDates(sqlresult string) string {
	return fixupDates.ReplaceAllString(sqlresult, `"${1}Z"`)

}

// Validate that the object was committed to storage
func (r *postgresProviderRunner) Validate(object types.ChangeEventPayload) error {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := exec.Command("docker", "exec", r.dockerContainerID, "psql", "-d", "test", "-U", "postgres", "-t", "-c", fmt.Sprintf(`SELECT row_to_json(t) FROM (SELECT * FROM "%s" WHERE id = '%s') t`, object.GetTable(), object.GetKey()[0]))
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		r.logger.Error("query returned: %s", stderr.String())
		return fmt.Errorf("error validating %s with key: %s", object.GetTable(), object.GetKey()[0])
	}
	sqlresult := strings.TrimSpace(r.fixSQLDates(stdout.String()))
	r.logger.Trace("query returned: %s", sqlresult)
	var sqlraw json.RawMessage
	json.Unmarshal([]byte(sqlresult), &sqlraw)
	buf1, _ := json.Marshal(object.GetAfter())
	if err := compareJSON(sqlraw, buf1); err != nil {
		return err
	}
	return nil
}
