//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/shopmonkeyus/eds-server/internal/types"
	"github.com/shopmonkeyus/go-common/logger"
)

type cockroachProviderRunner struct {
	logger            logger.Logger
	dockerContainerID string
}

var _ TestProviderRunner = (*cockroachProviderRunner)(nil)

func NewCockroachTestProviderRunner(logger logger.Logger) TestProviderRunner {
	return &cockroachProviderRunner{logger, ""}
}

func (r *cockroachProviderRunner) Start() error {
	id, err := runDockerContainer("cockroachdb/cockroach", []string{"start-single-node", "--insecure", "--store=crdb-single"}, []int{46257, 26257})
	if err != nil {
		return err
	}
	r.dockerContainerID = id
	started := time.Now()
	for {
		cmd := exec.Command("docker", "exec", id, "cockroach", "sql", "--insecure", "-e", "SELECT 1")
		if err := cmd.Run(); err != nil {
			if time.Since(started) > time.Second*10 {
				return fmt.Errorf("timed out waiting for cockroach to be ready")
			}
			time.Sleep(time.Second)
			continue
		}
		cmd = exec.Command("docker", "exec", id, "cockroach", "sql", "--insecure", "-e", "CREATE DATABASE test")
		if err := cmd.Run(); err != nil {
			return fmt.Errorf("error creating cockroach test db: %s", err)
		}
		r.logger.Trace("cockroach is ready")
		break
	}
	return nil
}

func (r *cockroachProviderRunner) Stop() error {
	if r.dockerContainerID != "" {
		err := runDockerDestroy(r.dockerContainerID)
		r.dockerContainerID = ""
		return err
	}
	return nil
}

func (r *cockroachProviderRunner) URL() string {
	return "postgresql://root@127.0.0.1:46257/test?sslmode=disable"
}

// Validate that the object was committed to storage
func (r *cockroachProviderRunner) Validate(object types.ChangeEventPayload) error {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := exec.Command("docker", "exec", r.dockerContainerID, "cockroach", "sql", "--insecure", "-d", "test", "--format", "records", "-e", fmt.Sprintf(`SELECT row_to_json(t) FROM (SELECT * FROM "%s" WHERE id = '%s') t`, object.GetTable(), object.GetKey()[0]))
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		r.logger.Error("query returned: %s", stderr.String())
		return fmt.Errorf("error validating %s with key: %s", object.GetTable(), object.GetKey()[0])
	}
	sqlresult := strings.TrimSpace(stdout.String())
	index := strings.Index(sqlresult, "row_to_json |")
	if index < 0 {
		return fmt.Errorf("couldn't find the result row from validation query")
	}
	sqlresult = strings.TrimSpace(sqlresult[15:])
	r.logger.Trace("query returned: %s", sqlresult)
	var sqlraw json.RawMessage
	json.Unmarshal([]byte(sqlresult), &sqlraw)
	buf1, _ := json.Marshal(object.GetAfter())
	if err := compareJSON(sqlraw, buf1); err != nil {
		return err
	}
	return nil
}
