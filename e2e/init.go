//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"time"

	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/go-datamodel/datatypes"
)

type TestProviderRunner interface {
	// Start the provider
	Start() error
	// Stop the provider
	Stop() error
	// URL will return the url for testing against this runner
	URL() string
	// Validate that the object was committed to storage
	Validate(datatypes.ChangeEventPayload) error
}

type DockerState struct {
	Status  string `json:"status"`
	Running bool   `json:"running"`
}

type DockerStatus struct {
	State DockerState
}

// runDockerDestroy will stop and remove the docker container specified by id
func runDockerDestroy(id string) error {
	cmd := exec.Command("docker", "rm", "-f", id)
	if err := cmd.Run(); err != nil {
		return err
	}
	return nil
}

// runDockerContainer will run a docker container in the background listen on ports and set optional
// environment variables and return the docker container id if successful
func runDockerContainer(name string, ports []int, env ...string) (string, error) {
	args := []string{
		"run", "-it", "-d", "-p", fmt.Sprintf("%d:%d", ports[0], ports[1]),
	}
	for _, e := range env {
		args = append(args, "-e", e)
	}
	args = append(args, name)
	var out bytes.Buffer
	cmd := exec.Command("docker", args...)
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("error starting docker container: %s", err)
	}
	id := strings.TrimSpace(out.String())
	out.Reset()
	for {
		cmd2 := exec.Command("docker", "inspect", id)
		cmd2.Stdout = &out
		if err := cmd2.Run(); err != nil {
			runDockerDestroy(id)
			return "", fmt.Errorf("error getting docker container status: %s", err)
		}
		status := make([]DockerStatus, 0)
		if err := json.Unmarshal(out.Bytes(), &status); err != nil {
			runDockerDestroy(id)
			return "", fmt.Errorf("error parsing docker container status: %s", err)
		}
		if len(status) > 0 && status[0].State.Running {
			break
		}
		out.Reset()
		time.Sleep(2 * time.Second)
	}
	return id, nil
}

func NewTestProviderRunner(logger internal.Logger, driver string) (TestProviderRunner, error) {
	return NewPostgresTestProviderRunner(logger), nil
}

func compareJSON(a []byte, b []byte) error {
	var aj map[string]interface{}
	var bj map[string]interface{}
	json.Unmarshal(a, &aj)
	json.Unmarshal(b, &bj)
	akeys := make([]string, 0)
	bkeys := make([]string, 0)
	for k := range aj {
		akeys = append(akeys, k)
	}
	for k := range bj {
		bkeys = append(bkeys, k)
	}
	sort.Strings(akeys)
	sort.Strings(bkeys)
	for _, k := range akeys {
		aval, _ := json.Marshal(aj[k])
		bval, _ := json.Marshal(bj[k])
		if string(aval) != string(bval) {
			return fmt.Errorf("the key: %s was not equal. expected %s = %s. object a=%s, b=%s", k, string(aval), string(bval), string(a), string(b))
		}
	}
	return nil
}
