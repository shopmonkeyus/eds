package provider

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"

	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/types"
	"github.com/shopmonkeyus/go-common/logger"
)

type FileProvider struct {
	logger logger.Logger
	cmd    []string
	stdin  io.WriteCloser
	stdout io.ReadCloser
	opts   *ProviderOpts
}

var _ internal.Provider = (*FileProvider)(nil)

func ReadOutput(output chan string, rc io.ReadCloser) {
	r := bufio.NewReader(rc)
	for {
		x, _ := r.ReadString('\n')
		output <- string(x)
	}
}

// NewFileProvider returns a provider that will stream files to a folder provided in the url
func NewFileProvider(logger logger.Logger, cmd []string, opts *ProviderOpts) (internal.Provider, error) {
	logger.Info("file provider will execute program: %s", cmd[0])
	theCmd := exec.Command(cmd[0], cmd[1:]...)
	stdout, err := theCmd.StdoutPipe()
	if err != nil {
		logger.Error("error occured %v", err)
	}

	stdin, err := theCmd.StdinPipe()
	if err != nil {
		logger.Error("error occured %v", err)
	}
	err = theCmd.Start()
	if err != nil {
		logger.Error("error occured %v", err)
	}

	output := make(chan string)
	go func(out chan string) {
		for o := range out {
			logger.Info("got some output: %s", o)
		}
	}(output)

	go ReadOutput(output, stdout)

	return &FileProvider{
		logger,
		cmd,
		stdin,
		stdout,
		opts,
	}, nil
}

// Start the provider and return an error or nil if ok
func (p *FileProvider) Start() error {
	return nil
}

// Stop the provider and return an error or nil if ok
func (p *FileProvider) Stop() error {
	return nil
}

// Process data received and return an error or nil if processed ok
func (p *FileProvider) Process(data types.ChangeEventPayload, schema types.Table) error {
	p.logger.Info("[file] starting processing")
	transport := types.Transport{
		DBChange: data,
		Schema:   schema,
	}
	buf, err := json.Marshal(transport)

	if err != nil {
		return fmt.Errorf("error converting to json: %s", err)
	}
	_, err = io.WriteString(p.stdin, fmt.Sprintf("%s/n", (buf)))
	if err != nil {
		return fmt.Errorf("error writing to file: %s", err)
	}

	// p.logger.Info("writing buf to file %s", string(buf))
	// p.logger.Info("writing schema to file %s", string(schemaBuf))

	// w.Flush()
	// p.logger.Trace("processed: %s", fn)
	return nil
}
