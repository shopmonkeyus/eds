package provider

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"

	"github.com/shirou/gopsutil/v3/process"
	"github.com/shopmonkeyus/eds-server/internal"
	dm "github.com/shopmonkeyus/eds-server/internal/model"
	"github.com/shopmonkeyus/eds-server/internal/types"
	"github.com/shopmonkeyus/go-common/logger"
)

var EOL = []byte("\n")

type FileProvider struct {
	logger logger.Logger
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	once   sync.Once
}

var _ internal.Provider = (*FileProvider)(nil)

// NewFileProvider returns a provider that will stream files to a folder provided in the url
func NewFileProvider(plogger logger.Logger, cmd []string, opts *ProviderOpts) (internal.Provider, error) {
	logger := plogger.WithPrefix("[file]")
	logger.Info("file provider will execute program: %s", cmd[0])
	if _, err := os.Stat(cmd[0]); os.IsNotExist(err) {
		return nil, fmt.Errorf("couldn't find: %s", cmd[0])
	}
	theCmd := exec.Command(cmd[0], cmd[1:]...)
	if opts.Verbose {
		theCmd.Stdout = os.Stdout
	} else {
		theCmd.Stdout = io.Discard
	}
	theCmd.Stderr = os.Stderr
	stdin, err := theCmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("stdin: %w", err)
	}
	return &FileProvider{
		logger: logger,
		cmd:    theCmd,
		stdin:  stdin,
	}, nil
}

// Start the provider and return an error or nil if ok
func (p *FileProvider) Start() error {
	p.logger.Info("start")
	if err := p.cmd.Start(); err != nil {
		return err
	}
	return nil
}

// Stop the provider and return an error or nil if ok
func (p *FileProvider) Stop() error {
	p.logger.Info("stop")
	p.once.Do(func() {
		p.logger.Debug("sending kill signal to pid: %d", p.cmd.Process.Pid)
		if p.cmd.Process != nil {
			toKill, _ := process.NewProcess(int32(p.cmd.Process.Pid))
			toKill.SendSignal(syscall.SIGINT)
			p.stdin.Close()
		}
		p.logger.Debug("stopped")
	})
	return nil
}

// Process data received and return an error or nil if processed ok
func (p *FileProvider) Process(data types.ChangeEventPayload, schema dm.Model) error {
	transport := types.Transport{
		DBChange: data,
		Schema:   schema,
	}
	buf, err := json.Marshal(transport)
	if err != nil {
		return fmt.Errorf("error converting to json: %s", err)
	}
	if _, err := p.stdin.Write(buf); err != nil {
		return fmt.Errorf("stdin: %w", err)
	}
	p.stdin.Write(EOL)
	return nil
}
