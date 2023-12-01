package provider

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"syscall"

	"github.com/nats-io/nats.go"
	"github.com/shirou/gopsutil/v3/process"
	"github.com/shopmonkeyus/eds-server/internal"
	"github.com/shopmonkeyus/eds-server/internal/datatypes"
	dm "github.com/shopmonkeyus/eds-server/internal/model"
	"github.com/shopmonkeyus/go-common/logger"
)

var EOL = []byte("\n")
var OK = "OK"
var ERR = "ERR"

type FileProvider struct {
	logger  logger.Logger
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	stdout  io.ReadCloser
	scanner *bufio.Scanner
	verbose bool
	once    sync.Once
}

var _ internal.Provider = (*FileProvider)(nil)

// NewFileProvider returns a provider that will stream files to a folder provided in the url
func NewFileProvider(plogger logger.Logger, cmd []string, opts *ProviderOpts) (internal.Provider, error) {
	logger := plogger.WithPrefix(fmt.Sprintf("[file] [%s]", cmd[0]))
	logger.Info("file provider will execute program: %s", cmd[0])
	if _, err := os.Stat(cmd[0]); os.IsNotExist(err) {
		return nil, fmt.Errorf("couldn't find: %s", cmd[0])
	}
	theCmd := exec.Command(cmd[0], cmd[1:]...)
	theCmd.Stderr = os.Stderr
	stdin, err := theCmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("stdin: %w", err)
	}
	stdout, err := theCmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout: %w", err)
	}
	scanner := bufio.NewScanner(stdout)
	return &FileProvider{
		logger:  logger,
		cmd:     theCmd,
		stdin:   stdin,
		stdout:  stdout,
		scanner: scanner,
		verbose: opts.Verbose,
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

func (p *FileProvider) readStout() error {
	for p.scanner.Scan() {
		line := p.scanner.Text()

		switch p.scanner.Text() {
		case OK:
			p.logger.Debug("success processing message")
			return nil
		case ERR:
			return fmt.Errorf("error processing message")
		default:
			if p.verbose {
				p.logger.Debug(line)
			}
		}

	}
	if err := p.scanner.Err(); err != nil {
		p.logger.Error("error reading stdout:", err)
	}
	return nil
}

// Process data received and return an error or nil if processed ok
func (p *FileProvider) Process(data datatypes.ChangeEventPayload, schema dm.Model) error {
	transport := datatypes.Transport{
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
	err = p.readStout()

	if err != nil {
		return err
	}

	return nil
}

func (p *FileProvider) Import(toDO []byte, nc *nats.Conn) error {

	return nil
}
