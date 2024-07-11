package util

import (
	"context"

	"github.com/charmbracelet/huh/spinner"
)

type Spinner struct {
	ctx     context.Context
	cancel  context.CancelFunc
	spinner *spinner.Spinner
}

func NewSpinner(c context.Context, msg string) *Spinner {
	ctx, cancel := context.WithCancel(c)
	s := &Spinner{
		ctx:    ctx,
		cancel: cancel,
	}
	s.spinner = spinner.New().Context(ctx).Title(msg)
	go s.spinner.Run()
	return s
}

func (s *Spinner) Stop() {
	s.cancel()
}

type Task func()
type TaskCallback func(spinner *spinner.Spinner)

func RunTaskWithSpinner(msg string, task Task) {
	s := NewSpinner(context.Background(), msg)
	task()
	s.Stop()
}

func RunTaskWithSpinnerCallback(msg string, task TaskCallback) {
	s := NewSpinner(context.Background(), msg)
	task(s.spinner)
	s.Stop()
}
