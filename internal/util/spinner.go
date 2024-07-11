package util

import (
	"context"

	"github.com/charmbracelet/huh/spinner"
)

type Spinner struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func NewSpinner(c context.Context, msg string) *Spinner {
	ctx, cancel := context.WithCancel(c)
	s := &Spinner{
		ctx:    ctx,
		cancel: cancel,
	}
	go spinner.New().Context(ctx).Title(msg).Run()
	return s
}

func (s *Spinner) Stop() {
	s.cancel()
}

type Task func()

func RunTaskWithSpinner(msg string, task Task) {
	s := NewSpinner(context.Background(), msg)
	task()
	s.Stop()
}
