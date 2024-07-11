package util

import (
	"context"

	"github.com/charmbracelet/huh/spinner"
)

type ProgressBar struct {
	spinner *spinner.Spinner
}

func (m *ProgressBar) SetProgress(p float64) {
	// FIXME remove
}

func (m *ProgressBar) SetMessage(msg string) {
	m.spinner.Title(msg)
}

type ProgressCallback func(progressbar *ProgressBar)

func RunWithProgress(ctx context.Context, cancel context.CancelFunc, callback ProgressCallback) {
	var pb ProgressBar
	RunTaskWithSpinnerCallback("", func(s *spinner.Spinner) {
		pb.spinner = s
		callback(&pb)
	})
}
