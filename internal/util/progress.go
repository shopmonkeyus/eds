package util

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/charmbracelet/bubbles/progress"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

const (
	padding  = 2
	maxWidth = 80
)

func NewProgressBar(ctx context.Context, cancelFunc context.CancelFunc) *ProgressBar {
	m := ProgressBar{
		progress:   progress.New(progress.WithDefaultGradient()),
		ctx:        ctx,
		cancelFunc: cancelFunc,
	}
	m.program = tea.NewProgram(&m, tea.WithAltScreen(), tea.WithContext(ctx), tea.WithoutSignalHandler())

	go func(m *ProgressBar) {
		if _, err := m.program.Run(); err != nil {
			panic(err)
		}
	}(&m)

	return &m
}

var helpStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("#626262")).Render

type ProgressBar struct {
	program    *tea.Program
	progress   progress.Model
	pending    float64
	stopped    bool
	mutex      sync.Mutex
	ctx        context.Context
	cancelFunc context.CancelFunc
	message    string
}

func (m *ProgressBar) Stop() {
	m.mutex.Lock()
	m.stopped = true
	m.mutex.Unlock()
	select {
	case <-m.ctx.Done():
	case <-time.After(time.Second):
		return
	}
}

func (m *ProgressBar) SetProgress(p float64) {
	m.mutex.Lock()
	m.pending = p
	m.mutex.Unlock()
}

func (m *ProgressBar) SetMessage(msg string) {
	m.mutex.Lock()
	m.message = msg
	m.mutex.Unlock()
}

type tickMsg time.Time

func tickCmd() tea.Cmd {
	return tea.Tick(time.Millisecond*500, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (m *ProgressBar) Init() tea.Cmd {
	return tickCmd()
}

func (m *ProgressBar) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	switch msg := msg.(type) {
	case tea.KeyMsg:
		if msg.Type == tea.KeyCtrlC {
			m.cancelFunc()
			return m, tea.Quit
		}
		return m, nil

	case tea.WindowSizeMsg:
		m.progress.Width = msg.Width - padding*2 - 4
		if m.progress.Width > maxWidth {
			m.progress.Width = maxWidth
		}
		return m, nil

	case tickMsg:
		if m.progress.Percent() == 1.0 || m.stopped {
			m.stopped = true
			return m, tea.Quit
		}
		return m, tickCmd()

	default:
		return m, nil
	}
}

func (m *ProgressBar) View() string {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	pad := strings.Repeat(" ", padding)
	return "\n" +
		pad + m.progress.ViewAs(m.pending) + "\n\n" +
		pad + helpStyle(m.message)
}

type ProgressCallback func(progressbar *ProgressBar)

func RunWithProgress(ctx context.Context, cancel context.CancelFunc, callback ProgressCallback) {
	progressbar := NewProgressBar(ctx, cancel)
	callback(progressbar)
	progressbar.SetProgress(1.0)
	progressbar.Stop()
}
