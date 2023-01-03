package internal

import (
	"context"
	"io"
	"log"
	"os"
	"strings"
	"time"

	glogger "gorm.io/gorm/logger"
)

// Colors
const (
	Reset       = "\033[0m"
	Red         = "\033[31m"
	Green       = "\033[32m"
	Yellow      = "\033[33m"
	Blue        = "\033[34m"
	Magenta     = "\033[35m"
	Cyan        = "\033[36m"
	White       = "\033[37m"
	BlueBold    = "\033[34;1m"
	MagentaBold = "\033[35;1m"
	RedBold     = "\033[31;1m"
	YellowBold  = "\033[33;1m"
	WhiteBold   = "\033[37;1m"
)

// LogLevel log level
type LogLevel int

const (
	// Silent silent log level
	Silent LogLevel = iota + 1
	// Error error log level
	Error
	// Warn warn log level
	Warn
	// Info info log level
	Info
	// Trace trace log level
	Trace
)

// Writer log writer interface
type Writer interface {
	Printf(string, ...interface{})
}

// Config logger config
type Config struct {
	Color    bool
	LogLevel LogLevel
}

// Logger logger interface
type Logger interface {
	LogMode(LogLevel) Logger
	Info(string, ...interface{})
	Warn(string, ...interface{})
	Error(string, ...interface{})
	Trace(string, ...interface{})
}

var (
	// Discard Discard logger will print any log to io.Discard
	Discard = New(log.New(io.Discard, "", log.Ltime), Config{})
	// Default Default logger
	Default = New(log.New(os.Stdout, Cyan, log.Ltime), Config{
		LogLevel: Info,
		Color:    true,
	})
	// Trace level logger
	Tracer = New(log.New(os.Stdout, Cyan, log.Ltime), Config{
		LogLevel: Trace,
		Color:    true,
	})
)

// New initialize logger
func New(writer Writer, config Config) Logger {
	return &xlogger{
		Writer: writer,
		Config: config,
	}
}

type xlogger struct {
	Writer
	Config
}

// LogMode log mode
func (l *xlogger) LogMode(level LogLevel) Logger {
	newlogger := *l
	newlogger.LogLevel = level
	return &newlogger
}

// Info print info
func (l xlogger) Info(msg string, data ...interface{}) {
	if l.LogLevel >= Info {
		if l.Color {
			l.Printf(Green+"[INFO] "+Reset+WhiteBold+msg+Reset+"\n", data...)
		} else {
			l.Printf("[INFO] "+msg+"\n", data...)
		}
	}
}

// Warn print warn messages
func (l xlogger) Warn(msg string, data ...interface{}) {
	if l.LogLevel >= Warn {
		if l.Color {
			l.Printf(BlueBold+"[WARN] "+Reset+Magenta+msg+Reset+"\n", data...)
		} else {
			l.Printf("[WARN] "+msg+"\n", data...)
		}
	}
}

// Error print error messages
func (l xlogger) Error(msg string, data ...interface{}) {
	if l.LogLevel >= Error {
		if l.Color {
			l.Printf(Red+"[ERROR] "+Reset+msg+"\n", data...)
		} else {
			l.Printf("[ERROR] "+msg+"\n", data...)
		}
	}
}

// Trace message
func (l xlogger) Trace(msg string, data ...interface{}) {
	if l.LogLevel >= Trace {
		if l.Color {
			l.Printf(BlueBold+"[TRACE] "+Reset+Yellow+msg+Reset+"\n", data...)
		} else {
			l.Printf("[TRACE] "+msg+"\n", data...)
		}
	}
}

type gormLoggerAdapter struct {
	logger Logger
}

var _ glogger.Interface = (*gormLoggerAdapter)(nil)

// NewGormLogAdapter returns a new Logger that implements the gorm Logger interface
func NewGormLogAdapter(logger Logger) glogger.Interface {
	return &gormLoggerAdapter{logger}
}

func (a *gormLoggerAdapter) LogMode(level glogger.LogLevel) glogger.Interface {
	return a
}

func (a *gormLoggerAdapter) Info(ctx context.Context, msg string, data ...interface{}) {
	if strings.Contains(msg, "replacing callback `") {
		a.logger.Trace(strings.TrimSpace(msg), data...)
		return
	}
	a.logger.Info(strings.TrimSpace(msg), data...)
}

func (a *gormLoggerAdapter) Warn(ctx context.Context, msg string, data ...interface{}) {
	a.logger.Warn(strings.TrimSpace(msg), data...)
}

func (a *gormLoggerAdapter) Error(ctx context.Context, msg string, data ...interface{}) {
	a.logger.Error(strings.TrimSpace(msg), data...)
}

func (a *gormLoggerAdapter) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	if err != nil {
		a.logger.Error("error running sql: %s", err)
	} else {
		sql, count := fc()
		a.logger.Trace("sql executed: %s, returned %d rows in %v", sql, count, time.Since(begin))
	}
}
