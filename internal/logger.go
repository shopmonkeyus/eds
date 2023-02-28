package internal

import (
	"context"
	"strings"
	"time"

	"github.com/shopmonkeyus/go-common/logger"
	glogger "gorm.io/gorm/logger"
)

type gormLoggerAdapter struct {
	logger logger.Logger
}

var _ glogger.Interface = (*gormLoggerAdapter)(nil)

// NewGormLogAdapter returns a new Logger that implements the gorm Logger interface
func NewGormLogAdapter(logger logger.Logger) glogger.Interface {
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
