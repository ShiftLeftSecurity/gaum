package logging

import "github.com/jackc/pgx"

// Logger provides a seemingly sane logging interface.
type Logger interface {
	Debug(msg string, ctx ...interface{})
	Info(msg string, ctx ...interface{})
	Warn(msg string, ctx ...interface{})
	Error(msg string, ctx ...interface{})
	Crit(msg string, ctx ...interface{})
}

var _ pgx.Logger = &PgxLogAdapter{}

// NewPgxLogAdapter returns a PgxLogAdapter wrapping the passed Logger.
func NewPgxLogAdapter(l Logger) *PgxLogAdapter {
	return &PgxLogAdapter{logger: l}
}

// PgxLogAdapter wraps anything that satisfies Logger into pgx.Logger
type PgxLogAdapter struct {
	logger Logger
}

// Log Satisfies pgx.Logger
func (l *PgxLogAdapter) Log(level pgx.LogLevel, msg string, data map[string]interface{}) {
	logArgs := make([]interface{}, 0, len(data))
	for k, v := range data {
		logArgs = append(logArgs, k, v)
	}

	switch level {
	case pgx.LogLevelTrace:
		l.logger.Debug(msg, append(logArgs, "PGX_LOG_LEVEL", level)...)
	case pgx.LogLevelDebug:
		l.logger.Debug(msg, logArgs...)
	case pgx.LogLevelInfo:
		l.logger.Info(msg, logArgs...)
	case pgx.LogLevelWarn:
		l.logger.Warn(msg, logArgs...)
	case pgx.LogLevelError:
		l.logger.Error(msg, logArgs...)
	default:
		l.logger.Error(msg, append(logArgs, "INVALID_PGX_LOG_LEVEL", level)...)
	}
}
