//    Copyright 2018 Horacio Duran <horacio@shiftleft.io>, ShiftLeft Inc.
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

package logging

import (
	"context"

	"github.com/jackc/pgx/v5/tracelog"
)

// Logger provides a seemingly sane logging interface.
type Logger interface {
	Debug(msg string, ctx ...interface{})
	Info(msg string, ctx ...interface{})
	Warn(msg string, ctx ...interface{})
	Error(msg string, ctx ...interface{})
	Crit(msg string, ctx ...interface{})
}

var _ tracelog.Logger = &PgxLogAdapter{}

// NewPgxLogAdapter returns a PgxLogAdapter wrapping the passed Logger.
func NewPgxLogAdapter(l Logger) *PgxLogAdapter {
	return &PgxLogAdapter{logger: l}
}

// PgxLogAdapter wraps anything that satisfies Logger into tracelog.Logger
type PgxLogAdapter struct {
	logger Logger
}

// Log Satisfies tracelog.Logger
func (l *PgxLogAdapter) Log(_ context.Context, level tracelog.LogLevel, msg string, data map[string]interface{}) {
	logArgs := make([]interface{}, 0, len(data))
	for k, v := range data {
		logArgs = append(logArgs, k, v)
	}

	switch level {
	case tracelog.LogLevelTrace:
		l.logger.Debug(msg, append(logArgs, "PGX_LOG_LEVEL", level)...)
	case tracelog.LogLevelDebug:
		l.logger.Debug(msg, logArgs...)
	case tracelog.LogLevelInfo:
		l.logger.Info(msg, logArgs...)
	case tracelog.LogLevelWarn:
		l.logger.Warn(msg, logArgs...)
	case tracelog.LogLevelError:
		l.logger.Error(msg, logArgs...)
	default:
		l.logger.Error(msg, append(logArgs, "INVALID_PGX_LOG_LEVEL", level)...)
	}
}
