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
	"fmt"
	"log"
	"strings"
)

var _ Logger = &GoLogger{}

// NewGoLogger returns a GoLogger wrapping the passed log.Logger
func NewGoLogger(l *log.Logger) *GoLogger {
	return &GoLogger{logger: l}
}

// GoLogger wraps the builtin log.Logger into our own Logger
type GoLogger struct {
	logger *log.Logger
}

// composeMessage makes a rudimentary pastiche of the passed structured data as displaying it
// properly is beyond the built-in log.Logger capabilities
func (g *GoLogger) composeMessage(msg, level string, ctx ...interface{}) string {
	messageComponents := []string{}
	var key interface{}
	if ctx == nil {
		ctx = []interface{}{}
	}
	newCtx := []interface{}{
		"message", msg,
		"level", level,
	}
	ctx = append(newCtx, ctx...)
	for index, keyval := range ctx {
		if index%2 == 0 {
			key = keyval
		} else {
			messageComponents = append(messageComponents,
				fmt.Sprintf("\"%v\":\"%v\"", key, keyval))
		}
	}
	return fmt.Sprintf("{%s}", strings.Join(messageComponents, ","))
}

// Debug implements Logger
func (g *GoLogger) Debug(msg string, ctx ...interface{}) {
	g.logger.Print(g.composeMessage(msg, "DEBUG", ctx...))
}

// Info implements Logger
func (g *GoLogger) Info(msg string, ctx ...interface{}) {
	g.logger.Print(g.composeMessage(msg, "INFO", ctx...))
}

// Warn implements Logger
func (g *GoLogger) Warn(msg string, ctx ...interface{}) {
	g.logger.Print(g.composeMessage(msg, "WARN", ctx...))
}

// Error implements Logger
func (g *GoLogger) Error(msg string, ctx ...interface{}) {
	g.logger.Print(g.composeMessage(msg, "ERROR", ctx...))
}

// Crit implements Logger
func (g *GoLogger) Crit(msg string, ctx ...interface{}) {
	g.logger.Print(g.composeMessage(msg, "CRITICAL", ctx...))
}
