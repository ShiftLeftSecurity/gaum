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
func (g *GoLogger) composeMessage(msg string, ctx ...interface{}) string {
	messageComponents := []string{}
	var key interface{}
	for index, keyval := range ctx {
		if index%2 == 0 {
			key = keyval
		} else {
			messageComponents = append(messageComponents,
				fmt.Sprintf("\"%v\":\"%v\"", key, keyval))
		}
	}
	return strings.Join(messageComponents, ",")
}

// Debug implements Logger
func (g *GoLogger) Debug(msg string, ctx ...interface{}) {
	g.logger.Print(g.composeMessage(msg, ctx...))
}

// Info implements Logger
func (g *GoLogger) Info(msg string, ctx ...interface{}) {
	g.logger.Print(g.composeMessage(msg, ctx...))
}

// Warn implements Logger
func (g *GoLogger) Warn(msg string, ctx ...interface{}) {
	g.logger.Print(g.composeMessage(msg, ctx...))
}

// Error implements Logger
func (g *GoLogger) Error(msg string, ctx ...interface{}) {
	g.logger.Print(g.composeMessage(msg, ctx...))
}

// Crit implements Logger
func (g *GoLogger) Crit(msg string, ctx ...interface{}) {
	g.logger.Print(g.composeMessage(msg, ctx...))
}
