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

package connection

import (
	"context"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/ShiftLeftSecurity/gaum/v2/db/logging"
	"github.com/pkg/errors"
)

// LogLevel is the type for the potential log levels a db can have
type LogLevel string

var (
	// Trace sets log level to trace.
	Trace LogLevel = "trace"
	// Debug sets log level to debug.
	Debug LogLevel = "debug"
	// Info sets log level to info.
	Info LogLevel = "info"
	// Warn sets log level to warn.
	Warn LogLevel = "warn"
	// Error sets log level to error.
	Error LogLevel = "error"
	// None sets log level to none.
	None LogLevel = "none"
)

// Information contains all required information to create a connection into a db.
// Copied almost verbatim from https://godoc.org/github.com/jackc/pgx#ConnConfig
type Information struct {
	Database        string
	User            string
	Password        string
	ConnMaxLifetime *time.Duration

	CustomDial func(ctx context.Context, network, addr string) (net.Conn, error)

	// MaxConnPoolConns where applies will be used to determine the maximum amount of connections
	// a pool can have.
	MaxConnPoolConns int

	Logger   logging.Logger
	LogLevel LogLevel
}

// DatabaseHandler represents the boundary with a db.
type DatabaseHandler interface {
	// Open must be able to connect to the handled engine and return a db.
	Open(context.Context, *Information) (DB, error)
}

// ResultFetchIter represents a closure that receives a receiver struct that will get the
// results assigned for one row and returns a tuple of `next item present`, `close function`, error
type ResultFetchIter func(interface{}) (bool, func(), error)

// ResultFetch represents a closure that receives a receiver struct and wil assign all the results
// it is expected that it receives a slice.
type ResultFetch func(interface{}) error

// DB represents an active database connection.
type DB interface {
	// Clone returns a stateful copy of this connection.
	Clone() DB
	// QueryIter returns closure allowing to load/fetch roads one by one.
	QueryIter(ctx context.Context, statement string, fields []string, args ...interface{}) (ResultFetchIter, error)
	// EQueryIter is QueryIter but will use EscapeArgs.
	EQueryIter(ctx context.Context, statement string, fields []string, args ...interface{}) (ResultFetchIter, error)
	// Query returns a closure that allows fetching of the results of the query.
	Query(ctx context.Context, statement string, fields []string, args ...interface{}) (ResultFetch, error)
	// EQuery is Query but will use EscapeArgs.
	EQuery(ctx context.Context, statement string, fields []string, args ...interface{}) (ResultFetch, error)
	// QueryPrimitive returns a closure that allows fetching of the results of a query to a
	// slice of primitives.
	QueryPrimitive(ctx context.Context, statement string, field string, args ...interface{}) (ResultFetch, error)
	// EQueryPrimitive is QueryPrimitive but will use EscapeArgs
	EQueryPrimitive(ctx context.Context, statement string, field string, args ...interface{}) (ResultFetch, error)
	// Raw ins intended to be an all raw query that runs statement with args and tries
	// to retrieve the results into fields without much magic whatsoever.
	Raw(ctx context.Context, statement string, args []interface{}, fields ...interface{}) error
	// ERaw is Raw but will use EscapeArgs
	ERaw(ctx context.Context, statement string, args []interface{}, fields ...interface{}) error
	// Exec is intended for queries that do not yield results (data modifiers)
	Exec(ctx context.Context, statement string, args ...interface{}) error
	// ExecResult is intended for queries that modify data and respond with how many rows were affected.
	ExecResult(ctx context.Context, statement string, args ...interface{}) (int64, error)
	// EExec is Exec but will use EscapeArgs.
	EExec(ctx context.Context, statement string, args ...interface{}) error
	// BeginTransaction returns a new DB that will use the transaction instead of the basic conn.
	BeginTransaction(ctx context.Context) (DB, error)
	// CommitTransaction commits the transaction
	CommitTransaction(ctx context.Context) error
	// RollbackTransaction rolls back the transaction
	RollbackTransaction(ctx context.Context) error
	// IsTransaction indicates if the DB is in the middle of a transaction.
	IsTransaction() bool
	// Set allows to change settings for the current transaction.
	Set(ctx context.Context, set string) error
	// BulkInsert Inserts in the most efficient way possible a lot of data.
	BulkInsert(ctx context.Context, tableName string, columns []string, values [][]interface{}) (execError error)
}

// EscapeArgs return the query and args with the argument placeholder escaped.
func EscapeArgs(query string, args []interface{}) (string, []interface{}, error) {
	// TODO: make this a bit less ugly
	// TODO: identify escaped question marks
	queryWithArgs := &strings.Builder{}
	argCounter := 1
	for _, queryChar := range query {
		if queryChar == '?' {
			queryWithArgs.WriteRune('$')
			queryWithArgs.WriteString(strconv.Itoa(argCounter))
			argCounter++
		} else {
			queryWithArgs.WriteRune(queryChar)
		}
	}
	if len(args) != argCounter-1 {
		return "", nil, errors.Errorf("the query has %d args but %d were passed: \n %q \n %#v",
			argCounter-1, len(args), queryWithArgs, args)
	}
	return queryWithArgs.String(), args, nil
}
