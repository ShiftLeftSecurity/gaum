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

package postgres

import (
	"context"
	"database/sql"
	"log"
	"os"
	"reflect"

	"github.com/ShiftLeftSecurity/gaum/v2/db/connection"
	gaumErrors "github.com/ShiftLeftSecurity/gaum/v2/db/errors"
	"github.com/ShiftLeftSecurity/gaum/v2/db/logging"
	"github.com/ShiftLeftSecurity/gaum/v2/db/srm"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
)

var _ connection.DatabaseHandler = &Connector{}
var _ connection.DB = &DB{}

// Connector implements connection.Handler
type Connector struct {
	ConnectionString string
}

// DefaultPGPoolMaxConn is an arbitrary number of connections that I decided was ok for the pool
const DefaultPGPoolMaxConn = 10

// Open opens a connection to postgres and returns it wrapped into a connection.DB
func (c *Connector) Open(ctx context.Context, ci *connection.Information) (connection.DB, error) {
	// I'll be opinionated here and use the most efficient params.
	config, err := pgxpool.ParseConfig(c.ConnectionString)
	if err != nil {
		return nil, errors.Wrap(err, "parsing connection string")
	}

	var conLogger logging.Logger
	cc := config.ConnConfig
	if ci != nil {
		llevel, llevelErr := pgx.LogLevelFromString(string(ci.LogLevel))
		if llevelErr != nil {
			llevel = pgx.LogLevelError
		}
		if ci.Database != "" {
			cc.Database = ci.Database
		}
		if ci.User != "" {
			cc.User = ci.User
		}
		if ci.Password != "" {
			cc.Password = ci.Password
		}
		cc.Logger = logging.NewPgxLogAdapter(ci.Logger)
		conLogger = ci.Logger
		cc.LogLevel = llevel
		if ci.MaxConnPoolConns > 0 {
			config.MaxConns = int32(ci.MaxConnPoolConns)
		}
		if ci.CustomDial != nil {
			cc.DialFunc = ci.CustomDial
		}
		if ci.ConnMaxLifetime != nil {
			config.MaxConnLifetime = *ci.ConnMaxLifetime
		}
	} else {
		defaultLogger := log.New(os.Stdout, "logger: ", log.Lshortfile)
		cc.Logger = logging.NewPgxLogAdapter(logging.NewGoLogger(defaultLogger))
		conLogger = logging.NewGoLogger(defaultLogger)
		config.MaxConns = DefaultPGPoolMaxConn
	}

	conn, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		return nil, errors.Wrap(err, "connecting to postgres database")
	}

	return &DB{
		conn:   conn,
		logger: conLogger,
	}, nil
}

// DB wraps pgx.Conn into a struct that implements connection.DB
type DB struct {
	conn   *pgxpool.Pool
	tx     pgx.Tx
	logger logging.Logger
}

// Clone returns a copy of DB with the same underlying Connection
func (d *DB) Clone() connection.DB {
	return &DB{
		conn:   d.conn,
		logger: d.logger,
	}
}

// Close closes the underlying connection, beware, this makes the DB useless.
func (d *DB) Close() error {
	d.conn.Close()
	return nil
}

// EQueryIter Calls EscapeArgs before invoking QueryIter
func (d *DB) EQueryIter(ctx context.Context, statement string, fields []string, args ...interface{}) (connection.ResultFetchIter, error) {
	s, a, err := connection.EscapeArgs(statement, args)
	if err != nil {
		return nil, errors.Wrap(err, "escaping arguments")
	}
	return d.QueryIter(ctx, s, fields, a)
}

// QueryIter returns an iterator that can be used to fetch results one by one, beware this holds
// the connection until fetching is done.
// the passed fields are supposed to correspond to the fields being brought from the db, no
// check is performed on this.
func (d *DB) QueryIter(ctx context.Context, statement string, fields []string, args ...interface{}) (connection.ResultFetchIter, error) {
	var rows pgx.Rows
	var err error
	var connQ func(context.Context, string, ...interface{}) (pgx.Rows, error)
	if d.tx != nil {
		connQ = d.tx.Query
	} else if d.conn != nil {
		connQ = d.conn.Query
	} else {
		return nil, gaumErrors.NoDB
	}

	if len(args) != 0 {
		rows, err = connQ(ctx, statement, args...)
	} else {
		rows, err = connQ(ctx, statement)
	}
	if err != nil {
		return func(interface{}) (bool, func(), error) { return false, func() {}, nil },
			errors.Wrap(err, "querying database")
	}

	var fieldMap map[string]reflect.StructField
	var typeName string
	if !rows.Next() {
		return func(interface{}) (bool, func(), error) { return false, func() {}, nil },
			sql.ErrNoRows
	}
	if len(fields) == 0 || (len(fields) == 1 && fields[0] == "*") {
		// This seems to make a query each time so perhaps it goes outside.
		sqlQueryfields := rows.FieldDescriptions()
		fields = make([]string, len(sqlQueryfields), len(sqlQueryfields))
		for i, v := range sqlQueryfields {
			fields[i] = string(v.Name)
		}
	}
	return func(destination interface{}) (bool, func(), error) {
		var err error
		if reflect.TypeOf(destination).Elem().Name() != typeName {
			typeName, fieldMap, err = srm.MapFromPtrType(destination, []reflect.Kind{}, []reflect.Kind{
				reflect.Map, reflect.Slice,
			})
			if err != nil {
				defer rows.Close()
				return false, func() {}, errors.Wrapf(err, "cant fetch data into %T", destination)
			}
		}
		fieldRecipients := srm.FieldRecipientsFromType(d.logger, fields, fieldMap, destination)

		err = rows.Scan(fieldRecipients...)
		if err != nil {
			defer rows.Close()
			return false, func() {}, errors.Wrap(err,
				"scanning values into recipient, connection was closed")
		}

		return rows.Next(), rows.Close, rows.Err()
	}, nil
}

// EQueryPrimitive calls EscapeArgs before invoking QueryPrimitive.
func (d *DB) EQueryPrimitive(ctx context.Context, statement string, field string, args ...interface{}) (connection.ResultFetch, error) {
	s, a, err := connection.EscapeArgs(statement, args)
	if err != nil {
		return nil, errors.Wrap(err, "escaping arguments")
	}
	return d.QueryPrimitive(ctx, s, field, a)
}

// QueryPrimitive returns a function that allows recovering the results of the query but to a slice
// of a primitive type, only allowed if the query fetches one field.
func (d *DB) QueryPrimitive(ctx context.Context, statement string, _ string, args ...interface{}) (connection.ResultFetch, error) {
	var rows pgx.Rows
	var err error
	var connQ func(context.Context, string, ...interface{}) (pgx.Rows, error)
	if d.tx != nil {
		connQ = d.tx.Query
	} else if d.conn != nil {
		connQ = d.conn.Query
	} else {
		return nil, gaumErrors.NoDB
	}

	if len(args) != 0 {
		rows, err = connQ(ctx, statement, args...)
	} else {
		rows, err = connQ(ctx, statement)
	}
	if err != nil {
		return func(interface{}) error { return nil },
			errors.Wrap(err, "querying database")
	}
	return func(destination interface{}) error {
		if reflect.TypeOf(destination).Kind() != reflect.Ptr {
			return errors.Errorf("the passed receiver is not a pointer, connection is still open")
		}
		// TODO add a timer that closes rows if nothing is done.
		defer rows.Close()
		var err error
		reflect.ValueOf(destination).Elem().Set(reflect.MakeSlice(reflect.TypeOf(destination).Elem(), 0, 0))

		// Obtain the actual slice
		destinationSlice := reflect.ValueOf(destination).Elem()

		// If this is not Ptr->Slice->Type it would have failed already.
		tod := reflect.TypeOf(destination).Elem().Elem()

		for rows.Next() {
			// Get a New ptr to the object of the type of the slice.
			newElemPtr := reflect.New(tod)

			// Try to fetch the data
			err = rows.Scan(newElemPtr.Interface())
			if err != nil {
				rows.Close()
				return errors.Wrap(err, "scanning values into recipient, connection was closed")
			}
			// Add to the passed slice, this will actually add to an already populated slice if one
			// passed, how cool is that?
			destinationSlice.Set(reflect.Append(destinationSlice, newElemPtr.Elem()))
		}
		return rows.Err()
	}, nil
}

// EQuery calls EscapeArgs before invoking Query
func (d *DB) EQuery(ctx context.Context, statement string, fields []string, args ...interface{}) (connection.ResultFetch, error) {
	s, a, err := connection.EscapeArgs(statement, args)
	if err != nil {
		return nil, errors.Wrap(err, "escaping arguments")
	}
	return d.Query(ctx, s, fields, a)
}

// Query returns a function that allows recovering the results of the query, beware the connection
// is held until the returned closure is invoked.
func (d *DB) Query(ctx context.Context, statement string, fields []string, args ...interface{}) (connection.ResultFetch, error) {
	var rows pgx.Rows
	var err error
	var connQ func(context.Context, string, ...interface{}) (pgx.Rows, error)
	if d.tx != nil {
		connQ = d.tx.Query
	} else if d.conn != nil {
		connQ = d.conn.Query
	} else {
		return nil, gaumErrors.NoDB
	}
	if len(args) != 0 {
		rows, err = connQ(ctx, statement, args...)
	} else {
		rows, err = connQ(ctx, statement)
	}
	if err != nil {
		return func(interface{}) error { return nil },
			errors.Wrap(err, "querying database")
	}
	var fieldMap map[string]reflect.StructField

	return func(destination interface{}) error {
		if reflect.TypeOf(destination).Kind() != reflect.Ptr {
			return errors.Errorf("the passed receiver is not a pointer, connection is still open")
		}
		// TODO add a timer that closes rows if nothing is done.
		defer rows.Close()
		var err error
		reflect.ValueOf(destination).Elem().Set(reflect.MakeSlice(reflect.TypeOf(destination).Elem(), 0, 0))

		// Obtain the actual slice
		destinationSlice := reflect.ValueOf(destination).Elem()

		// If this is not Ptr->Slice->Type it would have failed already.
		tod := reflect.TypeOf(destination).Elem().Elem()

		if len(fields) == 0 || (len(fields) == 1 && fields[0] == "*") {
			// This seems to make a query each time so perhaps it goes outside.
			sqlQueryfields := rows.FieldDescriptions()
			fields = make([]string, len(sqlQueryfields), len(sqlQueryfields))
			for i, v := range sqlQueryfields {
				fields[i] = string(v.Name)
			}
		}

		for rows.Next() {
			// Get a New ptr to the object of the type of the slice.
			newElemPtr := reflect.New(tod)
			// Get the concrete object
			var newElem reflect.Value
			var newElemType reflect.Type
			if tod.Kind() == reflect.Ptr {
				// Handle slice of pointer
				intermediatePtr := newElemPtr.Elem()
				concrete := tod.Elem()
				newElemType = concrete
				// this will most likely always be the case, but let's be defensive
				if intermediatePtr.IsNil() {
					concreteInstancePtr := reflect.New(concrete)
					intermediatePtr.Set(concreteInstancePtr)
				}
				newElem = intermediatePtr.Elem()
			} else {
				newElemType = newElemPtr.Elem().Type()
				newElem = newElemPtr.Elem()
			}
			// Get its type.
			ttod := newElem.Type()

			// map the fields of the type to their potential sql names, this is the only "magic"
			fieldMap = make(map[string]reflect.StructField, ttod.NumField())
			_, fieldMap, err = srm.MapFromTypeOf(newElemType,
				[]reflect.Kind{}, []reflect.Kind{
					reflect.Map, reflect.Slice,
				})
			if err != nil {
				rows.Close()
				return errors.Wrapf(err, "cant fetch data into %T", destination)
			}

			// Construct the recipient fields.
			fieldRecipients := srm.FieldRecipientsFromValueOf(d.logger, fields, fieldMap, newElem)

			// Try to fetch the data
			err = rows.Scan(fieldRecipients...)
			if err != nil {
				rows.Close()
				return errors.Wrap(err, "scanning values into recipient, connection was closed")
			}
			// Add to the passed slice, this will actually add to an already populated slice if one
			// passed, how cool is that?
			destinationSlice.Set(reflect.Append(destinationSlice, newElemPtr.Elem()))
		}
		return rows.Err()
	}, nil
}

// ERaw calls EscapeArgs before invoking Raw
func (d *DB) ERaw(ctx context.Context, statement string, args []interface{}, fields ...interface{}) error {
	s, a, err := connection.EscapeArgs(statement, args)
	if err != nil {
		return errors.Wrap(err, "escaping arguments")
	}
	return d.Raw(ctx, s, a, fields)
}

// Raw will run the passed statement with the passed args and scan the first result, if any,
// to the passed fields.
func (d *DB) Raw(ctx context.Context, statement string, args []interface{}, fields ...interface{}) error {
	var rows pgx.Row

	if d.tx != nil {
		rows = d.tx.QueryRow(ctx, statement, args...)
	} else if d.conn != nil {
		rows = d.conn.QueryRow(ctx, statement, args...)
	} else {
		return gaumErrors.NoDB
	}

	// Try to fetch the data
	err := rows.Scan(fields...)
	if err == pgx.ErrNoRows {
		return gaumErrors.ErrNoRows
	}
	if err != nil {
		return errors.Wrap(err, "scanning values into recipient")
	}
	return nil
}

// EExec calls EscapeArgs before invoking Exec
func (d *DB) EExec(ctx context.Context, statement string, args ...interface{}) error {
	s, a, err := connection.EscapeArgs(statement, args)
	if err != nil {
		return errors.Wrap(err, "escaping arguments")
	}
	return d.Exec(ctx, s, a...)
}

// Exec will run the statement and expect nothing in return.
func (d *DB) Exec(ctx context.Context, statement string, args ...interface{}) error {
	_, err := d.exec(ctx, statement, args...)
	return err
}

// ExecResult will run the statement and return the number of rows affected.
func (d *DB) ExecResult(ctx context.Context, statement string, args ...interface{}) (int64, error) {
	connTag, err := d.exec(ctx, statement, args...)
	if err != nil {
		return 0, err
	}
	return connTag.RowsAffected(), nil
}

func (d *DB) exec(ctx context.Context, statement string, args ...interface{}) (pgconn.CommandTag, error) {
	var connTag pgconn.CommandTag
	var err error

	if d.tx != nil {
		connTag, err = d.tx.Exec(ctx, statement, args...)
	} else if d.conn != nil {
		connTag, err = d.conn.Exec(ctx, statement, args...)
	} else {
		return connTag, gaumErrors.NoDB
	}

	if err != nil {
		return connTag, errors.Wrapf(err, "querying database, obtained %v", connTag)
	}
	return connTag, nil
}

// BeginTransaction returns a new DB that will use the transaction instead of the basic conn.
// if the transaction is already started the same will be returned.
func (d *DB) BeginTransaction(ctx context.Context) (connection.DB, error) {
	if d.tx != nil {
		return nil, gaumErrors.AlreadyInTX
	}
	tx, err := d.conn.Begin(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "trying to begin a transaction")
	}
	return &DB{
		tx:     tx,
		logger: d.logger,
	}, nil
}

// IsTransaction indicates if the DB is in the middle of a transaction.
func (d *DB) IsTransaction() bool {
	return d.tx != nil
}

// CommitTransaction commits the transaction if any is in course, behavior comes straight from
// pgx.
func (d *DB) CommitTransaction(ctx context.Context) error {
	if d.tx == nil {
		return gaumErrors.NoTX
	}

	return d.tx.Commit(ctx)
}

// RollbackTransaction rolls back the transaction if any is in course, behavior comes straight from
// pgx.
func (d *DB) RollbackTransaction(ctx context.Context) error {
	if d.tx == nil {
		return gaumErrors.NoTX
	}
	return d.tx.Rollback(ctx)
}

// Set tries to run `SET LOCAL` with the passed parameters if there is an ongoing transaction.
// https://www.postgresql.org/docs/9.2/static/sql-set.html
func (d *DB) Set(ctx context.Context, set string) error {
	if d.tx == nil {
		return gaumErrors.NoTX
	}
	// TODO check if this will work in the `SET LOCAL $1` arg format
	cTag, err := d.tx.Exec(ctx, "SET LOCAL "+set)
	if err != nil {
		return errors.Wrapf(err, "trying to set local, returned: %s", cTag)
	}
	return nil
}

// BulkInsert will use postgres copy function to try to insert a lot of data.
// You might need to use pgx types for the values to reduce probability of failure.
// https://godoc.org/github.com/jackc/pgx#Conn.CopyFrom
func (d *DB) BulkInsert(ctx context.Context, tableName string, columns []string, values [][]interface{}) (execError error) {
	tx := d.tx
	if d.tx == nil {
		var err error
		tx, err = d.conn.Begin(ctx)
		if err != nil {
			return errors.Wrap(err, "beginning transaction for bulk insert")
		}
		defer func() {
			if execError != nil {
				err := tx.Rollback(ctx)
				execError = errors.Wrapf(execError,
					"there was a failure running the expression and also rolling back te transaction: %v",
					err)
			} else {
				err := tx.Commit(ctx)
				execError = errors.Wrap(err, "could not commit the transaction")
			}
		}()
	}
	copySource := pgx.CopyFromRows(values)
	rowsAffected, err := tx.CopyFrom(ctx, pgx.Identifier{tableName}, columns, copySource)
	if rowsAffected != int64(len(values)) {
		return errors.Errorf("%d rows were passed but only %d inserted, will rollback",
			len(values), rowsAffected)
	}
	if err != nil {
		return errors.Wrap(err, "bulk inserting")
	}
	return nil
}
