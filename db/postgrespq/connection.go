package postgrespq

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

import (
	"context"
	"database/sql"
	"log"
	"os"
	"reflect"

	"github.com/ShiftLeftSecurity/gaum/db/connection"
	gaumErrors "github.com/ShiftLeftSecurity/gaum/db/errors"
	"github.com/ShiftLeftSecurity/gaum/db/logging"
	"github.com/ShiftLeftSecurity/gaum/db/srm"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/jackc/pgx/v4/stdlib"
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
func (c *Connector) Open(_ context.Context, ci *connection.Information) (connection.DB, error) {
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

	connString := stdlib.RegisterConnConfig(config.ConnConfig)

	conn, err := sql.Open("pgx", connString)
	if err != nil {
		return nil, errors.Wrap(err, "connecting to postgres database")
	}
	if ci != nil && ci.ConnMaxLifetime != nil {
		conn.SetConnMaxLifetime(*ci.ConnMaxLifetime)
	}
	return &DB{
		conn:   conn,
		logger: conLogger,
	}, nil
}

// DB wraps pgx.Conn into a struct that implements connection.DB
type DB struct {
	conn   *sql.DB
	tx     *sql.Tx
	logger logging.Logger
}

// Clone returns a copy of DB with the same underlying Connection
func (d *DB) Clone() connection.DB {
	return &DB{
		conn:   d.conn,
		logger: d.logger,
	}
}

// EQueryIter Calls EscapeArgs before invoking QueryIter
func (d *DB) EQueryIter(ctx context.Context, statement string, fields []string, args ...interface{}) (connection.ResultFetchIter, error) {
	s, a, err := connection.EscapeArgs(statement, args)
	if err != nil {
		return nil, errors.Wrap(err, "escaping arguments")
	}
	return d.QueryIter(ctx, s, fields, a...)
}

// QueryIter returns an iterator that can be used to fetch results one by one, beware this holds
// the connection until fetching is done.
// the passed fields are supposed to correspond to the fields being brought from the db, no
// check is performed on this.
func (d *DB) QueryIter(ctx context.Context, statement string, fields []string, args ...interface{}) (connection.ResultFetchIter, error) {
	var rows *sql.Rows
	var err error
	var connQ func(context.Context, string, ...interface{}) (*sql.Rows, error)
	if d.tx != nil {
		connQ = d.tx.QueryContext
	} else if d.conn != nil {
		connQ = d.conn.QueryContext
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
		fields, err = rows.Columns()
		if err != nil {
			return func(interface{}) (bool, func(), error) { return false, func() {}, nil },
				errors.Wrap(err, "could not fetch field information from query")
		}
	}
	return func(destination interface{}) (bool, func(), error) {
		var err error
		if reflect.TypeOf(destination).Elem().Name() != typeName {
			typeName, fieldMap, err = srm.MapFromPtrType(destination, []reflect.Kind{}, []reflect.Kind{
				reflect.Map, reflect.Slice,
			})
			if err != nil {
				_ = rows.Close()
				return false, func() {}, errors.Wrapf(err, "cant fetch data into %T", destination)
			}
		}
		fieldRecipients := srm.FieldRecipientsFromType(d.logger, fields, fieldMap, destination)

		err = rows.Scan(fieldRecipients...)
		if err != nil {
			_ = rows.Close()
			return false, func() {}, errors.Wrap(err,
				"scanning values into recipient, connection was closed")
		}

		return rows.Next(), func() { _ = rows.Close() }, rows.Err()
	}, nil
}

// EQueryPrimitive calls EscapeArgs before invoking QueryPrimitive.
func (d *DB) EQueryPrimitive(ctx context.Context, statement string, field string, args ...interface{}) (connection.ResultFetch, error) {
	s, a, err := connection.EscapeArgs(statement, args)
	if err != nil {
		return nil, errors.Wrap(err, "escaping arguments")
	}
	return d.QueryPrimitive(ctx, s, field, a...)
}

// QueryPrimitive returns a function that allows recovering the results of the query but to a slice
// of a primitive type, only allowed if the query fetches one field.
func (d *DB) QueryPrimitive(ctx context.Context, statement string, _ string, args ...interface{}) (connection.ResultFetch, error) {
	var rows *sql.Rows
	var err error
	var connQ func(context.Context, string, ...interface{}) (*sql.Rows, error)
	if d.tx != nil {
		connQ = d.tx.QueryContext
	} else if d.conn != nil {
		connQ = d.conn.QueryContext
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
		defer func() { _ = rows.Close() }()
		if reflect.TypeOf(destination).Kind() != reflect.Ptr {
			return errors.New("YOU NEED TO PASS A *[]T, if you pass a `[]T` or `[]*T` or `T` you'll get this message again")
		}
		// TODO add a timer that closes rows if nothing is done.
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
	return d.Query(ctx, s, fields, a...)
}

// Query returns a function that allows recovering the results of the query, beware the connection
// is held until the returned closure is invoked.
func (d *DB) Query(ctx context.Context, statement string, fields []string, args ...interface{}) (connection.ResultFetch, error) {
	var rows *sql.Rows
	var err error
	var connQ func(context.Context, string, ...interface{}) (*sql.Rows, error)
	if d.tx != nil {
		connQ = d.tx.QueryContext
	} else if d.conn != nil {
		connQ = d.conn.QueryContext
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
		defer func() { _ = rows.Close() }()
		if reflect.TypeOf(destination).Kind() != reflect.Ptr {
			return errors.New("YOU NEED TO PASS A `*[]T`, if you pass a `[]T` or `[]*T` or `T` you'll get this message again")
		}
		// TODO add a timer that closes rows if nothing is done.
		var err error
		reflect.ValueOf(destination).Elem().Set(reflect.MakeSlice(reflect.TypeOf(destination).Elem(), 0, 0))

		// Obtain the actual slice
		destinationSlice := reflect.ValueOf(destination).Elem()

		// If this is not Ptr->Slice->Type it would have failed already.
		tod := reflect.TypeOf(destination).Elem().Elem()

		if len(fields) == 0 || (len(fields) == 1 && fields[0] == "*") {
			fields, err = rows.Columns()
			if err != nil {
				return errors.Wrap(err, "could not fetch field information from query")
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
			ttod := newElem.Type()

			// map the fields of the type to their potential sql names, this is the only "magic"
			fieldMap = make(map[string]reflect.StructField, ttod.NumField())
			_, fieldMap, err = srm.MapFromTypeOf(newElemType,
				[]reflect.Kind{}, []reflect.Kind{
					reflect.Map, reflect.Slice,
				})
			if err != nil {
				return errors.Wrapf(err, "cant fetch data into %T", destination)
			}

			// Construct the recipient fields.
			fieldRecipients := srm.FieldRecipientsFromValueOf(d.logger, fields, fieldMap, newElem)

			// Try to fetch the data
			err = rows.Scan(fieldRecipients...)
			if err != nil {
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
	var rows *sql.Row

	if d.tx != nil {
		rows = d.tx.QueryRowContext(ctx, statement, args...)
	} else if d.conn != nil {
		rows = d.conn.QueryRowContext(ctx, statement, args...)
	} else {
		return gaumErrors.NoDB
	}

	// Try to fetch the data
	err := rows.Scan(fields...)
	if err == sql.ErrNoRows {
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
	rowsAffected, err := connTag.RowsAffected()
	if err != nil {
		return 0, errors.Wrap(err, "reading rowsAffected from connTag")
	}
	return rowsAffected, nil
}

func (d *DB) exec(ctx context.Context, statement string, args ...interface{}) (sql.Result, error) {
	var connTag sql.Result
	var err error
	if d.tx != nil {
		connTag, err = d.tx.ExecContext(ctx, statement, args...)
	} else if d.conn != nil {
		connTag, err = d.conn.ExecContext(ctx, statement, args...)
	} else {
		return nil, gaumErrors.NoDB
	}
	if err != nil {
		return nil, errors.Wrapf(err, "querying database, obtained %v", connTag)
	}
	return connTag, nil
}

// BeginTransaction returns a new DB that will use the transaction instead of the basic conn.
// if the transaction is already started the same will be returned.
func (d *DB) BeginTransaction(ctx context.Context) (connection.DB, error) {
	if d.tx != nil {
		return nil, gaumErrors.AlreadyInTX
	}
	tx, err := d.conn.BeginTx(ctx, nil)
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
func (d *DB) CommitTransaction(_ context.Context) error {
	if d.tx == nil {
		return gaumErrors.NoTX
	}

	return d.tx.Commit()
}

// RollbackTransaction rolls back the transaction if any is in course, behavior comes straight from
// pgx.
func (d *DB) RollbackTransaction(_ context.Context) error {
	if d.tx == nil {
		return gaumErrors.NoTX
	}
	return d.tx.Rollback()
}

// Set tries to run `SET LOCAL` with the passed parameters if there is an ongoing transaction.
// https://www.postgresql.org/docs/9.2/static/sql-set.html
func (d *DB) Set(ctx context.Context, set string) error {
	if d.tx == nil {
		return gaumErrors.NoTX
	}
	// TODO check if this will work in the `SET LOCAL $1` arg format
	cTag, err := d.tx.ExecContext(ctx, "SET LOCAL "+set)
	if err != nil {
		return errors.Wrapf(err, "trying to set local, returned: %s", cTag)
	}
	return nil
}

// BulkInsert only works with pgx driver.
func (d *DB) BulkInsert(_ context.Context, _ string, _ []string, _ [][]interface{}) (execError error) {
	return gaumErrors.NotImplemented
}
