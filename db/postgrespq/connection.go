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
	"strings"
	"time"

	"github.com/ShiftLeftSecurity/gaum/db/connection"
	gaumErrors "github.com/ShiftLeftSecurity/gaum/db/errors"
	"github.com/ShiftLeftSecurity/gaum/db/logging"
	"github.com/ShiftLeftSecurity/gaum/db/srm"
	"github.com/jackc/pgx"
	"github.com/jackc/pgx/stdlib"
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
func (c *Connector) Open(ci *connection.Information) (connection.DB, error) {
	// Ill be opinionated here and use the most efficient params.
	var config pgx.ConnPoolConfig
	var conLogger logging.Logger
	if ci != nil {
		llevel, llevelErr := pgx.LogLevelFromString(string(ci.LogLevel))
		if llevelErr != nil {
			llevel = pgx.LogLevelError
		}
		conLogger = ci.Logger
		config = pgx.ConnPoolConfig{
			ConnConfig: pgx.ConnConfig{
				Host:     ci.Host,
				Port:     ci.Port,
				Database: ci.Database,
				User:     ci.User,
				Password: ci.Password,

				TLSConfig:         ci.TLSConfig,
				UseFallbackTLS:    ci.UseFallbackTLS,
				FallbackTLSConfig: ci.FallbackTLSConfig,
				Logger:            logging.NewPgxLogAdapter(conLogger),
				LogLevel:          int(llevel),
			},
			MaxConnections: ci.MaxConnPoolConns,
		}
		if ci.CustomDial != nil {
			config.ConnConfig.Dial = ci.CustomDial
		}
	}
	if c.ConnectionString != "" {
		csconfig, err := pgx.ParseConnectionString(c.ConnectionString)
		if err != nil {
			return nil, errors.Wrap(err, "parsing connection string")
		}
		if ci != nil {
			llevel, llevelErr := pgx.LogLevelFromString(string(ci.LogLevel))
			if llevelErr != nil {
				llevel = pgx.LogLevelError
			}
			config.ConnConfig = csconfig.Merge(pgx.ConnConfig{
				Host:     ci.Host,
				Port:     ci.Port,
				Database: ci.Database,
				User:     ci.User,
				Password: ci.Password,

				TLSConfig:         ci.TLSConfig,
				UseFallbackTLS:    ci.UseFallbackTLS,
				FallbackTLSConfig: ci.FallbackTLSConfig,
				Logger:            logging.NewPgxLogAdapter(ci.Logger),
				LogLevel:          int(llevel),
			})
		} else {
			defaultLogger := log.New(os.Stdout, "logger: ", log.Lshortfile)
			csconfig.Logger = logging.NewPgxLogAdapter(logging.NewGoLogger(defaultLogger))
			conLogger = logging.NewGoLogger(defaultLogger)
			config = pgx.ConnPoolConfig{
				MaxConnections: DefaultPGPoolMaxConn,
				ConnConfig:     csconfig,
			}
		}

	}
	driverConfig := stdlib.DriverConfig{
		ConnConfig: config.ConnConfig,
	}

	stdlib.RegisterDriverConfig(&driverConfig)

	conn, err := sql.Open("pgx", driverConfig.ConnectionString(c.ConnectionString))
	if err != nil {
		return nil, errors.Wrap(err, "connecting to postgres database")
	}
	if ci.ConnMaxLifetime != nil {
		conn.SetConnMaxLifetime(*ci.ConnMaxLifetime)
	}
	return &DB{
		conn:        conn,
		logger:      conLogger,
		execTimeout: ci.QueryExecTimeout,
	}, nil
}

// DB wraps pgx.Conn into a struct that implements connection.DB
type DB struct {
	conn        *sql.DB
	tx          *sql.Tx
	logger      logging.Logger
	execTimeout *time.Duration
}

// Clone returns a copy of DB with the same underlying Connection
func (d *DB) Clone() connection.DB {
	return &DB{
		conn:   d.conn,
		logger: d.logger,
	}
}

func snakesToCamels(s string) string {
	var c string
	var snake bool
	for i, v := range s {
		if i == 0 {
			c += strings.ToUpper(string(v))
			continue
		}
		if v == '_' {
			snake = true
			continue
		}
		if snake {
			c += strings.ToUpper(string(v))
			continue
		}
		c += string(v)
	}
	return c
}

// EQueryIter Calls EscapeArgs before invoking QueryIter
func (d *DB) EQueryIter(statement string, fields []string, args ...interface{}) (connection.ResultFetchIter, error) {
	s, a, err := connection.EscapeArgs(statement, args)
	if err != nil {
		return nil, errors.Wrap(err, "escaping arguments")
	}
	return d.QueryIter(s, fields, a...)
}

// QueryIter returns an iterator that can be used to fetch results one by one, beware this holds
// the connection until fetching is done.
// the passed fields are supposed to correspond to the fields being brought from the db, no
// check is performed on this.
func (d *DB) QueryIter(statement string, fields []string, args ...interface{}) (connection.ResultFetchIter, error) {
	var rows *sql.Rows
	var err error
	var connQ func(string, ...interface{}) (*sql.Rows, error)
	if d.tx != nil {
		connQ = d.tx.Query
	} else if d.conn != nil {
		connQ = d.conn.Query
	} else {
		return nil, gaumErrors.NoDB
	}

	if len(args) != 0 {
		rows, err = connQ(statement, args...)
	} else {
		rows, err = connQ(statement)
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

		return rows.Next(), func() { rows.Close() }, nil
	}, nil
}

// EQueryPrimitive calls EscapeArgs before invoking QueryPrimitive.
func (d *DB) EQueryPrimitive(statement string, field string, args ...interface{}) (connection.ResultFetch, error) {
	s, a, err := connection.EscapeArgs(statement, args)
	if err != nil {
		return nil, errors.Wrap(err, "escaping arguments")
	}
	return d.QueryPrimitive(s, field, a...)
}

// QueryPrimitive returns a function that allowss recovering the results of the query but to a slice
// of a primitive type, only allowed if the query fetches one field.
func (d *DB) QueryPrimitive(statement string, field string, args ...interface{}) (connection.ResultFetch, error) {
	var rows *sql.Rows
	var err error
	var connQ func(string, ...interface{}) (*sql.Rows, error)
	if d.tx != nil {
		connQ = d.tx.Query
	} else if d.conn != nil {
		connQ = d.conn.Query
	} else {
		return nil, gaumErrors.NoDB
	}

	if len(args) != 0 {
		rows, err = connQ(statement, args...)
	} else {
		rows, err = connQ(statement)
	}
	if err != nil {
		return func(interface{}) error { return nil },
			errors.Wrap(err, "querying database")
	}
	return func(destination interface{}) error {
		defer rows.Close()
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
				defer rows.Close()
				return errors.Wrap(err, "scanning values into recipient, connection was closed")
			}
			// Add to the passed slice, this will actually add to an already populated slice if one
			// passed, how cool is that?
			destinationSlice.Set(reflect.Append(destinationSlice, newElemPtr.Elem()))
		}
		return nil
	}, nil
}

// EQuery calls EscapeArgs before invoking Query
func (d *DB) EQuery(statement string, fields []string, args ...interface{}) (connection.ResultFetch, error) {
	s, a, err := connection.EscapeArgs(statement, args)
	if err != nil {
		return nil, errors.Wrap(err, "escaping arguments")
	}
	return d.Query(s, fields, a...)
}

// Query returns a function that allows recovering the results of the query, beware the connection
// is held until the returned closusure is invoked.
func (d *DB) Query(statement string, fields []string, args ...interface{}) (connection.ResultFetch, error) {
	var rows *sql.Rows
	var err error
	var connQ func(string, ...interface{}) (*sql.Rows, error)
	if d.tx != nil {
		connQ = d.tx.Query
	} else if d.conn != nil {
		connQ = d.conn.Query
	} else {
		return nil, gaumErrors.NoDB
	}
	if len(args) != 0 {
		rows, err = connQ(statement, args...)
	} else {
		rows, err = connQ(statement)
	}
	if err != nil {
		return func(interface{}) error { return nil },
			errors.Wrap(err, "querying database")
	}
	var fieldMap map[string]reflect.StructField

	return func(destination interface{}) error {
		defer rows.Close()
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
			newElem := newElemPtr.Elem()
			// Get it's type.
			ttod := newElem.Type()

			// map the fields of the type to their potential sql names, this is the only "magic"
			fieldMap = make(map[string]reflect.StructField, ttod.NumField())
			_, fieldMap, err = srm.MapFromTypeOf(newElemPtr.Elem().Type(),
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
		return nil
	}, nil
}

// ERaw calls EscapeArgs before invoking Raw
func (d *DB) ERaw(statement string, args []interface{}, fields ...interface{}) error {
	s, a, err := connection.EscapeArgs(statement, args)
	if err != nil {
		return errors.Wrap(err, "escaping arguments")
	}
	return d.Raw(s, a, fields)
}

// Raw will run the passed statement with the passed args and scan the first resul, if any,
// to the passed fields.
func (d *DB) Raw(statement string, args []interface{}, fields ...interface{}) error {
	var rows *sql.Row

	if d.execTimeout != nil {
		ctx, cancel := context.WithTimeout(context.TODO(), *d.execTimeout)
		defer cancel()
		if d.tx != nil {
			rows = d.tx.QueryRowContext(ctx, statement, args...)
		} else if d.conn != nil {
			rows = d.conn.QueryRow(statement, args...)
		} else {
			return gaumErrors.NoDB
		}
	} else {
		if d.tx != nil {
			rows = d.tx.QueryRow(statement, args...)
		} else if d.conn != nil {
			rows = d.conn.QueryRow(statement, args...)
		} else {
			return gaumErrors.NoDB
		}
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
func (d *DB) EExec(statement string, args ...interface{}) error {
	s, a, err := connection.EscapeArgs(statement, args)
	if err != nil {
		return errors.Wrap(err, "escaping arguments")
	}
	return d.Exec(s, a)
}

// Exec will run the statement and expect nothing in return.
func (d *DB) Exec(statement string, args ...interface{}) error {
	_, err := d.exec(statement, args...)
	return err
}

// ExecResult will run the statement and return the number of rows affected.
func (d *DB) ExecResult(statement string, args ...interface{}) (int64, error) {
	connTag, err := d.exec(statement, args...)
	if err != nil {
		return 0, err
	}
	rowsAffected, err := connTag.RowsAffected()
	if err != nil {
		return 0, errors.Wrap(err, "reading rowsAffected from connTag")
	}
	return rowsAffected, nil
}

func (d *DB) exec(statement string, args ...interface{}) (sql.Result, error) {
	var connTag sql.Result
	var err error

	if d.execTimeout != nil {
		ctx, cancel := context.WithTimeout(context.TODO(), *d.execTimeout)
		defer cancel()
		if d.tx != nil {
			connTag, err = d.tx.ExecContext(ctx, statement, args...)
		} else if d.conn != nil {
			connTag, err = d.conn.ExecContext(ctx, statement, args...)
		} else {
			return nil, gaumErrors.NoDB
		}
	} else {
		if d.tx != nil {
			connTag, err = d.tx.Exec(statement, args...)
		} else if d.conn != nil {
			connTag, err = d.conn.Exec(statement, args...)
		} else {
			return nil, gaumErrors.NoDB
		}
	}
	if err != nil {
		return nil, errors.Wrapf(err, "querying database, obtained %s", connTag)
	}
	return connTag, nil
}

// BeginTransaction returns a new DB that will use the transaction instead of the basic conn.
// if the transaction is already started the same will be returned.
func (d *DB) BeginTransaction() (connection.DB, error) {
	if d.tx != nil {
		return nil, gaumErrors.AlreadyInTX
	}
	tx, err := d.conn.Begin()
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

// CommitTransaction commits the transaction if any is in course, beavior comes straight from
// pgx.
func (d *DB) CommitTransaction() error {
	if d.tx == nil {
		return gaumErrors.NoTX
	}

	return d.tx.Commit()
}

// RollbackTransaction rolls back the transaction if any is in course, beavior comes straight from
// pgx.
func (d *DB) RollbackTransaction() error {
	if d.tx == nil {
		return gaumErrors.NoTX
	}
	return d.tx.Rollback()
}

// Set tries to run `SET LOCAL` with the passed parameters if there is an ongoing transaction.
// https://www.postgresql.org/docs/9.2/static/sql-set.html
func (d *DB) Set(set string) error {
	if d.tx == nil {
		return gaumErrors.NoTX
	}
	// TODO check if this will work in the `SET LOCAL $1` arg format
	cTag, err := d.tx.Exec("SET LOCAL " + set)
	if err != nil {
		return errors.Wrapf(err, "trying to set local, returned: %s", cTag)
	}
	return nil
}

// BulkInsert only works with pgx driver.
func (d *DB) BulkInsert(tableName string, columns []string, values [][]interface{}) (execError error) {
	return gaumErrors.NotImplemented
}
