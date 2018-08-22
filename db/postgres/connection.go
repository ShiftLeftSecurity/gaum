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
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"time"

	gaumErrors "github.com/ShiftLeftSecurity/gaum/db/errors"
	"github.com/ShiftLeftSecurity/gaum/db/logging"
	"github.com/ShiftLeftSecurity/gaum/db/srm"
	"github.com/pkg/errors"

	"github.com/ShiftLeftSecurity/gaum/db/connection"
	"github.com/jackc/pgx"
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

	conn, err := pgx.NewConnPool(config)
	if err != nil {
		return nil, errors.Wrap(err, "connecting to postgres database")
	}
	return &DB{
		conn:        conn,
		logger:      conLogger,
		execTimeout: ci.QueryExecTimeout,
	}, nil
}

// DB wraps pgx.Conn into a struct that implements connection.DB
type DB struct {
	conn        *pgx.ConnPool
	tx          *pgx.Tx
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

// QueryIter returns an iterator that can be used to fetch results one by one, beware this holds
// the connection until fetching is done.
// the passed fields are supposed to correspond to the fields being brought from the db, no
// check is performed on this.
func (d *DB) QueryIter(statement string, fields []string, args ...interface{}) (connection.ResultFetchIter, error) {
	var rows *pgx.Rows
	var err error
	d.logger.Debug(fmt.Sprintf("will use fields: %#v", fields))
	var connQ func(string, ...interface{}) (*pgx.Rows, error)
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
	if len(fields) == 0 {
		// This seems to make a query each time so perhaps it goes outside.
		sqlQueryfields := rows.FieldDescriptions()
		fields = make([]string, len(sqlQueryfields), len(sqlQueryfields))
		for i, v := range sqlQueryfields {
			fields[i] = v.Name
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

		return rows.Next(), rows.Close, nil
	}, nil
}

// QueryPrimitive returns a function that allowss recovering the results of the query but to a slice
// of a primitive type, only allowed if the query fetches one field.
func (d *DB) QueryPrimitive(statement string, field string, args ...interface{}) (connection.ResultFetch, error) {
	var rows *pgx.Rows
	var err error
	var connQ func(string, ...interface{}) (*pgx.Rows, error)
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

// Query returns a function that allows recovering the results of the query, beware the connection
// is held until the returned closusure is invoked.
func (d *DB) Query(statement string, fields []string, args ...interface{}) (connection.ResultFetch, error) {
	var rows *pgx.Rows
	var err error
	var connQ func(string, ...interface{}) (*pgx.Rows, error)
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

		if len(fields) == 0 {
			// This seems to make a query each time so perhaps it goes outside.
			sqlQueryfields := rows.FieldDescriptions()
			fields = make([]string, len(sqlQueryfields), len(sqlQueryfields))
			for i, v := range sqlQueryfields {
				fields[i] = v.Name
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
				defer rows.Close()
				return errors.Wrapf(err, "cant fetch data into %T", destination)
			}

			// Construct the recipient fields.
			fieldRecipients := srm.FieldRecipientsFromValueOf(d.logger, fields, fieldMap, newElem)

			// Try to fetch the data
			err = rows.Scan(fieldRecipients...)
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

// Raw will run the passed statement with the passed args and scan the first resul, if any,
// to the passed fields.
func (d *DB) Raw(statement string, args []interface{}, fields ...interface{}) error {
	var rows *pgx.Row

	if d.execTimeout != nil {
		ctx, cancel := context.WithTimeout(context.TODO(), *d.execTimeout)
		defer cancel()
		if d.tx != nil {
			rows = d.tx.QueryRowEx(ctx, statement, nil, args...)
		} else if d.conn != nil {
			rows = d.conn.QueryRowEx(ctx, statement, nil, args...)
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
	if err == pgx.ErrNoRows {
		return gaumErrors.ErrNoRows
	}
	if err != nil {
		return errors.Wrap(err, "scanning values into recipient")
	}
	return nil
}

// Exec will run the statement and expect nothing in return.
func (d *DB) Exec(statement string, args ...interface{}) error {
	var connTag pgx.CommandTag
	var err error

	if d.execTimeout != nil {
		ctx, cancel := context.WithTimeout(context.TODO(), *d.execTimeout)
		defer cancel()
		if d.tx != nil {
			connTag, err = d.tx.ExecEx(ctx, statement, nil, args...)
		} else if d.conn != nil {
			connTag, err = d.conn.ExecEx(ctx, statement, nil, args...)
		} else {
			return gaumErrors.NoDB
		}
	} else {
		if d.tx != nil {
			connTag, err = d.tx.Exec(statement, args...)
		} else if d.conn != nil {
			connTag, err = d.conn.Exec(statement, args...)
		} else {
			return gaumErrors.NoDB
		}
	}
	if err != nil {
		return errors.Wrapf(err, "querying database, obtained %s", connTag)
	}
	return nil
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

// BulkInsert will use postgres copy function to try to insert a lot of data.
// You might need to use pgx types for the values to reduce probability of failure.
// https://godoc.org/github.com/jackc/pgx#Conn.CopyFrom
func (d *DB) BulkInsert(tableName string, columns []string, values [][]interface{}) (execError error) {
	//func (c *Conn) CopyFrom(tableName Identifier, columnNames []string, rowSrc CopyFromSource) (int, error)
	tx := d.tx
	if d.tx == nil {
		var err error
		tx, err = d.conn.Begin()
		if err != nil {
			return errors.Wrap(err, "beginning transaction for bulk insert")
		}
		defer func() {
			if execError != nil {
				err := tx.Rollback()
				execError = errors.Wrapf(execError,
					"there was a failure running the expression and also rolling back te transaction: %v",
					err)
			} else {
				err := tx.Commit()
				execError = errors.Wrap(err, "could not commit the transaction")
			}
		}()
	}
	copySource := pgx.CopyFromRows(values)
	rowsAffected, err := tx.CopyFrom(pgx.Identifier{tableName}, columns, copySource)
	if rowsAffected != len(values) {
		return errors.Errorf("%d rows were passed but only %d inserted, will rollback",
			len(values), rowsAffected)
	}
	if err != nil {
		return errors.Wrap(err, "bulk inserting")
	}
	return nil
}
