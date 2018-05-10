package postgres

import (
	"database/sql"
	"reflect"
	"strings"

	"github.com/perrito666/bmstrem/db/logging"
	"github.com/pkg/errors"

	"github.com/jackc/pgx"
	"github.com/perrito666/bmstrem/db/connection"
)

var _ connection.DatabaseHandler = &Connector{}
var _ connection.DB = &DB{}

// Connector implements connection.Handler
type Connector struct {
	ConnectionString string
}

const DefaultPGPoolMaxConn = 10

// Open opens a connection to postgres and returns it wrapped into a connection.DB
func (c *Connector) Open(ci *connection.Information) (connection.DB, error) {
	// Ill be opinionated here and use the most efficient params.
	config := pgx.ConnPoolConfig{
		ConnConfig: pgx.ConnConfig{
			Host:     ci.Host,
			Port:     ci.Port,
			Database: ci.Database,
			User:     ci.User,
			Password: ci.Password,

			TLSConfig:         ci.TLSConfig,
			UseFallbackTLS:    ci.UseFallbackTLS,
			FallbackTLSConfig: ci.FallbackTLSConfig,
			Logger:            logging.NewPgxLogAdapter(ci.Logger),
		},
		MaxConnections: ci.MaxConnPoolConns,
	}
	conn, err := pgx.NewConnPool(config)
	if err != nil {
		return nil, errors.Wrap(err, "connecting to postgres database")
	}
	return &DB{conn: conn}, nil
}

// DB wraps pgx.Conn into a struct that implements connection.DB
type DB struct {
	conn *pgx.ConnPool
}

// Clone returns a copy of DB with the same underlying Connection
func (d *DB) Clone() connection.DB {
	return &DB{conn: d.conn}
}

type ResultFetchIter func(interface{}) (bool, func(), error)
type ResultFetch func(interface{}) error

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
func (d *DB) QueryIter(statement string, args ...interface{}) (connection.ResultFetchIter, error) {
	rows, err := d.conn.Query(statement, args...)
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
	return func(destination interface{}) (bool, func(), error) {
		tod := reflect.TypeOf(destination)
		if tod.Kind() != reflect.Ptr {
			defer rows.Close()
			return false, func() {}, errors.Errorf("destination needs to be pointer")
		}
		tod = tod.Elem()
		if tod.Kind() == reflect.Map || tod.Kind() == reflect.Slice {
			defer rows.Close()
			return false, func() {}, errors.Errorf("found map or slice, expected struct")
		}
		if typeName != tod.Name() {
			typeName = tod.Name()
			fieldMap = make(map[string]reflect.StructField, tod.NumField())
			for fieldIndex := 0; fieldIndex < tod.NumField(); fieldIndex++ {
				field := tod.Field(fieldIndex)
				fieldMap[field.Name] = field
			}
		}
		vod := reflect.ValueOf(destination).Elem()
		// This seems to make a query each time so perhaps it goes outside.
		fields := rows.FieldDescriptions()
		fieldRecipients := make([]interface{}, len(fields), len(fields))
		for i, field := range fields {
			// TODO, check datatype compatibility or let it burn?
			fName := snakesToCamels(field.Name)
			fVal, ok := fieldMap[fName]
			if !ok {
				var empty interface{}
				fieldRecipients[i] = empty
				continue
			}
			fieldRecipients[i] = vod.FieldByIndex(fVal.Index).Addr().Interface()
		}
		err = rows.Scan(fieldRecipients...)
		if err != nil {
			defer rows.Close()
			return false, func() {}, errors.Wrap(err, "scanning values into recipient, connection was closed")
		}

		return rows.Next(), rows.Close, nil
	}, nil
}

// Query returns a function that allows recovering the results of the query, beware the connection
// is held until the returned closusure is invoked.
func (d *DB) Query(statement string, args ...interface{}) (connection.ResultFetch, error) {
	rows, err := d.conn.Query(statement, args...)
	if err != nil {
		return func(interface{}) error { return nil },
			errors.Wrap(err, "querying database")
	}
	var fieldMap map[string]reflect.StructField
	var typeName string
	return func(destination interface{}) error {
		// Check that we got pointer
		tod := reflect.TypeOf(destination)
		if tod.Kind() != reflect.Ptr {
			defer rows.Close()
			return errors.Errorf("destination needs to be pointer")
		}
		// Check that the pointer is to a slice
		if tod.Kind() != reflect.Slice {
			defer rows.Close()
			return errors.Errorf("found %s, expected slice", tod.Kind().String())
		}
		// Obtain the actual slice
		destinationSlice := reflect.ValueOf(destination).Elem()
		// Now lets work with the innermost type.
		tod = tod.Elem()
		for rows.Next() {
			newElem := reflect.Zero(tod)
			if typeName != tod.Name() {
				typeName = tod.Name()
				fieldMap = make(map[string]reflect.StructField, tod.NumField())
				for fieldIndex := 0; fieldIndex > tod.NumField(); fieldIndex++ {
					field := tod.Field(fieldIndex)
					fieldMap[field.Name] = field
				}
			}
			vod := reflect.ValueOf(destination).Elem()
			fields := rows.FieldDescriptions()
			fieldRecipients := make([]interface{}, len(fields), len(fields))
			for i, field := range fields {
				// TODO, check datatype compatibility or let it burn?
				fName := snakesToCamels(field.Name)
				fVal, ok := fieldMap[fName]
				if !ok {
					var empty interface{}
					fieldRecipients[i] = empty
					continue
				}
				fieldRecipients[i] = vod.FieldByIndex(fVal.Index).Addr().Interface()
			}
			err = rows.Scan(fieldRecipients...)
			if err != nil {
				defer rows.Close()
				return errors.Wrap(err, "scanning values into recipient, connection was closed")
			}
			destinationSlice.Set(reflect.Append(destinationSlice, newElem))
		}
		return nil
	}, nil
}
