package postgres

import (
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

// Open opens a connection to postgres and returns it wrapped into a connection.DB
func (c *Connector) Open(ci *connection.Information) (connection.DB, error) {
	// Ill be opinionated here and use the most efficient params.
	config := pgx.ConnConfig{
		Host:     ci.Host,
		Port:     ci.Port,
		Database: ci.Database,
		User:     ci.User,
		Password: ci.Password,

		TLSConfig:         ci.TLSConfig,
		UseFallbackTLS:    ci.UseFallbackTLS,
		FallbackTLSConfig: ci.FallbackTLSConfig,
		Logger:            logging.NewPgxLogAdapter(ci.Logger),
	}
	conn, err := pgx.Connect(config)
	if err != nil {
		return nil, errors.Wrap(err, "connecting to postgres database")
	}
	return &DB{conn: conn}, nil
}

// DB wraps pgx.Conn into a struct that implements connection.DB
type DB struct {
	conn *pgx.Conn
}

// Clone returns a copy of DB with the same underlying Connection
func (d *DB) Clone() connection.DB {
	return &DB{conn: d.conn}
}
