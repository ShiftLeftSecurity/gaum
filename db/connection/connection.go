package connection

import (
	"crypto/tls"

	"github.com/perrito666/bmstrem/db/logging"
)

// Information contains all required information to create a connection into a db.
// Copied almost verbatim from https://godoc.org/github.com/jackc/pgx#ConnConfig
type Information struct {
	Host     string // host (e.g. localhost) or path to unix domain socket directory (e.g. /private/tmp)
	Port     uint16
	Database string
	User     string
	Password string

	TLSConfig         *tls.Config // config for TLS connection -- nil disables TLS
	UseFallbackTLS    bool        // Try FallbackTLSConfig if connecting with TLSConfig fails. Used for preferring TLS, but allowing unencrypted, or vice-versa
	FallbackTLSConfig *tls.Config // config for fallback TLS connection (only used if UseFallBackTLS is true)-- nil disables TLS

	// MaxConnPoolConns where applies will be used to determine the maximum amount of connections
	// a pool can have.
	MaxConnPoolConns int

	Logger logging.Logger
}

// DatabaseHandler represents the boundary with a db.
type DatabaseHandler interface {
	// Open must be able to connect to the handled engine and return a db.
	Open(*Information) (DB, error)
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
	QueryIter(statement string, fields []string, args ...interface{}) (ResultFetchIter, error)
	Query(statement string, fields []string, args ...interface{}) (ResultFetch, error)
	// Raw ins intended to be an all raw query that runs statement with args and tries
	// to retrieve the results into fields without much magic whatsoever.
	Raw(statement string, args []interface{}, fields ...interface{}) error
	// Exec is intended for queries that do not yield results (data modifiers)
	Exec(statement string, args ...interface{}) error
	// BeginTransaction returns a new DB that will use the transaction instead of the basic conn.
	BeginTransaction() (DB, error)
	// CommitTransaction commits the transaction
	CommitTransaction() error
	// RollbackTransaction rolls back the transaction
	RollbackTransaction() error
	// IsTransaction indicates if the DB is in the middle of a transaction.
	IsTransaction() bool
	// Set allows to change settings for the current transaction.
	Set(set string) error
}
