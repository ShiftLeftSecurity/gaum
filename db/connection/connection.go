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

	Logger logging.Logger
}

// DatabaseHandler represents the boundary with a db.
type DatabaseHandler interface {
	// Open must be able to connect to the handled engine and return a db.
	Open(*Information) (DB, error)
}

// DB represents an active database connection.
type DB interface {
	// Clone returns a stateful copy of this connection.
	Clone() DB
}
