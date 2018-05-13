package chain

import (
	"github.com/perrito666/bmstrem/db/connection"
	"github.com/pkg/errors"
)

// QueryIter is a convenience function to run the current chain through the db query with iterator.
func (ec *ExpresionChain) QueryIter(statement string, args ...interface{}) (connection.ResultFetchIter, error) {
	q, args, err := ec.Render()
	if err != nil {
		return func(interface{}) (bool, func(), error) { return false, func() {}, nil },
			errors.Wrap(err, "rendering query to query with iterator")
	}
	return ec.db.QueryIter(q, args...)
}

// Run executes the chain, works for Insert and Update
func (ec *ExpresionChain) Exec() error {
	return nil
}

// TODO add batch running of many chains.

// TODO add transaction object that must contain array of chains, for this chain must contain a TX
// method that takes a db conn (with tx begin called) and each run must work transactionally so the
// Tx object can be reused if necessary.

// TODO Add pg Copy feature where possible to handle large inserts.

// TODO Add iterator method that allows to fetch rows one at a time by returning a
// `func next(result interface{}) (bool, error)` that allows iterating over results and a `end()`
// that allows dropping the query. (next must be a closure to avoid reflection at every row)
