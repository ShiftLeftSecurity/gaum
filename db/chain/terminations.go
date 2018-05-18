package chain

import (
	"github.com/perrito666/bmstrem/db/connection"
	"github.com/pkg/errors"
)

// QueryIter is a convenience function to run the current chain through the db query with iterator.
func (ec *ExpresionChain) QueryIter() (connection.ResultFetchIter, error) {
	if ec.mainOperation.segment != sqlSelect {
		return func(interface{}) (bool, func(), error) { return false, func() {}, nil },
			errors.Errorf("cannot invoke query iter with statements other than SELECT, please use Exec")
	}
	q, args, err := ec.Render()
	if err != nil {
		return func(interface{}) (bool, func(), error) { return false, func() {}, nil },
			errors.Wrap(err, "rendering query to query with iterator")
	}
	return ec.db.QueryIter(q, ec.mainOperation.fields(), args...)
}

// Query is a convenience function to run the current chain through the db query with iterator.
func (ec *ExpresionChain) Query() (connection.ResultFetch, error) {
	if ec.mainOperation.segment != sqlSelect {
		return func(interface{}) error { return nil },
			errors.Errorf("cannot invoke query with statements other than SELECT, please use Exec")
	}
	q, args, err := ec.Render()
	if err != nil {
		return func(interface{}) error { return nil },
			errors.Wrap(err, "rendering query to query")
	}
	return ec.db.Query(q, ec.mainOperation.fields(), args...)
}

// Exec executes the chain, works for Insert and Update
func (ec *ExpresionChain) Exec() (execError error) {
	var q string
	var args []interface{}
	q, args, execError = ec.Render()
	if execError != nil {
		return errors.Wrap(execError, "rendering query to exec")
	}
	var db connection.DB

	if ec.set != "" && !ec.db.IsTransaction() {
		db, execError = ec.db.BeginTransaction()
		if execError != nil {
			return errors.Wrap(execError, "starting transaction to run SET LOCAL")
		}
		defer func() {
			// TODO log if either rb or commit failed
			if execError != nil {
				err := db.RollbackTransaction()
				execError = errors.Wrapf(execError,
					"there was a failure running the expression and also rolling back te transaction: %v",
					err)
			} else {
				err := db.CommitTransaction()
				execError = errors.Wrap(err, "could not commit the transaction")
			}
		}()
	}

	if ec.set != "" && ec.db.IsTransaction() {
		execError = db.Set(ec.set)
		if execError != nil {
			return errors.Wrap(execError, "running set for this transaction")
		}
	}

	return db.Exec(q, args...)
}

// Raw executes the query and tries to scan the result into fields without much safeguard nor
// intelligence so you will have to put some of your own
func (ec *ExpresionChain) Raw(fields ...interface{}) error {
	if ec.mainOperation.segment != sqlSelect {
		return errors.Errorf("cannot invoke query with statements other than SELECT, please use Exec")
	}
	q, args, err := ec.Render()
	if err != nil {
		errors.Wrap(err, "rendering query to raw query")
	}
	return ec.db.Raw(q, args, fields...)
}

// TODO Add Set like gorm (I have no clue what it does) https://www.postgresql.org/docs/9.2/static/sql-set.html
// use SET LOCAL and wrap in a transaction when invoked.

// TODO add batch running of many chains.

// TODO add transaction object that must contain array of chains, for this chain must contain a TX
// method that takes a db conn (with tx begin called) and each run must work transactionally so the
// Tx object can be reused if necessary.

// TODO Add pg Copy feature where possible to handle large inserts.
