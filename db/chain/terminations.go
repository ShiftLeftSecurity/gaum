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

package chain

import (
	"github.com/ShiftLeftSecurity/gaum/db/connection"
	gaumErrors "github.com/ShiftLeftSecurity/gaum/db/errors"
	"github.com/pkg/errors"
)

// QueryIter is a convenience function to run the current chain through the db query with iterator.
func (ec *ExpresionChain) QueryIter() (connection.ResultFetchIter, error) {
	if ec.hasErr() {
		return nil, ec.getErr()
	}
	if !ec.queryable() {
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
	if ec.hasErr() {
		return nil, ec.getErr()
	}
	if !ec.queryable() {
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

// QueryPrimitive is a convenience function to run the current chain through the db query.
func (ec *ExpresionChain) QueryPrimitive() (connection.ResultFetch, error) {
	if ec.hasErr() {
		return nil, ec.getErr()
	}
	if !ec.queryable() {
		return func(interface{}) error { return nil },
			errors.Errorf("cannot invoke query for primitives with statements other than SELECT, please use Exec")
	}
	q, args, err := ec.Render()
	if err != nil {
		return func(interface{}) error { return nil },
			errors.Wrap(err, "rendering query to query")
	}
	fields := ec.mainOperation.fields()
	if len(fields) != 1 {
		return func(interface{}) error { return nil },
			errors.Errorf("querying for primitives can be done for 1 column only, got %d",
				len(fields))
	}
	return ec.db.QueryPrimitive(q, fields[0], args...)
}

// Exec executes the chain, works for Insert and Update
func (ec *ExpresionChain) Exec() (execError error) {
	if ec.hasErr() {
		execError = ec.getErr()
		return
	}
	var q string
	var args []interface{}
	q, args, execError = ec.Render()
	if execError != nil {
		return errors.Wrap(execError, "rendering query to exec")
	}
	var db connection.DB
	// default we use the current db and transaction
	db = ec.db

	// If Set is implied, we need to start a transaction
	if ec.set != "" && !ec.db.IsTransaction() {
		db, execError = ec.db.BeginTransaction()
		if execError != nil {
			return errors.Wrap(execError, "starting transaction to run SET LOCAL")
		}
		defer func() {
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
	if ec.hasErr() {
		return ec.getErr()
	}
	if !ec.queryable() {
		return errors.Errorf("cannot invoke query with statements other than SELECT, please use Exec")
	}
	q, args, err := ec.Render()
	if err != nil {
		return errors.Wrap(err, "rendering query to raw query")
	}
	err = ec.db.Raw(q, args, fields...)
	if err == gaumErrors.ErrNoRows {
		return err
	}
	return errors.Wrap(err, "running a raw query from within a chain")
}

// TODO add batch running of many chains.

// TODO Inspect stacklocation and try re-run queryies if arguments have similiar memory address to save serialization time

// TODO Add pg Copy feature where possible to handle large inserts.

// queryable handles checking if the function returns any results
func (ec *ExpresionChain) queryable() bool {
	if ec.mainOperation.segment == sqlInsert {
		for _, segment := range ec.segments {
			if segment.segment == sqlReturning {
				return true
			}
		}
		return false
	}
	return ec.mainOperation.segment == sqlSelect
}
