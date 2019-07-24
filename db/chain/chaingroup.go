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

import "github.com/pkg/errors"

// Group allows to group a set of expressions and run them together
// in a transaction.
type Group struct {
	chains []*ExpressionChain
	set    string
}

// Set will cause `SET LOCAL` to be run with this value before executing items of the group
// in Run.
func (cg *Group) Set(set string) {
	cg.set = set
}

// Add appends a chain to the group.
func (cg *Group) Add(ec *ExpressionChain) {
	cg.chains = append(cg.chains, ec)
}

// Run runs all the chains in a group in a transaction, for this the db of the first query
// will be used.
func (cg *Group) Run() (execError error) {
	if len(cg.chains) == 0 {
		return nil
	}
	for _, op := range cg.chains {
		if op.mainOperation.segment == sqlSelect {
			return errors.Errorf("cannot query as part of a chain.")
		}
	}
	db := cg.chains[0].db
	txdb, err := db.BeginTransaction()
	if err != nil {
		return errors.Wrap(err, "getting transaction to run chain group")
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

	if cg.set != "" {
		err := txdb.Set(cg.set)
		if err != nil {
			return errors.Wrapf(err, "setting %q to the transaction", cg.set)
		}
	}

	for _, op := range cg.chains {
		query, args, err := op.Render()
		if err != nil {
			return errors.Wrap(err, "rendeding part of chain transaction")
		}
		err = txdb.Exec(query, args...)
		if err != nil {
			return errors.Wrap(err, "error executing query in group")
		}
	}
	return nil
}
