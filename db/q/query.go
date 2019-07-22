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

/*
Package q provides a simple way to interact with a DataBase and craft queryes using gaum
through the Q struct and its helpers you can use most of gaum feature in a simple and
intuitive way that somehow is reminiscent of some go ORMs.
This package API might change overtime given that is being created from ux feedback
from our users.
*/
package q

import (
	"github.com/pkg/errors"

	c "github.com/ShiftLeftSecurity/gaum/db/chain"
	"github.com/ShiftLeftSecurity/gaum/db/connection"
	"github.com/ShiftLeftSecurity/gaum/db/logging"
	"github.com/ShiftLeftSecurity/gaum/db/postgres"
	"github.com/ShiftLeftSecurity/gaum/db/postgrespq"
)

// Driver represent the possible db connection drivers.
type Driver int

const (
	// PGX is Jackc [pgx driver](github.com/jackc/pgx) (binary)
	PGX Driver = iota
	// PQ is Postgres default driver (text)
	PQ
)

type connConstructor func(string) connection.DatabaseHandler

var driverConnectors = map[Driver]connConstructor{
	PGX: func(cs string) connection.DatabaseHandler {
		return &postgres.Connector{ConnectionString: cs}
	},
	PQ: func(cs string) connection.DatabaseHandler {
		return &postgrespq.Connector{ConnectionString: cs}
	},
}

// RawQueryOne runs the passed in <query> with the safely inserted <args> through <db> and fetches
// the first value into <recipient>.RawQueryOne
// <receiver> must be of a type that supports de-serialization of all columns into it.
func RawQueryOne(db connection.DB, recipient interface{}, query string, args ...interface{}) error {
	escapedQuery, explodedArgs, err := c.MarksToPlaceholders(query, args)
	if err != nil {
		return errors.Wrap(err, "escaping question marks in query")
	}
	fetcher, err := db.QueryIter(escapedQuery, []string{}, explodedArgs)
	if err != nil {
		return errors.Wrap(err, "querying database")
	}
	_, closer, err := fetcher(recipient)
	if err != nil {
		return errors.Wrap(err, "fetching data from database")
	}
	defer closer()
	return nil
}

// RawQuery runs the passed in <query> with the safely inserted <args> through <db> and fetches
// the values into <recipientSlice> that must be a slice of a type that supports de-serialization
// of all columns into it.
func RawQuery(db connection.DB, recipientSlice interface{}, query string, args ...interface{}) error {
	escapedQuery, explodedArgs, err := c.MarksToPlaceholders(query, args)
	if err != nil {
		return errors.Wrap(err, "escaping question marks in query")
	}
	fetcher, err := db.Query(escapedQuery, []string{}, explodedArgs)
	if err != nil {
		return errors.Wrap(err, "querying database")
	}
	err = fetcher(recipientSlice)
	if err != nil {
		return errors.Wrap(err, "fetching data from database")
	}
	return nil
}

// RawExec runs the passed in <query> with the safely inserted <args> through <db>, no values are
// returned except for success/error.
func RawExec(db connection.DB, query string, args ...interface{}) error {
	escapedQuery, explodedArgs, err := c.MarksToPlaceholders(query, args)
	if err != nil {
		return errors.Wrap(err, "escaping question marks in query")
	}
	err = db.Exec(escapedQuery, explodedArgs)
	if err != nil {
		return errors.Wrap(err, "executing statement")
	}
	return nil
}

// NewDB crafts a new `connection.DB` from the passed connection string, using the passed
// in <driver> and with the passed in <logger> and <logLevel> set.
// If you want more customization into your DB connection please refer to the documentation for
// `connection.DB` and `connection.Information` and the respective drivers:
//
// * github.com/ShiftLeftSecurity/gaum/db/postgres
//
// * github.com/ShiftLeftSecurity/gaum/db/postgrespq
func NewDB(connectionString string, driver Driver,
	logger logging.Logger, logLevel connection.LogLevel) (connection.DB, error) {
	buildConnector, exists := driverConnectors[driver]
	if !exists {
		return nil, errors.Errorf("the passed driver %d is not valid", driver)
	}
	connector := buildConnector(connectionString)
	connectionInfo := &connection.Information{
		Logger:   logger,
		LogLevel: logLevel,
	}
	dbConnection, err := connector.Open(connectionInfo)
	if err != nil {
		return nil, errors.Wrap(err, "opening a new connection to the database")
	}
	return dbConnection, nil
}

// New crafts a new Q query containing a db connection to the db specified by connectionString
// and the selected driver and logging settings.
func New(connectionString string, driver Driver,
	logger logging.Logger, logLevel connection.LogLevel) (*Q, error) {
	buildConnector, exists := driverConnectors[driver]
	if !exists {
		return nil, errors.Errorf("the passed driver %d is not valid", driver)
	}
	connector := buildConnector(connectionString)
	connectionInfo := &connection.Information{
		Logger:   logger,
		LogLevel: logLevel,
	}
	dbConnection, err := connector.Open(connectionInfo)
	if err != nil {
		return nil, errors.Wrap(err, "opening a new connection to the database")
	}
	queryChain := c.NewExpressionChain(dbConnection)
	return &Q{query: queryChain}, nil
}

// NewFromDB crafts a new Q query containing the passed db connection.
func NewFromDB(dbConnection connection.DB) (*Q, error) {
	queryChain := c.NewExpressionChain(dbConnection)
	return &Q{query: queryChain}, nil
}

// Q is the intended struct for interaction with SQL Queries.
type Q struct {
	query *c.ExpressionChain
}

// Select converts the existing Q query into a `SELECT ...` SQL statement, query is the
// actual body of the statement for example`column1, column2, expression, column4 AS alias`
// you can use `?` as a placeholder for values to be safely passed as variadic arguments after
// the expression
func (q *Q) Select(query string, args ...interface{}) *Q {
	if len(args) == 0 {
		q.query.Select(query)
		return q
	}
	q.query.SelectWithArgs(
		c.SelectArgument{
			Field: query,
			Args:  args,
		},
	)
	return q
}

// Insert converts the existing Q query into an `INSERT INTO ...` SQL statement, the passed
// map comprises the fields, represented by the keys of the map and values,
// represented by the values of the map to be inserted, Order in which the pair will appear is
// not guaranteed given go's map implementation (of course key/value will always be in the
// possition corresponding with each other within the query)
func (q *Q) Insert(insertPairs map[string]interface{}) *Q {
	q.query.Insert(insertPairs)
	return q
}

// Update converts the existing Q query into an `UPDATE ...` SQL statement, the passed map
// will be used to set column names (from map keys) and new values (from map values)
// the order of the assignements within the query is not guaranteed given go's map
// implementation so even if the resulting query of multiple calls might differ in the `SET`
// section it will be equivalent.
func (q *Q) Update(exprMap map[string]interface{}) *Q {
	q.query.UpdateMap(exprMap)
	return q
}

// Delete converts the existing Q query into an `DELETE FROM ...` SQL statement, be very mindful
// when using this since it can easily create a WHERE-less DELETE if you forget to invoke proper
// `AndWhere`/`OrWhere` statement before executing it.
func (q *Q) Delete() *Q {
	q.query.Delete()
	return q
}

// From sets the table or tables in which the SQL statement defined by the Q query will operate
// this method receives a free form string so you might as well pass a list of columns comma
// separated or actually anything that is valid input for a SQL `FROM` statement.
func (q *Q) From(table string) *Q {
	q.query.From(table)
	return q
}

// Join adds a `JOIN <table> ON <expression>` SQL statement to your Q query, the <on> argument
// can contain any valid SQL expression for the `ON` section of a JOIN
// you can use `?` as a placeholder for values to be safely passed as variadic arguments after
// the <on> argument
func (q *Q) Join(table string, on string, args ...interface{}) *Q {
	q.query.Join(table, on, args...)
	return q
}

// LeftJoin adds a `LEFT JOIN <table> ON <expression>` SQL statement to your Q query, the <on>
// argument can contain any valid SQL expression for the `ON` section of a JOIN
// you can use `?` as a placeholder for values to be safely passed as variadic arguments after
// the <on> argument
func (q *Q) LeftJoin(table string, on string, args ...interface{}) *Q {
	q.query.LeftJoin(table, on, args...)
	return q
}

// RightJoin adds a `RIGHT JOIN <table> ON <expression>` SQL statement to your Q query, the <on>
// argument can contain any valid SQL expression for the `ON` section of a JOIN
// you can use `?` as a placeholder for values to be safely passed as variadic arguments after
// the <on> argument
func (q *Q) RightJoin(table string, on string, args ...interface{}) *Q {
	q.query.RightJoin(table, on, args...)
	return q
}

// InnerJoin adds a `INNER JOIN <table> ON <expression>` SQL statement to your Q query, the <on>
// argument can contain any valid SQL expression for the `ON` section of a JOIN
// you can use `?` as a placeholder for values to be safely passed as variadic arguments after
// the <on> argument
func (q *Q) InnerJoin(table string, on string, args ...interface{}) *Q {
	q.query.InnerJoin(table, on, args...)
	return q
}

// OuterJoin adds a `OUTER JOIN <table> ON <expression>` SQL statement to your Q query, the <on>
// argument can contain any valid SQL expression for the `ON` section of a JOIN
// you can use `?` as a placeholder for values to be safely passed as variadic arguments after
// the <on> argument
func (q *Q) OuterJoin(table string, on string, args ...interface{}) *Q {
	q.query.OuterJoin(table, on, args...)
	return q
}

// AndWhere adds a `WHERE` condition section that can be:
//
// * The first one (decided in arbitrary way among all `AndWhere` expressions)
//
// * One of many that will be pre-pend by `AND` if it's not the first
//
// * The only condition.
//
// you might pass anything that is valid within a `WHERE` condition as expression, even a group
// of conditions separated by AND/OR in plain text.
// You can use `?` as a placeholder for values to be safely passed as variadic arguments after
// the expression.
func (q *Q) AndWhere(expr string, args ...interface{}) *Q {
	q.query.AndWhere(expr, args...)
	return q
}

// OrWhere adds a `WHERE` condition section that can be:
//
// * The first one if no `AndWhere` was invoked
//
// * One of many that will be pre-pend by `OR` if it's not the first
//
// * The only condition (although convention dictates that you use `AndWhere` in this case).
//
// you might pass anything that is valid within a `WHERE` condition as expression, even a group
// of conditions separated by AND/OR in plain text.
// You can use `?` as a placeholder for values to be safely passed as variadic arguments after
// the expression.
func (q *Q) OrWhere(expr string, args ...interface{}) *Q {
	q.query.OrWhere(expr, args...)
	return q
}

// OrderBy adds an ordering criteria to the Q query, you can either create an ordering operator
// by chaining all fields in it or invoke multiple times OrderBy, please refer to the
// documentation of `chain.OrderByOperator`.
func (q *Q) OrderBy(order *c.OrderByOperator) *Q {
	q.query.OrderBy(order)
	return q
}

// GroupBy adds a grouping criteria to the Q query, you may pass any valid column that SQL
// accepts as an ordering criteria.
func (q *Q) GroupBy(expr string) *Q {
	q.query.GroupBy(expr)
	return q
}

// Limit sets a result returning limit to the Q query, calling `Limit` multiple times overrides
// previous calls.
func (q *Q) Limit(limit int64) *Q {
	q.query.Limit(limit)
	return q
}

// Offset sets a result returning offset to the Q query, calling `Offset` multiple times
// overrides previous calls
func (q *Q) Offset(offset int64) *Q {
	q.query.Offset(offset)
	return q
}

// OnConflict allows to set behavior for the RDBMS to act upon a conflict triggered, please go
// to `chain.OnConflict` doc for references on all possible options.
func (q *Q) OnConflict(clause func(*c.OnConflict)) *Q {
	q.query.OnConflict(clause)
	return q
}

// Returning will add an "RETURNING" clause at the end of the query if the main operation
// is an INSERT, if you do this bear in mind that you will need to execute the Q query
// with `QueryOne` instead of `Exec`
func (q *Q) Returning(args ...string) *Q {
	q.query.Returning(args...)
	return q
}

// QueryOne executes and fetches one row of the result into <receiver>, ideally use this in
// conjunction with `.Limit(1)` or in queries that are not expected to return more than one
// value since the underlying query Will be executed infull but just one result will be
// retrieved before dropping the result set.
//
// <receiver> must be of a type that supports de-serialization of all columns into it.
// This works with `SELECT` and `INSERT INTO ... RETURNING ...`
func (q *Q) QueryOne(receiver interface{}) error {
	fetcher, err := q.query.QueryIter()
	if err != nil {
		return errors.Wrap(err, "running query")
	}
	_, closer, err := fetcher(receiver)
	if err != nil {
		return errors.Wrap(err, "fetching data")
	}
	defer closer()
	return nil
}

// QueryMany executes and fetches all results from a query into <receiverSlice> which is
// expected to be a slice of a type that supports de-serialization of all columns into it.
//
// This works with `SELECT` and `INSERT INTO ... RETURNING ...`
func (q *Q) QueryMany(receiverSlice interface{}) error {
	fetcher, err := q.query.Query()
	if err != nil {
		return errors.Wrap(err, "running query")
	}
	err = fetcher(receiverSlice)
	if err != nil {
		return errors.Wrap(err, "fetching data")
	}
	return nil
}

// Exec executes the query in Q not expecting nor returning any results other than success/error
// This works with any statement not returning values and potentially the ones returning values
// too but values are ignored (untested claim)
func (q *Q) Exec() error {
	return q.query.Exec()
}

// DB returns the `connection.DB` being used for this Q query execution.
func (q *Q) DB() connection.DB {
	return q.query.DB()
}
