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
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/ShiftLeftSecurity/gaum/db/connection"
	"github.com/pkg/errors"
)

// NewExpresionChain returns a new instance of ExpresionChain hooked to the passed DB
func NewExpresionChain(db connection.DB) *ExpresionChain {
	return &ExpresionChain{db: db}
}

// ExpresionChain holds all the atoms for the SQL expresions that make a query and allows to chain
// more assuming the chaining is valid.
type ExpresionChain struct {
	lock          sync.Mutex
	segments      []querySegmentAtom
	table         string
	mainOperation *querySegmentAtom
	ctes          map[string]*ExpresionChain
	ctesOrder     []string // because deterministic tests and co-dependency

	limit  *querySegmentAtom
	offset *querySegmentAtom

	set string

	conflict *OnConflict
	err      []error

	db connection.DB
}

// Set will produce your chain to be run inside a Transaction and used for `SET LOCAL`
// For the moment this is only used with Exec.
func (ec *ExpresionChain) Set(set string) *ExpresionChain {
	ec.set = set
	return ec
}

// NewDB sets the passed db as this chain's db.
func (ec *ExpresionChain) NewDB(db connection.DB) *ExpresionChain {
	ec.db = db
	return ec
}

// DB returns the chain DB
func (ec *ExpresionChain) DB() connection.DB {
	return ec.db
}

// Clone returns a copy of the ExpresionChain
func (ec *ExpresionChain) Clone() *ExpresionChain {
	var limit *querySegmentAtom
	var offset *querySegmentAtom
	var mainOperation *querySegmentAtom
	if ec.limit != nil {
		eclimit := ec.limit.clone()
		limit = &eclimit
	}
	if ec.offset != nil {
		ecoffset := ec.offset.clone()
		offset = &ecoffset
	}
	if ec.mainOperation != nil {
		ecmainOperation := ec.mainOperation.clone()
		mainOperation = &ecmainOperation
	}
	segments := make([]querySegmentAtom, len(ec.segments))
	for i, s := range ec.segments {
		segments[i] = s.clone()
	}
	ctes := make(map[string]*ExpresionChain, len(ec.ctes))
	order := make([]string, len(ec.ctesOrder), len(ec.ctesOrder))
	for i, k := range ec.ctesOrder {
		ctes[k] = ec.ctes[k].Clone()
		order[i] = k
	}
	return &ExpresionChain{
		limit:         limit,
		offset:        offset,
		segments:      segments,
		mainOperation: mainOperation,
		table:         ec.table,
		ctes:          ctes,
		ctesOrder:     order,

		db: ec.db,
	}
}

func (ec *ExpresionChain) setLimit(limit *querySegmentAtom) {
	ec.lock.Lock()
	defer ec.lock.Unlock()
	ec.limit = limit
}

func (ec *ExpresionChain) setOffset(offset *querySegmentAtom) {
	ec.lock.Lock()
	defer ec.lock.Unlock()
	ec.offset = offset
}

func (ec *ExpresionChain) setTable(table string) {
	ec.lock.Lock()
	defer ec.lock.Unlock()
	// This will override whetever has been set and might be in turn ignored if the finalization
	// method used (ie Find(Object)) specifies one.
	ec.table = table
}

func (ec *ExpresionChain) append(atom querySegmentAtom) {
	ec.lock.Lock()
	defer ec.lock.Unlock()
	ec.segments = append(ec.segments, atom)
}

func (ec *ExpresionChain) removeOfType(atomType sqlSegment) {
	ec.lock.Lock()
	defer ec.lock.Unlock()
	newSegments := []querySegmentAtom{}
	for i, s := range ec.segments {
		if s.segment == atomType {
			continue
		}
		newSegments = append(newSegments, ec.segments[i])
	}
	ec.segments = newSegments
}

func (ec *ExpresionChain) mutateLastBool(operation sqlBool) {
	ec.lock.Lock()
	defer ec.lock.Unlock()
	if len(ec.segments) == 0 {
		return
	}
	last := &ec.segments[len(ec.segments)-1]
	if last.segment == sqlWhere {
		switch {
		case last.sqlBool == SQLAnd && operation == SQLNot:
			last.sqlBool = SQLAndNot
		case last.sqlBool == SQLAnd && operation == SQLOr:
			last.sqlBool = SQLOr
		case last.sqlBool == SQLOr && operation == SQLNot:
			last.sqlBool = SQLOrNot
		case last.sqlBool == SQLAndNot && operation == SQLOr:
			last.sqlBool = SQLOrNot
		// This behavior is preventive as this has no way of happening yet
		case last.sqlBool == SQLNot && operation == SQLAnd:
			last.sqlBool = SQLAndNot
		case last.sqlBool == SQLNot && operation == SQLOr:
			last.sqlBool = SQLOrNot
		}
	}
}

// AndWhereGroup adds an AND ( a = b AND/OR c = d...) basically a group of conditions preceded by
// AND unless it's the first condition then just the group.
// It takes an expression chain as a parameter which does not need an DB or any other expresion
// other than WHEREs `(&ExpressionChain{}).AndWhere(...).OrWhere(...)`
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpresionChain) AndWhereGroup(c *ExpresionChain) *ExpresionChain {
	wheres, whereArgs := c.renderWhereRaw()
	if len(whereArgs) > 0 {
		return ec.AndWhere(fmt.Sprintf("(%s)", wheres), whereArgs...)
	}
	return ec
}

// OrWhereGroup adds an OR ( a = b AND/OR c = d...) basically a group of conditions preceded by
// OR unless it's the first condition and there are no ANDs present.
// It takes an expression chain as a parameter which does not need an DB or any other expresion
// other than WHEREs `(&ExpressionChain{}).AndWhere(...).OrWhere(...)`
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpresionChain) OrWhereGroup(c *ExpresionChain) *ExpresionChain {
	wheres, whereArgs := c.renderWhereRaw()
	if len(whereArgs) > 0 {
		return ec.OrWhere(fmt.Sprintf("(%s)", wheres), whereArgs...)
	}
	return ec
}

// AndWhere adds a 'AND WHERE' to the 'ExpresionChain' and returns the same chan to facilitate
// further chaining.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpresionChain) AndWhere(expr string, args ...interface{}) *ExpresionChain {
	ec.append(
		querySegmentAtom{
			segment:   sqlWhere,
			expresion: expr,
			arguments: args,
			sqlBool:   SQLAnd,
		})
	return ec
}

// OrWhere adds a 'OR WHERE' to the 'ExpresionChain' and returns the same chan to facilitate
// further chaining.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpresionChain) OrWhere(expr string, args ...interface{}) *ExpresionChain {
	ec.append(
		querySegmentAtom{
			segment:   sqlWhere,
			expresion: expr,
			arguments: args,
			sqlBool:   SQLOr,
		})
	return ec
}

// AndHaving adds a 'HAVING' to the 'ExpresionChain' and returns the same chan to facilitate
// further chaining.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpresionChain) AndHaving(expr string, args ...interface{}) *ExpresionChain {
	ec.append(
		querySegmentAtom{
			segment:   sqlHaving,
			expresion: expr,
			arguments: args,
			sqlBool:   SQLAnd,
		})
	return ec
}

// OrHaving adds a 'HAVING' to the 'ExpresionChain' and returns the same chan to facilitate
// further chaining.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpresionChain) OrHaving(expr string, args ...interface{}) *ExpresionChain {
	ec.append(
		querySegmentAtom{
			segment:   sqlHaving,
			expresion: expr,
			arguments: args,
			sqlBool:   SQLOr,
		})
	return ec
}

// Select set fields to be returned by the final query.
func (ec *ExpresionChain) Select(fields ...string) *ExpresionChain {
	ec.mainOperation = &querySegmentAtom{
		segment:   sqlSelect,
		expresion: strings.Join(fields, ", "),
		arguments: nil,
		sqlBool:   SQLNothing,
	}
	return ec
}

// SelectArgument contains the components of a select column
type SelectArgument struct {
	Field string
	as    string
	Args  []interface{}
}

// As aliases the argument
func (s SelectArgument) As(alias string) SelectArgument {
	s.as = alias
	return s
}

// SelectWithArgs set fields to be returned by the final query.
func (ec *ExpresionChain) SelectWithArgs(fields ...SelectArgument) *ExpresionChain {
	var statements = make([]string, len(fields), len(fields))
	var args = []interface{}{}
	for i, v := range fields {
		if v.as != "" {
			v.Field = As(v.Field, v.as)
		}
		statements[i] = v.Field
		args = append(args, v.Args...)
	}
	ec.mainOperation = &querySegmentAtom{
		segment:   sqlSelect,
		expresion: strings.Join(statements, ", "),
		arguments: args,
		sqlBool:   SQLNothing,
	}
	return ec
}

// Delete determines a deletion will be made with the results of the query.
func (ec *ExpresionChain) Delete() *ExpresionChain {
	ec.mainOperation = &querySegmentAtom{
		segment:   sqlDelete,
		arguments: nil,
		sqlBool:   SQLNothing,
	}
	return ec
}

// OnConflict will add a "ON CONFLICT" clause at the end of the query if the main operation
// is an INSERT.
func (ec *ExpresionChain) OnConflict(clause func(*OnConflict)) *ExpresionChain {
	if ec.conflict != nil {
		ec.err = append(ec.err, errors.New("only 1 ON CONFLICT clause can be associated per statement"))
		return ec
	}
	ec.conflict = &OnConflict{}
	clause(ec.conflict)
	return ec
}

// Returning will add an "RETURNING" clause at the end of the query if the main operation
// is an INSERT.
//
// Please note that `Returning` likely doesn't do what you expect. There are systemic issues
// with dependencies and `go-lang` standard library that prevent it from operating correctly
// in many scenarios.
func (ec *ExpresionChain) Returning(args ...string) *ExpresionChain {
	if ec.mainOperation == nil ||
		(ec.mainOperation.segment != sqlInsert && ec.mainOperation.segment != sqlUpdate) {
		ec.err = append(ec.err, errors.New("Returning is only valid on UPDATE and INSERT statements"))
	}
	ec.append(
		querySegmentAtom{
			segment:   sqlReturning,
			expresion: "RETURNING " + strings.Join(args, ", "),
		})
	return ec
}

// InsertMulti set fields/values for insertion.
func (ec *ExpresionChain) InsertMulti(insertPairs map[string][]interface{}) (*ExpresionChain, error) {
	exprKeys := make([]string, len(insertPairs))

	i := 0
	insertLen := 0
	for k, v := range insertPairs {
		exprKeys[i] = k
		i++
		if insertLen != 0 {
			if len(v) != insertLen {
				return nil, errors.Errorf("lenght of insert columns missmatch on column %s", k)
			}
		}
		insertLen = len(v)
	}
	// This is not really necessary but it makes things a bit more deterministic when debugging.
	sort.Strings(exprKeys)
	exprValues := make([]interface{}, len(exprKeys)*insertLen, len(exprKeys)*insertLen)
	position := 0
	for row := 0; row < insertLen; row++ {
		for _, k := range exprKeys {
			exprValues[position] = insertPairs[k][row]
			position++
		}
	}
	ec.mainOperation = &querySegmentAtom{
		segment:   sqlInsertMulti,
		expresion: strings.Join(exprKeys, ", "),
		arguments: exprValues,
		sqlBool:   SQLNothing,
	}
	return ec, nil
}

// Insert set fields/values for insertion.
func (ec *ExpresionChain) Insert(insertPairs map[string]interface{}) *ExpresionChain {
	exprKeys := make([]string, len(insertPairs))
	exprValues := make([]interface{}, len(insertPairs))

	i := 0
	for k := range insertPairs {
		exprKeys[i] = k
		i++
	}
	// This is not really necessary but it makes things a bit more deterministic when debugging.
	sort.Strings(exprKeys)
	for i, k := range exprKeys {
		exprValues[i] = insertPairs[k]
	}
	ec.mainOperation = &querySegmentAtom{
		segment:   sqlInsert,
		expresion: strings.Join(exprKeys, ", "),
		arguments: exprValues,
		sqlBool:   SQLNothing,
	}
	return ec
}

// Update set fields/values for updates.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
//
// NOTE: values of `nil` will be treated as `NULL`
func (ec *ExpresionChain) Update(expr string, args ...interface{}) *ExpresionChain {
	ec.mainOperation = &querySegmentAtom{
		segment:   sqlUpdate,
		expresion: expr,
		arguments: args,
		sqlBool:   SQLNothing,
	}
	return ec
}

// UpdateMap set fields/values for updates but does so from a map of key/value.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
//
// NOTE: values of `nil` will be treated as `NULL`
func (ec *ExpresionChain) UpdateMap(exprMap map[string]interface{}) *ExpresionChain {
	exprParts := []string{}
	args := []interface{}{}
	keys := make([]string, len(exprMap))
	i := 0
	for k := range exprMap {
		keys[i] = k
		i++
	}
	sort.Strings(keys)
	for _, k := range keys {
		exprParts = append(exprParts, fmt.Sprintf("%s = ?", k))
		args = append(args, exprMap[k])
	}
	expr := strings.Join(exprParts, ", ")
	ec.mainOperation = &querySegmentAtom{
		segment:   sqlUpdate,
		expresion: expr,
		arguments: args,
		sqlBool:   SQLNothing,
	}
	return ec
}

// Table sets the table to be used in the 'FROM' expresion.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpresionChain) Table(table string) *ExpresionChain {
	ec.setTable(table)
	return ec
}

// From sets the table to be used in the `FROM` expresion.
// Functionally this is identical to `Table()`, but it makes
// code more readable in some circumstances.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpresionChain) From(table string) *ExpresionChain {
	ec.setTable(table)
	return ec
}

// Limit adds a 'LIMIT' to the 'ExpresionChain' and returns the same chan to facilitate
// further chaining.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpresionChain) Limit(limit int64) *ExpresionChain {
	ec.setLimit(
		&querySegmentAtom{
			segment:   sqlLimit,
			expresion: fmt.Sprintf("%d", limit),
			arguments: nil,
			sqlBool:   SQLNothing,
		})
	return ec
}

// Offset adds a 'OFFSET' to the 'ExpresionChain' and returns the same chan to facilitate
// further chaining.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpresionChain) Offset(offset int64) *ExpresionChain {
	ec.setOffset(
		&querySegmentAtom{
			segment:   sqlOffset,
			expresion: fmt.Sprintf("%d", offset),
			arguments: nil,
			sqlBool:   SQLNothing,
		})
	return ec
}

// Join adds a 'JOIN' to the 'ExpresionChain' and returns the same chan to facilitate
// further chaining.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpresionChain) Join(expr, on string, args ...interface{}) *ExpresionChain {
	expr = fmt.Sprintf("%s ON %s", expr, on)
	ec.append(
		querySegmentAtom{
			segment:   sqlJoin,
			expresion: expr,
			arguments: args,
			sqlBool:   SQLNothing,
		})
	return ec
}

// LeftJoin adds a 'LEFT JOIN' to the 'ExpresionChain' and returns the same chan to facilitate
// further chaining.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpresionChain) LeftJoin(expr, on string, args ...interface{}) *ExpresionChain {
	expr = fmt.Sprintf("%s ON %s", expr, on)
	ec.append(
		querySegmentAtom{
			segment:   sqlLeftJoin,
			expresion: expr,
			arguments: args,
			sqlBool:   SQLNothing,
		})
	return ec
}

// RightJoin adds a 'RIGHT JOIN' to the 'ExpresionChain' and returns the same chan to facilitate
// further chaining.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpresionChain) RightJoin(expr, on string, args ...interface{}) *ExpresionChain {
	expr = fmt.Sprintf("%s ON %s", expr, on)
	ec.append(
		querySegmentAtom{
			segment:   sqlRightJoin,
			expresion: expr,
			arguments: args,
			sqlBool:   SQLNothing,
		})
	return ec
}

// InnerJoin adds a 'INNER JOIN' to the 'ExpresionChain' and returns the same chan to facilitate
// further chaining.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpresionChain) InnerJoin(expr, on string, args ...interface{}) *ExpresionChain {
	expr = fmt.Sprintf("%s ON %s", expr, on)
	ec.append(
		querySegmentAtom{
			segment:   sqlInnerJoin,
			expresion: expr,
			arguments: args,
			sqlBool:   SQLNothing,
		})
	return ec
}

// FullJoin adds a 'FULL JOIN' to the 'ExpresionChain' and returns the same chan to facilitate
// further chaining.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpresionChain) FullJoin(expr, on string, args ...interface{}) *ExpresionChain {
	expr = fmt.Sprintf("%s ON %s", expr, on)
	ec.append(
		querySegmentAtom{
			segment:   sqlFullJoin,
			expresion: expr,
			arguments: args,
			sqlBool:   SQLNothing,
		})
	return ec
}

// OrderBy adds a 'ORDER BY' to the 'ExpresionChain' and returns the same chan to facilitate
// further chaining.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpresionChain) OrderBy(order *OrderByOperator) *ExpresionChain {
	ec.append(
		querySegmentAtom{
			segment:   sqlOrder,
			expresion: order.String(),
			arguments: nil,
			sqlBool:   SQLNothing,
		})
	return ec
}

// GroupBy adds a 'GROUP BY' to the 'ExpresionChain' and returns the same chan to facilitate
// further chaining.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpresionChain) GroupBy(expr string, args ...interface{}) *ExpresionChain {
	ec.append(
		querySegmentAtom{
			segment:   sqlGroup,
			expresion: expr,
			arguments: args,
			sqlBool:   SQLNothing,
		})
	return ec
}

// GroupByReplace adds a 'GROUP BY' to the 'ExpresionChain' and returns the same chain to facilitate
// further chaining, this version of group by removes all other group by entries.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpresionChain) GroupByReplace(expr string, args ...interface{}) *ExpresionChain {
	ec.removeOfType(sqlGroup)
	ec.append(
		querySegmentAtom{
			segment:   sqlGroup,
			expresion: expr,
			arguments: args,
			sqlBool:   SQLNothing,
		})
	return ec
}

// AddUnionFromChain renders the passed chain and adds it to the current one as a Union
// returned ExpresionChain pointer is of current chain modified.
func (ec *ExpresionChain) AddUnionFromChain(union *ExpresionChain, all bool) (*ExpresionChain, error) {
	if len(union.ctes) != 0 {
		return nil, errors.Errorf("cannot handle unions with CTEs outside of the primary query.")
	}
	expr, args, err := union.RenderRaw()
	if err != nil {
		return nil, errors.Wrap(err, "rendering union query")
	}

	return ec.Union(expr, all, args...), nil
}

// Union adds the passed SQL expresion and args as a union to be made on this expresion, the
// change is in place, there are no checks about correctness of the query.
func (ec *ExpresionChain) Union(unionExpr string, all bool, args ...interface{}) *ExpresionChain {
	atom := querySegmentAtom{
		segment:   sqlUnion,
		expresion: unionExpr,
		arguments: args,
	}
	if all {
		atom.sqlModifier = SQLAll
	}
	ec.append(atom)
	return ec
}

func extract(ec *ExpresionChain, seg sqlSegment) []querySegmentAtom {
	qs := []querySegmentAtom{}
	for _, item := range ec.segments {
		if item.segment == seg {
			qs = append(qs, item)
		}
	}
	return qs
}

// fetchErrors is a private thingy for checking if errors exist
func (ec *ExpresionChain) hasErr() bool {
	return len(ec.err) > 0
}

// getErr returns an error message about the stuff
func (ec *ExpresionChain) getErr() error {
	if ec.err == nil {
		return nil
	}
	errMsg := make([]string, len(ec.err))
	for index, anErr := range ec.err {
		errMsg[index] = anErr.Error()
	}
	return errors.New(strings.Join(errMsg, " "))
}
