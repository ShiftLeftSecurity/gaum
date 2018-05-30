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
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/ShiftLeftSecurity/gaum/db/connection"
	"github.com/pkg/errors"
)

// NewExpresionChain returns a new instance of ExpresionChain hooked to the passed DB
func NewExpresionChain(db connection.DB) *ExpresionChain {
	return &ExpresionChain{db: db, conflict: map[string]string{}}
}

// ExpresionChain holds all the atoms for the SQL expresions that make a query and allows to chain
// more assuming the chaining is valid.
type ExpresionChain struct {
	lock          sync.Mutex
	segments      []querySegmentAtom
	table         string
	mainOperation *querySegmentAtom

	limit  *querySegmentAtom
	offset *querySegmentAtom

	set string

	conflict map[string]string

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
	return &ExpresionChain{
		limit:         limit,
		offset:        offset,
		segments:      segments,
		mainOperation: mainOperation,
		table:         ec.table,

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

// Delete determines a deletion will be made with the results of the query.
func (ec *ExpresionChain) Delete() *ExpresionChain {
	ec.mainOperation = &querySegmentAtom{
		segment:   sqlDelete,
		arguments: nil,
		sqlBool:   SQLNothing,
	}
	return ec
}

// ConflictAction represents a possible conflict resolution action.
type ConflictAction string

const (
	// ConflictActionNothing represents a nil action on conflict
	ConflictActionNothing ConflictAction = "NOTHING"
)

// Conflict will add a "ON CONFLICT" clause at the end of the query if the main operation
// is an INSERT.
// This requires a constraint or field name because I really want to be explicit when things
// are to be ignored.
func (ec *ExpresionChain) Conflict(constraint string, action ConflictAction) *ExpresionChain {
	if ec.conflict == nil {
		ec.conflict = map[string]string{}
	}
	ec.conflict[constraint] = string(action)
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
func (ec *ExpresionChain) Update(expr string, args ...interface{}) *ExpresionChain {
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
func (ec *ExpresionChain) Join(expr string, args ...interface{}) *ExpresionChain {
	ec.append(
		querySegmentAtom{
			segment:   sqlJoin,
			expresion: expr,
			arguments: args,
			sqlBool:   SQLNothing,
		})
	return ec
}

// OrderBy adds a 'ORDER BY' to the 'ExpresionChain' and returns the same chan to facilitate
// further chaining.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpresionChain) OrderBy(expr string, args ...interface{}) *ExpresionChain {
	ec.append(
		querySegmentAtom{
			segment:   sqlOrder,
			expresion: expr,
			arguments: args,
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

func extract(ec *ExpresionChain, seg sqlSegment) []querySegmentAtom {
	qs := []querySegmentAtom{}
	for _, item := range ec.segments {
		if item.segment == seg {
			qs = append(qs, item)
		}
	}
	return qs
}

// marks to placeholder replaces `?` in the query with `$1` style placeholders, this must be
// done with a finished query and requires the args as they depend on the position of the
// already rendered query, it does some consistency control and finally expands `(?)`.
func marksToPlaceholders(q string, args []interface{}) (string, []interface{}, error) {
	// TODO: make this a bit less ugly
	// TODO: identify escaped questionmarks
	queryWithArgs := ""
	argCounter := 1
	argPositioner := 0
	expandedArgs := []interface{}{}
	for _, queryChar := range q {
		if queryChar == '?' {
			arg := args[argPositioner]
			if reflect.TypeOf(arg).Kind() == reflect.Slice {
				s := reflect.ValueOf(arg)
				placeholders := []string{}
				for i := 0; i < s.Len(); i++ {
					expandedArgs = append(expandedArgs, s.Index(i).Interface())
					placeholders = append(placeholders, fmt.Sprintf("$%d", argCounter))
					argCounter++
				}
				queryWithArgs += strings.Join(placeholders, ", ")
			} else {
				expandedArgs = append(expandedArgs, arg)
				queryWithArgs += fmt.Sprintf("$%d", argCounter)
				argCounter++
			}
			argPositioner++
		} else {
			queryWithArgs += string(queryChar)
		}
	}
	if len(expandedArgs) != argCounter-1 {
		return "", nil, errors.Errorf("the query has %d args but %d were passed: \n %q \n %#v",
			argCounter-1, len(args), queryWithArgs, args)
	}
	return queryWithArgs, expandedArgs, nil
}

// RenderInsert does render for the very particular case of insert
func (ec *ExpresionChain) renderInsert(raw bool) (string, []interface{}, error) {
	if ec.table == "" {
		return "", nil, errors.Errorf("no table specified for this insert")
	}
	placeholders := make([]string, len(ec.mainOperation.arguments))
	for i := range ec.mainOperation.arguments {
		placeholders[i] = "?"
	}
	args := make([]interface{}, 0)
	args = append(args, ec.mainOperation.arguments...)
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		ec.table,
		ec.mainOperation.expresion,
		strings.Join(placeholders, ", "))

	conflicts := []string{}
	for k, v := range ec.conflict {
		if v == "" {
			continue
		}
		// TODO make parentheses be magic
		conflicts = append(conflicts,
			fmt.Sprintf("ON CONFLICT %s DO %s", k, v))
	}
	if len(conflicts) > 0 {
		query += " " + strings.Join(conflicts, ", ")
	}

	if !raw {
		var err error
		query, args, err = marksToPlaceholders(query, args)
		if err != nil {
			return "", nil, errors.Wrap(err, "rendering insert")
		}
		return query, args, nil
	}
	return query, args, nil
}

// renderInsertMulti does render for the very particular case of a multiple insertion
func (ec *ExpresionChain) renderInsertMulti(raw bool) (string, []interface{}, error) {
	if ec.table == "" {
		return "", nil, errors.Errorf("no table specified for this insert")
	}
	argCount := strings.Count(ec.mainOperation.expresion, ",") + 1
	placeholders := make([]string, argCount, argCount)
	for i := 0; i < argCount; i++ {
		placeholders[i] = "?"
	}

	values := make([]string, len(ec.mainOperation.arguments)/argCount,
		len(ec.mainOperation.arguments)/argCount)
	for i := 0; i < len(ec.mainOperation.arguments)/argCount; i++ {
		values[i] += fmt.Sprintf("(%s)", strings.Join(placeholders, ", "))
	}

	args := make([]interface{}, 0)
	args = append(args, ec.mainOperation.arguments...)
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		ec.table,
		ec.mainOperation.expresion,
		strings.Join(values, ", "))

	conflicts := []string{}
	for k, v := range ec.conflict {
		if v == "" {
			continue
		}
		// TODO make parentheses be magic
		conflicts = append(conflicts,
			fmt.Sprintf("ON CONFLICT %s DO %s", k, v))
	}
	if len(conflicts) > 0 {
		query += " " + strings.Join(conflicts, ", ")
	}

	if !raw {
		var err error
		query, args, err = marksToPlaceholders(query, args)
		if err != nil {
			return "", nil, errors.Wrap(err, "rendering insert multi")
		}
		return query, args, nil
	}
	return query, args, nil
}

// Render returns the SQL expresion string and the arguments of said expresion, there is no checkig
// of validity or consistency for the time being.
func (ec *ExpresionChain) Render() (string, []interface{}, error) {
	return ec.render(false)
}

// RenderRaw returns the SQL expresion string and the arguments of said expresion,
// No positional argument replacement is done.
func (ec *ExpresionChain) RenderRaw() (string, []interface{}, error) {
	return ec.render(true)
}

// renderWhereRaw renders only the where portion of an ExpresionChain and returns it without
// placeholder markers replaced.
func (ec *ExpresionChain) renderWhereRaw() (string, []interface{}) {
	// WHERE
	wheres := extract(ec, sqlWhere)
	// Separate where statements that are not ANDed since they will need
	// to go after others with AND.
	whereOrs := []querySegmentAtom{}
	if len(wheres) != 0 {
		whereStatement := ""
		args := []interface{}{}
		whereCount := 0
		for i, item := range wheres {
			if item.sqlBool != SQLAnd {
				whereOrs = append(whereOrs, item)
				continue
			}
			expr, arguments := item.render(i == 0, i == len(wheres)-1)
			whereStatement += expr
			args = append(args, arguments...)
			whereCount++
		}
		for i, item := range whereOrs {
			expr, arguments := item.render(whereCount+i == 0, i == len(whereOrs)-1)
			whereStatement += expr
			args = append(args, arguments...)
		}
		return whereStatement, args
	}
	return "", nil
}

// render returns the rendered expression along with an arguments list and all marker placeholders
// replaced by their positional placeholder.
func (ec *ExpresionChain) render(raw bool) (string, []interface{}, error) {
	args := []interface{}{}
	var query string
	if ec.mainOperation == nil {
		return "", nil, errors.Errorf("missing main operation to perform on the db")
	}
	// INSERT
	switch ec.mainOperation.segment {
	case sqlInsert:
		// Too much of a special cookie for the general case.
		return ec.renderInsert(raw)
	case sqlInsertMulti:
		// Too much of a special cookie for the general case.
		return ec.renderInsertMulti(raw)
	// UPDATE
	case sqlUpdate:
		if ec.table == "" {
			return "", nil, errors.Errorf("no table specified for update")
		}
		expresion := ec.mainOperation.expresion
		if len(expresion) == 0 {
			return "", nil, errors.Errorf("empty update expresion")
		}
		query = fmt.Sprintf("UPDATE ? SET (%s)",
			ec.mainOperation.expresion)
		args = append(args, ec.table)
		args = append(args, ec.mainOperation.arguments...)
	// SELECT, DELETE
	case sqlSelect, sqlDelete:
		expresion := ec.mainOperation.expresion
		if len(expresion) == 0 {
			expresion = "*"
		}
		if ec.mainOperation.segment == sqlSelect {
			query = fmt.Sprintf("SELECT %s",
				expresion)
		} else {
			query = "DELETE "
		}
		// FROM
		if ec.table == "" {
			return "", nil, errors.Errorf("no table specified for this query")
		}
		query += fmt.Sprintf(" FROM %s", ec.table)

	}
	if ec.mainOperation.segment == sqlSelect ||
		ec.mainOperation.segment == sqlDelete ||
		ec.mainOperation.segment == sqlUpdate {
		// JOIN
		joins := extract(ec, sqlJoin)
		if len(joins) != 0 {
			joinSubQueries := make([]string, len(joins))
			joinArguments := []interface{}{}
			for i, item := range joins {
				joinSubQueries[i] = item.expresion
				joinArguments = append(joinArguments, item.arguments...)
			}
			query += fmt.Sprintf(" JOIN %s",
				strings.Join(joinSubQueries, " "))
			args = append(args, joinArguments...)
		}
	}

	// WHERE
	wheres, whereArgs := ec.renderWhereRaw()
	if len(whereArgs) != 0 {
		query += " WHERE" + wheres
		args = append(args, whereArgs...)
	}

	// GROUP BY
	groups := extract(ec, sqlGroup)
	groupByStatement := " GROUP BY "
	if len(groups) != 0 {
		groupCriteria := []string{}
		for _, item := range groups {
			expr := item.expresion
			arguments := item.arguments
			args = append(args, arguments...)
			groupCriteria = append(groupCriteria, expr)
		}
		query += groupByStatement
		query += strings.Join(groupCriteria, ", ")
	}

	// ORDER BY
	orders := extract(ec, sqlOrder)
	orderByStatemet := " ORDER BY "
	if len(orders) != 0 {
		orderCriteria := []string{}
		for _, item := range orders {
			expr := item.expresion
			arguments := item.arguments
			args = append(args, arguments...)
			orderCriteria = append(orderCriteria, expr)
		}
		query += orderByStatemet
		query += strings.Join(orderCriteria, ", ")
	}

	// LIMIT and OFFSET only make sense in SELECT, I think.
	if ec.mainOperation.segment == sqlSelect {
		if ec.limit != nil {
			query += " LIMIT " + ec.limit.expresion
			args = append(args, ec.limit.arguments...)
		}

		if ec.offset != nil {
			query += " OFFSET " + ec.offset.expresion
			args = append(args, ec.offset.arguments...)
		}
	}

	if !raw {
		var err error
		query, args, err = marksToPlaceholders(query, args)
		if err != nil {
			return "", nil, errors.Wrap(err, "rendering query")
		}
		return query, args, nil
	}
	return query, args, nil
}
