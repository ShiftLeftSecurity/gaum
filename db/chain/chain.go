package chain

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"

	"github.com/perrito666/bmstrem/db/connection"
	"github.com/pkg/errors"
)

type sqlBool string

const (
	// SQLNothing is the default value for an SQLBool
	SQLNothing sqlBool = ""
	// SQLAnd represents AND in SQL
	SQLAnd sqlBool = "AND"
	// SQLOr represents OR in SQL
	SQLOr sqlBool = "OR"
	// SQLNot represents NOT in SQL
	SQLNot sqlBool = "NOT"
	// SQLAndNot Negates the expresion after AND
	SQLAndNot sqlBool = "AND NOT"
	// SQLOrNot Neates the expresion after OR
	SQLOrNot sqlBool = "OR NOT"
)

type sqlSegment string

const (
	sqlWhere  sqlSegment = "WHERE"
	sqlLimit  sqlSegment = "LIMIT"
	sqlOffset sqlSegment = "OFFSET"
	sqlJoin   sqlSegment = "JOIN"
	sqlSelect sqlSegment = "SELECT"
	sqlDelete sqlSegment = "DELETE"
	sqlInsert sqlSegment = "INSERT"
	sqlUpdate sqlSegment = "UPDATE"
	sqlFrom   sqlSegment = "FROM"
	sqlGroup  sqlSegment = "GROUP BY"
	sqlOrder  sqlSegment = "ORDER BY"
	// SPECIAL CASES
	sqlInsertMulti sqlSegment = "INSERTM"
)

type querySegmentAtom struct {
	segment   sqlSegment
	expresion string
	arguments []interface{}
	sqlBool   sqlBool
}

func (q *querySegmentAtom) clone() querySegmentAtom {
	arguments := make([]interface{}, len(q.arguments))
	for i, a := range q.arguments {
		// TODO: This will not work as expected for pointers and arrays/slices, add reflection
		// and deep copy to solve that. (ie, it's functional but not safe)
		arguments[i] = a
	}
	return querySegmentAtom{
		segment:   q.segment,
		expresion: q.expresion,
		sqlBool:   q.sqlBool,
		arguments: arguments,
	}
}

func (q *querySegmentAtom) fields() []string {
	fields := []string{}
	if q.segment == sqlSelect {
		rawFields := strings.Split(q.expresion, ",")
		for _, field := range rawFields {
			field = strings.ToLower(field)
			field := strings.TrimRight(strings.TrimLeft(field, " "), " ")
			fieldParts := strings.Split(field, " as ")
			fieldName := fieldParts[len(fieldParts)-1]
			fieldName = strings.TrimRight(strings.TrimLeft(fieldName, " "), " ")
			if fieldName == "" {
				continue
			}
			fields = append(fields, fieldName)
		}
	}
	// TODO make UPDATE and INSERT for completion's sake
	return fields
}

func (q *querySegmentAtom) render(firstForSegment, lastForSegment bool) (string, []interface{}) {
	expresion := ""
	if !firstForSegment {
		expresion = fmt.Sprintf(" %s", q.sqlBool)
	}
	expresion += fmt.Sprintf(" %s", q.expresion)
	return expresion, q.arguments
}

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

// Where adds a 'WHERE' to the 'ExpresionChain' and returns the same chan to facilitate
// further chaining.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpresionChain) Where(expr string, args ...interface{}) *ExpresionChain {
	ec.append(
		querySegmentAtom{
			segment:   sqlWhere,
			expresion: expr,
			arguments: args,
			sqlBool:   SQLAnd,
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

// Constraint wraps the passed constraint name with the required SQL to use it.
func Constraint(constraint string) string {
	return "ON CONSTRAINT " + constraint
}

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

// Or replaces the chaining operation in the last segment atom by 'OR' or 'OR NOT' depending on
// what the previous one was (either 'AND' or 'AND NOT') as long as the last operation is a
// 'WHERE' segment atom.
func Or(ec *ExpresionChain) *ExpresionChain {
	ec.mutateLastBool(SQLOr)
	return ec
}

// Not replaces the chaining operation in the last segment atom by 'AND NOT' or 'OR NOT' depending on
// what the previous one was (either 'AND' or 'OR') as long as the last operation is a
// 'WHERE' segment atom.
func Not(ec *ExpresionChain) *ExpresionChain {
	ec.mutateLastBool(SQLNot)
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

// RenderInsertMulti does render for the very particular case of insert
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
	wheres := extract(ec, sqlWhere)
	if len(wheres) != 0 {
		whereStatement := " WHERE"
		for i, item := range wheres {
			expr, arguments := item.render(i == 0, i == len(wheres)-1)
			whereStatement += expr
			args = append(args, arguments...)
		}
		query += whereStatement
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
