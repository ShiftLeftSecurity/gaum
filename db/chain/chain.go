package chain

import (
	"fmt"
	"strings"
	"sync"

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
	sqlInsert sqlSegment = "INSERT"
	sqlUpdate sqlSegment = "UPDATE"
	sqlFrom   sqlSegment = "FROM"
	sqlGroup  sqlSegment = "GROUP BY"
	sqlOrder  sqlSegment = "ORDER BY"
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

func (q *querySegmentAtom) render(firstForSegment, lastForSegment bool) (string, []interface{}) {
	expresion := ""
	if !firstForSegment {
		expresion = fmt.Sprintf(" %s", q.sqlBool)
	}
	expresion += fmt.Sprintf(" %s", q.expresion)
	return expresion, q.arguments
}

// ExpresionChain holds all the atoms for the SQL expresions that make a query and allows to chain
// more assuming the chaining is valid.
type ExpresionChain struct {
	lock          sync.Mutex
	segments      []querySegmentAtom
	table         string
	mainOperation querySegmentAtom
	// only makes sense on insert
	limit  *querySegmentAtom
	offset *querySegmentAtom
}

func (ec *ExpresionChain) setLimit(limit *querySegmentAtom) {
	ec.lock.Lock()
	defer ec.lock.Unlock()
	ec.limit = limit
}

func (ec *ExpresionChain) setOffset(offset *querySegmentAtom) {
	ec.lock.Lock()
	defer ec.lock.Unlock()
	ec.limit = offset
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
func (ec *ExpresionChain) Select(fields []string) *ExpresionChain {
	ec.mainOperation = querySegmentAtom{
		segment:   sqlSelect,
		expresion: strings.Join(fields, ", "),
		arguments: nil,
		sqlBool:   SQLNothing,
	}
	return ec
}

// Insert set fields/values for insertion.
func (ec *ExpresionChain) Insert(fields []string, values []interface{}) *ExpresionChain {
	// TODO: fail somehow if fields and values have different
	ec.mainOperation = querySegmentAtom{
		segment:   sqlInsert,
		expresion: strings.Join(fields, ", "),
		arguments: values,
		sqlBool:   SQLNothing,
	}
	return ec
}

// Update set fields/values for updates.
func (ec *ExpresionChain) Update(expr string, args ...interface{}) *ExpresionChain {
	ec.mainOperation = querySegmentAtom{
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

// RenderInsert does render for the very particular case of insert
func (ec *ExpresionChain) RenderInsert() (string, []interface{}, error) {
	if ec.table == "" {
		return "", nil, errors.Errorf("no table specified for this insert")
	}
	placeholders := make([]string, len(ec.mainOperation.arguments))
	for i := range ec.mainOperation.arguments {
		placeholders[i] = "?"
	}
	args := make([]interface{}, 0)
	args = append(args, ec.table)
	args = append(args, ec.mainOperation.arguments...)
	return fmt.Sprintf("INSERT INTO ? (%s) VALUES (%s)",
			ec.mainOperation.expresion,
			strings.Join(placeholders, ", ")),
		args, nil
}

// Render returns the SQL expresion string and the arguments of said expresion, there is no checkig
// of validity or consistency for the time being.
func (ec *ExpresionChain) Render() (string, []interface{}, error) {
	args := []interface{}{}
	var query string
	// INSERT
	switch ec.mainOperation.segment {
	case sqlInsert:
		// Too much of a special cookie for the general case.
		return ec.RenderInsert()
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
		args = append(ec.mainOperation.arguments)
	// SELECT
	case sqlSelect:
		expresion := ec.mainOperation.expresion
		if len(expresion) == 0 {
			expresion = "*"
		}
		query = fmt.Sprintf("SELECT %s",
			expresion)
		// FROM
		if ec.table == "" {
			return "", nil, errors.Errorf("no table specified for this query")
		}
		query += fmt.Sprintf(" FROM %s", ec.table)
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
	// TODO
	// ORDER BY
	// TODO

	return query, args, nil
}

// Clone returns a copy of the ExpresionChain
func (ec *ExpresionChain) Clone() *ExpresionChain {
	limit := *ec.limit
	offset := *ec.offset
	segments := make([]querySegmentAtom, len(ec.segments))
	for i, s := range ec.segments {
		segments[i] = s.clone()
	}
	return &ExpresionChain{
		limit:    &limit,
		offset:   &offset,
		segments: segments,
	}
}
