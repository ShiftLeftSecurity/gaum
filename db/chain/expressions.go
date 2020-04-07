package chain

//    Copyright 2019 Horacio Duran <horacio@shiftleft.io>, ShiftLeft Inc.
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

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

type baseSegmentFunc func(string, ...interface{}) *ExpressionChain

// AndWhereGroup adds an AND ( a = b AND/OR c = d...) basically a group of conditions preceded by
// AND unless it's the first condition then just the group.
// It takes an expression chain as a parameter which does not need an DB or any other expression
// other than WHEREs `NewNoDB().AndWhere(...).OrWhere(...)`
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpressionChain) AndWhereGroup(c *ExpressionChain) *ExpressionChain {
	ec.whereGroup(c, ec.AndWhere)
	return ec
}

// OrWhereGroup adds an OR ( a = b AND/OR c = d...) basically a group of conditions preceded by
// OR unless it's the first condition and there are no ANDs present.
// It takes an expression chain as a parameter which does not need an DB or any other expression
// other than WHEREs `NewNoDB().AndWhere(...).OrWhere(...)`
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpressionChain) OrWhereGroup(c *ExpressionChain) *ExpressionChain {
	ec.whereGroup(c, ec.OrWhere)
	return ec
}

func (ec *ExpressionChain) whereGroup(c *ExpressionChain, whereFunc baseSegmentFunc) {
	dst := &strings.Builder{}
	dst.WriteRune('(')
	whereArgs := c.renderWhereRaw(dst)
	dst.WriteRune(')')
	if len(whereArgs) > 0 {
		whereFunc(dst.String(), whereArgs...)
	}
}

// appendExpandedOp is the constructor of the most common chain segment.
func (ec *ExpressionChain) appendExpandedOp(expr string,
	op sqlSegment, boolOp sqlBool,
	args ...interface{}) *ExpressionChain {
	expr, args = ExpandArgs(args, expr)
	ec.append(
		querySegmentAtom{
			segment:    op,
			expression: ec.populateTablePrefixes(expr),
			arguments:  args,
			sqlBool:    boolOp,
		})
	return ec
}

// setExpandedOp is the constructor of the most common chain main operation.
func (ec *ExpressionChain) setExpandedMainOp(expr string,
	op sqlSegment, boolOp sqlBool,
	args ...interface{}) *ExpressionChain {
	expr, args = ExpandArgs(args, expr)
	ec.mainOperation = &querySegmentAtom{
		segment:    op,
		expression: ec.populateTablePrefixes(expr),
		arguments:  args,
		sqlBool:    boolOp,
	}
	return ec
}

// AndWhere adds a 'AND WHERE' to the 'ExpressionChain' and returns the same chan to facilitate
// further chaining.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpressionChain) AndWhere(expr string, args ...interface{}) *ExpressionChain {
	return ec.appendExpandedOp(expr, sqlWhere, SQLAnd, args...)
}

// OrWhere adds a 'OR WHERE' to the 'ExpressionChain' and returns the same chan to facilitate
// further chaining.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpressionChain) OrWhere(expr string, args ...interface{}) *ExpressionChain {
	return ec.appendExpandedOp(expr, sqlWhere, SQLOr, args...)

}

// AndHaving adds a 'HAVING' to the 'ExpressionChain' and returns the same chan to facilitate
// further chaining.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpressionChain) AndHaving(expr string, args ...interface{}) *ExpressionChain {
	return ec.appendExpandedOp(expr, sqlHaving, SQLAnd, args...)
}

// OrHaving adds a 'HAVING' to the 'ExpressionChain' and returns the same chan to facilitate
// further chaining.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpressionChain) OrHaving(expr string, args ...interface{}) *ExpressionChain {
	return ec.appendExpandedOp(expr, sqlHaving, SQLOr, args...)
}

// OnConflict will add a "ON CONFLICT" clause at the end of the query if the main operation
// is an INSERT.
func (ec *ExpressionChain) OnConflict(clause func(*OnConflict)) *ExpressionChain {
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
func (ec *ExpressionChain) Returning(args ...string) *ExpressionChain {
	if ec.mainOperation == nil ||
		(ec.mainOperation.segment != sqlInsert && ec.mainOperation.segment != sqlUpdate) {
		ec.err = append(ec.err, errors.New("Returning is only valid on UPDATE and INSERT statements"))
	}
	ec.append(
		querySegmentAtom{
			segment:    sqlReturning,
			expression: "RETURNING " + strings.Join(args, ", "),
		})
	return ec
}

// Table sets the table to be used in the 'FROM' expression.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpressionChain) Table(table string) *ExpressionChain {
	ec.setTable(table)
	return ec
}

// From sets the table to be used in the `FROM` expression.
// Functionally this is identical to `Table()`, but it makes
// code more readable in some circumstances.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpressionChain) From(table string) *ExpressionChain {
	ec.setTable(table)
	return ec
}

// FromUpdate adds a special case of from, for UPDATE where FROM is used as JOIN
func (ec *ExpressionChain) FromUpdate(expr string, args ...interface{}) *ExpressionChain {
	ec.appendExpandedOp(expr, sqlFromUpdate, SQLNothing, args...)
	return ec
}

// Limit adds a 'LIMIT' to the 'ExpressionChain' and returns the same chan to facilitate
// further chaining.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpressionChain) Limit(limit int64) *ExpressionChain {
	ec.setLimit(
		&querySegmentAtom{
			segment:    sqlLimit,
			expression: strconv.FormatInt(limit, 10),
			arguments:  nil,
			sqlBool:    SQLNothing,
		})
	return ec
}

// Offset adds a 'OFFSET' to the 'ExpressionChain' and returns the same chan to facilitate
// further chaining.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpressionChain) Offset(offset int64) *ExpressionChain {
	ec.setOffset(
		&querySegmentAtom{
			segment:    sqlOffset,
			expression: strconv.FormatInt(offset, 10),
			arguments:  nil,
			sqlBool:    SQLNothing,
		})
	return ec
}

// Join adds a 'JOIN' to the 'ExpressionChain' and returns the same chan to facilitate
// further chaining.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpressionChain) Join(expr, on string, args ...interface{}) *ExpressionChain {
	ec.appendExpandedOp(fmt.Sprintf("%s ON %s", expr, on), sqlJoin, SQLNothing, args...)
	return ec
}

// LeftJoin adds a 'LEFT JOIN' to the 'ExpressionChain' and returns the same chan to facilitate
// further chaining.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpressionChain) LeftJoin(expr, on string, args ...interface{}) *ExpressionChain {
	ec.appendExpandedOp(fmt.Sprintf("%s ON %s", expr, on), sqlLeftJoin, SQLNothing, args...)
	return ec
}

// RightJoin adds a 'RIGHT JOIN' to the 'ExpressionChain' and returns the same chan to facilitate
// further chaining.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpressionChain) RightJoin(expr, on string, args ...interface{}) *ExpressionChain {
	ec.appendExpandedOp(fmt.Sprintf("%s ON %s", expr, on), sqlRightJoin, SQLNothing, args...)
	return ec
}

// InnerJoin adds a 'INNER JOIN' to the 'ExpressionChain' and returns the same chan to facilitate
// further chaining.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpressionChain) InnerJoin(expr, on string, args ...interface{}) *ExpressionChain {
	ec.appendExpandedOp(fmt.Sprintf("%s ON %s", expr, on), sqlInnerJoin, SQLNothing, args...)
	return ec
}

// FullJoin adds a 'FULL JOIN' to the 'ExpressionChain' and returns the same chan to facilitate
// further chaining.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpressionChain) FullJoin(expr, on string, args ...interface{}) *ExpressionChain {
	ec.appendExpandedOp(fmt.Sprintf("%s ON %s", expr, on), sqlFullJoin, SQLNothing, args...)
	return ec
}

// OrderBy adds a 'ORDER BY' to the 'ExpressionChain' and returns the same chan to facilitate
// further chaining.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpressionChain) OrderBy(order *OrderByOperator) *ExpressionChain {
	ec.appendExpandedOp(order.String(), sqlOrder, SQLNothing)
	return ec
}

// GroupBy adds a 'GROUP BY' to the 'ExpressionChain' and returns the same chan to facilitate
// further chaining.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpressionChain) GroupBy(expr string, args ...interface{}) *ExpressionChain {
	ec.appendExpandedOp(expr, sqlGroup, SQLNothing, args...)
	return ec
}

// GroupByReplace adds a 'GROUP BY' to the 'ExpressionChain' and returns the same chain to facilitate
// further chaining, this version of group by removes all other group by entries.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
func (ec *ExpressionChain) GroupByReplace(expr string, args ...interface{}) *ExpressionChain {
	ec.removeOfType(sqlGroup)
	ec.appendExpandedOp(expr, sqlGroup, SQLNothing, args...)
	return ec
}

// AddUnionFromChain renders the passed chain and adds it to the current one as a Union
// returned ExpressionChain pointer is of current chain modified.
func (ec *ExpressionChain) AddUnionFromChain(union *ExpressionChain, all bool) (*ExpressionChain, error) {
	if len(union.ctes) != 0 {
		return nil, errors.Errorf("cannot handle unions with CTEs outside of the primary query.")
	}
	expr, args, err := union.RenderRaw()
	if err != nil {
		return nil, errors.Wrap(err, "rendering union query")
	}

	return ec.Union(expr, all, args...), nil
}

// Union adds the passed SQL expression and args as a union to be made on this expression, the
// change is in place, there are no checks about correctness of the query.
func (ec *ExpressionChain) Union(unionExpr string, all bool, args ...interface{}) *ExpressionChain {
	atom := querySegmentAtom{
		segment:    sqlUnion,
		expression: ec.populateTablePrefixes(unionExpr),
		arguments:  args,
	}
	if all {
		atom.sqlModifier = SQLAll
	}
	ec.append(atom)
	return ec
}

// ForUpdate appends `FOR UPDATE` to a SQL SELECT
func (ec *ExpressionChain) ForUpdate() *ExpressionChain {
	ec.append(querySegmentAtom{
		segment:     gaumSuffix,
		sqlModifier: SQLForUpdate,
	})
	return ec
}
