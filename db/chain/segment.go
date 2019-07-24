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
	"regexp"
	"strings"

	"github.com/ShiftLeftSecurity/gaum/selectparse"
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
	// SQLAndNot Negates the expression after AND
	SQLAndNot sqlBool = "AND NOT"
	// SQLOrNot Neates the expression after OR
	SQLOrNot sqlBool = "OR NOT"
)

type sqlModifier string

const (
	// SQLAll is a modifier that can be append to UNION, INTERSECT and EXCEPT
	SQLAll sqlModifier = "ALL"
)

type sqlSegment string

const (
	sqlWhere     sqlSegment = "WHERE"
	sqlLimit     sqlSegment = "LIMIT"
	sqlOffset    sqlSegment = "OFFSET"
	sqlJoin      sqlSegment = "JOIN"
	sqlLeftJoin  sqlSegment = "LEFT JOIN"
	sqlRightJoin sqlSegment = "RIGHT JOIN"
	sqlInnerJoin sqlSegment = "INNER JOIN"
	sqlFullJoin  sqlSegment = "FULL JOIN"
	sqlSelect    sqlSegment = "SELECT"
	sqlDelete    sqlSegment = "DELETE"
	sqlInsert    sqlSegment = "INSERT"
	sqlUpdate    sqlSegment = "UPDATE"
	sqlFrom      sqlSegment = "FROM"
	sqlGroup     sqlSegment = "GROUP BY"
	sqlOrder     sqlSegment = "ORDER BY"
	sqlReturning sqlSegment = "RETURNING"
	sqlHaving    sqlSegment = "HAVING"
	// SPECIAL CASES
	sqlInsertMulti sqlSegment = "INSERTM"
	sqlUnion                  = "UNION"
)

type querySegmentAtom struct {
	segment     sqlSegment
	expression   string
	arguments   []interface{}
	sqlBool     sqlBool
	sqlModifier sqlModifier
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
		expression: q.expression,
		sqlBool:   q.sqlBool,
		arguments: arguments,
	}
}

var nonFields = []struct {
	re      *regexp.Regexp
	replace []byte
}{
	{
		re:      regexp.MustCompile(`distinct on \(.+\)`),
		replace: []byte{},
	},
	{
		re:      regexp.MustCompile(`[a-z|A-Z]*\([^\(\)]*\)`), // strips funcs
		replace: []byte("placeholder"),
	},
}

func (q *querySegmentAtom) fields() []string {
	fields := []string{}
	if q.segment == sqlSelect {
		var err error
		fields, err = selectparse.FieldsFromSelect(q.expression)
		if err != nil {
			// We do not have a case for errors here since missing fields will just
			// prompt the DB for the columns
			return []string{}
		}
	}
	// TODO make UPDATE and INSERT for completion's sake
	return fields
}

func (q *querySegmentAtom) render(firstForSegment, lastForSegment bool,
	dst *strings.Builder) []interface{} {

	if !firstForSegment {
		dst.WriteRune(' ')
		dst.WriteString(string(q.sqlBool))
	}
	dst.WriteRune(' ')
	dst.WriteString(q.expression)
	return q.arguments
}
