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
	"regexp"

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
	// SQLAndNot Negates the expresion after AND
	SQLAndNot sqlBool = "AND NOT"
	// SQLOrNot Neates the expresion after OR
	SQLOrNot sqlBool = "OR NOT"
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
		fields, err = selectparse.FieldsFromSelect(q.expresion)
		if err != nil {
			// We do not have a case for errors here since missing fields will just
			// prompt the DB for the columns
			return []string{}
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
