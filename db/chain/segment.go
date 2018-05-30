package chain

import (
	"fmt"
	"strings"
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
