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
	"sort"
	"strings"

	"github.com/pkg/errors"
)

// Select set fields to be returned by the final query.
func (ec *ExpressionChain) Select(fields ...string) *ExpressionChain {
	ec.mainOperation = &querySegmentAtom{
		segment:    sqlSelect,
		expression: ec.populateTablePrefixes(strings.Join(fields, ", ")),
		arguments:  nil,
		sqlBool:    SQLNothing,
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
func (ec *ExpressionChain) SelectWithArgs(fields ...SelectArgument) *ExpressionChain {
	var statements = make([]string, len(fields), len(fields))
	var args = []interface{}{}
	for i, v := range fields {
		if v.as != "" {
			v.Field = As(v.Field, v.as)
		}
		statements[i] = v.Field
		args = append(args, v.Args...)
	}
	ec.setExpandedMainOp(strings.Join(statements, ", "), sqlSelect, SQLNothing, args...)
	return ec
}

// Delete determines a deletion will be made with the results of the query.
func (ec *ExpressionChain) Delete() *ExpressionChain {
	ec.mainOperation = &querySegmentAtom{
		segment:   sqlDelete,
		arguments: nil,
		sqlBool:   SQLNothing,
	}
	return ec
}

// InsertMulti set fields/values for insertion.
func (ec *ExpressionChain) InsertMulti(insertPairs map[string][]interface{}) (*ExpressionChain, error) {
	exprKeys := make([]string, len(insertPairs), len(insertPairs))

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

	// No Escape Args for insert, it will be done upon render given its nature
	ec.mainOperation = &querySegmentAtom{
		segment:    sqlInsertMulti,
		expression: strings.Join(exprKeys, ", "),
		arguments:  exprValues,
		sqlBool:    SQLNothing,
	}
	return ec, nil
}

// Insert set fields/values for insertion.
func (ec *ExpressionChain) Insert(insertPairs map[string]interface{}) *ExpressionChain {
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
	// No Escape Args for insert, it will be done upon render given its nature
	ec.mainOperation = &querySegmentAtom{
		segment:    sqlInsert,
		expression: strings.Join(exprKeys, ", "),
		arguments:  exprValues,
		sqlBool:    SQLNothing,
	}
	return ec
}

// Update set fields/values for updates.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
//
// NOTE: values of `nil` will be treated as `NULL`
func (ec *ExpressionChain) Update(expr string, args ...interface{}) *ExpressionChain {
	ec.setExpandedMainOp(expr, sqlUpdate, SQLNothing, args...)
	return ec
}

// UpdateMap set fields/values for updates but does so from a map of key/value.
// THIS DOES NOT CREATE A COPY OF THE CHAIN, IT MUTATES IN PLACE.
//
// NOTE: values of `nil` will be treated as `NULL`
func (ec *ExpressionChain) UpdateMap(exprMap map[string]interface{}) *ExpressionChain {
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
	ec.setExpandedMainOp(expr, sqlUpdate, SQLNothing, args...)
	return ec
}
