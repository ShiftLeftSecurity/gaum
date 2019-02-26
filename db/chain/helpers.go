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
	"strings"
)

const (
	// NullValue represents the NULL value in SQL
	NullValue = "NULL"
	// CurrentTimestampPGFn is the name of the function of postgres that returns current
	// timestamp with tz.
	CurrentTimestampPGFn = "CURRENT_TIMESTAMP"
)

// SimpleFunction returns the rendered fName invocation passing params as argument
func SimpleFunction(fName, params string) string {
	return fmt.Sprintf("%s(%s)", fName, params)
}

// AVG Renders SQL AVG of the expresion in column
func AVG(column string) string {
	return SimpleFunction("AVG", column)
}

// COUNT Renders SQL COUNT of the expresion in column
func COUNT(column string) string {
	return SimpleFunction("COUNT", column)
}

// MIN Renders SQL MIN of the expresion in column
func MIN(column string) string {
	return SimpleFunction("MIN", column)
}

// MAX Renders SQL MAX of the expresion in column
func MAX(column string) string {
	return SimpleFunction("MAX", column)
}

// SUM Renders SQL SUM of the expresion in column
func SUM(column string) string {
	return SimpleFunction("SUM", column)
}

// Function represents a SQL function.
type Function interface {
	// Static adds an argument to the function
	Static(string) Function
	// Parametric adds a placeholder and an argument to the function
	Parametric(interface{}) Function
	// Fn returns the rendered statemtn and list of arguments.
	Fn() (string, []interface{})
	// FnSelect returns a SelectArgument from this function
	FnSelect() SelectArgument
}

type complexFunction struct {
	name          string
	argumentItems []interface{}
	arguments     []string
}

// Static implements Function
func (cf *complexFunction) Static(field string) Function {
	cf.arguments = append(cf.arguments, field)
	return cf
}

// Parametric implements Function
func (cf *complexFunction) Parametric(arg interface{}) Function {
	cf.arguments = append(cf.arguments, "?")
	cf.argumentItems = append(cf.argumentItems, arg)
	return cf
}

// Fn implements Function
func (cf *complexFunction) Fn() (string, []interface{}) {
	return fmt.Sprintf("%s(%s)", cf.name, strings.Join(cf.arguments, ", ")), cf.argumentItems
}

// FnSelect implements Function
func (cf *complexFunction) FnSelect() SelectArgument {
	return SelectArgument{
		Field: fmt.Sprintf("%s(%s)", cf.name, strings.Join(cf.arguments, ", ")),
		Args:  cf.argumentItems,
	}
}

// ComplexFunction constructs a complexFunction
func ComplexFunction(name string) Function {
	return &complexFunction{
		name:          name,
		argumentItems: []interface{}{},
		arguments:     []string{},
	}
}

// TablePrefix returns a function that prefixes column names with the passed table name.
func TablePrefix(n string) func(string) string {
	if n == "" {
		return func(c string) string {
			return c
		}
	}
	return func(c string) string {
		return fmt.Sprintf("%s.%s", n, c)
	}
}

// ColumnGroup returns a list of columns, comma separated and between parenthesis.
func ColumnGroup(columns ...string) string {
	return fmt.Sprintf("(%s)", strings.Join(columns, ", "))
}

// AndConditions returns a list of conditions separated by AND
func AndConditions(conditions ...string) string {
	return strings.Join(conditions, " AND ")
}

// CompOperator represents a possible operator to compare two SQL expresions
type CompOperator string

var (
	// Eq is the = operand
	Eq CompOperator = "="
	// Neq is the != operand
	Neq CompOperator = "!="
	// Gt is the > operand
	Gt CompOperator = ">"
	// GtE is the >= operand
	GtE CompOperator = ">="
	// Lt is the < operand
	Lt CompOperator = "<"
	// LtE is the <= operand
	LtE CompOperator = "<="
	// Lk is the LIKE operand
	Lk CompOperator = "LIKE"
	// NLk is the NOT LIKE operand
	NLk CompOperator = "NOT LIKE"
)

// CompareExpresions returns a comparision between two SQL expresions using operator
func CompareExpresions(operator CompOperator, columnLeft, columnRight string) string {
	return fmt.Sprintf("%s %s %s", columnLeft, operator, columnRight)
}

// NillableString returns a safely dereferenced string from it's pointer.
func NillableString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// NillableInt64 returns a safely dereferenced int64 from it's pointer.
func NillableInt64(i *int64) int64 {
	if i == nil {
		return 0
	}
	return *i
}

// Constraint wraps the passed constraint name with the required SQL to use it.
func Constraint(constraint string) string {
	return "ON CONSTRAINT " + constraint
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

// Allow selection of distinct results only.
func Distinct(field string) string {
	return fmt.Sprintf("DISTINCT %s", field)
}

// As is a convenience function to define column alias in go in order to be a bit less error prone
// and more go semantic.
func As(field, alias string) string {
	return fmt.Sprintf("%s AS %s", field, alias)
}

// Equals is a convenience function to enable use of go for where definitions
func Equals(field string) string {
	return fmt.Sprintf("%s = ?", field)
}

// NotEquals is a convenience function to enable use of go for where definitions
func NotEquals(field string) string {
	return fmt.Sprintf("%s != ?", field)
}

// GreaterThan is a convenience function to enable use of go for where definitions
func GreaterThan(field string) string {
	return fmt.Sprintf("%s > ?", field)
}

// GreaterOrEqualThan is a convenience function to enable use of go for where definitions
func GreaterOrEqualThan(field string) string {
	return fmt.Sprintf("%s >= ?", field)
}

// LesserThan is a convenience function to enable use of go for where definitions
func LesserThan(field string) string {
	return fmt.Sprintf("%s < ?", field)
}

// LesserOrEqualThan is a convenience function to enable use of go for where definitions
func LesserOrEqualThan(field string) string {
	return fmt.Sprintf("%s <= ?", field)
}

// In is a convenience function to enable use of go for where definitions
func In(field string, value ...interface{}) (string, []interface{}) {
	return fmt.Sprintf("%s IN (?)", field), value
}

// Like is a convenience function to enable use of go for where definitions
func Like(field string) string {
	return fmt.Sprintf("%s LIKE ?", field)
}

// NotLike is a convenience function to enable use of go for where definitions
func NotLike(field string) string {
	return fmt.Sprintf("%s NOT LIKE ?", field)
}

// InSlice is a convenience function to enable use of go for where definitions and assumes the
// passed value is already a slice.
func InSlice(field string, value interface{}) (string, interface{}) {
	return fmt.Sprintf("%s IN (?)", field), value
}

// NotNull is a convenience function to enable use of go for where definitions
func NotNull(field string) string {
	return fmt.Sprintf("%s IS NOT NULL", field)
}

// Null is a convenience function to enable use of go for where definitions
func Null(field string) string {
	return fmt.Sprintf("%s IS NULL", field)
}

// INSERT/UPDATE helpers

// SetToCurrentTimestamp crafts a postgres SQL assignement of the field to the current timestamp
// with timezone.
func SetToCurrentTimestamp(field string) string {
	return fmt.Sprintf("%s = %s", field, CurrentTimestampPGFn)
}
