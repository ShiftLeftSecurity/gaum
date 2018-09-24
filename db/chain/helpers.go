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

import "fmt"

const (
	// NullValue represents the NULL value in SQL
	NullValue = "NULL"
	// CurrentTimestampPGFn is the name of the function of postgres that returns current
	// timestamp with tz.
	CurrentTimestampPGFn = "CURRENT_TIMESTAMP"
)

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
