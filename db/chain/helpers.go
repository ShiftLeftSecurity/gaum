package chain

import "fmt"

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
func Equals(field string, value ...interface{}) (string, []interface{}) {
	return fmt.Sprintf("%s == ?", field), value
}

// NotEquals is a convenience function to enable use of go for where definitions
func NotEquals(field string, value ...interface{}) (string, []interface{}) {
	return fmt.Sprintf("%s != ?", field), value
}

// GreaterThan is a convenience function to enable use of go for where definitions
func GreaterThan(field string, value ...interface{}) (string, []interface{}) {
	return fmt.Sprintf("%s > ?", field), value
}

// GreaterOrEqualThan is a convenience function to enable use of go for where definitions
func GreaterOrEqualThan(field string, value ...interface{}) (string, []interface{}) {
	return fmt.Sprintf("%s >= ?", field), value
}

// LesserThan is a convenience function to enable use of go for where definitions
func LesserThan(field string, value ...interface{}) (string, []interface{}) {
	return fmt.Sprintf("%s < ?", field), value
}

// LesserOrEqualThan is a convenience function to enable use of go for where definitions
func LesserOrEqualThan(field string, value ...interface{}) (string, []interface{}) {
	return fmt.Sprintf("%s <= ?", field), value
}

// In is a convenience function to enable use of go for where definitions
func In(field string, value ...interface{}) (string, []interface{}) {
	return fmt.Sprintf("%s IN (?)", field), value
}
