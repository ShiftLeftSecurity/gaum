package chain

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// ExpandArgs will unravel a slice of arguments, converting slices into individual items
// to determine if an item needs unraveling it uses the placeholders (? marks) for the
// future positional arguments in a query segment.
func ExpandArgs(args []interface{}, querySegment string) (string, []interface{}) {
	expandedArgs := []interface{}{}
	newQuery := &strings.Builder{}
	var argPosition = 0
	for _, queryChar := range querySegment {
		if queryChar == '?' {
			arg := args[argPosition]
			if arg == nil {
				// nil pointer is considered NULL and this must be part of the query string to avoid
				// being escaped as the string "NULL"
				argPosition++
				newQuery.WriteString("NULL")
				continue
			}
			// If this is a supported slice we will expand it
			switch reflect.TypeOf(arg).Kind() {
			case reflect.Slice:
				elementType := reflect.TypeOf(arg).Elem().Kind()
				// So, if I recall correctly this avoids converting []byte into individual
				// byte arguments and passes it as one to most likely a bytea pg type
				if elementType != reflect.Int8 && elementType != reflect.Uint8 {
					s := reflect.ValueOf(arg)
					for i := 0; i < s.Len(); i++ {
						newQuery.WriteRune('?')
						if i != s.Len()-1 {
							newQuery.WriteString(", ")
						}
						expandedArgs = append(expandedArgs, s.Index(i).Interface())
					}
				} else {
					newQuery.WriteRune('?')
					expandedArgs = append(expandedArgs, arg)
				}
			default:
				newQuery.WriteRune('?')
				expandedArgs = append(expandedArgs, arg)
			}
			argPosition++
			continue
		}
		newQuery.WriteRune(queryChar)

	}
	return newQuery.String(), expandedArgs
}

// MarksToPlaceholders replaces `?` in the query with `$1` style placeholders, this must be
// done with a finished query and requires the args as they depend on the position of the
// already rendered query, it does some consistency control and finally expands `(?)`.
func MarksToPlaceholders(q string, args []interface{}) (string, []interface{}, error) {

	// assume a nill pointer is a null
	// this is hacky, but it should work
	otherArgs := make([]interface{}, len(args))
	for index, arg := range args {
		if arg == nil {
			otherArgs[index] = "NULL"
		} else {
			otherArgs[index] = arg
		}
	}
	args = otherArgs

	// TODO: make this a bit less ugly
	// TODO: identify escaped questionmarks
	// TODO: use an actual parser <3
	// TODO: structure query segments around SQL-Standard AST
	queryWithArgs := ""
	argCounter := 1
	argPositioner := 0
	expandedArgs := []interface{}{}
	for _, queryChar := range q {
		if queryChar == '?' {
			arg := args[argPositioner]
			switch reflect.TypeOf(arg).Kind() {
			case reflect.Slice:
				elementType := reflect.TypeOf(arg).Elem().Kind()
				if elementType != reflect.Int8 && elementType != reflect.Uint8 {
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
			default:
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

// PlaceholdersToPositional converts ? in a query into $<argument number> which postgres expects
func PlaceholdersToPositional(q *strings.Builder) (*strings.Builder, int, error) {
	// TODO: identify escaped questionmarks
	// TODO: use an actual parser <3
	// TODO: structure query segments around SQL-Standard AST
	newQ := &strings.Builder{}
	if newQ.Len() < q.Len() {
		newQ.Grow(q.Len() - newQ.Len())
	}

	argCounter := 1
	for _, queryChar := range q.String() {
		if queryChar == '?' {
			newQ.WriteRune('$')
			newQ.WriteString(strconv.Itoa(argCounter))
			argCounter++
			continue
		}
		newQ.WriteRune(queryChar)
	}

	return newQ, argCounter - 1, nil
}
