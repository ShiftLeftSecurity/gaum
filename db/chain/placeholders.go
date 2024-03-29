package chain

import (
	"math"
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
	skip := false
	for i, queryChar := range querySegment {
		if skip {
			skip = false
			continue
		}

		if queryChar == '\\' && i < len(querySegment)-1 && querySegment[i+1] == '?' {
			// Escaped '?'
			newQuery.WriteString("\\?")
			skip = true
			continue
		}

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

	// assume a nil pointer is a null
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
	// TODO: use an actual parser <3
	// TODO: structure query segments around SQL-Standard AST
	queryWithArgs := &strings.Builder{}
	argCounter := 1
	argPositioner := 0
	expandedArgs := []interface{}{}
	skip := false
	for i, queryChar := range q {
		if skip {
			skip = false
			continue
		}

		if queryChar == '\\' && i < len(q)-1 && q[i+1] == '?' {
			// Escaped '?'
			queryWithArgs.WriteRune('?')
			skip = true
			continue
		}
		if queryChar == '?' {
			arg := args[argPositioner]
			switch reflect.TypeOf(arg).Kind() {
			case reflect.Slice:
				elementType := reflect.TypeOf(arg).Elem().Kind()
				if elementType != reflect.Int8 && elementType != reflect.Uint8 {
					s := reflect.ValueOf(arg)
					for i := 0; i < s.Len(); i++ {
						expandedArgs = append(expandedArgs, s.Index(i).Interface())
						queryWithArgs.WriteRune('$')
						queryWithArgs.WriteString(strconv.Itoa(argCounter))
						if i != s.Len()-1 {
							queryWithArgs.WriteString(", ")
						}
						argCounter++
					}
				} else {
					expandedArgs = append(expandedArgs, arg)
					queryWithArgs.WriteRune('$')
					queryWithArgs.WriteString(strconv.Itoa(argCounter))
					argCounter++
				}
			default:
				expandedArgs = append(expandedArgs, arg)
				queryWithArgs.WriteRune('$')
				queryWithArgs.WriteString(strconv.Itoa(argCounter))
				argCounter++
			}
			argPositioner++
		} else {
			queryWithArgs.WriteRune(queryChar)
		}
	}
	if len(expandedArgs) != argCounter-1 {
		return "", nil, errors.Errorf("the query has %d args but %d were passed: \n %q \n %#v",
			argCounter-1, len(args), queryWithArgs, args)
	}
	return queryWithArgs.String(), expandedArgs, nil
}

// PlaceholdersToPositional converts ? in a query into $<argument number> which postgres expects
func PlaceholdersToPositional(q *strings.Builder, argCount int) (*strings.Builder, int, error) {
	// TODO: use an actual parser <3
	// TODO: structure query segments around SQL-Standard AST
	newQ := &strings.Builder{}
	// new string should accommodate the digits we are adding for positional arguments.
	renderedLength := q.Len() + digitSize(argCount)
	if newQ.Len() < renderedLength {
		newQ.Grow(renderedLength - newQ.Len())
	}

	queryString := q.String()
	argCounter := 1
	skip := false
	for i, queryChar := range queryString {
		if skip {
			skip = false
			continue
		}

		if queryChar == '\\' && i < len(queryString)-1 && queryString[i+1] == '?' {
			// Escaped '?'
			newQ.WriteRune('?')
			skip = true
			continue
		}

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

// digitSize returns the amount of digits required to represent the argument placeholders
// of a query, not including the $ symbol, pg will not like more than max(uint16) arguments
// but we won't enforce that here.
func digitSize(argLen int) int {
	var repSize int
	argLenLen := len(strconv.Itoa(argLen))
	for i := 1; i < argLenLen; i++ {
		a := (9 * int(math.Pow10(i-1))) * i
		repSize += a
	}

	pow10 := math.Pow10(argLenLen - 1)
	repSize += (argLen - (int(pow10) - 1)) * argLenLen

	return repSize
}
