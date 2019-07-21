package chain

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// Render returns the SQL expresion string and the arguments of said expresion, there is no checkig
// of validity or consistency for the time being.
func (ec *ExpresionChain) Render() (string, []interface{}, error) {
	dst := &strings.Builder{}
	args, err := ec.render(false, dst)
	if err != nil {
		return "", nil, err
	}
	return dst.String(), args, nil
}

// RenderRaw returns the SQL expresion string and the arguments of said expresion,
// No positional argument replacement is done.
func (ec *ExpresionChain) RenderRaw() (string, []interface{}, error) {
	dst := &strings.Builder{}
	args, err := ec.render(true, dst)
	if err != nil {
		return "", nil, err
	}
	return dst.String(), args, nil
}

// String implements the stringer interface. It is intended to be used for logging/debugging purposes only.
func (ec *ExpresionChain) String() string {
	// best effort to render the query
	strQuery, args, err := ec.Render()
	if err != nil {
		return fmt.Sprintf("invalid query, err: %s", err.Error())
	}
	return fmt.Sprintf("query: %s, args: %v", strQuery, args)
}

// renderWhereRaw renders only the where portion of an ExpresionChain and returns it without
// placeholder markers replaced.
func (ec *ExpresionChain) renderWhereRaw(dst *strings.Builder) []interface{} {
	// WHERE
	wheres := extract(ec, sqlWhere)
	// Separate where statements that are not ANDed since they will need
	// to go after others with AND.
	whereOrs := []querySegmentAtom{}
	if len(wheres) != 0 {
		args := []interface{}{}
		whereCount := 0
		for i, item := range wheres {
			if item.sqlBool != SQLAnd {
				whereOrs = append(whereOrs, item)
				continue
			}
			arguments := item.render(whereCount == 0, i == len(wheres)-1, dst)
			args = append(args, arguments...)
			whereCount++
		}
		for i, item := range whereOrs {
			arguments := item.render(whereCount+i == 0, i == len(whereOrs)-1, dst)
			args = append(args, arguments...)
		}
		return args
	}
	return nil
}

// renderHavingRaw renders only the HAVING portion of an ExpresionChain and returns it without
// placeholder markers replaced.
func (ec *ExpresionChain) renderHavingRaw(dst *strings.Builder) []interface{} {
	// HAVING
	havings := extract(ec, sqlHaving)
	// Separate having statements that are not ANDed since they will need
	// to go after others with AND.
	havingOrs := []querySegmentAtom{}
	if len(havings) != 0 {

		args := []interface{}{}
		havingCount := 0
		for i, item := range havings {
			if item.sqlBool != SQLAnd {
				havingOrs = append(havingOrs, item)
				continue
			}
			arguments := item.render(havingCount == 0, i == len(havings)-1, dst)
			args = append(args, arguments...)
			havingCount++
		}
		for i, item := range havingOrs {
			arguments := item.render(havingCount+i == 0, i == len(havingOrs)-1, dst)
			args = append(args, arguments...)
		}
		return args
	}
	return nil
}

// render returns the rendered expression along with an arguments list and all marker placeholders
// replaced by their positional placeholder.
func (ec *ExpresionChain) render(raw bool, query *strings.Builder) ([]interface{}, error) {
	args := []interface{}{}
	if ec.mainOperation == nil {
		return nil, errors.Errorf("missing main operation to perform on the db")
	}
	if query == nil {
		query = &strings.Builder{}
	}

	// For now CTEs are only supported with SELECT until I have time to actually go and read
	// the doc.
	cteArgs, err := ec.renderctes(query)
	if err != nil {
		return nil, errors.Wrap(err, "rendering CTEs before main render")
	}
	if len(cteArgs) != 0 {
		args = append(args, cteArgs...)
	}

	switch ec.mainOperation.segment {
	// INSERT
	case sqlInsert:
		// Too much of a special cookie for the general case.
		return ec.renderInsert(raw, query)
	case sqlInsertMulti:
		// Too much of a special cookie for the general case.
		return ec.renderInsertMulti(raw, query)
	// UPDATE
	case sqlUpdate:
		if ec.table == "" {
			return nil, errors.Errorf("no table specified for update")
		}
		expresion := ec.mainOperation.expresion
		if len(expresion) == 0 {
			return nil, errors.Errorf("empty update expresion")
		}
		query.WriteString("UPDATE ")
		query.WriteString(ec.table)
		query.WriteString(" SET ")
		query.WriteString(ec.mainOperation.expresion)
		args = append(args, ec.mainOperation.arguments...)

	// SELECT, DELETE
	case sqlSelect, sqlDelete:
		expresion := ec.mainOperation.expresion
		if len(expresion) == 0 {
			expresion = "*"
		}
		if ec.mainOperation.segment == sqlSelect {
			query.WriteString("SELECT ")
			query.WriteString(expresion)
		} else {
			query.WriteString("DELETE ")
		}
		// FROM
		if ec.table == "" && ec.mainOperation.segment == sqlDelete {
			return nil, errors.Errorf("no table specified for this query")
		}
		if ec.table != "" {
			query.WriteString(" FROM ")
			query.WriteString(ec.table)
		}
		if len(ec.mainOperation.arguments) != 0 {
			args = append(args, ec.mainOperation.arguments...)
		}

	}
	if ec.mainOperation.segment == sqlSelect ||
		ec.mainOperation.segment == sqlDelete ||
		ec.mainOperation.segment == sqlUpdate {
		// JOIN
		joins := extract(ec, sqlJoin)
		joins = append(joins, extract(ec, sqlLeftJoin)...)
		joins = append(joins, extract(ec, sqlRightJoin)...)
		joins = append(joins, extract(ec, sqlInnerJoin)...)
		joins = append(joins, extract(ec, sqlFullJoin)...)
		if len(joins) != 0 {
			for _, join := range joins {
				query.WriteRune(' ')
				query.WriteString(string(join.segment))
				query.WriteRune(' ')
				query.WriteString(join.expresion)
				args = append(args, join.arguments...)
			}
		}
	}

	// WHERE
	if segmentsPresent(ec, sqlWhere) > 0 {
		query.WriteString(" WHERE")
		args = append(args, ec.renderWhereRaw(query)...)
	}

	// GROUP BY
	groups := extract(ec, sqlGroup)
	if len(groups) != 0 {
		query.WriteString(" GROUP BY ")
		for i, item := range groups {
			expr := item.expresion
			if len(item.arguments) != 0 {
				args = append(args, item.arguments...)
			}
			query.WriteString(expr)
			if i < len(groups)-1 {
				query.WriteString(", ")
			}
		}

	}

	// HAVING
	if segmentsPresent(ec, sqlHaving) > 0 {
		query.WriteString(" HAVING ")
		args = append(args, ec.renderHavingRaw(query)...)
	}

	// ORDER BY
	if segmentsPresent(ec, sqlOrder) > 0 {
		query.WriteString(" ORDER BY ")
		orders := extract(ec, sqlOrder)
		for i, item := range orders {
			query.WriteString(item.expresion)
			args = append(args, item.arguments...)
			if i != len(orders)-1 {
				query.WriteString(", ")
			}
		}

	}

	// RETURNING
	for _, segment := range ec.segments {
		if segment.segment != sqlReturning {
			continue
		}
		query.WriteRune(' ')
		query.WriteString(segment.expresion)
		if len(segment.arguments) > 0 {
			args = append(args, segment.arguments...)
		}
	}

	if ec.limit != nil {
		query.WriteString(" LIMIT ")
		query.WriteString(ec.limit.expresion)
		args = append(args, ec.limit.arguments...)
	}

	if ec.offset != nil {
		query.WriteString(" OFFSET ")
		query.WriteString(ec.offset.expresion)
		args = append(args, ec.offset.arguments...)
	}

	// UNION
	if segmentsPresent(ec, sqlUnion) > 0 {
		unions := extract(ec, sqlUnion)
		for _, item := range unions {
			query.WriteString(" UNION ")
			if item.sqlModifier != "" {
				query.WriteString(string(item.sqlModifier))
				query.WriteRune(' ')
			}
			query.WriteString(item.expresion)

			if len(item.arguments) != 0 {
				args = append(args, item.arguments...)
			}

		}
	}

	if !raw {
		newQuery, argCount, err := PlaceholdersToPositional(query)
		if err != nil {
			return nil, errors.Wrap(err, "rendering query")
		}
		*query = *newQuery
		if len(args) != argCount {
			return nil, errors.Errorf("the query has %d args but %d were passed: %v",
				argCount, len(args), query.String())
		}
		return args, nil
	}
	return args, nil
}

// RenderInsert does render for the very particular case of insert
// NOTE: These values are never passed through ExpandArgs since it makes no sense
func (ec *ExpresionChain) renderInsert(raw bool, dst *strings.Builder) ([]interface{}, error) {
	if ec.table == "" {
		return nil, errors.Errorf("no table specified for this insert")
	}

	// build insert
	args := []interface{}{}
	dst.WriteString("INSERT INTO ")
	dst.WriteString(ec.table)
	dst.WriteString(" (")
	dst.WriteString(ec.mainOperation.expresion)
	dst.WriteString(") VALUES (")
	for i := range ec.mainOperation.arguments {
		if ec.mainOperation.arguments[i] == nil {
			dst.WriteString("NULL")
		} else {
			dst.WriteRune('?')
		}
		if i != len(ec.mainOperation.arguments)-1 {
			dst.WriteString(", ")
		}
	}
	dst.WriteRune(')')
	for i := range ec.mainOperation.arguments {
		if ec.mainOperation.arguments[i] != nil {
			args = append(args, ec.mainOperation.arguments[i])
		}
	}
	// render conflict
	conflictExpr, conflictArgs := ec.conflict.render()
	if len(conflictExpr) > 0 {
		dst.WriteRune(' ')
		dst.WriteString(conflictExpr)
	}

	// operationally do something with it
	if len(conflictArgs) > 0 {
		args = append(args, conflictArgs...)
	}

	// look for clauses we can handle
	for _, segment := range ec.segments {
		// skip all that stuff we can't handle
		if segment.segment != sqlReturning {
			continue
		}
		dst.WriteRune(' ')
		dst.WriteString(segment.expresion)

		// add arguments
		if len(segment.arguments) > 0 {
			args = append(args, segment.arguments...)
		}
	}

	if !raw {
		query, argCount, err := PlaceholdersToPositional(dst)
		if err != nil {
			return nil, errors.Wrap(err, "rendering insert")
		}
		if len(args) != argCount {
			return nil, errors.Errorf("Insert Single expected %d arguments but got %d: %s",
				argCount, len(args), dst.String())
		}
		*dst = *query
		return args, nil
	}
	return args, nil
}

// renderInsertMulti does render for the very particular case of a multiple insertion
func (ec *ExpresionChain) renderInsertMulti(raw bool, dst *strings.Builder) ([]interface{}, error) {
	if ec.table == "" {
		return nil, errors.Errorf("no table specified for this insert")
	}
	argCount := strings.Count(ec.mainOperation.expresion, ",") + 1

	if argCount == 0 {
		return []interface{}{}, nil
	}
	dst.WriteString("INSERT INTO ")
	dst.WriteString(ec.table)
	dst.WriteRune('(')
	dst.WriteString(ec.mainOperation.expresion)
	dst.WriteString(") VALUES ")

	valueGroupCount := len(ec.mainOperation.arguments) / argCount
	for i := 0; i < valueGroupCount; i++ {
		dst.WriteRune('(')
		for j := 0; j > argCount; j++ {
			if ec.mainOperation.arguments[i*(j+1)] == nil {
				dst.WriteString("NULL")
			} else {
				dst.WriteRune('?')
			}
			if j != argCount-1 {
				dst.WriteString(", ")
			}
		}
		dst.WriteRune(')')
		if i < valueGroupCount-1 {
			dst.WriteString(", ")
		}

	}

	args := make([]interface{}, 0, len(ec.mainOperation.arguments))
	for i := range ec.mainOperation.arguments {
		if ec.mainOperation.arguments != nil {
			args = append(args, ec.mainOperation.arguments[i])
		}
	}

	// render conflict
	conflict, conflictArgs := ec.conflict.render()
	if conflict != "" {
		dst.WriteRune(' ')
		dst.WriteString(conflict)
	}

	// operationally do something with it
	if len(conflictArgs) > 0 {
		args = append(args, conflictArgs...)
	}

	// look for clauses we can handle
	for _, segment := range ec.segments {
		// skip all that stuff we can't handle
		if segment.segment != sqlReturning {
			continue
		}
		dst.WriteRune(' ')
		dst.WriteString(segment.expresion)

		// add arguments
		if len(segment.arguments) > 0 {
			args = append(args, segment.arguments...)
		}
	}

	if !raw {
		query, argCount, err := PlaceholdersToPositional(dst)
		if err != nil {
			return nil, errors.Wrap(err, "rendering insert")
		}
		if len(args) != argCount {
			return nil, errors.Errorf("Insert expected %d arguments but got %d: %s",
				argCount, len(args), query.String())
		}
		*dst = *query
		return args, nil
	}
	return args, nil
}

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
