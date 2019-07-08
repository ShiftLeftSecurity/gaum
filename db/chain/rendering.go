package chain

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// Render returns the SQL expresion string and the arguments of said expresion, there is no checkig
// of validity or consistency for the time being.
func (ec *ExpresionChain) Render() (string, []interface{}, error) {
	return ec.render(false)
}

// RenderRaw returns the SQL expresion string and the arguments of said expresion,
// No positional argument replacement is done.
func (ec *ExpresionChain) RenderRaw() (string, []interface{}, error) {
	return ec.render(true)
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
func (ec *ExpresionChain) renderWhereRaw() (string, []interface{}) {
	// WHERE
	wheres := extract(ec, sqlWhere)
	// Separate where statements that are not ANDed since they will need
	// to go after others with AND.
	whereOrs := []querySegmentAtom{}
	if len(wheres) != 0 {
		whereStatement := ""
		args := []interface{}{}
		whereCount := 0
		for i, item := range wheres {
			if item.sqlBool != SQLAnd {
				whereOrs = append(whereOrs, item)
				continue
			}
			expr, arguments := item.render(whereCount == 0, i == len(wheres)-1)
			whereStatement += expr
			args = append(args, arguments...)
			whereCount++
		}
		for i, item := range whereOrs {
			expr, arguments := item.render(whereCount+i == 0, i == len(whereOrs)-1)
			whereStatement += expr
			args = append(args, arguments...)
		}
		return whereStatement, args
	}
	return "", nil
}

// renderHavingRaw renders only the HAVING portion of an ExpresionChain and returns it without
// placeholder markers replaced.
func (ec *ExpresionChain) renderHavingRaw() (string, []interface{}) {
	// HAVING
	havings := extract(ec, sqlHaving)
	// Separate having statements that are not ANDed since they will need
	// to go after others with AND.
	havingOrs := []querySegmentAtom{}
	if len(havings) != 0 {
		havingStatement := ""
		args := []interface{}{}
		havingCount := 0
		for i, item := range havings {
			if item.sqlBool != SQLAnd {
				havingOrs = append(havingOrs, item)
				continue
			}
			expr, arguments := item.render(havingCount == 0, i == len(havings)-1)
			havingStatement += expr
			args = append(args, arguments...)
			havingCount++
		}
		for i, item := range havingOrs {
			expr, arguments := item.render(havingCount+i == 0, i == len(havingOrs)-1)
			havingStatement += expr
			args = append(args, arguments...)
		}
		return havingStatement, args
	}
	return "", nil
}

// render returns the rendered expression along with an arguments list and all marker placeholders
// replaced by their positional placeholder.
func (ec *ExpresionChain) render(raw bool) (string, []interface{}, error) {
	args := []interface{}{}
	var query string
	if ec.mainOperation == nil {
		return "", nil, errors.Errorf("missing main operation to perform on the db")
	}

	// For now CTEs are only supported with SELECT until I have time to actually go and read
	// the doc.
	cteQ, cteArgs, err := ec.renderctes()
	if err != nil {
		return "", nil, errors.Wrap(err, "rendering CTEs before main render")
	}

	switch ec.mainOperation.segment {
	// INSERT
	case sqlInsert:
		// Too much of a special cookie for the general case.
		return ec.renderInsert(raw)
	case sqlInsertMulti:
		// Too much of a special cookie for the general case.
		return ec.renderInsertMulti(raw)
	// UPDATE
	case sqlUpdate:
		if ec.table == "" {
			return "", nil, errors.Errorf("no table specified for update")
		}
		expresion := ec.mainOperation.expresion
		if len(expresion) == 0 {
			return "", nil, errors.Errorf("empty update expresion")
		}
		query = fmt.Sprintf("UPDATE %s SET %s",
			ec.table, ec.mainOperation.expresion)
		args = append(args, ec.mainOperation.arguments...)

	// SELECT, DELETE
	case sqlSelect, sqlDelete:
		expresion := ec.mainOperation.expresion
		if len(expresion) == 0 {
			expresion = "*"
		}
		if ec.mainOperation.segment == sqlSelect {
			query = fmt.Sprintf("SELECT %s",
				expresion)
			if len(cteQ) != 0 {
				query = fmt.Sprintf("%s %s", cteQ, query)
				args = append(args, cteArgs...)
			}
		} else {
			query = "DELETE "
		}
		// FROM
		if ec.table == "" && ec.mainOperation.segment == sqlDelete {
			return "", nil, errors.Errorf("no table specified for this query")
		}
		if ec.table != "" {
			query += fmt.Sprintf(" FROM %s", ec.table)
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

				query += fmt.Sprintf(" %s %s",
					join.segment,
					join.expresion)
				args = append(args, join.arguments...)
			}
		}
	}

	// WHERE
	wheres, whereArgs := ec.renderWhereRaw()
	if wheres != "" {
		query += " WHERE" + wheres
		args = append(args, whereArgs...)
	}

	// GROUP BY
	groups := extract(ec, sqlGroup)
	groupByStatement := " GROUP BY "
	if len(groups) != 0 {
		groupCriteria := []string{}
		for _, item := range groups {
			expr := item.expresion
			if len(item.arguments) != 0 {
				arguments := item.arguments
				args = append(args, arguments...)
			}
			groupCriteria = append(groupCriteria, expr)
		}
		query += groupByStatement
		query += strings.Join(groupCriteria, ", ")
	}

	// HAVING
	having, havingArgs := ec.renderHavingRaw()
	if having != "" {
		query += " HAVING " + having
		args = append(args, havingArgs...)
	}

	// ORDER BY
	orders := extract(ec, sqlOrder)
	orderByStatemet := " ORDER BY "
	if len(orders) != 0 {
		orderCriteria := []string{}
		for _, item := range orders {
			expr := item.expresion
			arguments := item.arguments
			args = append(args, arguments...)
			orderCriteria = append(orderCriteria, expr)
		}
		query += orderByStatemet
		query += strings.Join(orderCriteria, ", ")
	}

	// RETURNING
	for _, segment := range ec.segments {
		if segment.segment != sqlReturning {
			continue
		}
		query += " " + segment.expresion
		if len(segment.arguments) > 0 {
			args = append(args, segment.arguments...)
		}
	}

	if ec.limit != nil {
		query += " LIMIT " + ec.limit.expresion
		args = append(args, ec.limit.arguments...)
	}

	if ec.offset != nil {
		query += " OFFSET " + ec.offset.expresion
		args = append(args, ec.offset.arguments...)
	}

	// UNION
	unions := extract(ec, sqlUnion)
	unionStatement := " UNION "
	if len(unions) != 0 {
		unionQueries := []string{}
		for _, item := range unions {
			expr := item.expresion
			if item.sqlModifier != "" {
				expr = fmt.Sprintf("%s %s", item.sqlModifier, expr)
			}
			if len(item.arguments) != 0 {
				arguments := item.arguments
				args = append(args, arguments...)
			}
			unionQueries = append(unionQueries, expr)
		}
		query += unionStatement
		query += strings.Join(unionQueries, " UNION ")
	}

	if !raw {
		var err error
		query, args, err = MarksToPlaceholders(query, args)
		if err != nil {
			return "", nil, errors.Wrap(err, "rendering query")
		}
		return query, args, nil
	}
	return query, args, nil
}

// RenderInsert does render for the very particular case of insert
func (ec *ExpresionChain) renderInsert(raw bool) (string, []interface{}, error) {
	if ec.table == "" {
		return "", nil, errors.Errorf("no table specified for this insert")
	}
	placeholders := make([]string, len(ec.mainOperation.arguments))
	for i := range ec.mainOperation.arguments {
		placeholders[i] = "?"
	}

	// build insert
	args := make([]interface{}, 0)
	args = append(args, ec.mainOperation.arguments...)
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		ec.table,
		ec.mainOperation.expresion,
		strings.Join(placeholders, ", "))

	// render conflict
	conflict, conflictArgs := ec.conflict.render()
	if conflict != "" {
		query += " " + conflict
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
		query += " " + segment.expresion
		// add arguments
		if len(segment.arguments) > 0 {
			args = append(args, segment.arguments...)
		}
	}

	if !raw {
		var err error
		query, args, err = MarksToPlaceholders(query, args)
		if err != nil {
			return "", nil, errors.Wrap(err, "rendering insert")
		}
		return query, args, nil
	}
	return query, args, nil
}

// renderInsertMulti does render for the very particular case of a multiple insertion
func (ec *ExpresionChain) renderInsertMulti(raw bool) (string, []interface{}, error) {
	if ec.table == "" {
		return "", nil, errors.Errorf("no table specified for this insert")
	}
	argCount := strings.Count(ec.mainOperation.expresion, ",") + 1
	placeholders := make([]string, argCount, argCount)
	for i := 0; i < argCount; i++ {
		placeholders[i] = "?"
	}

	values := make([]string, len(ec.mainOperation.arguments)/argCount,
		len(ec.mainOperation.arguments)/argCount)
	for i := 0; i < len(ec.mainOperation.arguments)/argCount; i++ {
		values[i] += fmt.Sprintf("(%s)", strings.Join(placeholders, ", "))
	}

	args := make([]interface{}, 0)
	args = append(args, ec.mainOperation.arguments...)
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		ec.table,
		ec.mainOperation.expresion,
		strings.Join(values, ", "))

	// render conflict
	conflict, conflictArgs := ec.conflict.render()
	if conflict != "" {
		query += " " + conflict
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
		query += " " + segment.expresion
		// add arguments
		if len(segment.arguments) > 0 {
			args = append(args, segment.arguments...)
		}
	}

	if !raw {
		var err error
		query, args, err = MarksToPlaceholders(query, args)
		if err != nil {
			return "", nil, errors.Wrap(err, "rendering insert multi")
		}
		return query, args, nil
	}
	return query, args, nil
}

// digitSize returns the amount of digits required to represent the argument placeholders
// of a query, not including the $ symbol, pg will not like more than max(uint16) arguments
// but we won't enforce that here.
func digitSize(argLen int) int {
	var repSize int
	argLenLen := int(len(strconv.Itoa(argLen)))
	for i := 1; i < argLenLen; i++ {
		a := (9 * int(math.Pow10(int(i)-1))) * i
		repSize += a
	}

	pow10 := math.Pow10(int(argLenLen) - 1)
	repSize += (argLen - (int(pow10) - 1)) * argLenLen

	return repSize
}

const commaSeparator = ", "

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
	//queryWithArgs := ""
	argCounter := 1
	argPositioner := 0
	totalArgLen := len(args)
	commas := 0
	for _, arg := range args {
		switch reflect.TypeOf(arg).Kind() {
		case reflect.Slice:
			elementType := reflect.TypeOf(arg).Elem().Kind()
			if elementType != reflect.Int8 && elementType != reflect.Uint8 {
				v := reflect.ValueOf(arg).Len()
				totalArgLen += v
				commas += (v - 1) * 2 //commans have spaces, we are civilized people.
			}
		}
	}
	expandedArgs := make([]interface{}, 0, totalArgLen)
	repSize := digitSize(int(totalArgLen)) + commas
	expectedSize := repSize + len(q)
	var queryWithArgs strings.Builder
	queryWithArgs.Grow(expectedSize)

	for _, queryChar := range q {
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
						if i < s.Len()-1 {
							queryWithArgs.WriteString(commaSeparator)
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
		return "", nil, errors.Errorf("the query has %d args but %d were passed: \n %#v",
			argCounter-1, len(args), args)
	}

	return queryWithArgs.String(), expandedArgs, nil
}
