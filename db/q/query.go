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

package q

import (
	c "github.com/ShiftLeftSecurity/gaum/db/chain"
)

// Q is the intended interface for interaction with SQL Queries.
type Q interface {
	Select(fields ...string) Q
	Insert(insertPairs map[string]interface{}) Q
	//Update(expr string, args ...interface{}) *ExpresionChain
	Update(exprMap map[string]interface{}) Q
	Delete() Q

	From(table string) Q
	Join(expr, on string, args ...interface{}) Q
	LeftJoin(expr, on string, args ...interface{}) Q
	RightJoin(expr, on string, args ...interface{}) Q
	InnerJoin(expr, on string, args ...interface{}) Q
	OuterJoin(expr, on string, args ...interface{}) Q

	AndWhere(expr string, args ...interface{}) Q
	OrWhere(expr string, args ...interface{}) Q
	AndWhereGroup(c Q) Q
	OrWhereGroup(c Q) Q

	OrderBy(order *c.OrderByOperator) Q
	GroupBy(expr string, args ...interface{}) Q

	Limit(limit int64) Q
	Offset(offset int64) Q

	OnConflict(clause func(*c.OnConflict)) Q
	Returning(args ...string) Q

	QueryOne(receiver interface{}) error
	QueryMany(receiverSlice interface{}) error
	Exec() error
}

func RawQueryOne(recipient interface{}, query string, args ...interface{}) error {
	return nil
}

func RawQuery(recipientSlice interface{}, query string, args ...interface{}) error {
	return nil
}

func Exec(query string, args ...interface{}) error {
	return nil
}

/*
BTW, I'd name these QueryOne and QueryMany then - it's simpler and more suggestive
How about naming all these RawQueryOne, RawQueryMany and RawExec (merge QueryRow and QueryRowIntoFields into RawQueryOne)?
I added Raw prefix because I imagine you can construct the query without the gaum special stuff like chain etc.
*/
