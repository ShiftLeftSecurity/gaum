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
	"strings"
)

// OnConflict is chained to build `OnConflict` statements
type OnConflict struct {
	prefix string
	action *OnConflictAction
}

// OnConstraint is used to create an `On CONFLICT ON CONSTRAINT $arg` statement
func (o *OnConflict) OnConstraint(arg string) *OnConflictAction {
	o.prefix = strings.Join([]string{"ON", "CONSTRAINT", arg}, " ")
	o.action = &OnConflictAction{}
	return o.action
}

// OnColumn is used to construct `ON CONFLICT ( arg0, arg1, arg2 )`.
// This allows for build things like `ON COLUMN ( myindex, COLLATE my_other_index )`
func (o *OnConflict) OnColumn(args ...string) *OnConflictAction {
	o.prefix = strings.Join([]string{"(", strings.Join(args, ", "), ")"}, " ")
	o.action = &OnConflictAction{}
	return o.action
}

// OnConflictAction is used to limit developer actions
type OnConflictAction struct {
	phrase       string
	operatorList []argList
}

// DoNothing terminates the `ON CONFLICT` chain
func (o *OnConflictAction) DoNothing() {
	o.phrase = "DO NOTHING"
	o.operatorList = nil
}

// DoUpdate continues the `ON CONFLICT` chain
func (o *OnConflictAction) DoUpdate() *OnUpdate {
	o.phrase = "DO UPDATE SET"
	o.operatorList = []argList{}
	return &OnUpdate{operatorList: &o.operatorList}
}

// OnUpdate is used to limit developer actions
type OnUpdate struct {
	operatorList *[]argList
}

// SetDefault sets a field to a default value.
// This is useful to build `ON CONFLICT ON CONSTRAINT my_constraint DO UPDATE SET field = DEFAULT`.
func (o *OnUpdate) SetDefault(column string) *OnUpdate {
	*o.operatorList = append(*o.operatorList, argList{
		text: column + " = DEFAULT",
	})
	return o
}

// SetNow is incrediably useful to set `now()` values.
// For example: `ON CONFLICT ON CONSTRAINT my_constraint DO UPDATE SET time_value = now()`.
func (o *OnUpdate) SetNow(column string) *OnUpdate {
	*o.operatorList = append(*o.operatorList, argList{
		text: column + " = now()",
	})
	return o
}

// Set Sets a field to a value
func (o *OnUpdate) Set(args ...interface{}) *OnUpdate {
	if len(args)%2 != 0 {
		panic("arguments to `DoUpdate().Set(...)` must be even in length")
	}
	var key string
	for index, arg := range args {
		if index%2 == 0 {
			key = arg.(string)
		} else {
			*o.operatorList = append(*o.operatorList, argList{
				text: key + " = ?",
				data: arg,
			})
		}
	}
	return o
}

// SetSQL Sets a field to a value that needs no escaping, it is assumed to be SQL valid (an
// expression or column)
func (o *OnUpdate) SetSQL(args ...string) *OnUpdate {
	if len(args)%2 != 0 {
		panic("arguments to `DoUpdate().SetSQL(...)` must be even in length")
	}
	var key string
	for index, arg := range args {
		if index%2 == 0 {
			key = arg
		} else {
			*o.operatorList = append(*o.operatorList, argList{
				text: key + " = " + arg,
			})
		}
	}
	return o
}

// Where Adds Where condition to an update on conflict, does not return the OnUpdate because it
// is intended to be the last part of the expresion.
func (o *OnUpdate) Where(ec *ExpresionChain) {
	whereCondition, whereArgs := ec.renderWhereRaw()
	*o.operatorList = append(*o.operatorList, argList{
		text:        "WHERE " + whereCondition,
		data:        whereArgs,
		termination: true,
	})
}

// argList handles the messy argument collection work
type argList struct {
	text        string
	data        interface{}
	termination bool
}

// render handles walking the OnConflict object
func (o *OnConflict) render() (string, []interface{}) {

	// return early if there is nothing to do
	if o == nil ||
		o.prefix == "" ||
		o.action == nil ||
		o.action.phrase == "" {
		return "", nil
	}

	// start building output
	var outputArgs []interface{}
	formatOutput := []string{
		"ON", "CONFLICT", o.prefix, o.action.phrase,
	}

	// collect args
	var localArgs []string
	for _, arg := range o.action.operatorList {
		if arg.termination {
			continue
		}
		localArgs = append(localArgs, arg.text)
		if arg.data != nil {
			outputArgs = append(outputArgs, arg.data)
		}
	}

	// collect termination args, a complexity gifted to us by update
	var terminationArgs []string
	for _, arg := range o.action.operatorList {
		if !arg.termination {
			continue
		}
		terminationArgs = append(terminationArgs, arg.text)
		if arg.data != nil {
			outputArgs = append(outputArgs, arg.data)
		}
	}

	// build output
	if len(localArgs) > 0 {
		formatOutput = append(formatOutput, strings.Join(localArgs, ", "))
	}
	if len(terminationArgs) > 0 {
		formatOutput = append(formatOutput, strings.Join(terminationArgs, " "))
	}
	return strings.Join(formatOutput, " "), outputArgs
}
