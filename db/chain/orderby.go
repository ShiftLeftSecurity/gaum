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

// OrderByOperator unifies the `Asc` and `Desc` functions
type OrderByOperator struct {
	others    *OrderByOperator
	direction bool
	data      []string
}

// Asc declares OrderBy ascending, so least to greatest
func Asc(columns ...string) *OrderByOperator {
	return &OrderByOperator{
		direction: false,
		data:      columns,
		others:    nil,
	}
}

// Desc declares OrderBy descending, or greatest to least
func Desc(columns ...string) *OrderByOperator {
	return &OrderByOperator{
		direction: true,
		data:      columns,
		others:    nil,
	}
}

// Asc allows for complex chained OrderBy clauses
func (o *OrderByOperator) Asc(columns ...string) *OrderByOperator {
	o.append(Asc(columns...))
	return o
}

// Desc allows for complex chained OrderBy clauses
func (o *OrderByOperator) Desc(columns ...string) *OrderByOperator {
	o.append(Desc(columns...))
	return o
}

// append makes walking the singly linked list a lot easier
func (o *OrderByOperator) append(arg *OrderByOperator) {
	if o == nil {
		o = arg
	} else if o.others == nil {
		o.others = arg
	} else {
		o.others.append(arg)
	}
}

// String converts the operator to a string
func (o *OrderByOperator) String() string {

	// guard to simply recursion of walking
	// the internal linked list
	if o == nil ||
		(o != nil && len(o.data) == 0 && o.others == nil) {
		return ""
	} else if o != nil && len(o.data) == 0 && o.others != nil {
		// weird condition that may arrise from bad code
		// we'll handle it b/c we're a nice library
		return o.others.String()
	}

	var way string
	if o.direction {
		way = "DESC"
	} else {
		way = "ASC"
	}

	var fields []string
	for _, column := range o.data {
		fields = append(fields, fmt.Sprintf("%s %s", column, way))
	}

	// recursively serialize
	internal := o.others.String()
	if internal != "" {
		fields = append(fields, internal)
	}
	return strings.Join(fields, ", ")
}
