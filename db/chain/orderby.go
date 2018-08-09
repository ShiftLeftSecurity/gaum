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
	}
}

// Desc declares OrderBy descending, or greatest to least
func Desc(columns ...string) *OrderByOperator {
	return &OrderByOperator{
		direction: true,
		data:      columns,
	}
}

// Asc allows for complex chained OrderBy clauses
func (o *OrderByOperator) Asc(columns ...string) *OrderByOperator {
	// walk singly linked list to update others
	others := o.others
	for {
		if others != nil {
			others = o.others
		} else {
			break
		}
	}
	others.others = Asc(columns...)
}

// Desc allows for complex chained OrderBy clauses
func (o *OrderByOperator) Desc(columns ...string) *OrderByOperator {
	// walk singly linked list to update the last item
	others := o.others
	for {
		if others != nil {
			others = o.others
		} else {
			break
		}
	}
	others.others = Desc(columns...)
}

// String converts the operator to a string
func (o *OrderByOperator) String() string {

	// guard to simply recursion of walking
	// the internal linked list
	if o == nil ||
		(o != nil && len(o.data) == 0) {
		return ""
	}

	var way string
	if direction {
		way = "DESC"
	} else {
		way = "ASC"
	}

	var section string
	if len(o.data) == 1 {
		section = fmt.Sprintf("%s %s", o.data[0], way)
	} else {
		var fields []string
		for _, column := range o.data {
			fields = append(fields, fmt.Sprintf("%s %s", column, way))
		}
		section = strings.Join(fields, ", ")
	}

	internal := o.others.String()
	if internal == "" {
		return section
	}
	return strings.Join([]string{section, internal}, ", ")
}
