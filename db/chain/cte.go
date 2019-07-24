package chain

import (
	"strings"

	"github.com/pkg/errors"
)

//    Copyright 2019 Horacio Duran <horacio@shiftleft.io>, ShiftLeft Inc.
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

// With adds a CTE to your query (https://www.postgresql.org/docs/11/queries-with.html)
func (ec *ExpressionChain) With(name string, cte *ExpressionChain) *ExpressionChain {
	if len(ec.ctes) == 0 {
		ec.ctes = map[string]*ExpressionChain{}
		ec.ctesOrder = []string{}
	}
	_, ok := ec.ctes[name]
	ec.ctes[name] = cte
	if !ok {
		ec.ctesOrder = append(ec.ctesOrder, name)
	}
	return ec
}

func (ec *ExpressionChain) renderctes(dst *strings.Builder) ([]interface{}, error) {
	if len(ec.ctes) == 0 {
		return []interface{}{}, nil
	}

	args := []interface{}{}
	dst.WriteString("WITH ")
	for _, name := range ec.ctesOrder {
		expr := ec.ctes[name]
		dst.WriteString(name)
		dst.WriteString(" AS (")
		cteArgs, err := expr.render(true, dst)
		if err != nil {
			return nil, errors.Wrapf(err, "rendering cte %s", name)
		}
		dst.WriteRune(')')
		args = append(args, cteArgs...)
	}
	dst.WriteRune(' ')

	return args, nil
}
