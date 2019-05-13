package chain

import (
	"fmt"
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
func (ec *ExpresionChain) With(name string, cte *ExpresionChain) *ExpresionChain {
	if len(ec.ctes) == 0 {
		ec.ctes = map[string]*ExpresionChain{}
		ec.ctesOrder = []string{}
	}
	_, ok := ec.ctes[name]
	ec.ctes[name] = cte
	if !ok {
		ec.ctesOrder = append(ec.ctesOrder, name)
	}
	return ec
}

func (ec *ExpresionChain) renderctes() (string, []interface{}, error) {
	if len(ec.ctes) == 0 {
		return "", []interface{}{}, nil
	}
	args := []interface{}{}
	queries := []string{}
	for _, name := range ec.ctesOrder {
		expr := ec.ctes[name]
		cteQ, cteArgs, err := expr.render(true)
		if err != nil {
			return "", nil, errors.Wrapf(err, "rendering cte %s", name)
		}
		queries = append(queries, fmt.Sprintf("%s AS (%s)", name, cteQ))
		args = append(args, cteArgs...)
	}
	query := fmt.Sprintf("WITH %s", strings.Join(queries, ", "))
	return query, args, nil
}
