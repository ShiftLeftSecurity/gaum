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

package gaum

import (
	"github.com/ShiftLeftSecurity/gaum/db/connection"
	"github.com/ShiftLeftSecurity/gaum/db/postgres"
	"github.com/pkg/errors"
)

var handlers = map[string]connection.DatabaseHandler{
	"postgresql": &postgres.Connector{},
}

// Open returns a DB connected to the passed db if possible.
func Open(driver string, connInfo *connection.Information) (connection.DB, error) {
	handler, ok := handlers[driver]
	if !ok {
		return nil, errors.Errorf("do not know how to handle %s", driver)
	}
	return handler.Open(connInfo)
}
