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

package postgres

import (
	"log"
	"os"
	"testing"

	"github.com/ShiftLeftSecurity/gaum/db/connection"
	"github.com/ShiftLeftSecurity/gaum/db/connection_testing"
	"github.com/ShiftLeftSecurity/gaum/db/logging"
)

func newDB(t *testing.T) connection.DB {
	connector := Connector{
		ConnectionString: "TODO",
	}
	defaultLogger := log.New(os.Stdout, "logger: ", log.Lshortfile)
	goLoggerWrapped := logging.NewGoLogger(defaultLogger)
	db, err := connector.Open(
		&connection.Information{
			Host:             "127.0.0.1",
			Port:             5469,
			Database:         "postgres",
			User:             "postgres",
			Password:         "mysecretpassword",
			MaxConnPoolConns: 10,
			Logger:           goLoggerWrapped,
		},
	)
	if err != nil {
		t.Errorf("failed to connect to db: %v", err)
	}
	connection_testing.Cleanup(t, db)
	return db
}

func TestConnector_QueryIter(t *testing.T) {
	connection_testing.DoTestConnector_QueryIter(t, newDB)
}

func TestConnector_Query(t *testing.T) {
	connection_testing.DoTestConnector_Query(t, newDB)
}

func TestConnector_Distinct(t *testing.T) {
	connection_testing.DoTestConnector_Distinct(t, newDB)
}

func TestConnector_DistinctAs(t *testing.T) {
	connection_testing.DoTestConnector_DistinctAs(t, newDB)
}

func TestConnector_Raw(t *testing.T) {
	connection_testing.DoTestConnector_Raw(t, newDB)
}

func TestConnector_Insert(t *testing.T) {
	connection_testing.DoTestConnector_Insert(t, newDB)
}

func TestConnector_MultiInsert(t *testing.T) {
	connection_testing.DoTestConnector_MultiInsert(t, newDB)
}

func TestConnector_InsertConstraint(t *testing.T) {
	connection_testing.DoTestConnector_InsertConstraint(t, newDB)
}

func TestConnector_Transaction(t *testing.T) {
	connection_testing.DoTestConnector_Transaction(t, newDB)
}

func TestConnector_QueryPrimitives(t *testing.T) {
	connection_testing.DoTestConnector_QueryPrimitives(t, newDB)
}

func TestConnector_RegressionReturning(t *testing.T) {
	connection_testing.DoTestConnector_Regression_Returning(t, newDB)
}

func TestConnector_ExecResult(t *testing.T) {
	connection_testing.DoTestConnector_ExecResult(t, newDB)
}
