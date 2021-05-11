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
	"context"
	"log"
	"os"
	"testing"

	"github.com/ShiftLeftSecurity/gaum/db/connection"
	"github.com/ShiftLeftSecurity/gaum/db/connection_testing"
	"github.com/ShiftLeftSecurity/gaum/db/logging"
)

func newDB(t *testing.T) connection.DB {
	// postgres://jack:secret@pg.example.com:5432/mydb?sslmode=verify-ca&pool_max_conns=10
	connector := Connector{
		ConnectionString: "postgres://postgres:mysecretpassword@127.0.0.1:5469/postgres?sslmode=disable&pool_max_conns=10",
	}
	defaultLogger := log.New(os.Stdout, "logger: ", log.Lshortfile)
	goLoggerWrapped := logging.NewGoLogger(defaultLogger)
	db, err := connector.Open(context.TODO(),
		&connection.Information{
			Logger: goLoggerWrapped,
		},
	)
	if err != nil {
		t.Fatalf("failed to connect to db: %v", err)
	}
	connection_testing.Cleanup(t, db)
	return db
}

func TestConnector_QueryIter(t *testing.T) {
	connection_testing.DotestconnectorQueryiter(t, newDB)
}

func TestConnector_Query(t *testing.T) {
	connection_testing.DotestconnectorQuery(t, newDB)
}

func TestConnector_QueryReflection(t *testing.T) {
	connection_testing.DotestconnectorQueryreflection(t, newDB)
}

func TestConnector_QueryStar(t *testing.T) {
	connection_testing.DotestconnectorQuerystar(t, newDB)
}

func TestConnector_QueryReturningWithError(t *testing.T) {
	connection_testing.DotestconnectorQueryreturningwitherror(t, newDB)
}

func TestConnector_Distinct(t *testing.T) {
	connection_testing.DotestconnectorDistinct(t, newDB)
}

func TestConnector_DistinctAs(t *testing.T) {
	connection_testing.DotestconnectorDistinctas(t, newDB)
}

func TestConnector_Raw(t *testing.T) {
	connection_testing.DotestconnectorRaw(t, newDB)
}

func TestConnector_Insert(t *testing.T) {
	connection_testing.DotestconnectorInsert(t, newDB)
}

func TestConnector_MultiInsert(t *testing.T) {
	connection_testing.DotestconnectorMultiinsert(t, newDB)
}

func TestConnector_InsertConstraint(t *testing.T) {
	connection_testing.DotestconnectorInsertconstraint(t, newDB)
}

func TestConnector_Transaction(t *testing.T) {
	connection_testing.DotestconnectorTransaction(t, newDB)
}

func TestConnector_QueryPrimitives(t *testing.T) {
	connection_testing.DotestconnectorQueryprimitives(t, newDB)
}

func TestConnector_RegressionReturning(t *testing.T) {
	connection_testing.DotestconnectorRegressionReturning(t, newDB)
}

func TestConnector_ExecResult(t *testing.T) {
	connection_testing.DotestconnectorExecresult(t, newDB)
}
