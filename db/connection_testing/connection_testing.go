package connection_testing

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

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/ShiftLeftSecurity/gaum/v2/db/chain"
	"github.com/ShiftLeftSecurity/gaum/v2/db/connection"
	"github.com/jackc/pgconn"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

// Cleanup deletes everything created for a test in the db
func Cleanup(t *testing.T, db connection.DB) {
	query := chain.New(db)
	query.Delete().Table("justforfun").AndWhere("id > ?", 10)
	err := query.Exec(context.TODO())
	if err != nil {
		t.Logf("failed cleanup queries: %v", err)
		t.FailNow()
	}
}

func DotestconnectorQueryiter(t *testing.T, newDB NewDB) {
	testconnectorQueryiter(t, newDB)
}

func DotestconnectorQuery(t *testing.T, newDB NewDB) {
	testconnectorQuery(t, newDB)
}

func DotestconnectorQueryreflection(t *testing.T, newDB NewDB) {
	testconnectorQueryreflection(t, newDB)
}

func DotestconnectorQuerystar(t *testing.T, newDB NewDB) {
	testconnectorQuerystar(t, newDB)
}

func DotestconnectorQueryreturningwitherror(t *testing.T, newDB NewDB) {
	testconnectorQueryreturningwitherror(t, newDB)
}

func DotestconnectorQuerynorows(t *testing.T, newDB NewDB) {
	testconnectorQuerynorows(t, newDB)
}

func DotestconnectorDistinct(t *testing.T, newDB NewDB) {
	testconnectorDistinct(t, newDB)
}

func DotestconnectorDistinctas(t *testing.T, newDB NewDB) {
	testconnectorDistinctas(t, newDB)
}

func DotestconnectorRaw(t *testing.T, newDB NewDB) {
	testconnectorRaw(t, newDB)
}

func DotestconnectorInsert(t *testing.T, newDB NewDB) {
	testconnectorInsert(t, newDB)
}

func DotestconnectorMultiinsert(t *testing.T, newDB NewDB) {
	testconnectorMultiinsert(t, newDB)
}

func DotestconnectorInsertconstraint(t *testing.T, newDB NewDB) {
	testconnectorInsertconstraint(t, newDB)
}

func DotestconnectorTransaction(t *testing.T, newDB NewDB) {
	testconnectorTransaction(t, newDB)
}

func DotestconnectorQueryprimitives(t *testing.T, newDB NewDB) {
	testconnectorQueryprimitives(t, newDB)
}

func DotestconnectorRegressionReturning(t *testing.T, newDB NewDB) {
	testconnectorRegressionReturning(t, newDB)
}

func DotestconnectorExecresult(t *testing.T, newDB NewDB) {
	testconnectorExecresult(t, newDB)
}

type NewDB func(t *testing.T) connection.DB

func testconnectorQueryiter(t *testing.T, newDB NewDB) {
	db := newDB(t)
	query := chain.New(db)
	query.Select("id, description").Table("justforfun").AndWhere("id = ?", 1)

	// Debug print query
	q, args, err := query.Render()
	if err != nil {
		t.Errorf("failed to render: %v", err)
	}
	t.Logf("will perform query %q", q)
	t.Logf("with arguments %#v", args)

	iter, err := query.QueryIter(context.TODO())
	if err != nil {
		t.Errorf("failed to query: %v", err)
	}
	type row struct {
		Id          int
		Description string
	}
	// Test one row
	var oneRow row
	next, closer, err := iter(&oneRow)
	if err != nil {
		t.Errorf("failed to iterate: %v", err)
	}
	defer closer()
	if oneRow.Id != 1 {
		t.Logf("row Id is %d expected 1", oneRow.Id)
		t.FailNow()
	}
	if oneRow.Description != "first" {
		t.Logf("row Description is %q expected \"first\"", oneRow.Description)
		t.FailNow()
	}
	if next {
		t.Log("got next row, there should not be one")
		t.FailNow()
	}
	closer()

	// Test Multiple row Iterator
	query = chain.New(db)
	query.Select("id, description").Table("justforfun").OrderBy(chain.Asc("id"))
	iter, err = query.QueryIter(context.TODO())
	if err != nil {
		t.Errorf("failed to query: %v", err)
	}
	// Debug print query
	q, args, err = query.Render()
	if err != nil {
		t.Errorf("failed to render: %v", err)
	}
	t.Logf("will perform query %q", q)
	t.Logf("with arguments %#v", args)
	var oneRowMultiple row
	var closerMulti func()
	ordinals := []string{
		"first",
		"second",
		"third",
		"fourth",
		"fift",
		"sixt",
		"seventh",
		"eight",
		"ninth",
		"tenth",
	}

	for i := 1; i < 11; i++ {
		t.Logf("Iteration %d", i)
		next, closerMulti, err = iter(&oneRowMultiple)
		if err != nil {
			t.Errorf("failed to iterate: %v", err)
		}
		if oneRowMultiple.Id != i {
			t.Logf("row Id is %d expected 1", oneRowMultiple.Id)
			t.FailNow()
		}
		if oneRowMultiple.Description != ordinals[i-1] {
			t.Logf("row Description is %q expected %q", oneRowMultiple.Description, ordinals[i-1])
			t.FailNow()
		}
		if i < 10 && !next {
			t.Log("didn't get next row, there should be one")
			t.FailNow()
		}
		if i == 10 && next {
			t.Log("got next row, there should not be one")
			t.FailNow()
		}
	}
	closerMulti()

}

func testconnectorQueryreflection(t *testing.T, newDB NewDB) {

	db := newDB(t)
	type row struct {
		Id          int
		Description string
		NotUsed     string
		NotUsedTime time.Time
	}

	// Test Multiple row Iterator
	query := chain.New(db)
	query.Select("*").Table("justforfun").OrderBy(chain.Asc("id"))
	fetcher, err := query.Query(context.TODO())
	if err != nil {
		t.Errorf("failed to query: %v", err)
	}

	q, args, err := query.Render()
	if err != nil {
		t.Errorf("failed to render: %v", err)
	}
	t.Logf("will perform query %q", q)
	t.Logf("with arguments %#v", args)

	var multiRow []row
	ordinals := []string{
		"first",
		"second",
		"third",
		"fourth",
		"fift",
		"sixt",
		"seventh",
		"eight",
		"ninth",
		"tenth",
	}

	expectedNotUsed := map[int]string{
		2: "meh",
		8: "meh8",
	}

	tTest, _ := time.Parse("2006-01-02", "1985-10-26") // the day marty goes back to the past
	expectedNotUsedTime := map[int]time.Time{
		10: tTest,
	}
	err = fetcher(&multiRow)
	if err != nil {
		t.Errorf("failed to fetch data: %v", err)
	}

	if len(multiRow) != 10 {
		t.Logf("expected 10 results got %d", len(multiRow))
		t.FailNow()
	}
	for i := 1; i < 11; i++ {
		t.Logf("Iteration %d", i)
		oneRowMulti := multiRow[i-1]

		if oneRowMulti.Id != i {
			t.Logf("row Id is %d expected 1", oneRowMulti.Id)
			t.FailNow()
		}
		if oneRowMulti.Description != ordinals[i-1] {
			t.Logf("row Description is %q expected %q", oneRowMulti.Description, ordinals[i-1])
			t.FailNow()
		}
		if nu, ok := expectedNotUsed[i]; ok {
			if oneRowMulti.NotUsed == "" {
				t.Logf("expected NotUsed value, got empty")
				t.FailNow()
			}
			if oneRowMulti.NotUsed != nu {
				t.Logf("expected NotUsed value to be %s but is %s", nu, oneRowMulti.NotUsed)
				t.FailNow()
			}
		}
		if nud, ok := expectedNotUsedTime[i]; ok {
			noTime := time.Time{}
			if oneRowMulti.NotUsedTime == noTime {
				t.Logf("expected NotUsedTime value, got empty")
				t.FailNow()
			}
			if oneRowMulti.NotUsedTime.Year() != nud.Year() || oneRowMulti.NotUsedTime.Month() != nud.Month() || oneRowMulti.NotUsedTime.Day() != nud.Day() {
				t.Logf("got date %v but expected %v", nud, oneRowMulti.NotUsedTime)
				t.FailNow()
			}
		}

	}
}

func testconnectorQuery(t *testing.T, newDB NewDB) {

	db := newDB(t)
	type InnerRow struct {
		Id int
	}
	type row struct {
		InnerRow
		Description string
	}

	// Test Multiple row Iterator
	query := chain.New(db)
	query.Select("id, description").Table("justforfun").OrderBy(chain.Asc("id"))
	fetcher, err := query.Query(context.TODO())
	if err != nil {
		t.Errorf("failed to query: %v", err)
	}

	// Debug print query
	q, args, err := query.Render()
	if err != nil {
		t.Errorf("failed to render: %v", err)
	}
	t.Logf("will perform query %q", q)
	t.Logf("with arguments %#v", args)

	var multiRow []row
	ordinals := []string{
		"first",
		"second",
		"third",
		"fourth",
		"fift",
		"sixt",
		"seventh",
		"eight",
		"ninth",
		"tenth",
	}
	err = fetcher(&multiRow)
	if err != nil {
		t.Errorf("failed to fetch data: %v", err)
	}

	if len(multiRow) != 10 {
		t.Logf("expected 10 results got %d", len(multiRow))
		t.FailNow()
	}
	for i := 1; i < 11; i++ {
		t.Logf("Iteration %d", i)
		oneRowMulti := multiRow[i-1]

		if oneRowMulti.Id != i {
			t.Logf("row Id is %d expected 1", oneRowMulti.Id)
			t.FailNow()
		}
		if oneRowMulti.Description != ordinals[i-1] {
			t.Logf("row Description is %q expected %q", oneRowMulti.Description, ordinals[i-1])
			t.FailNow()
		}

	}
	// same with slice of ptr receiver
	var multiRowPtr []*row

	fetcher, err = query.Query(context.TODO())
	if err != nil {
		t.Errorf("failed to query: %v", err)
	}
	err = fetcher(&multiRowPtr)
	if err != nil {
		t.Errorf("failed to fetch data: %v", err)
	}

	if len(multiRowPtr) != 10 {
		t.Logf("expected 10 results got %d", len(multiRowPtr))
		t.FailNow()
	}
	for i := 1; i < 11; i++ {
		t.Logf("Iteration %d", i)
		oneRowMulti := multiRowPtr[i-1]

		if oneRowMulti.Id != i {
			t.Logf("row Id is %d expected 1", oneRowMulti.Id)
			t.FailNow()
		}
		if oneRowMulti.Description != ordinals[i-1] {
			t.Logf("row Description is %q expected %q", oneRowMulti.Description, ordinals[i-1])
			t.FailNow()
		}

	}

}

func testconnectorQuerystar(t *testing.T, newDB NewDB) {
	db := newDB(t)
	type row struct {
		Id          int    `gaum:"field_name:id"`
		Description string `gaum:"field_name:description"`
	}

	// Test Multiple row Iterator
	query := chain.New(db)
	query.Select("*").Table("justforfun").OrderBy(chain.Asc("id"))
	fetcher, err := query.Query(context.TODO())
	if err != nil {
		t.Errorf("failed to query: %v", err)
	}

	var multiRow []row
	ordinals := []string{
		"first",
		"second",
		"third",
		"fourth",
		"fift",
		"sixt",
		"seventh",
		"eight",
		"ninth",
		"tenth",
	}
	err = fetcher(&multiRow)
	if err != nil {
		t.Errorf("failed to fetch data: %v", err)
	}

	if len(multiRow) != 10 {
		t.Logf("expected 10 results got %d", len(multiRow))
		t.FailNow()
	}
	for i := 1; i < 11; i++ {
		t.Logf("Iteration %d", i)
		oneRowMulti := multiRow[i-1]

		if oneRowMulti.Id != i {
			t.Logf("row Id is %d expected 1", oneRowMulti.Id)
			t.FailNow()
		}
		if oneRowMulti.Description != ordinals[i-1] {
			t.Logf("row Description is %q expected %q", oneRowMulti.Description, ordinals[i-1])
			t.FailNow()
		}

	}

}

func testconnectorQueryreturningwitherror(t *testing.T, newDB NewDB) {
	db := newDB(t)
	type row struct {
		Id          int
		Description string
	}

	query := chain.New(db)
	query.Insert(map[string]interface{}{
		"id":          1,
		"description": "this id already exists",
	}).
		Table("justforfun").
		Returning("*")

	fetcher, err := query.Query(context.TODO())

	if err != nil {
		// this might be PQ failing before we get to the fetch part
		t.Log("Cause of error:")
		t.Logf("err is : %T: %#v", err, err)
		cause := errors.Cause(err)
		if pgErr, ok := cause.(*pgconn.PgError); ok {
			if pgErr.Severity != "ERROR" {
				t.Errorf("expected to receive a PgError with severity: 'Error', instead got: %s", pgErr.Severity)
			}
			if pgErr.Code != "23505" {
				t.Errorf("expected to receive a PgError error Code: 23505, instead got: %s", pgErr.Code)
			}
			return // the rest of the test will fail if this is the case
		}
		t.Fatalf("failed to query: %v", err)
	}

	var multiRow []row
	err = fetcher(&multiRow)
	if err == nil {
		t.Fatalf("expected to receive an error, instead got nil")
	}
	if pgErr, ok := err.(*pgconn.PgError); ok {
		if pgErr.Severity != "ERROR" {
			t.Fatalf("expected to receive a PgError with severity: 'Error', instead got: %s", pgErr.Severity)
		}
		if pgErr.Code != "23505" {
			t.Fatalf("expected to receive a PgError error Code: 23505, instead got: %s", pgErr.Code)
		}
	} else {
		t.Fatalf("expected to receive a PgError error, instead got %T, %+v", err, err)
	}

}

func testconnectorQuerynorows(t *testing.T, newDB NewDB) {
	db := newDB(t)
	type row struct {
		Id          int
		Description string
	}

	query := chain.New(db)
	query.Select("*").AndWhere("id = ?", 99999999).Table("justforfun")

	fetcher, err := query.Query(context.TODO())
	if err != nil {
		t.Errorf("failed to query: %v", err)
	}

	var multiRow []row
	err = fetcher(&multiRow)
	if err != nil {
		t.Errorf("expected to receive no error, instead got %v", err)
	}
}

func testconnectorDistinct(t *testing.T, newDB NewDB) {
	db := newDB(t)

	type idRecipient struct {
		ID          int    `gaum:"field_name:id"`
		Description string `gaum:"field_name:description"`
	}
	var ids []idRecipient

	// Test Multiple row Iterator
	query := chain.New(db)
	prefix := chain.TablePrefix("justforfun")
	query.Select(chain.Distinct(prefix("id")), prefix("description")).Table("justforfun").OrderBy(chain.Asc("id"))
	fetcher, err := query.Query(context.TODO())
	if err != nil {
		t.Errorf("failed to query: %v", err)
	}

	// Debug print query
	q, args, err := query.Render()
	if err != nil {
		t.Errorf("failed to render: %v", err)
	}
	t.Logf("will perform query %q", q)
	t.Logf("with arguments %#v", args)

	err = fetcher(&ids)
	if err != nil {
		t.Errorf("failed to fetch data: %v", err)
	}

	if len(ids) != 10 {
		t.Logf("expected 10 results got %d", len(ids))
		t.FailNow()
	}
}

func testconnectorDistinctas(t *testing.T, newDB NewDB) {
	db := newDB(t)

	type idRecipientRenamed struct {
		ID          int    `gaum:"field_name:renamed"`
		Description string `gaum:"field_name:description"`
	}
	var ids []idRecipientRenamed

	// Test Multiple row Iterator
	query := chain.New(db)
	prefix := chain.TablePrefix("justforfun")
	query.Select(chain.As(chain.Distinct(prefix("id")), "renamed"), prefix("description")).Table("justforfun").OrderBy(chain.Asc("id"))
	fetcher, err := query.Query(context.TODO())
	if err != nil {
		t.Errorf("failed to query: %v", err)
	}

	// Debug print query
	q, args, err := query.Render()
	if err != nil {
		t.Errorf("failed to render: %v", err)
	}
	t.Logf("will perform query %q", q)
	t.Logf("with arguments %#v", args)

	err = fetcher(&ids)
	if err != nil {
		t.Errorf("failed to fetch data: %v", err)
	}

	if len(ids) != 10 {
		t.Logf("expected 10 results got %d", len(ids))
		t.FailNow()
	}
}

func testconnectorRaw(t *testing.T, newDB NewDB) {

	db := newDB(t)
	type row struct {
		Id          int
		Description string
	}
	aRow := row{}
	// Test Multiple row Iterator
	query := chain.New(db)
	query.Select("id, description").Table("justforfun").AndWhere("id = ?", 1)
	err := query.Raw(context.TODO(), &aRow.Id, &aRow.Description)
	if err != nil {
		t.Errorf("failed to query: %v", err)
	}

	if aRow.Id != 1 {
		t.Logf("row Id is %d expected 1", aRow.Id)
		t.FailNow()
	}
	if aRow.Description != "first" {
		t.Logf("row Description is %q expected \"first\"", aRow.Description)
		t.FailNow()
	}

	query = chain.New(db)
	query.Select("id, description").AndWhere("id = ?", 1)
	err = query.Raw(context.TODO(), &aRow.Id, &aRow.Description)
	if err == nil {
		t.Errorf("should have failed because of invalid query")
	}

}

func testconnectorInsert(t *testing.T, newDB NewDB) {

	db := newDB(t)
	type row struct {
		Id          int
		Description string
	}
	aRow := row{}
	// Test Multiple row Iterator
	query := chain.New(db)
	tempDescriptionUUID := uuid.NewV4()
	tempDescription := tempDescriptionUUID.String()
	query.Select("id, description").Table("justforfun").AndWhere("description = ?", tempDescription)
	err := query.Raw(context.TODO(), &aRow.Id, &aRow.Description)
	if err == nil {
		t.Log("querying for our description should fail, this record should not exist")
		t.FailNow()
	}
	rand.Seed(time.Now().UnixNano())
	tempID := rand.Intn(11000)

	insertQuery := chain.New(db)
	insertQuery.Insert(map[string]interface{}{"id": tempID, "description": tempDescription}).
		Table("justforfun")
	err = insertQuery.Exec(context.TODO())
	if err != nil {
		t.Logf("failed to insert: %v", err)
		t.FailNow()
	}

	err = query.Raw(context.TODO(), &aRow.Id, &aRow.Description)
	if err != nil {
		t.Log("querying for our description should fail, this record should not exist")
		t.FailNow()
	}
	if aRow.Id != tempID {
		t.Logf("row Id is %d expected %d", aRow.Id, tempID)
		t.FailNow()
	}
	if aRow.Description != tempDescription {
		t.Logf("row Description is %q expected %q", aRow.Description, tempDescription)
		t.FailNow()
	}

}

func testconnectorMultiinsert(t *testing.T, newDB NewDB) {

	db := newDB(t)
	type row struct {
		Id          int
		Description string
	}
	aRow := row{}
	// Test Multiple row Iterator
	query := chain.New(db)
	query1 := query.Clone()
	tempDescription := uuid.NewV4().String()
	tempDescription1 := uuid.NewV4().String()
	query.Select("id, description").Table("justforfun").AndWhere("description = ?", tempDescription)
	err := query.Raw(context.TODO(), &aRow.Id, &aRow.Description)
	if err == nil {
		t.Log("querying for our description should fail, this record should not exist")
		t.FailNow()
	}

	query1.Select("id, description").Table("justforfun").AndWhere("description = ?", tempDescription1)
	err = query1.Raw(context.TODO(), &aRow.Id, &aRow.Description)
	if err == nil {
		t.Log("querying for our second description should fail, this record should not exist")
		t.FailNow()
	}

	rand.Seed(time.Now().UnixNano())
	tempID := rand.Intn(11000)
	tempID1 := tempID + 1

	insertQuery := chain.New(db)
	_, err = insertQuery.InsertMulti(map[string][]interface{}{
		"description": {tempDescription, tempDescription1},
		"id":          {tempID, tempID1},
	})
	insertQuery.Table("justforfun")
	err = insertQuery.Exec(context.TODO())
	if err != nil {
		t.Logf("failed to insert: %v", err)
		t.FailNow()
	}

	err = query.Raw(context.TODO(), &aRow.Id, &aRow.Description)
	if err != nil {
		t.Log("querying for our description should fail, this record should not exist")
		t.FailNow()
	}
	if aRow.Id != tempID {
		t.Logf("row Id is %d expected %d", aRow.Id, tempID)
		t.FailNow()
	}
	if aRow.Description != tempDescription {
		t.Logf("row Description is %q expected %q", aRow.Description, tempDescription)
		t.FailNow()
	}

	err = query1.Raw(context.TODO(), &aRow.Id, &aRow.Description)
	if err != nil {
		t.Log("querying for our description should fail, this record should not exist")
		t.FailNow()
	}
	if aRow.Id != tempID1 {
		t.Logf("row Id is %d expected %d", aRow.Id, tempID1)
		t.FailNow()
	}
	if aRow.Description != tempDescription1 {
		t.Logf("row Description is %q expected %q", aRow.Description, tempDescription1)
		t.FailNow()
	}

}

func testconnectorInsertconstraint(t *testing.T, newDB NewDB) {
	db := newDB(t)
	type row struct {
		Id          int
		Description string
	}
	aRow := row{}
	// Test Multiple row Iterator
	query := chain.New(db)
	tempDescriptionUUID := uuid.NewV4()
	tempDescription := tempDescriptionUUID.String()
	query.Select("id, description").Table("justforfun").AndWhere("description = ?", tempDescription)
	err := query.Raw(context.TODO(), &aRow.Id, &aRow.Description)
	if err == nil {
		t.Log("querying for our description should fail, this record should not exist")
		t.FailNow()
	}
	rand.Seed(time.Now().UnixNano())
	tempID := rand.Intn(11000)

	// First insert, this is to have a colliding value
	insertQuery := chain.New(db)
	insertQuery.Insert(map[string]interface{}{"id": tempID, "description": tempDescription}).
		Table("justforfun")
	err = insertQuery.Exec(context.TODO())
	if err != nil {
		t.Logf("failed to insert to test constraint: %v", err)
		t.FailNow()
	}

	err = query.Raw(context.TODO(), &aRow.Id, &aRow.Description)
	if err != nil {
		t.Log("querying for our description should fail, this record should not exist")
		t.FailNow()
	}
	if aRow.Id != tempID {
		t.Logf("row Id is %d expected %d", aRow.Id, tempID)
		t.FailNow()
	}
	if aRow.Description != tempDescription {
		t.Logf("row Description is %q expected %q", aRow.Description, tempDescription)
		t.FailNow()
	}

	// Second attempt at inserting, this should fail
	insertQuery.Insert(map[string]interface{}{"id": tempID, "description": tempDescription}).
		Table("justforfun")
	queryString, queryArgs, _ := insertQuery.Render()
	t.Logf("conflicting insert query: %s", queryString)
	t.Logf("conflicting insert args: %v", queryArgs)
	err = insertQuery.Exec(context.TODO())
	if err == nil {
		t.Log("an insert that breaks an uniqueness constraint should not be allowed, yet it was")
		t.FailNow()
	}

	// Third attempt, this should work
	insertQuery.OnConflict(func(c *chain.OnConflict) {
		c.OnConstraint("therecanbeonlyone").DoNothing()
	})
	insertQuery.Insert(map[string]interface{}{"id": tempID, "description": tempDescription}).
		Table("justforfun")
	queryString, queryArgs, _ = insertQuery.Render()
	t.Logf("conflicting insert query: %s", queryString)
	t.Logf("conflicting insert args: %v", queryArgs)
	err = insertQuery.Exec(context.TODO())
	if err != nil {
		t.Logf("the insertion conflict should have been ignored, yet it wasnt: %v", err)
		t.FailNow()
	}
}

func testconnectorTransaction(t *testing.T, newDB NewDB) {
	db := newDB(t)
	type row struct {
		Id          int
		Description string
	}
	aRow := row{}
	// Test Multiple row Iterator
	query := chain.New(db)
	tempDescriptionUUID := uuid.NewV4()
	tempDescription := tempDescriptionUUID.String()
	query.Select("id, description").Table("justforfun").AndWhere("description = ?", tempDescription)
	err := query.Raw(context.TODO(), &aRow.Id, &aRow.Description)
	if err == nil {
		t.Log("querying for our description should fail, this record should not exist")
		t.FailNow()
	}
	rand.Seed(time.Now().UnixNano())
	tempID := rand.Intn(11000)

	transactionalDB, err := db.Clone().BeginTransaction(context.TODO())
	if err != nil {
		t.Logf("attempting to begin a transaction: %v", err)
		t.FailNow()
	}
	// Let's try this with transactions
	insertQuery := chain.New(transactionalDB)
	insertQuery.Insert(map[string]interface{}{"id": tempID, "description": tempDescription}).
		Table("justforfun")
	err = insertQuery.Exec(context.TODO())
	if err != nil {
		t.Logf("an insert in a transaction was attempted but failed: %v", err)
		t.FailNow()
	}

	// Transaction was not committed so no result should be here
	err = query.Raw(context.TODO(), &aRow.Id, &aRow.Description)
	if err == nil {
		t.Log("querying for our description should fail, this record should not exist")
		t.FailNow()
	}

	err = transactionalDB.RollbackTransaction(context.TODO())
	if err != nil {
		t.Logf("attempting to rollback a transaction: %v", err)
		t.FailNow()
	}

	// Transaction was rolled back so still no row
	err = query.Raw(context.TODO(), &aRow.Id, &aRow.Description)
	if err == nil {
		t.Log("querying for our description should fail, this record should not exist")
		t.FailNow()
	}

	// a new transaction is required to try again.
	transactionalDB, err = db.BeginTransaction(context.TODO())
	if err != nil {
		t.Logf("attempting to start a new transaction: %v", err)
		t.FailNow()
	}
	insertQuery.NewDB(transactionalDB)

	// lets insert with the idea of a commit now
	err = insertQuery.Exec(context.TODO())
	if err != nil {
		t.Logf("an insert in a transaction was attempted but failed: %v", err)
		t.FailNow()
	}

	// Transaction is still not committed so it should fail.
	err = query.Raw(context.TODO(), &aRow.Id, &aRow.Description)
	if err == nil {
		t.Log("querying for our description should fail, this record should not exist")
		t.FailNow()
	}

	// Commit the transaction
	err = transactionalDB.CommitTransaction(context.TODO())
	if err != nil {
		t.Logf("attempting to commit a transaction: %v", err)
		t.FailNow()
	}

	// let's make sure commit worked.
	err = query.Raw(context.TODO(), &aRow.Id, &aRow.Description)
	if err != nil {
		t.Logf("transaction commit did not insert the object: %v", err)
		t.FailNow()
	}
	if aRow.Id != tempID {
		t.Logf("row Id is %d expected %d", aRow.Id, tempID)
		t.FailNow()
	}
	if aRow.Description != tempDescription {
		t.Logf("row Description is %q expected %q", aRow.Description, tempDescription)
		t.FailNow()
	}
}

func testconnectorQueryprimitives(t *testing.T, newDB NewDB) {

	db := newDB(t)

	// Test Multiple row Iterator
	query := chain.New(db)
	query.Select("id").Table("justforfun").OrderBy(chain.Asc("id"))
	fetcher, err := query.QueryPrimitive(context.TODO())
	if err != nil {
		t.Errorf("failed to query: %v", err)
	}

	// Debug print query
	q, args, err := query.Render()
	if err != nil {
		t.Errorf("failed to render: %v", err)
	}
	t.Logf("will perform query %q", q)
	t.Logf("with arguments %#v", args)

	var multiRow []int
	err = fetcher(&multiRow)
	if err != nil {
		t.Errorf("failed to fetch data: %v", err)
	}

	if len(multiRow) != 10 {
		t.Logf("expected 10 results got %d", len(multiRow))
		t.FailNow()
	}
	for i := 1; i < 11; i++ {
		t.Logf("Iteration %d", i)
		oneRowMulti := multiRow[i-1]

		if oneRowMulti != i {
			t.Logf("row Id is %d expected 1", oneRowMulti)
			t.FailNow()
		}

	}

}

func testconnectorRegressionReturning(t *testing.T, newDB NewDB) {
	db := newDB(t)
	var oneID int64
	var oneDescription string
	// Test Multiple row Iterator
	query := chain.New(db)

	err := query.Insert(map[string]interface{}{
		"id":          11,
		"description": "this should be in the db",
	}).
		Table("justforfun").Exec(context.TODO())
	if err != nil {
		t.Errorf("failed to query: %v", err)
	}

	query.Insert(map[string]interface{}{
		"id":          11,
		"description": "this should be the updated value",
	}).
		Table("justforfun").
		OnConflict(func(c *chain.OnConflict) {
			c.OnConstraint("therecanbeonlyone").DoUpdate().Set("description",
				"this should be the updated value")
		}).
		Returning("id, description")
	render, _, _ := query.Render()
	t.Log(render)
	err = query.Raw(context.TODO(), &oneID, &oneDescription)
	if err != nil {
		t.Errorf("failed to query: %v", err)
	}

	if oneID != 11 {
		t.Logf("row Id is %d expected 1", oneID)
		t.FailNow()
	}
	if oneDescription != "this should be the updated value" {
		t.Logf("row Description is %q expected \"first\"", oneDescription)
		t.FailNow()
	}
}

func testconnectorExecresult(t *testing.T, newDB NewDB) {
	db := newDB(t)

	rand.Seed(time.Now().UnixNano())
	tempID1 := rand.Intn(11000) + 10
	tempID2 := rand.Intn(11000) + 10
	tempID3 := rand.Intn(11000) + 10
	initialDesc1 := uuid.NewV4().String()
	initialDesc2And3 := uuid.NewV4().String()

	insertQuery := chain.New(db)
	_, err := insertQuery.InsertMulti(
		map[string][]interface{}{
			"id":          {tempID1, tempID2, tempID3},
			"description": {initialDesc1, initialDesc2And3, initialDesc2And3},
		})
	insertQuery.Table("justforfun")
	if err != nil {
		t.Logf("failed to generate insertQuery: %v", err)
		t.FailNow()
	}
	rowsAffected, err := insertQuery.ExecResult(context.TODO())
	if err != nil {
		t.Logf("failed to insert: %v", err)
		t.FailNow()
	}
	if rowsAffected != 3 {
		t.Logf("expected 3 row to be affected by insert, instead got: %d", rowsAffected)
		t.FailNow()
	}

	newDesc1 := uuid.NewV4().String()
	newDesc2And3 := uuid.NewV4().String()

	// First test 0 rows affected.
	updateQuery := chain.New(db)
	updateQuery.UpdateMap(map[string]interface{}{"description": newDesc1}).
		Table("justforfun").
		AndWhere("id = ?", tempID1).
		AndWhere("description = ?", "expect that this description does not exist")
	rowsAffected, err = updateQuery.ExecResult(context.TODO())
	if err != nil {
		t.Logf("failed to update: %v", err)
		t.FailNow()
	}
	if rowsAffected != 0 {
		t.Logf("expected 0 row to be affected by update, instead got: %d", rowsAffected)
		t.FailNow()
	}

	// test 1 rows affected.
	updateQuery = chain.New(db)
	updateQuery.UpdateMap(map[string]interface{}{"id": tempID1, "description": newDesc1}).
		Table("justforfun").
		AndWhere("id = ?", tempID1).
		AndWhere("description = ?", initialDesc1)
	rowsAffected, err = updateQuery.ExecResult(context.TODO())
	if err != nil {
		t.Logf("failed to update: %v", err)
		t.FailNow()
	}
	if rowsAffected != 1 {
		t.Logf("expected 1 row to be affected by update, instead got: %d", rowsAffected)
		t.FailNow()
	}

	// test multiple rows affected
	updateQuery = chain.New(db)
	updateQuery = chain.New(db)
	updateQuery.UpdateMap(map[string]interface{}{"description": newDesc2And3}).
		Table("justforfun").
		AndWhere("id = ? OR id = ?", tempID2, tempID3).
		AndWhere("description = ?", initialDesc2And3)
	rowsAffected, err = updateQuery.ExecResult(context.TODO())
	if err != nil {
		t.Logf("failed to update: %v", err)
		t.FailNow()
	}
	if rowsAffected != 2 {
		t.Logf("expected 2 row to be affected by update, instead got: %d", rowsAffected)
		t.FailNow()
	}

	// test query that does not have rows affected
	tempTable := "test_exec_result_temp_table"
	rowsAffected, err = db.ExecResult(context.TODO(), fmt.Sprintf("CREATE TABLE %s (id int)", tempTable))
	if err != nil {
		t.Logf("create table failed: %v", err)
		t.FailNow()
	}
	if rowsAffected != 0 {
		t.Logf("expected 0 rows to be affected by create table, instead got: %d", rowsAffected)
		t.FailNow()
	}
	rowsAffected, err = db.ExecResult(context.TODO(), fmt.Sprintf("DROP TABLE %s", tempTable))
	if err != nil {
		t.Logf("drop table failed: %v", err)
		t.FailNow()
	}
	if rowsAffected != 0 {
		t.Logf("expected 0 rows to be affected by drop table, instead got: %d", rowsAffected)
		t.FailNow()
	}
}
