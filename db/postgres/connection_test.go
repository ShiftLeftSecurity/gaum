package postgres

import (
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/perrito666/bmstrem/db/chain"
	"github.com/perrito666/bmstrem/db/logging"
	uuid "github.com/satori/go.uuid"

	"github.com/perrito666/bmstrem/db/connection"
)

func cleanup(t *testing.T, db connection.DB) {
	query := chain.NewExpresionChain(db)
	query.Delete().Table("justforfun").AndWhere("id > ?", 10)
	err := query.Exec()
	if err != nil {
		t.Logf("failed cleanup queries: %v", err)
		t.FailNow()
	}
}

func newDB(t *testing.T) connection.DB {
	connector := Connector{
		ConnectionString: "TODO",
	}
	defaultLogger := log.New(os.Stdout, "logger: ", log.Lshortfile)
	goLoggerWrapped := logging.NewGoLogger(defaultLogger)
	db, err := connector.Open(
		&connection.Information{
			Host:             "127.0.0.1",
			Port:             5432,
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
	cleanup(t, db)
	return db
}

func TestConnector_QueryIter(t *testing.T) {
	// Requirements for now
	/*
		docker run --name some-postgres -p 5432:5432 -e POSTGRES_PASSWORD=mysecretpassword -d postgres

		CREATE TABLE justforfun (id int, description text, CONSTRAINT therecanbeonlyone UNIQUE (id));
		INSERT INTO justforfun (id, description) VALUES (1, 'first');
		INSERT INTO justforfun (id, description) VALUES (2, 'second');
		INSERT INTO justforfun (id, description) VALUES (3, 'third');
		INSERT INTO justforfun (id, description) VALUES (4, 'fourth');
		INSERT INTO justforfun (id, description) VALUES (5, 'fift');
		INSERT INTO justforfun (id, description) VALUES (6, 'sixt');
		INSERT INTO justforfun (id, description) VALUES (7, 'seventh');
		INSERT INTO justforfun (id, description) VALUES (8, 'eight');
		INSERT INTO justforfun (id, description) VALUES (9, 'ninth');
		INSERT INTO justforfun (id, description) VALUES (10, 'tenth');
	*/

	db := newDB(t)
	query := chain.NewExpresionChain(db)
	query.Select("id, description").Table("justforfun").AndWhere("id = ?", 1)

	// Debug print query
	q, args, err := query.Render()
	if err != nil {
		t.Errorf("failed to render: %v", err)
	}
	t.Logf("will perform query %q", q)
	t.Logf("with arguments %#v", args)

	iter, err := query.QueryIter()
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
	defer closer()

	if err != nil {
		t.Errorf("failed to iterate: %v", err)
	}
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
	query = chain.NewExpresionChain(db)
	query.Select("id, description").Table("justforfun").OrderBy("id")
	iter, err = query.QueryIter()
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

func TestConnector_Query(t *testing.T) {

	db := newDB(t)
	type row struct {
		Id          int
		Description string
	}

	// Test Multiple row Iterator
	query := chain.NewExpresionChain(db)
	query.Select("id, description").Table("justforfun").OrderBy("id")
	fetcher, err := query.Query()
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

}

func TestConnector_Raw(t *testing.T) {

	db := newDB(t)
	type row struct {
		Id          int
		Description string
	}
	aRow := row{}
	// Test Multiple row Iterator
	query := chain.NewExpresionChain(db)
	query.Select("id, description").Table("justforfun").AndWhere("id = ?", 1)
	err := query.Raw(&aRow.Id, &aRow.Description)
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

}

func TestConnector_Insert(t *testing.T) {

	db := newDB(t)
	type row struct {
		Id          int
		Description string
	}
	aRow := row{}
	// Test Multiple row Iterator
	query := chain.NewExpresionChain(db)
	tempDescriptionUUID := uuid.NewV4()
	tempDescription := tempDescriptionUUID.String()
	query.Select("id, description").Table("justforfun").AndWhere("description = ?", tempDescription)
	err := query.Raw(&aRow.Id, &aRow.Description)
	if err == nil {
		t.Log("querying for our description should fail, this record should not exist")
		t.FailNow()
	}
	rand.Seed(time.Now().UnixNano())
	tempID := rand.Intn(11000)

	insertQuery := chain.NewExpresionChain(db)
	insertQuery.Insert(map[string]interface{}{"id": tempID, "description": tempDescription}).
		Table("justforfun")
	err = insertQuery.Exec()
	if err != nil {
		t.Logf("failed to insert: %v", err)
		t.FailNow()
	}

	err = query.Raw(&aRow.Id, &aRow.Description)
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

func TestConnector_MultiInsert(t *testing.T) {

	db := newDB(t)
	type row struct {
		Id          int
		Description string
	}
	aRow := row{}
	// Test Multiple row Iterator
	query := chain.NewExpresionChain(db)
	query1 := query.Clone()
	tempDescription := uuid.NewV4().String()
	tempDescription1 := uuid.NewV4().String()
	query.Select("id, description").Table("justforfun").AndWhere("description = ?", tempDescription)
	err := query.Raw(&aRow.Id, &aRow.Description)
	if err == nil {
		t.Log("querying for our description should fail, this record should not exist")
		t.FailNow()
	}

	query1.Select("id, description").Table("justforfun").AndWhere("description = ?", tempDescription1)
	err = query1.Raw(&aRow.Id, &aRow.Description)
	if err == nil {
		t.Log("querying for our second description should fail, this record should not exist")
		t.FailNow()
	}

	rand.Seed(time.Now().UnixNano())
	tempID := rand.Intn(11000)
	tempID1 := tempID + 1

	insertQuery := chain.NewExpresionChain(db)
	_, err = insertQuery.InsertMulti(map[string][]interface{}{
		"description": []interface{}{tempDescription, tempDescription1},
		"id":          []interface{}{tempID, tempID1},
	})
	insertQuery.Table("justforfun")
	err = insertQuery.Exec()
	if err != nil {
		t.Logf("failed to insert: %v", err)
		t.FailNow()
	}

	err = query.Raw(&aRow.Id, &aRow.Description)
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

	err = query1.Raw(&aRow.Id, &aRow.Description)
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

func TestConnector_InsertConstraint(t *testing.T) {
	db := newDB(t)
	type row struct {
		Id          int
		Description string
	}
	aRow := row{}
	// Test Multiple row Iterator
	query := chain.NewExpresionChain(db)
	tempDescriptionUUID := uuid.NewV4()
	tempDescription := tempDescriptionUUID.String()
	query.Select("id, description").Table("justforfun").AndWhere("description = ?", tempDescription)
	err := query.Raw(&aRow.Id, &aRow.Description)
	if err == nil {
		t.Log("querying for our description should fail, this record should not exist")
		t.FailNow()
	}
	rand.Seed(time.Now().UnixNano())
	tempID := rand.Intn(11000)

	// First insert, this is to have a colliding value
	insertQuery := chain.NewExpresionChain(db)
	insertQuery.Insert(map[string]interface{}{"id": tempID, "description": tempDescription}).
		Table("justforfun")
	err = insertQuery.Exec()
	if err != nil {
		t.Logf("failed to insert to test constraint: %v", err)
		t.FailNow()
	}

	err = query.Raw(&aRow.Id, &aRow.Description)
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
	err = insertQuery.Exec()
	if err == nil {
		t.Log("an insert that breaks an uniqueness constraint should not be allowed, yet it was")
		t.FailNow()
	}

	// Third attempt, this should work
	insertQuery.Conflict(chain.Constraint("therecanbeonlyone"), chain.ConflictActionNothing)
	insertQuery.Insert(map[string]interface{}{"id": tempID, "description": tempDescription}).
		Table("justforfun")
	queryString, queryArgs, _ = insertQuery.Render()
	t.Logf("conflicting insert query: %s", queryString)
	t.Logf("conflicting insert args: %v", queryArgs)
	err = insertQuery.Exec()
	if err != nil {
		t.Logf("the insertion conflict should have been ignored, yet it wasnt: %v", err)
		t.FailNow()
	}
}

func TestConnector_Transaction(t *testing.T) {
	db := newDB(t)
	type row struct {
		Id          int
		Description string
	}
	aRow := row{}
	// Test Multiple row Iterator
	query := chain.NewExpresionChain(db)
	tempDescriptionUUID := uuid.NewV4()
	tempDescription := tempDescriptionUUID.String()
	query.Select("id, description").Table("justforfun").AndWhere("description = ?", tempDescription)
	err := query.Raw(&aRow.Id, &aRow.Description)
	if err == nil {
		t.Log("querying for our description should fail, this record should not exist")
		t.FailNow()
	}
	rand.Seed(time.Now().UnixNano())
	tempID := rand.Intn(11000)

	transactionalDB, err := db.Clone().BeginTransaction()
	if err != nil {
		t.Logf("attempting to begin a transaction: %v", err)
		t.FailNow()
	}
	// Let's try this with transactions
	insertQuery := chain.NewExpresionChain(transactionalDB)
	insertQuery.Insert(map[string]interface{}{"id": tempID, "description": tempDescription}).
		Table("justforfun")
	err = insertQuery.Exec()
	if err != nil {
		t.Logf("an insert in a transaction was attempted but failed: %v", err)
		t.FailNow()
	}

	// Transaction was not committed so no result should be here
	err = query.Raw(&aRow.Id, &aRow.Description)
	if err == nil {
		t.Log("querying for our description should fail, this record should not exist")
		t.FailNow()
	}

	err = transactionalDB.RollbackTransaction()
	if err != nil {
		t.Logf("attempting to rollback a transaction: %v", err)
		t.FailNow()
	}

	// Transaction was rolled back so still no row
	err = query.Raw(&aRow.Id, &aRow.Description)
	if err == nil {
		t.Log("querying for our description should fail, this record should not exist")
		t.FailNow()
	}

	// a new transaction is required to try again.
	transactionalDB, err = db.BeginTransaction()
	if err != nil {
		t.Logf("attempting to start a new transaction: %v", err)
		t.FailNow()
	}
	insertQuery.NewDB(transactionalDB)

	// lets insert with the idea of a commit now
	err = insertQuery.Exec()
	if err != nil {
		t.Logf("an insert in a transaction was attempted but failed: %v", err)
		t.FailNow()
	}

	// Transaction is still not committed so it should fail.
	err = query.Raw(&aRow.Id, &aRow.Description)
	if err == nil {
		t.Log("querying for our description should fail, this record should not exist")
		t.FailNow()
	}

	// Commit the transaction
	err = transactionalDB.CommitTransaction()
	if err != nil {
		t.Logf("attempting to commit a transaction: %v", err)
		t.FailNow()
	}

	// let's make sure commit worked.
	err = query.Raw(&aRow.Id, &aRow.Description)
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
