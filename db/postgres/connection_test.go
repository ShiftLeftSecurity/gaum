package postgres

import (
	"log"
	"os"
	"testing"

	"github.com/perrito666/bmstrem/db/chain"
	"github.com/perrito666/bmstrem/db/logging"

	"github.com/perrito666/bmstrem/db/connection"
)

func TestConnector_QueryIter(t *testing.T) {
	// Requirements for now
	/*
		docker run --name some-postgres -p 5432:5432 -e POSTGRES_PASSWORD=mysecretpassword -d postgres

		CREATE TABLE justforfun (id int, description text);
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
	query := chain.NewExpresionChain(db)
	query.Select("id, description").Table("justforfun").Where("id = ?", 1)

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
	type row struct {
		Id          int
		Description string
	}
	aRow := row{}
	// Test Multiple row Iterator
	query := chain.NewExpresionChain(db)
	query.Select("id, description").Table("justforfun").Where("id = ?", 1)
	err = query.Raw(&aRow.Id, &aRow.Description)
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
