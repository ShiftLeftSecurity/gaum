package postgres

import (
	"log"
	"os"
	"testing"

	"github.com/perrito666/bmstrem/db/chain"
	"github.com/perrito666/bmstrem/db/logging"

	"github.com/perrito666/bmstrem/db/connection"
)

func TestConnector_Open(t *testing.T) {
	// Requirements for now
	// docker run --name some-postgres -p 5432:5432 -e POSTGRES_PASSWORD=mysecretpassword -d postgres
	// CREATE TABLE justforfun (id int, description text);
	// INSERT INTO justforfun (id, description) VALUES (1, 'first');
	// INSERT INTO justforfun (id, description) VALUES (2, 'second');

	connector := Connector{
		ConnectionString: "TODO",
	}
	db, err := connector.Open(
		&connection.Information{
			Host:             "127.0.0.1",
			Port:             5432,
			Database:         "postgres",
			User:             "postgres",
			Password:         "mysecretpassword",
			MaxConnPoolConns: 10,
			Logger:           logging.NewGoLogger(log.New(os.Stdout, "logger: ", log.Lshortfile)),
		},
	)
	if err != nil {
		t.Errorf("failed to connect to db: %v", err)
	}
	query := chain.NewExpresionChain(db)
	query.Select("id, description").Table("justforfun").Where("id = ?", 1)
	q, args, err := query.Render()
	if err != nil {
		t.Errorf("failed to render: %v", err)
	}
	t.Logf("will perform query %q", q)
	t.Logf("with arguments %#v", args)
	iter, err := query.QueryIter(q, args...)
	if err != nil {
		t.Errorf("failed to query: %v", err)
	}
	type row struct {
		Id          int
		Description string
	}
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
}
