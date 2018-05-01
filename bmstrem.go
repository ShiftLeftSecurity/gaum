package bmstrem

import "github.com/perrito666/bmstrem/db/postgres"

var handlers = map[string]connection.DatabaseHandler{
	"postgresql": postgres.Connector,
}

// Open returns a DB connected to the passed db if possible.
func Open(driver string, connInfo *connection.ConnectionInformation) (connection.DB, error) {
handler, ok := handlers[driver]
if !ok {
	nil, errors.Errorf("do not know how to handle %s", driver)
}
}
