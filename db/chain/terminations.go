package chain

// Find will populate interface with the results of the chain execution, works for Select
// If the table is missing it will be guessed from the passed type.
// If the mainOperation is empty a select from * will be attempted and the matchinf fields will
// fill the passed object.
// If only one object is passed then a `LIMIT 1` will be assumed.
func (ec *ExpresionChain) Find(result interface{}) error {
	return nil
}

// RawQuery will execute `query` with `args` arguments and if any rows are returned they will populate
// `result`.
// `result` may be nil and in that case returned rows will be ignored.
func (ec *ExpresionChain) RawQuery(query string, result interface{}, args ...interface{}) error {
	return nil
}

// Run executes the chain, works for Insert and Update
func (ec *ExpresionChain) Run() error {
	return nil
}

// TODO add batch running of many chains.

// TODO add transaction object that must contain array of chains, for this chain must contain a TX
// method that takes a db conn (with tx begin called) and each run must work transactionally so the
// Tx object can be reused if necessary.

// TODO Add pg Copy feature where possible to handle large inserts.

// TODO Add iterator method that allows to fetch rows one at a time by returning a
// `func next(result interface{}) (bool, error)` that allows iterating over results and a `end()`
// that allows dropping the query. (next must be a closure to avoid reflection at every row)
