# gaum

*A story about Bare Minimum Struct Relational Mapping, by Horacio Duran*

This intends to provide a bare minimum ORM-Like-but-not-quite interface to work with postgres.

The original intent behind this project is to allow us to replace [gorm](https://github.com/jinzhu/gorm) in a project at work because it is falling a bit short.

This library is, as it's name indicates, a bare minimum. It is not a "drop in" replacement for gorm because I wanted to change a bit the behavior.

How to use it, there are two components that can be used separately:

 * [The DB connector](#db): Which allows interaction with the underlying DB, it adds almost nothing to the underlying API but some minor level of magic when fetching the data and a set of helpers to convert from the more practical `?` argument placeholders to the numbered positionals required by the DB `$1, $2,....` with a few convenient expansions like making `(?)` into `($1...n)` based on the passed arguments (with limitations).
 * [The Chain](#chain): Provides a set of convenience functions and methods that allows chaning and combining to produce a struct that can render itself into a consistent SQL query (the promise is that the same object renders itself into the same SQL object each)

 ## DB
 
 **Note** all the examples in this doc are using the [postgres](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/postgres) driver because it is the itch I am scratching, along with it we provide a [standard](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/postgrespq) postgres driver, which can be used if you hit a limitation in the other one (which we did), I do not intend to write other drivers and most of the code will be strongly opinionated towards postgres, this said, if someone else feels like writing different drivers I'll gladly accept PRs
 
 As stated above, the [**DB**](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/connection#DB) component provides a set of convenience functions that allow querying and executing statements in the db and retrieving the results if any with slightlty less friction than the bare bones functions.
 
 To first open a connection we will need an instance of [`connection.DatabaseHandler`](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/connection#DatabaseHandler) and invoke the `Open` method.
 
 ### DatabaseHandler.[Open](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/connection#DatabaseHandler) ([postgrespq flavor](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/postgrespq#Connector.Open))
 
 Open creates a db connection pool and returns a [`connection.DB`](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/connection#DB) object containing it. By default it uses a pool, once again, because I have no use for single connection.
 
```golang
// imports used
// "github.com/ShiftLeftSecurity/gaum/db/connection"
// "github.com/ShiftLeftSecurity/gaum/db/postgrespq"
// "github.com/ShiftLeftSecurity/gaum/db/logging"
 
var connector connection.DatabaseHandler

// This uses Postgres PQ driver, which is the standard sql driver for postgres, you can use
// pgx one for a bit of efficiency increase in some aspects but outcome is not always guaranteed
// as we use PQ for real life testing (automated tests use both)
connector = postgrespq.Connector{
	ConnectionString: "a connection string",
}

// Wrappers are provider for go standard logging and testging t.Log but build more is trivial
// and can be pretty much cargo culted from the existing one.
logger := logging.NewGoLogger(standardgologger)

maxConnLifetime := 1 * time.Minute

dbConnection, err := gaumConnector.Open(&connection.Information{
	Logger:          logger,
	// For production `Error` is the recommended logging level as the driver and the underlying
	// library are quite chatty.
	LogLevel:        connection.Error,
	// You can omit this if you don't have special db requirements, most uses should be ok
	// with default but we provide just in case.
	// Side note: this only works with PQ driver.
	ConnMaxLifetime: &maxConnLifetime,
	// This is possible but rarely necessary, I just added the example in case you hit one of
	// the corner cases which require it.
	CustomDial: func(network, addr string) (net.Conn, error) {
		d := &net.Dialer{
			KeepAlive: time.Minute,
		}
		return d.Dial(network, addr)
	},
	// Most of these can be provided as the ConnectionString when instantiating the connector
	Host:             "127.0.0.1",
	Port:             5432,
	Database:         "postgres",
	User:             "postgres",
	Password:         "mysecretpassword",
	MaxConnPoolConns: 10,
})

if err != nil {
	// do something
}
	
```
The connection string is enough to open a connection but if `Open` receives a non nil parameter the overlapping parameters will be taken from the `connection.Information` in the `Open` invocation.

The [Information](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/connection#Information) struct contains most of the possible data one can use for a connection, strongly biased to postgres.

A note about the `logger` object passed to `Open`, its an instance of [`logging.Logger`](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/logging#Logger) which is basically an Interface for logging that I consider sane enough and that I in turn addapt to what [`pgx`](https://godoc.org/github.com/jackc/pgx) takes.

For ease of use, as stated in this example [a wrapper](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/logging#NewGoLogger) for the standard [go log](https://godoc.org/log#Logger) is provided.
For testing purposes, [another wrapper](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/logging#NewGoTestingLoggerr) is provided that wraps on the `*testing.T` object to facilitate testing info.

### Some of the items in DB

#### [DB](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/postgrespq#DB).[Clone](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/postgrespq#DB.Clone)

DB.Clone returns a deep copy of the db.

#### [EscapeArgs](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/connection#EscapeArgs)

EscapeArgs is in the wrong place in the code, but will do for now. This is something to be known before any querying function. To avoid the hassle of having to put `$<argnumber>` in each query argument placeholder, the convenience gorm provides was taken and it's possible to use `?` as a placeholder. To allow for our lazy side to take over, we need to invoke EscapeArgs on the query and args to both check for number of arg consistency and properly escape the placeholders before calling any of the queries, now for all of these functions, there is also another with the same name provided with an `E` prepend that invokes [EscapeArgs](#escapeargs) for you.

#### [DB](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/postgrespq#DB).[EQueryIter](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/postgrespq#DB.EQueryIter)

( See [EscapeArgs](#escapeargs) And [QueryIter](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/postgrespq#DB.QueryIter) )

EQueryIter will execute the query and return a closure that holds the cursor. Calling the returned closure produces advancement of the cursor, one can pass the pointer to a struct that one wants populated. The rules for populating a struct are made from the passed list of fields (containing the column names to be fetched in the query, beware no consistency is checked until query time and by then all will go boom or you will be missing data) there will be snake to camel case conversion and matching that to the struct member name (or the contents of `gaum:"field_name:something"`). If no fields are specified we will make a query to the db to ask for a description of the fields returned, try not to let that happen as it requires extra roundtrips and adds uncertainty.

Ideally this and all other queries will be used through chain that will take care of the ugly parts.

**Note**: this WILL hold a connection from the pool until you either invoke `close()` function returned in each iteration or deplete results, a timer option is planned but don't hold your breath.

#### [DB](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/postgrespq#DB).[EQuery](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/postgrespq#DB.EQuery)

(See [EscapeArgs](#escapeargs) And [Query](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/postgrespq#DB.Query) )

EQuery will return a closure, similar to `QueryIter` but it will take a slice pointer (sorry, reflection) only since it will fetch all the results in one call and populate the slice. The rest of the behavior is the same.

**Note**: this WILL hold a connection from the pool until you either invoke `close()` function returned or run the closure, a timer option is planned but don't hold your breath.


#### [DB](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/postgrespq#DB).[EQueryPrimitive](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/postgrespq#DB.EQueryPrimitive)

( See [EscapeArgs](#escapeargs) And [QueryPrimitive](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/postgrespq#DB.QueryPrimitive) )

QueryPrimitive will return a closure, similar to `Query` but it will take a pointer to slice of primitives, for this the `SELECT` statement **must** return only one column or the query will fail before even executing. The rest of the behavior is the same.

**Note**: this WILL hold a connection from the pool until you either invoke `close()` function returned or run the closure, a timer option is planned but don't hold your breath.

#### [DB](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/postgrespq#DB).[ERaw](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/postgrespq#DB.ERaw)

( See [EscapeArgs](#escapeargs) And [Raw](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/postgrespq#DB.Raw) )

Raw will run the passed query with the passed arguments and try to fetch the resulting row into the passed pointer receivers, this will do for one row only and you have to be careful to pass enough (and properly typed, [see sql.Scanner](https://golang.org/pkg/database/sql/#Scanner) ) receivers for the fields you are querying and no more.


#### [DB](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/postgrespq#DB).[EExec](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/postgrespq#DB.EExec)

( See [EscapeArgs](#escapeargs) And [Exec](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/postgrespq#DB.Exec) )

Exec is intended for queries that do not return results such as... well anything that is not a `SELECT` (and sometimes `SELECT` if you do not expect results, such as when invoking a stored procedure) you just pass the query and the arguments.

### Transactions

Transactions are fairly simple. [`DB`](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/postgrespq#DB) offers [`BeginTransaction`](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/postgrespq#DB.BeginTransaction) that returns a disposable [`DB`](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/postgrespq#DB) object whose life extends only to the boundary of the transaction and will end when you either [`RollbackTransaction`](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/postgrespq#RollbackTransaction) or [`CommitTransaction`](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/postgrespq#CommitTransaction). These things are idempotent so if you call Begin on a transaction nothing bad happens and equally if you call Rollback or Commit on a non transaction. To know if your db is a transaction use [`IsTransaction`](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/postgrespq#IsTransaction)

### DB.BulkInsert

This has not yet been tested, it's intention is to use the `COPY` statement.
 
## Chain

Chain is intended to ease the burden of SQL by hand (just kidding, I love SQL) and add a small layer of compile and pre-query time checks.

To use a [`Chain`](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#ExpresionChain) you must create one with [`NewExpresionChain`](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#NewExpresionChain)

Crafting the SQL is made by just calling the corresponding methods for the SQL we want added, the changes happen in place, the call returns nevertheless a pointer to it's own struct so it is more natural to chain commands.

`SELECT`, `INSERT`, `UPDATE`, `DELETE` and any other exclusive SQL keywords will replace the existing one as the chain will have a main operation.

[The main reference](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain)

### Composing

#### Chain Helpers

Before the actual statements, let's see some of the helpers available so the crafting of complete SQL is easier.

##### [TablePrefix](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#TablePrefix)

Table prefix returns a function that when invoked with a string as parameter returns the passed in string namespaced with the construcing table name.

```golang
tn := chain.TablePrefix("TableName")
tn("column") // -> "TableName.column
```

#### [SimpleFunction](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#SimpleFunction)

Simple Function allows to craft a simple function call that takes a column or constant as parameter, ideally you will use to construct totalization functions, for
convenience we provide some of the basic ones (check to see if he one you need is there already, we add more when we need) at the time of writing this we had:

* [AVG](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#AVG)
* [MIN](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#MIN)
* [MAX](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#MAX)
* [COUNT](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#COUNT)
* [SUM](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#SUM)

```golang
chain.AVG("colum") // -> AVG(column)

MyCustomFunction := func(column string) string { 
	return chain.SimpleFunction("MyCustomFunction", column)
}
MyCustomFunction("acolumn") // -> MyCustomFunction(acolumn)

AnotherFN := func(columns ...string) string {
	return chain.SimpleFunction("AnotherFn", strings.Join(columns, ","))
}
AnotherFN("one", "two", "three") // -> AnotherFn(one, two, three)
```

#### [ComplexFunction](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#ComplexFunction)

Complex Function returns, much like Simple function, a struct that allows chain calling of arguments to construct
a function that takes both static (columns, constants, etc) parameters and external ones (which will be passed as positional
to the query in the form `func($1, $2..., $n)` with separated args for safety)

```golang
c := ComplexFunction("afn")
something := 1
c.Static("column").Static("column2").Parametric(something)
c.Fn() // -> afn(column, column2, ?) // []interface{}{1}
// For the next, see `chain.SelectWithArgs`
c.FnSelect() // -> SelectArgument { Field: "afn(column, column2, ?)", Args: []interface{}{1} }
```

#### Select

```golang
chain.Select("one", "two", "three as four")
```

or, with helper `chain.AS`

```golang
chain.Select("one", "two", chain.As("three", "four"))
```


will produce (not really, it will fail at the lack of a table):

```sql
SELECT one, two, three
```

#### Table

```golang
chain.Select("one", "two", "three as four").Table("something")
``` 

will produce:

```sql
SELECT one, two, three FROM something
```

#### Where

The available helpers for `AndWhere`/`OrWhere` are:

* Equals
* GreaterThan
* GreaterOrEqualThan
* LesserThan
* LesserOrEqualThan
* In

all in the for `func Helper(field, ...args) (string, []interface{})` which can be used directly as a replacement of a where statement arguments. When using helpers the best way to write a statement is with one condition per Where.

```golang
chain.Select("one", "two", "three as four").Table("something").AndWhere("arg1=?", 1).AndWhere("arg2>?", 4).AndWhere("arg4 = ?", 3)
``` 

or with helpers

```golang
chain.Select("one", "two", chain.As("three","four")).Table("something").
AndWhere(chain.Equals("arg1", 1)).
AndWhere(chain.GreaterThan("arg2", 4)).
AndWhere(chain.Equals("arg4", 3))
``` 

will produce :

```sql
SELECT one, two, three FROM something WHERE arg1=$1 AND arg2>$2 AND arg4 = $3
```

Using **Or**

```golang
query := chain.Select("one", "two", "three as four").Table("something").AndWhere("arg1=?", 1).AndWhere("arg2>?", 4).AndWhere("arg4 = ?", 3).OrWhere("other_condition = ?", 1)
``` 
will produce :

```sql
SELECT one, two, three FROM something WHERE arg1=$1 AND arg2>$2 AND arg4 = $3 OR other_condition = $4
```

Using **Groups** (`AndWhereGroup` and `OrWhereGroup`)

```golang
query := chain.Select("one", "two", "three as four").Table("something").AndWhere("arg1=?", 1).AndWhere("arg2>?", 4).AndWhere("arg4 = ?", 3).OrWhereGroup((&ExpresionChain{}).AndWhere("inner == ?", 1).AndWhere("inner2 > ?", 2))
``` 
will produce :

```sql
SELECT one, two, three FROM something WHERE arg1=$1 AND arg2>$2 AND arg4 = $3 OR (inner == $4 AND inner2 == $5)
```

#### Insert

```golang
chain.Insert(map[string]interface{}{"field1": "value1", "field2": 2, "field3": "blah"}).Table("something")
```

will produce:

```sql
INSERT INTO something (field1, field2, field3) VALUES ($1, $2, $3)
```

#### Conflict

```golang
chain.Insert(map[string]interface{}{"field1": "value1", "field2": 2, "field3": "blah"}).Table("something").Conflict(chain.Constraint("therecanbeonlyone"), chain.ConflictActionNothing)
```

will produce:

```sql
INSERT INTO something (field1, field2, field3) VALUES ($1, $2, $3) ON CONFLICT ON CONSTRAINT (therecanbeonlyone) DO NOTHING
```

Constraint to a field


```golang
chain.Insert(map[string]interface{}{"field1": "value1", "field2": 2, "field3": "blah"}).Table("something").Conflict("id", chain.ConflictActionNothing)
```

will produce:

```sql
INSERT INTO something (field1, field2, field3) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING
```

#### Update

```golang
chain.Update("field1 = ?, field3 = ?", "value2", 9).Table("something").Where("id = ?", 1)
```

will produce:

```sql
UPDATE something SET (field1 = $1, field3 = $2) WHERE id = $3
```

#### Join

```golang
chain.Select("one, two, three as four, other.five").Table("something").Join("other ON field = ?", "fieldvalue").Where("arg1=? AND arg2>?", 1,4)
``` 

will produce:

```sql
SELECT one, two, three as four, other.five FROM something JOIN other ON field = $1 WHERE arg1=$2 AND arg2>$3).Where("arg1=? AND arg2>?", 1,4)
```

#### Delete

```golang
chain.Delete().Table("something").Where("arg1=? AND arg2>?", 1,4)
```

will produce:

```sql
DELETE FROM something WHERE arg1 = $1 AND arg2>$2
```
#### InsertMulti

```golang
query, err := chain.InsertMulti(map[string][]interface{}{
	"field1": []interface{"value1", "value2"},
	"field2": []interface{2, 3}, 
	"field3": []interface{"blah", "foo"},
	}).Table("something")
```

will produce:

```sql
INSERT INTO something (field1, field2, field3) VALUES ($1, $2, $3), ($4, $5, $6), ($7, $8, $9)
```

#### Clone

Returns a deep copy of this query.

### Rendering

There are two forms of rendering, both will return the query string and a slice of the args:

#### Render

Returns the query string with the `?` appearances replaced by the positional argument

#### RenderRaw

Returns the query and args but without replacement, ideal for subqueries or for the future implementation of `Constraint`

### Running

For running all the same functions that are available on [DB](#db) are here but you don't need to pass on the query components, only the receivers, if any:

* [Query](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#ExpresionChain.Query)
* [QueryIter](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#ExpresionChain.QueryIter)
* [Raw](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#ExpresionChain.Raw)
* [Exec](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#ExpresionChain.Exec)

## GroupChain (untested)

Therefore Undocumented, but ideally it is to make groups of queries in one go.
