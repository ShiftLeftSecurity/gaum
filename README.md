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

### Composing Helpers

#### Chain Helpers

Before the actual statements, let's see some of the helpers available so the crafting of complete SQL is easier.
Many of these do simple string concatenation or formatted printing but when the query grows to a decent lenght they are much
less error prone and definitely much easier to read.

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

// For FnSelect, see `chain.SelectWithArgs`
c.FnSelect() // -> SelectArgument { Field: "afn(column, column2, ?)", Args: []interface{}{1} }
```

#### [ColumnGroup](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#ColumnGroup)

Column Group groups columns in parenthesis for cases like:

```sql
WHERE (col1, col2, col3) IN ((1,2,3), (4,5,6))
```

```golang
chain.ColumnGroup("col1", "col2", "col3") // -> (col1, col2, col3)
```

#### [AndConditions](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#AndConditions)

And Conditions concatenates Several conditions using `AND`

```golang
chain.AndConditions("a = 1", "b = 2") // -> a = 1 AND b = 2
```

#### [CompareExpresions](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#CompareExpresions)

Compare Expresions makes a comparision between two SQL values (columns, constants, etc) using one of
the [predefined operators](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#CompOperator) 
(you may define your own too)

```golang
chain.CompareExpresions(chain.Eq, "column1", "column2") // -> column1 = column2
```

This particular helper is useful when crafting `JOIN` 

**Example**

```golang
t1 := chain.TablePrefix("Table1")
t2 := chain.TablePrefix("Table2")

joinConditions := chain.AndConditions(
	chain.CompareExpresions(
		chain.Eq,
		chain.ColumnGroup(t1("col1"),t1("col2"),t1("col3")),
		chain.ColumnGroup(t2("col1"),t2("col2"),t2("col3")),
	),
	chain.CompareExpresions(
		chain.Gt,
		t1("gtColumn"),
		t2("gtColumn"),
	)
)
```

The value of `joinConditions` in this case is:

```sql
(Table1.col1, Table1.col2, Table1.col3) = (Table2.col1, Table2.col2, Table2.col3) AND Table1.gtColumn = Table2.gtColumn
```

#### [As](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#CompareExpresions)

As returns the string with an added SQL alias

```golang
chain.As("column", "analias") // -> column AS analias
```

#### [Equals](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#Equals), [NotEquals](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#NotEquals), [GreaterThan](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#GreaterThan), [GreaterOrEqualThan](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#GreaterOrEqualThan), [LesserThan](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#LesserThan), [LesserOrEqualThan](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#LesserOrEqualThan)

These are all simple comparision operators and are here to prevent typos mostly.

```golang
chain.Equals("column") // -> column = ?
chain.NotEquals("column") // -> column != ?
chain.GreaterThan("column") // -> column > ?
chain.GreaterOrEqualThan("column") // -> column >=
chain.LesserThan("column") // -> column <
chain.LesserOrEqualThan("column") // ->  column <=
```

#### [In](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#In)

In creates the `IN (....)` construction.

```golang
chain.In("column", 1,2,3) // column IN (?) // []interface{}{1,2,3}
```

#### [InSlice](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#InSlice)

In creates the `IN (....)` construction but helps if you already have the items in slice form
so you do not have to unpack them

```golang
chain.In("column", []int64{1,2,3}) // column IN (?) // interface{} = []int64{1,2,3}
```

#### [Null](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#Null), [NotNull](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#NotNull)

Null and Not Null respectively craft the `x IS NULL` and `x IS NOT NULL` constructions.

```golang
chain.Null("column") // -> column IS NULL
chain.NotNull("column") // -> column IS NOT NULL
```

#### [SetToCurrentTimestamp](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#SetToCurrentTimestamp)

Set to current timestamp creates a set construction that uses the pg current timestamp value.

```golang
chain.SetToCurrentTimestamp("column") // -> column = CURRENT_TIMESTAMP
```

#### [NillableString](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#NillableString), [NillableInt64](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#NillableInt64)

These two are useful when fetching results, not when composing the query.
Nillable* converts safely from pointer to it's concrete type for the two most common types, when retrieving nillable columns from db is best that the recipient be a "pointer to" instead
of a concrete type otherwise encountering `NULL` values to return would result in failure to assign. To avoid the typicall issues of nil pointer dereference these methods were added that
in case of nil return the zero value of the type, If you need other types you will have to copy from these:

```golang
var s *string
var as = "astring"
chain.NillableString(s) // ""
s = &as
chain.NillableString(s) // "astring"

var i *int64
var ai = "42"
chain.NillableInt64(i) // 0
i = &ai
chain.NillableString(ai) // 42
```

### Composing Main Operations

#### Before Beginning

Any invocation to the methods of [`ExpresionChain`](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#ExpresionChain) requires you to have an instance of it, which can be obtained in two ways.

* With a constructor: If you require this expression chain to conclude with a call to the DB you might want to use the 
[`NewExpresionChain`](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#NewExpresionChain) constructor that receives 
a [`connection.DB`](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/postgrespq#DB)

	```golang
	c, err := postgrespq.Connector{
		ConnectionString: "a valid connection string",
	}.Open()

	// [snip] error checking

	q := chain.NewExpresionChain(c)
	```

* Creating an instance: If your purpose is to just render the query or use it as a subquery you can just instantiate yourself, without a `connection.DB`

	```golang
	q := chain.ExpresionChain{}
	```

All chain methods mutate the query in place, the only reason a pointer is returned is for easy of composing queries.

#### [Clone](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#ExpresionChain.Clone)

At any point in the use of the query you can invoque `q.Clone()` to obtain a copy of it (safe the `connection.DB` which is copied as is).
This is useful in cases where a query departs from the same root but at some point you want to fork it to create two similar queries.

#### [Select](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#ExpresionChain.Select)

Select allows you to craft a select of multiple columns or expresions, this method allows you to query on all the expresions `SELECT` can 
excepting for expresions requiring an external positional parameter, for that use [SelectWithArguments](#SelectWithArguments)

```golang
tp := chain.TablePrefix("ATable")
q.Select(
	"one", 
	"two", 
	"three AS four",
	chain.As("four", "five"),
	chain.As(tp("five"), "six"),
	AVG("something"))
```

will produce:

```sql
SELECT one, two, three AS four, four AS five, ATable.five as six, AVG(something)
```

#### [SelectWithArguments](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#ExpresionChain.SelectWithArguments)

Select With Arguments acts the same as `ExpresionChain.Select` but allows for external arguments, since its produced code is a bit
more complex, instead of receiving a variadic list of arguments, it receives a variadic list of `chain.SelectArgument`

```golang
q.Select(
	chain.SelectArgument{Field:"one"},
	chain.SelectArgument{Field: chain.As("two", "something")},
	chain.ComplexFunction("afn").Static("oneparam").Parametric(3).FnSelect(),
)
```

will produce:

```sql
SELECT one, two AS something, afn(oneparam, ?)
-- And the int 3 will be passed to the final render/call to db
```

#### [Update](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#ExpresionChain.Update)

```golang
chain.Update("field1 = ?, field3 = ?", "value2", 9).Table("something").Where("id = ?", 1)
```

will produce:

```sql
UPDATE something SET (field1 = $1, field3 = $2) WHERE id = $3
```


#### [Delete](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#ExpresionChain.Delete)

```golang
chain.Delete().Table("something").Where("arg1=? AND arg2>?", 1,4)
```

will produce:

```sql
DELETE FROM something WHERE arg1 = $1 AND arg2>$2
```

#### [Insert](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#ExpresionChain.Insert)

```golang
q.Insert(map[string]interface{}{
	"field1": "value1", 
	"field2": 2, 
	"field3": "blah"}).
	Table("something")
```

will produce:

```sql
INSERT INTO something (field1, field2, field3) VALUES ($1, $2, $3)
```

And Arguments

```golang
[]interface{}{"value1", 2, "blah"}
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


#### [InsertMulti](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#ExpresionChain.InsertMulti)

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


### Composing Query modificators


#### [Table](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#ExpresionChain.Table), [From](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#ExpresionChain.From)

```golang
chain.Select("one", "two", "three as four").Table("something")
// or
chain.Select("one", "two", "three as four").From("something")
``` 

will produce:

```sql
SELECT one, two, three FROM something
```


#### [AndWhere](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#ExpresionChain.AndWhere), [OrWhere](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#ExpresionChain.OrWhere)

Using **And**

```golang
q.Select("one", "two", chain.As("three","four")).From("something").
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
q.Select("one", "two", "three as four").From("something").AndWhere("arg1=?", 1).AndWhere("arg2>?", 4).AndWhere("arg4 = ?", 3).OrWhere("other_condition = ?", 1)
``` 
will produce :

```sql
SELECT one, two, three FROM something WHERE arg1=$1 AND arg2>$2 AND arg4 = $3 OR other_condition = $4
```

**Bear in mind** when this is rendered, the first condition rendered are always the `AND` ones, the first of course will not have effect since the operator refers to the 
prepend operation, so `q.AndWhere("1=1").OrWhere("2=2")` will be equivalent to `q.OrWhere("2=2").AndWhere("1=1")` and to `q.OrWhere("1=1").OrWhere("2=2")` but not to
`q.OrWhere("2=2").OrWhere("1=1")` in general the order and operator dictated by the common sense is the righ one.

Using **Groups** (`AndWhereGroup` and `OrWhereGroup`)

```golang
q.Select("one", "two", "three as four").
	From("something").
	AndWhere(chain.Equals("arg1"), 1).
	AndWhere(chain.GreaterThan("arg2"), 4).
	AndWhere(chain.Equals("arg4"), 3).
	OrWhereGroup(
		(&ExpresionChain{}).AndWhere("inner == ?", 1).AndWhere("inner2 > ?", 2))
``` 
will produce :

```sql
SELECT one, two, three FROM something WHERE arg1=$1 AND arg2>$2 AND arg4 = $3 OR (inner == $4 AND inner2 == $5)
```

And Arguments

```golang
[]interface{}{1,4,3,1,2}
```


#### [Join](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#ExpresionChain.Join)

```golang
chain.Select("one, two, three as four, other.five").Table("something").Join("other ON field = ?", "fieldvalue").Where("arg1=? AND arg2>?", 1,4)
``` 

will produce:

```sql
SELECT one, two, three as four, other.five FROM something JOIN other ON field = $1 WHERE arg1=$2 AND arg2>$3).Where("arg1=? AND arg2>?", 1,4)
```


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
* [QueryPrimitives](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#ExpresionChain.QueryPrimitives)
* [Raw](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#ExpresionChain.Raw)
* [Exec](https://godoc.org/github.com/ShiftLeftSecurity/gaum/db/chain#ExpresionChain.Exec)

## GroupChain (untested)

Therefore Undocumented, but ideally it is to make groups of queries in one go.
