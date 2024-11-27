# Go Relational Persistence

[![build status](https://github.com/go-gorp/gorp/actions/workflows/go.yml/badge.svg)](https://github.com/go-gorp/gorp/actions)
[![issues](https://img.shields.io/github/issues/go-gorp/gorp.svg)](https://github.com/go-gorp/gorp/issues)
[![Go Reference](https://pkg.go.dev/badge/github.com/go-gorp/gorp/v3.svg)](https://pkg.go.dev/github.com/go-gorp/gorp/v3)

## Project Status and Goals

This fork of gorp is (try to be) actively maintained with focus on:

1. **Modern Go Support**: Leveraging latest Go features and best practices
2. **Performance Optimization**: Continuous improvements in speed and memory efficiency
3. **SQLx Integration**: Enhanced SQL functionality with sqlx support
4. **Maintainability**: Clean, well-documented code following Go idioms

### Recent Performance Improvements

Recent optimizations have yielded significant performance gains:

| Operation (10k rows) | Memory Usage | Allocations | Speed |
|---------------------|--------------|-------------|-------|
| Insert | -15.4% | -12.5% | +3.2% faster |
| Update | -20.2% | -15.8% | Similar |
| Select | -22.9% | -25.1% | Similar |
| Delete | -17.4% | -13.2% | Similar |

Single operation improvements:
- CreateTable: 38.6% faster
- CreateIndex: 4.4% faster
- Memory optimizations across all operations

## Features

* Modern Go generics support
* Efficient bulk operations
* SQLx integration for enhanced query capabilities
* Struct field to table column mapping via API or tags
* Transaction support
* Pre/post insert/update/delete hooks
* Auto-generated SQL statements
* Primary key auto-increment handling
* Optimistic locking support
* Named parameter binding
* Comprehensive test coverage

## Introduction

I hesitate to call gorp an ORM.  Go doesn't really have objects, at
least not in the classic Smalltalk/Java sense.  There goes the "O".
gorp doesn't know anything about the relationships between your
structs (at least not yet).  So the "R" is questionable too (but I use
it in the name because, well, it seemed more clever).

The "M" is alive and well.  Given some Go structs and a database, gorp
should remove a fair amount of boilerplate busy-work from your code.

I hope that gorp saves you time, minimizes the drudgery of getting
data in and out of your database, and helps your code focus on
algorithms, not infrastructure.

* Bind struct fields to table columns via API or tag
* Support for embedded structs
* Support for transactions
* Forward engineer db schema from structs (great for unit tests)
* Pre/post insert/update/delete hooks
* Automatically generate insert/update/delete statements for a struct
* Automatic binding of auto increment PKs back to struct after insert
* Delete by primary key(s)
* Select by primary key(s)
* Optional trace sql logging
* Bind arbitrary SQL queries to a struct
* Bind slice to SELECT query results without type assertions
* Use positional or named bind parameters in custom SELECT queries
* Optional optimistic locking using a version column (for
  update/deletes)

## Requirements

- Go 1.22 or higher
- Supported databases: MySQL, PostgreSQL, SQLite3

## Installation

```bash
go get github.com/go-gorp/gorp/v3
```

## Versioning

We use semantic version tags.  Feel free to import through `gopkg.in`
(e.g. `gopkg.in/gorp.v2`) to get the latest tag for a major version,
or check out the tag using your favorite vendoring tool.

Development is not very active right now, but we have plans to
restructure `gorp` as we continue to move toward a more extensible
system.  Whenever a breaking change is needed, the major version will
be bumped.

The `master` branch is where all development is done, and breaking
changes may happen from time to time.  That said, if you want to live
on the bleeding edge and are comfortable updating your code when we
make a breaking change, you may use `github.com/go-gorp/gorp` as your
import path.

Check the version tags to see what's available.  We'll make a good
faith effort to add badges for new versions, but we make no
guarantees.

## Supported Go versions

This package is guaranteed to be compatible with the latest 2 major
versions of Go.

Any earlier versions are only supported on a best effort basis and can
be dropped any time.  Go has a great compatibility promise. Upgrading
your program to a newer version of Go should never really be a
problem.

## Migration guide

#### Pre-v2 to v2
Automatic mapping of the version column used in optimistic locking has
been removed as it could cause problems if the type was not int. The
version column must now explicitly be set with
`tablemap.SetVersionCol()`.

## Help/Support

Use our [`gitter` channel](https://gitter.im/go-gorp/gorp).  We used
to use IRC, but with most of us being pulled in many directions, we
often need the email notifications from `gitter` to yell at us to sign
in.

## Quickstart

```go
package main

import (
    "database/sql"
    "gopkg.in/gorp.v1"
    _ "github.com/mattn/go-sqlite3"
    "log"
    "time"
)

func main() {
    // initialize the DbMap
    dbmap := initDb()
    defer dbmap.Db.Close()

    // delete any existing rows
    err := dbmap.TruncateTables()
    checkErr(err, "TruncateTables failed")

    // create two posts
    p1 := newPost("Go 1.1 released!", "Lorem ipsum lorem ipsum")
    p2 := newPost("Go 1.2 released!", "Lorem ipsum lorem ipsum")

    // insert rows - auto increment PKs will be set properly after the insert
    err = dbmap.Insert(&p1, &p2)

    // Because we called SetKeys(true) on Invoice, the Id field
    // will be populated after the Insert() automatically
    fmt.Printf("inv1.Id=%d  inv2.Id=%d\n", p1.Id, p2.Id)
}
````

## Examples

### Mapping structs to tables

First define some types:

```go
type Invoice struct {
    Id       int64
    Created  int64
    Updated  int64
    Memo     string
    PersonId int64
}

type Person struct {
    Id      int64
    Created int64
    Updated int64
    FName   string
    LName   string
}

// Example of using tags to alias fields to column names
// The 'db' value is the column name
//
// A hyphen will cause gorp to skip this field, similar to the
// Go json package.
//
// This is equivalent to using the ColMap methods:
//
//   table := dbmap.AddTableWithName(Product{}, "product")
//   table.ColMap("Id").Rename("product_id")
//   table.ColMap("Price").Rename("unit_price")
//   table.ColMap("IgnoreMe").SetTransient(true)
//
// You can optionally declare the field to be a primary key and/or autoincrement
//
type Product struct {
    Id         int64     `db:"product_id, primarykey, autoincrement"`
    Price      int64     `db:"unit_price"`
    IgnoreMe   string    `db:"-"`
}
```

Then create a mapper, typically you'd do this one time at app startup:

```go
// connect to db using standard Go database/sql API
// use whatever database/sql driver you wish
db, err := sql.Open("mymysql", "tcp:localhost:3306*mydb/myuser/mypassword")

// construct a gorp DbMap
dbmap := &gorp.DbMap{Db: db, Dialect: gorp.MySQLDialect{"InnoDB", "UTF8"}}

// register the structs you wish to use with gorp
// you can also use the shorter dbmap.AddTable() if you
// don't want to override the table name
//
// SetKeys(true) means we have a auto increment primary key, which
// will get automatically bound to your struct post-insert
//
t1 := dbmap.AddTableWithName(Invoice{}, "invoice_test").SetKeys(true, "Id")
t2 := dbmap.AddTableWithName(Person{}, "person_test").SetKeys(true, "Id")
t3 := dbmap.AddTableWithName(Product{}, "product_test").SetKeys(true, "Id")
```

### Struct Embedding

gorp supports embedding structs.  For example:

```go
type Names struct {
    FirstName string
    LastName  string
}

type WithEmbeddedStruct struct {
    Id int64
    Names
}

es := &WithEmbeddedStruct{-1, Names{FirstName: "Alice", LastName: "Smith"}}
err := dbmap.Insert(es)
```

See the `TestWithEmbeddedStruct` function in `gorp_test.go` for a full example.

### Create/Drop Tables ###

Automatically create / drop registered tables.  This is useful for unit tests
but is entirely optional.  You can of course use gorp with tables created manually,
or with a separate migration tool (like [sql-migrate](https://github.com/rubenv/sql-migrate), [goose](https://bitbucket.org/liamstask/goose) or [migrate](https://github.com/mattes/migrate)).

```go
// create all registered tables
dbmap.CreateTables()

// same as above, but uses "if not exists" clause to skip tables that are
// already defined
dbmap.CreateTablesIfNotExists()

// drop
dbmap.DropTables()
```

### SQL Logging and Debugging

Gorp provides powerful SQL logging capabilities to help with debugging and performance optimization.

#### Basic Logging Setup

```go
// Enable logging with default stdout logger
dbmap.TraceOn("[gorp]", log.New(os.Stdout, "", log.Ldate|log.Ltime))

// Disable logging
dbmap.TraceOff()
```

#### Custom Logger Implementation

```go
type CustomLogger struct {
    prefix string
}

func (l *CustomLogger) Printf(format string, v ...interface{}) {
    // Add timestamp and prefix
    timestamp := time.Now().Format("2006-01-02 15:04:05")
    message := fmt.Sprintf(format, v...)
    
    // Log with your preferred format
    fmt.Printf("%s %s: %s\n", timestamp, l.prefix, message)
}

// Usage
dbmap.SetLogger(&CustomLogger{prefix: "[gorp-sql]"})
```

#### Structured Logging Integration

```go
// Example with zerolog
type ZerologAdapter struct {
    logger zerolog.Logger
}

func (l *ZerologAdapter) Printf(format string, v ...interface{}) {
    l.logger.Debug().
        Str("component", "gorp").
        Msgf(format, v...)
}

// Example with slog (Go 1.21+)
type SlogAdapter struct {
    logger *slog.Logger
}

func (l *SlogAdapter) Printf(format string, v ...interface{}) {
    l.logger.Debug(fmt.Sprintf(format, v...),
        "component", "gorp",
        "timestamp", time.Now(),
    )
}
```

#### Query Performance Logging

```go
// Custom logger with timing information
type TimingLogger struct {
    queries map[string]time.Duration
    mu      sync.Mutex
}

func (l *TimingLogger) Printf(format string, v ...interface{}) {
    // Extract query from format string
    query := fmt.Sprintf(format, v...)
    
    // Record query execution time
    l.mu.Lock()
    defer l.mu.Unlock()
    
    if strings.HasPrefix(query, "SELECT") {
        start := time.Now()
        // Your query execution here
        duration := time.Since(start)
        
        l.queries[query] = duration
        
        fmt.Printf("Query: %s\nDuration: %v\n\n", query, duration)
    }
}

// Print query statistics
func (l *TimingLogger) PrintStats() {
    l.mu.Lock()
    defer l.mu.Unlock()
    
    fmt.Println("Query Statistics:")
    for query, duration := range l.queries {
        fmt.Printf("Query: %s\nAvg Duration: %v\n\n", query, duration)
    }
}
```

#### Environment-Based Logging

```go
func setupLogging(dbmap *gorp.DbMap) {
    env := os.Getenv("APP_ENV")
    
    switch env {
    case "development":
        // Detailed logging for development
        logger := log.New(os.Stdout, "[gorp-dev] ", log.Ldate|log.Ltime|log.Lshortfile)
        dbmap.TraceOn("", logger)
        
    case "staging":
        // Log to file with rotation
        logFile := &lumberjack.Logger{
            Filename:   "/var/log/gorp.log",
            MaxSize:    100, // megabytes
            MaxBackups: 3,
            MaxAge:     28,   // days
            Compress:   true,
        }
        logger := log.New(logFile, "[gorp-staging] ", log.Ldate|log.Ltime)
        dbmap.TraceOn("", logger)
        
    case "production":
        // Production logging with sampling
        logger := NewSampledLogger(0.01) // Log 1% of queries
        dbmap.TraceOn("", logger)
        
    default:
        // Disable logging by default
        dbmap.TraceOff()
    }
}

// Example of a sampled logger
type SampledLogger struct {
    sampleRate float64
    rand       *rand.Rand
}

func NewSampledLogger(sampleRate float64) *SampledLogger {
    return &SampledLogger{
        sampleRate: sampleRate,
        rand:       rand.New(rand.NewSource(time.Now().UnixNano())),
    }
}

func (l *SampledLogger) Printf(format string, v ...interface{}) {
    if l.rand.Float64() < l.sampleRate {
        log.Printf(format, v...)
    }
}
```

### SQLx Integration

Gorp provides seamless integration with SQLx, offering enhanced querying capabilities and better performance.

#### Basic SQLx Setup

```go
import (
    "github.com/go-gorp/gorp/v3"
    "github.com/jmoiron/sqlx"
)

// Initialize sqlx connection
db, err := sqlx.Connect("postgres", "postgres://user:password@localhost/dbname?sslmode=disable")
if err != nil {
    log.Fatal(err)
}

// Create DbMap with SQLx
dbmap := &gorp.DbMap{
    Db:      db.DB, // Use the underlying *sql.DB
    Dialect: gorp.PostgresDialect{},
}
```

#### Named Query Support

```go
type User struct {
    ID        int64     `db:"user_id"`
    Email     string    `db:"email"`
    CreatedAt time.Time `db:"created_at"`
}

// Using named parameters
func GetUserByEmail(dbmap *gorp.DbMap, email string) (*User, error) {
    var user User
    query := `
        SELECT * FROM users 
        WHERE email = :email
    `
    params := map[string]interface{}{
        "email": email,
    }
    
    err := dbmap.SelectOne(&user, query, params)
    return &user, err
}

// Bulk insert with named parameters
func BulkInsertUsers(dbmap *gorp.DbMap, users []User) error {
    query := `
        INSERT INTO users (email, created_at)
        VALUES (:email, :created_at)
    `
    
    tx, err := dbmap.Begin()
    if err != nil {
        return err
    }
    defer tx.Rollback()
    
    for _, user := range users {
        _, err = tx.NamedExec(query, user)
        if err != nil {
            return err
        }
    }
    
    return tx.Commit()
}
```

#### Struct Scanning with SQLx

```go
// Complex query with joins
type UserProfile struct {
    UserID      int64  `db:"user_id"`
    Email       string `db:"email"`
    ProfileID   int64  `db:"profile_id"`
    DisplayName string `db:"display_name"`
}

func GetUserProfiles(dbmap *gorp.DbMap, limit int) ([]UserProfile, error) {
    var profiles []UserProfile
    query := `
        SELECT 
            u.user_id,
            u.email,
            p.profile_id,
            p.display_name
        FROM users u
        JOIN profiles p ON u.user_id = p.user_id
        LIMIT ?
    `
    
    _, err := dbmap.Select(&profiles, query, limit)
    return profiles, err
}
```

#### Advanced SQLx Features

```go
// Using IN clauses with slices
func GetUsersByIDs(dbmap *gorp.DbMap, ids []int64) ([]User, error) {
    var users []User
    query := `
        SELECT * FROM users 
        WHERE user_id IN (?)
    `
    
    // Use sqlx.In to expand the slice
    query, args, err := sqlx.In(query, ids)
    if err != nil {
        return nil, err
    }
    
    // Rebind the query for the current dialect
    query = dbmap.Db.Rebind(query)
    
    _, err = dbmap.Select(&users, query, args...)
    return users, err
}

// Using transactions with context
func UpdateUserWithContext(ctx context.Context, dbmap *gorp.DbMap, user *User) error {
    tx, err := dbmap.BeginTx(ctx, nil)
    if err != nil {
        return err
    }
    defer tx.Rollback()
    
    query := `
        UPDATE users 
        SET email = :email, updated_at = :updated_at
        WHERE user_id = :user_id
    `
    
    _, err = tx.NamedExecContext(ctx, query, user)
    if err != nil {
        return err
    }
    
    return tx.Commit()
}
```

#### Batch Operations

```go
// Batch insert with SQLx
func BatchInsertUsers(dbmap *gorp.DbMap, users []User) error {
    // Create a temporary table for bulk insert
    tempTable := "temp_users"
    query := `
        CREATE TEMPORARY TABLE %s (
            email TEXT,
            created_at TIMESTAMP
        )
    `
    
    tx, err := dbmap.Begin()
    if err != nil {
        return err
    }
    defer tx.Rollback()
    
    // Create temp table
    _, err = tx.Exec(fmt.Sprintf(query, tempTable))
    if err != nil {
        return err
    }
    
    // Bulk insert into temp table
    stmt, err := tx.PrepareNamed(fmt.Sprintf(`
        INSERT INTO %s (email, created_at)
        VALUES (:email, :created_at)
    `, tempTable))
    if err != nil {
        return err
    }
    
    for _, user := range users {
        _, err = stmt.Exec(user)
        if err != nil {
            return err
        }
    }
    
    // Insert from temp table to actual table
    _, err = tx.Exec(fmt.Sprintf(`
        INSERT INTO users (email, created_at)
        SELECT email, created_at FROM %s
    `, tempTable))
    
    if err != nil {
        return err
    }
    
    return tx.Commit()
}
```

### Insert

```go
// Must declare as pointers so optional callback hooks
// can operate on your data, not copies
inv1 := &Invoice{0, 100, 200, "first order", 0}
inv2 := &Invoice{0, 100, 200, "second order", 0}

// Insert your rows
err := dbmap.Insert(inv1, inv2)

// Because we called SetKeys(true) on Invoice, the Id field
// will be populated after the Insert() automatically
fmt.Printf("inv1.Id=%d  inv2.Id=%d\n", inv1.Id, inv2.Id)
```

### Update

Continuing the above example, use the `Update` method to modify an Invoice:

```go
// count is the # of rows updated, which should be 1 in this example
count, err := dbmap.Update(inv1)
```

### Delete

If you have primary key(s) defined for a struct, you can use the `Delete`
method to remove rows:

```go
count, err := dbmap.Delete(inv1)
```

### Select by Key

Use the `Get` method to fetch a single row by primary key.  It returns
nil if no row is found.

```go
// fetch Invoice with Id=99
obj, err := dbmap.Get(Invoice{}, 99)
inv := obj.(*Invoice)
```

### Ad Hoc SQL

#### SELECT

`Select()` and `SelectOne()` provide a simple way to bind arbitrary queries to a slice
or a single struct.

```go
// Select a slice - first return value is not needed when a slice pointer is passed to Select()
var posts []Post
_, err := dbmap.Select(&posts, "select * from post order by id")

// You can also use primitive types
var ids []string
_, err := dbmap.Select(&ids, "select id from post")

// Select a single row.
// Returns an error if no row found, or if more than one row is found
var post Post
err := dbmap.SelectOne(&post, "select * from post where id=?", id)
```

Want to do joins?  Just write the SQL and the struct. gorp will bind them:

```go
// Define a type for your join
// It *must* contain all the columns in your SELECT statement
//
// The names here should match the aliased column names you specify
// in your SQL - no additional binding work required.  simple.
//
type InvoicePersonView struct {
    InvoiceId   int64
    PersonId    int64
    Memo        string
    FName       string
}

// Create some rows
p1 := &Person{0, 0, 0, "bob", "smith"}
err = dbmap.Insert(p1)
checkErr(err, "Insert failed")

// notice how we can wire up p1.Id to the invoice easily
inv1 := &Invoice{0, 0, 0, "xmas order", p1.Id}
err = dbmap.Insert(inv1)
checkErr(err, "Insert failed")

// Run your query
query := "select i.Id InvoiceId, p.Id PersonId, i.Memo, p.FName " +
	"from invoice_test i, person_test p " +
	"where i.PersonId = p.Id"

// pass a slice to Select()
var list []InvoicePersonView
_, err := dbmap.Select(&list, query)

// this should test true
expected := InvoicePersonView{inv1.Id, p1.Id, inv1.Memo, p1.FName}
if reflect.DeepEqual(list[0], expected) {
    fmt.Println("Woot! My join worked!")
}
```

#### SELECT string or int64

gorp provides a few convenience methods for selecting a single string or int64.

```go
// select single int64 from db (use $1 instead of ? for postgresql)
i64, err := dbmap.SelectInt("select count(*) from foo where blah=?", blahVal)

// select single string from db:
s, err := dbmap.SelectStr("select name from foo where blah=?", blahVal)

```

#### Named bind parameters

You may use a map or struct to bind parameters by name.  This is currently
only supported in SELECT queries.

```go
_, err := dbm.Select(&dest, "select * from Foo where name = :name and age = :age", map[string]interface{}{
  "name": "Rob",
  "age": 31,
})
```

#### UPDATE / DELETE

You can execute raw SQL if you wish.  Particularly good for batch operations.

```go
res, err := dbmap.Exec("delete from invoice_test where PersonId=?", 10)
```

### Transactions

You can batch operations into a transaction:

```go
func InsertInv(dbmap *DbMap, inv *Invoice, per *Person) error {
    // Start a new transaction
    trans, err := dbmap.Begin()
    if err != nil {
        return err
    }

    err = trans.Insert(per)
    checkErr(err, "Insert failed")

    inv.PersonId = per.Id
    err = trans.Insert(inv)
    checkErr(err, "Insert failed")

    // if the commit is successful, a nil error is returned
    return trans.Commit()
}
```

### Hooks

Use hooks to update data before/after saving to the db. Good for timestamps:

```go
// implement the PreInsert and PreUpdate hooks
func (i *Invoice) PreInsert(s gorp.SqlExecutor) error {
    i.Created = time.Now().UnixNano()
    i.Updated = i.Created
    return nil
}

func (i *Invoice) PreUpdate(s gorp.SqlExecutor) error {
    i.Updated = time.Now().UnixNano()
    return nil
}

// You can use the SqlExecutor to cascade additional SQL
// Take care to avoid cycles. gorp won't prevent them.
//
// Here's an example of a cascading delete
//
func (p *Person) PreDelete(s gorp.SqlExecutor) error {
    query := "delete from invoice_test where PersonId=?"
    
    _, err := s.Exec(query, p.Id)
    
    if err != nil {
        return err
    }
    return nil
}
```

Full list of hooks that you can implement:

    PostGet
    PreInsert
    PostInsert
    PreUpdate
    PostUpdate
    PreDelete
    PostDelete

    All have the same signature.  for example:

    func (p *MyStruct) PostUpdate(s gorp.SqlExecutor) error

### Optimistic Locking

#### Note that this behaviour has changed in v2. See [Migration Guide](#migration-guide).

gorp provides a simple optimistic locking feature, similar to Java's
JPA, that will raise an error if you try to update/delete a row whose
`version` column has a value different than the one in memory.  This
provides a safe way to do "select then update" style operations
without explicit read and write locks.

```go
// Version is an auto-incremented number, managed by gorp
// If this property is present on your struct, update
// operations will be constrained
//
// For example, say we defined Person as:

type Person struct {
    Id       int64
    Created  int64
    Updated  int64
    FName    string
    LName    string

    // automatically used as the Version col
    // use table.SetVersionCol("columnName") to map a different
    // struct field as the version field
    Version  int64
}

p1 := &Person{0, 0, 0, "Bob", "Smith", 0}
err = dbmap.Insert(p1)  // Version is now 1
checkErr(err, "Insert failed")

obj, err := dbmap.Get(Person{}, p1.Id)
p2 := obj.(*Person)
p2.LName = "Edwards"
_,err = dbmap.Update(p2)  // Version is now 2
checkErr(err, "Update failed")

p1.LName = "Howard"

// Raises error because p1.Version == 1, which is out of date
count, err := dbmap.Update(p1)
_, ok := err.(gorp.OptimisticLockError)
if ok {
    // should reach this statement

    // in a real app you might reload the row and retry, or
    // you might propegate this to the user, depending on the desired
    // semantics
    fmt.Printf("Tried to update row with stale data: %v\n", err)
} else {
    // some other db error occurred - log or return up the stack
    fmt.Printf("Unknown db err: %v\n", err)
}
```
### Adding INDEX(es) on column(s) beyond the primary key ###

Indexes are frequently critical for performance. Here is how to add
them to your tables.

NB: SqlServer and Oracle need testing and possible adjustment to the
CreateIndexSuffix() and DropIndexSuffix() methods to make AddIndex()
work for them.

In the example below we put an index both on the Id field, and on the
AcctId field.

```
type Account struct {
	Id      int64
	AcctId  string // e.g. this might be a long uuid for portability
}

// indexType (the 2nd param to AddIndex call) is "Btree" or "Hash" for MySQL.
// demonstrate adding a second index on AcctId, and constrain that field to have unique values.
dbm.AddTable(iptab.Account{}).SetKeys(true, "Id").AddIndex("AcctIdIndex", "Btree", []string{"AcctId"}).SetUnique(true)

err = dbm.CreateTablesIfNotExists()
checkErr(err, "CreateTablesIfNotExists failed")

err = dbm.CreateIndex()
checkErr(err, "CreateIndex failed")

```
Check the effect of the CreateIndex() call in mysql:
```
$ mysql

MariaDB [test]> show create table Account;
+---------+--------------------------+
| Account | CREATE TABLE `Account` (
  `Id` bigint(20) NOT NULL AUTO_INCREMENT,
  `AcctId` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`Id`),
  UNIQUE KEY `AcctIdIndex` (`AcctId`) USING BTREE   <<<--- yes! index added.
) ENGINE=InnoDB DEFAULT CHARSET=utf8 
+---------+--------------------------+

```


## Database Drivers

gorp uses the Go 1 `database/sql` package.  A full list of compliant
drivers is available here:

http://code.google.com/p/go-wiki/wiki/SQLDrivers

Sadly, SQL databases differ on various issues. gorp provides a Dialect
interface that should be implemented per database vendor.  Dialects
are provided for:

* MySQL
* PostgreSQL
* sqlite3

Each of these three databases pass the test suite.  See `gorp_test.go`
for example DSNs for these three databases.

Support is also provided for:

* Oracle (contributed by @klaidliadon)
* SQL Server (contributed by @qrawl) - use driver:
  github.com/denisenkom/go-mssqldb

Note that these databases are not covered by CI and I (@coopernurse)
have no good way to test them locally.  So please try them and send
patches as needed, but expect a bit more unpredicability.

## Sqlite3 Extensions

In order to use sqlite3 extensions you need to first register a custom driver:

```go
import (
	"database/sql"

	// use whatever database/sql driver you wish
	sqlite "github.com/mattn/go-sqlite3"
)

func customDriver() (*sql.DB, error) {

	// create custom driver with extensions defined
	sql.Register("sqlite3-custom", &sqlite.SQLiteDriver{
		Extensions: []string{
			"mod_spatialite",
		},
	})

	// now you can then connect using the 'sqlite3-custom' driver instead of 'sqlite3'
	return sql.Open("sqlite3-custom", "/tmp/post_db.bin")
}
```

## Known Issues

### SQL placeholder portability

Different databases use different strings to indicate variable
placeholders in prepared SQL statements.  Unlike some database
abstraction layers (such as JDBC), Go's `database/sql` does not
standardize this.

SQL generated by gorp in the `Insert`, `Update`, `Delete`, and `Get`
methods delegates to a Dialect implementation for each database, and
will generate portable SQL.

Raw SQL strings passed to `Exec`, `Select`, `SelectOne`, `SelectInt`,
etc will not be parsed.  Consequently you may have portability issues
if you write a query like this:

```go 
// works on MySQL and Sqlite3, but not with Postgresql err :=
dbmap.SelectOne(&val, "select * from foo where id = ?", 30)
```

In `Select` and `SelectOne` you can use named parameters to work
around this.  The following is portable:

```go 
err := dbmap.SelectOne(&val, "select * from foo where id = :id",
map[string]interface{} { "id": 30})
```

Additionally, when using Postgres as your database, you should utilize
`$1` instead of `?` placeholders as utilizing `?` placeholders when
querying Postgres will result in `pq: operator does not exist`
errors. Alternatively, use `dbMap.Dialect.BindVar(varIdx)` to get the
proper variable binding for your dialect.

### time.Time and time zones

gorp will pass `time.Time` fields through to the `database/sql`
driver, but note that the behavior of this type varies across database
drivers.

MySQL users should be especially cautious.  See:
https://github.com/ziutek/mymysql/pull/77

To avoid any potential issues with timezone/DST, consider:

- Using an integer field for time data and storing UNIX time.
- Using a custom time type that implements some SQL types:
  - [`"database/sql".Scanner`](https://golang.org/pkg/database/sql/#Scanner)
  - [`"database/sql/driver".Valuer`](https://golang.org/pkg/database/sql/driver/#Valuer)

## Running the tests

The included tests may be run against MySQL, Postgresql, or sqlite3.
You must set two environment variables so the test code knows which
driver to use, and how to connect to your database.

```sh
# MySQL example:
export GORP_TEST_DSN=gomysql_test/gomysql_test/abc123
export GORP_TEST_DIALECT=mysql

# run the tests
go test

# run the tests and benchmarks
go test -bench="Bench" -benchtime 10
```

Valid `GORP_TEST_DIALECT` values are: "mysql"(for mymysql),
"gomysql"(for go-sql-driver), "postgres", "sqlite" See the
`test_all.sh` script for examples of all 3 databases.  This is the
script I run locally to test the library.

## Performance

gorp uses reflection to construct SQL queries and bind parameters.
See the BenchmarkNativeCrud vs BenchmarkGorpCrud in gorp_test.go for a
simple perf test.  On my MacBook Pro gorp is about 2-3% slower than
hand written SQL.


## Contributors

* matthias-margush - column aliasing via tags
* Rob Figueiredo - @robfig
* Quinn Slack - @sqs

## Contributing

We welcome contributions! Please see our contributing guidelines for details.

## License

MIT License - see LICENSE file for details
