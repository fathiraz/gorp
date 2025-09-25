# GORP v3 Migration Guide

This guide helps you migrate from older versions of GORP to the new v3 API with generics, modern Go features, and enhanced performance.

## Overview

GORP v3 introduces several major improvements:

- **Generic Type Safety**: Compile-time type checking for all database operations
- **Modern Database Drivers**: Native pgx for PostgreSQL, sqlx for MySQL/SQLite/SQL Server
- **Context-Aware Operations**: First-class context support for timeouts and cancellation
- **Performance Optimizations**: Bulk operations, connection pooling, and query optimization
- **Backward Compatibility**: Gradual migration path through compatibility layer

## Migration Strategies

### 1. Immediate Compatibility (Recommended)

Use the legacy compatibility layer for immediate upgrade with minimal code changes:

```go
// Before (GORP v2)
import "github.com/go-gorp/gorp"

dbmap := &gorp.DbMap{Db: db, Dialect: dialect}
dbmap.AddTable(User{}).SetKeys(true, "Id")

user := &User{Name: "John"}
err := dbmap.Insert(user)

// After (GORP v3 with compatibility layer)
import "github.com/go-gorp/gorp/v3/legacy"

dbmap := legacy.NewLegacyDbMap(db, dialect, nil)
dbmap.AddTable(User{}).SetKeys(true, "Id")

user := &User{Name: "John"}
err := dbmap.Insert(user) // Same API, modern backend
```

### 2. Gradual Migration

Enable modern features incrementally:

```go
import "github.com/go-gorp/gorp/v3/legacy"

config := &legacy.CompatibilityConfig{
    Mode:            legacy.MigrationMode,
    EnableWarnings:  true,
    LogMigrations:   true,
    FeatureFlags: map[string]bool{
        "enable_context_aware": true,  // Start with context support
        "enable_sqlx":         false, // Enable later
        "enable_generics":     false, // Enable last
    },
}

dbmap := legacy.NewLegacyDbMap(db, dialect, config)
// Your existing code works, but you get migration suggestions
```

### 3. Full Migration to Modern API

For new code or complete refactoring:

```go
import (
    "context"
    "github.com/go-gorp/gorp/v3/db"
    "github.com/go-gorp/gorp/v3/mapping"
    "github.com/go-gorp/gorp/v3/query"
)

// Modern connection management
connMgr, err := db.NewConnectionManager().
    WithPostgreSQL(database, db.ConnectionConfig{}).
    Build()

// Type-safe table mapping
mapper := mapping.NewMapper[User]()
userTable := mapper.RegisterTable[User]().
    WithName("users").
    WithPrimaryKey("id")

// Type-safe query building
builder := query.NewBuilder[User]()

// All operations are context-aware and type-safe
ctx := context.Background()
user, err := builder.Get[User](ctx, 123)
users, err := builder.Query[User](ctx, "SELECT * FROM users WHERE active = ?", true)
```

## Detailed Migration Examples

### Database Connection Setup

#### Before (GORP v2)
```go
import (
    "database/sql"
    "github.com/go-gorp/gorp"
    _ "github.com/lib/pq"
)

db, err := sql.Open("postgres", "user=test dbname=test sslmode=disable")
if err != nil {
    panic(err)
}

dialect := gorp.PostgresDialect{}
dbmap := &gorp.DbMap{Db: db, Dialect: dialect}
```

#### After (GORP v3 - Compatibility)
```go
import (
    "database/sql"
    "github.com/go-gorp/gorp/v3/legacy"
    _ "github.com/lib/pq"
)

db, err := sql.Open("postgres", "user=test dbname=test sslmode=disable")
if err != nil {
    panic(err)
}

dialect := gorp.PostgresDialect{}
dbmap := legacy.NewLegacyDbMap(db, dialect, nil)
```

#### After (GORP v3 - Modern)
```go
import (
    "context"
    "github.com/go-gorp/gorp/v3/db"
    "github.com/jackc/pgx/v5/pgxpool"
)

// Native pgx connection (better performance)
config, err := pgxpool.ParseConfig("user=test dbname=test sslmode=disable")
if err != nil {
    panic(err)
}

pool, err := pgxpool.New(context.Background(), config.ConnString())
if err != nil {
    panic(err)
}

connMgr, err := db.NewConnectionManager().
    WithPostgreSQLPool(pool, db.ConnectionConfig{
        Role: db.ReadWriteRole,
    }).
    Build()
```

### Table Mapping

#### Before (GORP v2)
```go
type User struct {
    Id       int64     `db:"id"`
    Name     string    `db:"name"`
    Email    string    `db:"email"`
    Created  time.Time `db:"created_at"`
}

table := dbmap.AddTable(User{}).SetKeys(true, "Id")
table.ColMap("Created").SetNotNull(true)
```

#### After (GORP v3 - Compatibility)
```go
// Same code works! Internal implementation upgraded
table := dbmap.AddTable(User{}).SetKeys(true, "Id")
table.ColMap("Created").SetNotNull(true)
```

#### After (GORP v3 - Modern)
```go
import "github.com/go-gorp/gorp/v3/mapping"

type User struct {
    Id       int64     `db:"id" gorp:"primary_key,auto_increment"`
    Name     string    `db:"name" gorp:"not_null"`
    Email    string    `db:"email" gorp:"unique"`
    Created  time.Time `db:"created_at" gorp:"not_null"`
}

mapper := mapping.NewMapper[User]()
userTable := mapper.RegisterTable[User]().
    WithName("users").
    WithPrimaryKey("id").
    WithIndex("idx_email", "email").
    WithUniqueIndex("uniq_email", "email")
```

### CRUD Operations

#### Before (GORP v2)
```go
// Insert
user := &User{Name: "John", Email: "john@example.com"}
err := dbmap.Insert(user)

// Get
obj, err := dbmap.Get(User{}, user.Id)
if err != nil {
    return err
}
foundUser := obj.(*User)

// Update
user.Name = "John Doe"
count, err := dbmap.Update(user)

// Delete
count, err := dbmap.Delete(user)

// Query
var users []User
_, err = dbmap.Select(&users, "SELECT * FROM users WHERE active = ?", true)
```

#### After (GORP v3 - Compatibility)
```go
// Exact same code works! But you get performance benefits and warnings
user := &User{Name: "John", Email: "john@example.com"}
err := dbmap.Insert(user)

obj, err := dbmap.Get(User{}, user.Id)
foundUser := obj.(*User)

user.Name = "John Doe"
count, err := dbmap.Update(user)

count, err := dbmap.Delete(user)

var users []User
_, err = dbmap.Select(&users, "SELECT * FROM users WHERE active = ?", true)
```

#### After (GORP v3 - Modern)
```go
import (
    "context"
    "github.com/go-gorp/gorp/v3/query"
)

builder := query.NewBuilder[User]()
ctx := context.Background()

// Insert - type safe, no casting needed
user := &User{Name: "John", Email: "john@example.com"}
err := builder.Insert[User](ctx, user)

// Get - returns typed result directly
foundUser, err := builder.Get[User](ctx, user.Id)
if err != nil {
    return err
}

// Update - type safe
user.Name = "John Doe"
count, err := builder.Update[User](ctx, user)

// Delete - type safe
count, err := builder.Delete[User](ctx, user)

// Query - type safe results
users, err := builder.Query[User](ctx, "SELECT * FROM users WHERE active = ?", true)

// Bulk operations (new feature)
newUsers := []*User{
    {Name: "Alice", Email: "alice@example.com"},
    {Name: "Bob", Email: "bob@example.com"},
}
err = builder.BulkInsert[User](ctx, newUsers)
```

### Transactions

#### Before (GORP v2)
```go
tx, err := dbmap.Begin()
if err != nil {
    return err
}
defer tx.Rollback()

err = tx.Insert(user1)
if err != nil {
    return err
}

err = tx.Insert(user2)
if err != nil {
    return err
}

return tx.Commit()
```

#### After (GORP v3 - Compatibility)
```go
// Same code works!
tx, err := dbmap.Begin()
if err != nil {
    return err
}
defer tx.Rollback()

err = tx.Insert(user1)
if err != nil {
    return err
}

err = tx.Insert(user2)
if err != nil {
    return err
}

return tx.Commit()
```

#### After (GORP v3 - Modern)
```go
// Context-aware transactions with automatic rollback
ctx := context.Background()
conn := connMgr.GetWriteConnection()

err := conn.WithTransaction(ctx, func(tx db.Transaction) error {
    builder := query.NewBuilder[User]().WithConnection(tx)

    err := builder.Insert[User](ctx, user1)
    if err != nil {
        return err // Automatic rollback
    }

    err = builder.Insert[User](ctx, user2)
    if err != nil {
        return err // Automatic rollback
    }

    return nil // Automatic commit
})
```

### Custom Queries

#### Before (GORP v2)
```go
// Single value
count, err := dbmap.SelectInt("SELECT COUNT(*) FROM users")

name, err := dbmap.SelectStr("SELECT name FROM users WHERE id = ?", userId)

// Single row
var user User
err = dbmap.SelectOne(&user, "SELECT * FROM users WHERE email = ?", email)

// Multiple rows with custom query
var results []struct {
    Name  string `db:"name"`
    Count int    `db:"count"`
}
_, err = dbmap.Select(&results, `
    SELECT name, COUNT(*) as count
    FROM users
    GROUP BY name
    ORDER BY count DESC
`)
```

#### After (GORP v3 - Compatibility)
```go
// Same code works!
count, err := dbmap.SelectInt("SELECT COUNT(*) FROM users")
name, err := dbmap.SelectStr("SELECT name FROM users WHERE id = ?", userId)

var user User
err = dbmap.SelectOne(&user, "SELECT * FROM users WHERE email = ?", email)

var results []struct {
    Name  string `db:"name"`
    Count int    `db:"count"`
}
_, err = dbmap.Select(&results, `
    SELECT name, COUNT(*) as count
    FROM users
    GROUP BY name
    ORDER BY count DESC
`)
```

#### After (GORP v3 - Modern)
```go
builder := query.NewBuilder[any]() // Generic builder for custom queries
ctx := context.Background()

// Single values - type safe
count, err := builder.QuerySingle[int64](ctx, "SELECT COUNT(*) FROM users")
name, err := builder.QuerySingle[string](ctx, "SELECT name FROM users WHERE id = ?", userId)

// Single row - type safe
user, err := builder.QueryOne[User](ctx, "SELECT * FROM users WHERE email = ?", email)

// Custom result types - type safe
type UserStats struct {
    Name  string `db:"name"`
    Count int    `db:"count"`
}

results, err := builder.Query[UserStats](ctx, `
    SELECT name, COUNT(*) as count
    FROM users
    GROUP BY name
    ORDER BY count DESC
`)

// Advanced: Fluent query builder (new feature)
fluentBuilder := query.NewFluentBuilder[User]()
users, err := fluentBuilder.
    Select("name", "email").
    Where("active = ?", true).
    Where("created_at > ?", time.Now().AddDate(0, 0, -30)).
    OrderBy("name").
    Limit(10).
    Execute(ctx)
```

## Advanced Features in GORP v3

### Database-Specific Features

#### PostgreSQL with native pgx
```go
// JSON/JSONB support
type UserProfile struct {
    ID       int64                    `db:"id"`
    Settings db.JSONB[map[string]any] `db:"settings"`
    Tags     db.PostgreSQLArray[string] `db:"tags"`
}

// Bulk operations with COPY protocol
users := []*User{ /* ... */ }
count, err := postgresDialect.BulkInsert(ctx, "users", users)

// Listen/Notify
conn, err := postgresDialect.Listen(ctx, "user_updates")
defer conn.Close()

for {
    notification, err := conn.WaitForNotification(ctx)
    if err != nil {
        break
    }
    log.Printf("Received: %s", notification.Payload)
}
```

#### MySQL optimizations
```go
// JSON columns
type Product struct {
    ID       int64                     `db:"id"`
    Metadata db.JSONColumn[ProductMeta] `db:"metadata"`
}

// Bulk upsert with ON DUPLICATE KEY UPDATE
products := []*Product{ /* ... */ }
count, err := mysqlDialect.BulkUpsert(ctx, "products", products, []string{"sku"})

// Generated columns (MySQL 5.7+)
type User struct {
    FirstName string `db:"first_name"`
    LastName  string `db:"last_name"`
    FullName  string `db:"full_name" gorp:"generated,CONCAT(first_name, ' ', last_name)"`
}
```

#### SQLite optimizations
```go
// Pragma management
config := &db.SQLitePragmaConfig{
    JournalMode:    "WAL",
    Synchronous:    "NORMAL",
    CacheSize:      -2000, // 2MB
    TempStore:      "memory",
    MMapSize:       268435456, // 256MB
}

err := sqliteDialect.ApplyPragmas(ctx, config)

// Vacuum and analyze
err = sqliteDialect.VacuumAnalyze(ctx, "users")
```

### Connection Management

#### Read/Write Splitting
```go
connMgr, err := db.NewConnectionManager().
    WithPostgreSQL(primaryDB, db.ConnectionConfig{Role: db.WriteRole}).
    WithPostgreSQL(replicaDB1, db.ConnectionConfig{Role: db.ReadRole}).
    WithPostgreSQL(replicaDB2, db.ConnectionConfig{Role: db.ReadRole}).
    Build()

// Reads automatically use replica connections
users, err := builder.Query[User](ctx, "SELECT * FROM users")

// Writes use primary connection
err = builder.Insert[User](ctx, newUser)
```

#### Health Monitoring
```go
healthChecker := db.NewHealthChecker(connMgr)

status, err := healthChecker.CheckHealth(ctx)
if err != nil {
    log.Printf("Database unhealthy: %v", err)
} else {
    log.Printf("Database status: %+v", status)
}
```

### Query Optimization

#### Query Plans and Analysis
```go
plan, err := postgresDialect.ExplainQuery(ctx,
    "SELECT * FROM users WHERE created_at > ?",
    time.Now().AddDate(0, 0, -30))

if plan.TotalCost > 1000 {
    log.Printf("Expensive query detected: cost=%.2f", plan.TotalCost)
}
```

#### Prepared Statements
```go
// Automatic statement caching
builder := query.NewBuilder[User]().WithStatementCaching(true)

// Statements are automatically prepared and cached
for i := 0; i < 1000; i++ {
    user, err := builder.Get[User](ctx, int64(i))
    // First call prepares, subsequent calls reuse
}
```

## Migration Tools

### Automatic Code Transformation

GORP v3 includes tools to help transform your existing code:

```bash
# Install migration tool
go install github.com/go-gorp/gorp/v3/cmd/gorp-migrate@latest

# Transform a single file
gorp-migrate transform --file=main.go --output=main_v3.go

# Transform entire project
gorp-migrate transform --dir=./src --output-dir=./src_v3

# Generate migration report
gorp-migrate analyze --dir=./src --report=migration-report.md
```

### Configuration File

Create `gorp-migrate.yaml` for consistent transformations:

```yaml
transformation:
  target_go_version: "1.24"
  enable_generics: true
  enable_sqlx: true
  enable_context_aware: true
  add_imports: true
  add_migration_comments: true
  preserve_comments: true

features:
  enable_bulk_operations: true
  enable_health_checks: true
  enable_query_optimization: true
  enable_connection_pooling: true

output:
  generate_report: true
  backup_original: true
  validate_syntax: true
```

### Migration Checklist

1. **Preparation**
   - [ ] Backup your existing code
   - [ ] Update to Go 1.24 or later
   - [ ] Review database driver compatibility
   - [ ] Plan migration strategy (immediate vs. gradual)

2. **Code Transformation**
   - [ ] Run migration analysis tool
   - [ ] Transform code using automated tools
   - [ ] Review generated warnings and suggestions
   - [ ] Update imports and dependencies

3. **Testing**
   - [ ] Run existing tests with compatibility layer
   - [ ] Gradually enable modern features
   - [ ] Test database operations thoroughly
   - [ ] Verify performance improvements

4. **Deployment**
   - [ ] Deploy with compatibility mode first
   - [ ] Monitor for deprecation warnings
   - [ ] Gradually enable advanced features
   - [ ] Update documentation and examples

## Troubleshooting

### Common Issues

#### Import Path Changes
```go
// Old
import "github.com/go-gorp/gorp"

// New - Compatibility
import "github.com/go-gorp/gorp/v3/legacy"

// New - Modern
import (
    "github.com/go-gorp/gorp/v3/db"
    "github.com/go-gorp/gorp/v3/mapping"
    "github.com/go-gorp/gorp/v3/query"
)
```

#### Type Assertions No Longer Needed
```go
// Old - required type assertion
obj, err := dbmap.Get(User{}, id)
user := obj.(*User)

// New - direct typed result
user, err := builder.Get[User](ctx, id)
```

#### Context Requirements
```go
// Old - no context
users, err := dbmap.Select(&users, query)

// New - context required
users, err := builder.Query[User](ctx, query)
```

### Performance Tuning

#### Connection Pool Configuration
```go
config := &db.ConnectionConfig{
    MaxOpenConnections: 25,
    MaxIdleConnections: 10,
    ConnMaxLifetime:    time.Hour,
    ConnMaxIdleTime:    30 * time.Minute,
}
```

#### Bulk Operations
```go
// Instead of individual inserts
for _, user := range users {
    err := builder.Insert[User](ctx, user) // Slow
}

// Use bulk operations
err := builder.BulkInsert[User](ctx, users) // Fast
```

## Getting Help

- **Documentation**: [https://pkg.go.dev/github.com/go-gorp/gorp/v3](https://pkg.go.dev/github.com/go-gorp/gorp/v3)
- **Examples**: [https://github.com/go-gorp/gorp/tree/v3/examples](https://github.com/go-gorp/gorp/tree/v3/examples)
- **Issues**: [https://github.com/go-gorp/gorp/issues](https://github.com/go-gorp/gorp/issues)
- **Discussions**: [https://github.com/go-gorp/gorp/discussions](https://github.com/go-gorp/gorp/discussions)

## What's Next

After completing your migration:

1. **Explore Advanced Features**: Try bulk operations, query optimization, and database-specific features
2. **Monitor Performance**: Use built-in health checks and query analysis tools
3. **Contribute**: Share your migration experience and help improve the tools
4. **Stay Updated**: Follow the project for new features and performance improvements

The migration to GORP v3 brings significant improvements in type safety, performance, and maintainability. The compatibility layer ensures you can upgrade incrementally, while the modern API provides powerful new capabilities for high-performance applications.