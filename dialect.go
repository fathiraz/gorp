// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package gorp

import (
	"fmt"
	"reflect"
)

// DialectType represents the type of database dialect
type DialectType string

const (
	// MySQL dialect
	MySQL DialectType = "mysql"
	// PostgreSQL dialect
	PostgreSQL DialectType = "postgres"
	// SQLite dialect
	SQLite DialectType = "sqlite3"
	// Oracle dialect
	Oracle DialectType = "oracle"
	// SQLServer dialect
	SQLServer DialectType = "sqlserver"
	// Snowflake dialect
	Snowflake DialectType = "snowflake"
)

// ColumnOptions represents options for column creation
type ColumnOptions struct {
	// MaxSize is the maximum size of the column (e.g., VARCHAR(MaxSize))
	MaxSize int
	// IsAutoIncr indicates if the column is auto-incrementing
	IsAutoIncr bool
	// IsPK indicates if the column is a primary key
	IsPK bool
	// IsNullable indicates if the column can be NULL
	IsNullable bool
	// DefaultValue specifies the default value for the column
	DefaultValue string
}

// IndexOptions represents options for index creation
type IndexOptions struct {
	// Name is the name of the index
	Name string
	// Columns are the columns to include in the index
	Columns []string
	// Type is the type of index (e.g., BTREE, HASH, GIN, GIST)
	Type IndexType
	// Unique indicates if this is a unique index
	Unique bool
	// Using specifies the index method
	Using string
	// Where specifies a partial index condition
	Where string
}

// The Dialect interface encapsulates behaviors that differ across
// SQL databases. At present the Dialect is only used by CreateTables()
// but this could change in the future
type Dialect interface {
	// Type returns the type of dialect
	Type() DialectType

	// QuerySuffix adds a Suffix to any query, usually ";"
	QuerySuffix() string

	// ToSqlType returns the SQL column type to use when creating a
	// table of the given Go Type. maxsize can be used to switch based on
	// size. For example, in MySQL []byte could map to BLOB, MEDIUMBLOB,
	// or LONGBLOB depending on the maxsize
	ToSqlType(val reflect.Type, opts ColumnOptions) string

	// AutoIncrStr returns string to append to primary key column definitions
	AutoIncrStr() string

	// AutoIncrBindValue returns string to bind autoincrement columns to.
	// Empty string will remove reference to those columns in the INSERT statement.
	AutoIncrBindValue() string

	// AutoIncrInsertSuffix returns Suffix for inserting auto-increment columns
	AutoIncrInsertSuffix(col *ColumnMap) string

	// CreateTableSuffix returns string to append to "create table" statement
	// for vendor specific table attributes
	CreateTableSuffix() string

	// CreateIndexSuffix returns string to append to "create index" statement
	CreateIndexSuffix() string

	// DropIndexSuffix returns string to append to "drop index" statement
	DropIndexSuffix() string

	// TruncateClause returns string to truncate tables
	TruncateClause() string

	// BindVar returns bind variable string to use when forming SQL statements
	// in many dbs it is "?", but Postgres appears to use $1
	//
	// i is a zero based index of the bind variable in this statement
	BindVar(i int) string

	// QuoteField handles quoting of a field name to ensure that it doesn't
	// raise any SQL parsing exceptions by using a reserved word as a field name.
	QuoteField(field string) string

	// QuotedTableForQuery handles building up of a schema.database string
	// that is compatible with the given dialect
	//
	// schema - The schema that <table> lives in
	// table - The table name
	QuotedTableForQuery(schema string, table string) string

	// IfSchemaNotExists returns existence clause for schema creation
	IfSchemaNotExists(command, schema string) string

	// IfTableExists returns existence clause for table operations
	IfTableExists(command, schema, table string) string

	// IfTableNotExists returns non-existence clause for table operations
	IfTableNotExists(command, schema, table string) string

	// Placeholder returns the placeholder for a column value
	Placeholder(i int) string

	// SupportsCascade returns whether the dialect supports CASCADE in DROP TABLE
	SupportsCascade() bool

	// SupportsMultipleSchema returns whether the dialect supports multiple schemas
	SupportsMultipleSchema() bool

	// SupportsLastInsertId returns whether the dialect supports LastInsertId
	SupportsLastInsertId() bool
}

// TypedDialect provides type-safe dialect operations
type TypedDialect[T any] struct {
	dialect Dialect
}

// NewTypedDialect creates a new TypedDialect for type T
func NewTypedDialect[T any](dialect Dialect) *TypedDialect[T] {
	return &TypedDialect[T]{dialect: dialect}
}

// ToColumnType returns the SQL column type for the given field
func (d *TypedDialect[T]) ToColumnType(field string, opts ColumnOptions) (string, error) {
	var zero T
	t := reflect.TypeOf(zero)
	f, ok := t.FieldByName(field)
	if !ok {
		return "", fmt.Errorf("field %s not found in type %s", field, t.Name())
	}
	return d.dialect.ToSqlType(f.Type, opts), nil
}

// IntegerAutoIncrInserter is implemented by dialects that can perform
// inserts with automatically incremented integer primary keys. If
// the dialect can handle automatic assignment of more than just
// integers, see TargetedAutoIncrInserter.
type IntegerAutoIncrInserter interface {
	InsertAutoIncr(exec SqlExecutor, insertSql string, params ...interface{}) (int64, error)
}

// TargetedAutoIncrInserter is implemented by dialects that can
// perform automatic assignment of any primary key type (i.e. strings
// for uuids, integers for serials, etc).
type TargetedAutoIncrInserter interface {
	// InsertAutoIncrToTarget runs an insert operation and assigns the
	// automatically generated primary key directly to the passed in
	// target. The target should be a pointer to the primary key
	// field of the value being inserted.
	InsertAutoIncrToTarget(exec SqlExecutor, insertSql string, target interface{}, params ...interface{}) error
}

// TargetQueryInserter is implemented by dialects that can perform
// assignment of integer primary key type by executing a query
// like "select sequence.currval from dual".
type TargetQueryInserter interface {
	// InsertQueryToTarget runs an insert operation and assigns the
	// automatically generated primary key retrieved by the query
	// extracted from the GeneratedIdQuery field of the id column.
	InsertQueryToTarget(exec SqlExecutor, insertSql, idSql string, target interface{}, params ...interface{}) error
}

// ValidateDialect checks if the given dialect type is valid
func ValidateDialect(d DialectType) error {
	switch d {
	case MySQL, PostgreSQL, SQLite, Oracle, SQLServer, Snowflake:
		return nil
	default:
		return &InvalidDialectError{Dialect: string(d)}
	}
}

// standardInsertAutoIncr provides a standard implementation of auto-increment insert
func standardInsertAutoIncr(exec SqlExecutor, insertSql string, params ...interface{}) (int64, error) {
	res, err := exec.Exec(insertSql, params...)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}
