// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package gorp

import (
	"fmt"
	"reflect"
	"time"
)

type SqliteDialect struct {
	Suffix string
}

// Type returns the dialect type
func (d SqliteDialect) Type() DialectType {
	return SQLite
}

// QuerySuffix adds a Suffix to any query, usually ";"
func (d SqliteDialect) QuerySuffix() string { return ";" }

// ToSqlType returns the SQL column type to use when creating a
// table of the given Go Type. maxsize can be used to switch based on
// size. For example, in SQLite []byte maps to blob
func (d SqliteDialect) ToSqlType(val reflect.Type, opts ColumnOptions) string {
	switch val.Kind() {
	case reflect.Ptr:
		return d.ToSqlType(val.Elem(), opts)
	case reflect.Bool:
		return "integer"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "integer"
	case reflect.Float64, reflect.Float32:
		return "real"
	case reflect.Slice:
		if val.Elem().Kind() == reflect.Uint8 {
			return "blob"
		}
	}

	switch val.Name() {
	case "NullInt64":
		return "integer"
	case "NullFloat64":
		return "real"
	case "NullBool":
		return "integer"
	case "Time", "NullTime":
		return "datetime"
	}

	if opts.MaxSize < 1 {
		opts.MaxSize = 255
	}
	return fmt.Sprintf("varchar(%d)", opts.MaxSize)
}

// Returns "autoincrement"
func (d SqliteDialect) AutoIncrStr() string {
	return "autoincrement"
}

// Returns "null"
func (d SqliteDialect) AutoIncrBindValue() string {
	return "null"
}

// Returns empty string
func (d SqliteDialect) AutoIncrInsertSuffix(col *ColumnMap) string {
	return ""
}

// Returns Suffix
func (d SqliteDialect) CreateTableSuffix() string {
	return d.Suffix
}

// Returns empty string
func (d SqliteDialect) CreateIndexSuffix() string {
	return ""
}

// Returns empty string
func (d SqliteDialect) DropIndexSuffix() string {
	return ""
}

// Returns "delete from" since SQLite doesn't have a TRUNCATE statement,
// but DELETE FROM uses a truncate optimization:
// http://www.sqlite.org/lang_delete.html
func (d SqliteDialect) TruncateClause() string {
	return "delete from"
}

// Returns sleep(s)
func (d SqliteDialect) SleepClause(s time.Duration) string {
	ms := s.Milliseconds()
	return fmt.Sprintf("select sleep(%d)", ms)
}

// Returns "?"
func (d SqliteDialect) BindVar(i int) string {
	return "?"
}

// Handles auto-increment values for SQLite
func (d SqliteDialect) InsertAutoIncr(exec SqlExecutor, insertSql string, params ...interface{}) (int64, error) {
	return standardInsertAutoIncr(exec, insertSql, params...)
}

// Returns quoted field name
func (d SqliteDialect) QuoteField(f string) string {
	return `"` + f + `"`
}

// Returns quoted table name (SQLite doesn't support schemas)
func (d SqliteDialect) QuotedTableForQuery(schema string, table string) string {
	return d.QuoteField(table)
}

// Returns "if not exists"
func (d SqliteDialect) IfSchemaNotExists(command, schema string) string {
	return fmt.Sprintf("%s if not exists", command)
}

// Returns "if exists"
func (d SqliteDialect) IfTableExists(command, schema, table string) string {
	return fmt.Sprintf("%s if exists", command)
}

// Returns "if not exists"
func (d SqliteDialect) IfTableNotExists(command, schema, table string) string {
	return fmt.Sprintf("%s if not exists", command)
}
