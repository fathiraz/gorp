// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package gorp

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

// Implementation of Dialect for Microsoft SQL Server databases.
// Use gorp.SqlServerDialect{"2005"} for legacy datatypes.
// Tested with driver: github.com/denisenkom/go-mssqldb
type SqlServerDialect struct {
	// If set to "2005" legacy datatypes will be used
	Version string
}

// Type returns the dialect type
func (d SqlServerDialect) Type() DialectType {
	return SQLServer
}

// QuerySuffix adds a Suffix to any query, usually ";"
func (d SqlServerDialect) QuerySuffix() string { return ";" }

// ToSqlType returns the SQL column type to use when creating a
// table of the given Go Type. maxsize can be used to switch based on
// size. For example, in SQL Server []byte maps to varbinary
func (d SqlServerDialect) ToSqlType(val reflect.Type, opts ColumnOptions) string {
	switch val.Kind() {
	case reflect.Ptr:
		return d.ToSqlType(val.Elem(), opts)
	case reflect.Bool:
		return "bit"
	case reflect.Int8:
		return "tinyint"
	case reflect.Uint8:
		return "smallint"
	case reflect.Int16:
		return "smallint"
	case reflect.Uint16:
		return "int"
	case reflect.Int, reflect.Int32:
		return "int"
	case reflect.Uint, reflect.Uint32:
		return "bigint"
	case reflect.Int64:
		return "bigint"
	case reflect.Uint64:
		return "numeric(20,0)"
	case reflect.Float32:
		return "float(24)"
	case reflect.Float64:
		return "float(53)"
	case reflect.Slice:
		if val.Elem().Kind() == reflect.Uint8 {
			return "varbinary"
		}
	}

	switch val.Name() {
	case "NullInt64":
		return "bigint"
	case "NullFloat64":
		return "float(53)"
	case "NullBool":
		return "bit"
	case "NullTime", "Time":
		if d.Version == "2005" {
			return "datetime"
		}
		return "datetime2"
	}

	if opts.MaxSize < 1 {
		if d.Version == "2005" {
			return fmt.Sprintf("nvarchar(%d)", 255)
		}
		return "nvarchar(max)"
	}
	return fmt.Sprintf("nvarchar(%d)", opts.MaxSize)
}

// Returns "identity(0,1)"
func (d SqlServerDialect) AutoIncrStr() string {
	return "identity(0,1)"
}

// Returns empty string since SQL Server removes autoincrement columns from INSERT statements
func (d SqlServerDialect) AutoIncrBindValue() string {
	return ""
}

// Returns empty string
func (d SqlServerDialect) AutoIncrInsertSuffix(col *ColumnMap) string {
	return ""
}

// Returns ";"
func (d SqlServerDialect) CreateTableSuffix() string {
	return ";"
}

// Returns empty string
func (d SqlServerDialect) CreateIndexSuffix() string {
	return ""
}

// Returns empty string
func (d SqlServerDialect) DropIndexSuffix() string {
	return ""
}

// Returns "truncate table"
func (d SqlServerDialect) TruncateClause() string {
	return "truncate table"
}

// Returns waitfor delay 'time'
func (d SqlServerDialect) SleepClause(s time.Duration) string {
	return fmt.Sprintf("waitfor delay '%d:%02d:%02d.%03d'",
		int(s.Hours()),
		int(s.Minutes())%60,
		int(s.Seconds())%60,
		int(s.Milliseconds())%1000)
}

// Returns "?"
func (d SqlServerDialect) BindVar(i int) string {
	return "?"
}

// Handles auto-increment values for SQL Server
func (d SqlServerDialect) InsertAutoIncr(exec SqlExecutor, insertSql string, params ...interface{}) (int64, error) {
	return standardInsertAutoIncr(exec, insertSql, params...)
}

// Returns quoted field name with SQL Server-style escaping
func (d SqlServerDialect) QuoteField(f string) string {
	return "[" + strings.Replace(f, "]", "]]", -1) + "]"
}

// Returns quoted table name with schema support
func (d SqlServerDialect) QuotedTableForQuery(schema string, table string) string {
	if strings.TrimSpace(schema) == "" {
		return d.QuoteField(table)
	}
	return d.QuoteField(schema) + "." + d.QuoteField(table)
}

// Returns SQL Server-specific schema existence check
func (d SqlServerDialect) IfSchemaNotExists(command, schema string) string {
	return fmt.Sprintf("if schema_id(N'%s') is null %s", schema, command)
}

// Returns SQL Server-specific table existence check
func (d SqlServerDialect) IfTableExists(command, schema, table string) string {
	var schema_clause string
	if strings.TrimSpace(schema) != "" {
		schema_clause = fmt.Sprintf("%s.", d.QuoteField(schema))
	}
	return fmt.Sprintf("if object_id('%s%s') is not null %s",
		schema_clause, d.QuoteField(table), command)
}

// Returns SQL Server-specific table non-existence check
func (d SqlServerDialect) IfTableNotExists(command, schema, table string) string {
	var schema_clause string
	if strings.TrimSpace(schema) != "" {
		schema_clause = fmt.Sprintf("%s.", schema)
	}
	return fmt.Sprintf("if object_id('%s%s') is null %s",
		schema_clause, table, command)
}
