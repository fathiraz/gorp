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

type PostgresDialect struct {
	suffix          string
	LowercaseFields bool
}

// Type returns the dialect type
func (d PostgresDialect) Type() DialectType {
	return PostgreSQL
}

// QuerySuffix adds a Suffix to any query, usually ";"
func (d PostgresDialect) QuerySuffix() string { return ";" }

// ToSqlType returns the SQL column type to use when creating a
// table of the given Go Type. maxsize can be used to switch based on
// size. For example, in PostgreSQL []byte maps to bytea
func (d PostgresDialect) ToSqlType(val reflect.Type, opts ColumnOptions) string {
	switch val.Kind() {
	case reflect.Ptr:
		return d.ToSqlType(val.Elem(), opts)
	case reflect.Bool:
		return "boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		if opts.IsAutoIncr {
			return "serial"
		}
		return "integer"
	case reflect.Int64, reflect.Uint64:
		if opts.IsAutoIncr {
			return "bigserial"
		}
		return "bigint"
	case reflect.Float64:
		return "double precision"
	case reflect.Float32:
		return "real"
	case reflect.Slice:
		if val.Elem().Kind() == reflect.Uint8 {
			return "bytea"
		}
	}

	switch val.Name() {
	case "NullInt64":
		return "bigint"
	case "NullFloat64":
		return "double precision"
	case "NullBool":
		return "boolean"
	case "Time", "NullTime":
		return "timestamp with time zone"
	}

	if opts.MaxSize > 0 {
		return fmt.Sprintf("varchar(%d)", opts.MaxSize)
	}
	return "text"
}

// Returns empty string
func (d PostgresDialect) AutoIncrStr() string {
	return ""
}

// Returns "default"
func (d PostgresDialect) AutoIncrBindValue() string {
	return "default"
}

// Returns " returning [columnname]"
func (d PostgresDialect) AutoIncrInsertSuffix(col *ColumnMap) string {
	return " returning " + d.QuoteField(col.ColumnName)
}

// Returns Suffix
func (d PostgresDialect) CreateTableSuffix() string {
	return d.suffix
}

// Returns "using"
func (d PostgresDialect) CreateIndexSuffix() string {
	return "using"
}

// Returns empty string
func (d PostgresDialect) DropIndexSuffix() string {
	return ""
}

// Returns "truncate"
func (d PostgresDialect) TruncateClause() string {
	return "truncate"
}

// Returns pg_sleep(s)
func (d PostgresDialect) SleepClause(s time.Duration) string {
	return fmt.Sprintf("pg_sleep(%f)", s.Seconds())
}

// Returns "$(i+1)"
func (d PostgresDialect) BindVar(i int) string {
	return fmt.Sprintf("$%d", i+1)
}

// InsertAutoIncrToTarget executes the insert statement and assigns the auto-generated id to the target pointer
func (d PostgresDialect) InsertAutoIncrToTarget(exec SqlExecutor, insertSql string, target interface{}, params ...interface{}) error {
	rows, err := exec.Query(insertSql, params...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		return fmt.Errorf("No serial value returned for insert: %s Encountered error: %s", insertSql, rows.Err())
	}
	if err := rows.Scan(target); err != nil {
		return err
	}
	if rows.Next() {
		return fmt.Errorf("more than two serial value returned for insert: %s", insertSql)
	}
	return rows.Err()
}

// Returns quoted field name based on LowercaseFields setting
func (d PostgresDialect) QuoteField(f string) string {
	if d.LowercaseFields {
		return `"` + strings.ToLower(f) + `"`
	}
	return `"` + f + `"`
}

// Returns quoted table name for query with optional schema
func (d PostgresDialect) QuotedTableForQuery(schema string, table string) string {
	if strings.TrimSpace(schema) == "" {
		return d.QuoteField(table)
	}
	return schema + "." + d.QuoteField(table)
}

// Returns "if not exists"
func (d PostgresDialect) IfSchemaNotExists(command, schema string) string {
	return fmt.Sprintf("%s if not exists", command)
}

// Returns "if exists"
func (d PostgresDialect) IfTableExists(command, schema, table string) string {
	return fmt.Sprintf("%s if exists", command)
}

// Returns "if not exists"
func (d PostgresDialect) IfTableNotExists(command, schema, table string) string {
	return fmt.Sprintf("%s if not exists", command)
}
