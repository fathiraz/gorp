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

// Implementation of Dialect for Snowflake databases.
// Tested with driver: github.com/snowflakedb/gosnowflake
type SnowflakeDialect struct {
	suffix          string
	LowercaseFields bool
}

// Type returns the dialect type
func (d SnowflakeDialect) Type() DialectType {
	return Snowflake
}

// QuerySuffix adds a Suffix to any query, usually ";"
func (d SnowflakeDialect) QuerySuffix() string { return ";" }

// ToSqlType returns the SQL column type to use when creating a
// table of the given Go Type. maxsize can be used to switch based on
// size. For example, in Snowflake []byte maps to binary
func (d SnowflakeDialect) ToSqlType(val reflect.Type, opts ColumnOptions) string {
	switch val.Kind() {
	case reflect.Ptr:
		return d.ToSqlType(val.Elem(), opts)
	case reflect.Bool:
		return "boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		if opts.IsAutoIncr {
			return "autoincrement"
		}
		return "integer"
	case reflect.Int64, reflect.Uint64:
		if opts.IsAutoIncr {
			return "autoincrement"
		}
		return "bigint"
	case reflect.Float64:
		return "double precision"
	case reflect.Float32:
		return "real"
	case reflect.Slice:
		if val.Elem().Kind() == reflect.Uint8 {
			return "binary"
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

// Returns empty string since Snowflake uses AUTOINCREMENT keyword
func (d SnowflakeDialect) AutoIncrStr() string {
	return ""
}

// Returns "default"
func (d SnowflakeDialect) AutoIncrBindValue() string {
	return "default"
}

// Returns empty string
func (d SnowflakeDialect) AutoIncrInsertSuffix(col *ColumnMap) string {
	return ""
}

// Returns Suffix
func (d SnowflakeDialect) CreateTableSuffix() string {
	return d.suffix
}

// Returns empty string
func (d SnowflakeDialect) CreateIndexSuffix() string {
	return ""
}

// Returns empty string
func (d SnowflakeDialect) DropIndexSuffix() string {
	return ""
}

// Returns "truncate"
func (d SnowflakeDialect) TruncateClause() string {
	return "truncate"
}

// Returns call system$wait(s)
func (d SnowflakeDialect) SleepClause(s time.Duration) string {
	return fmt.Sprintf("call system$wait(%d)", s.Milliseconds())
}

// Returns "?"
func (d SnowflakeDialect) BindVar(i int) string {
	return "?"
}

// Handles auto-increment values for Snowflake
func (d SnowflakeDialect) InsertAutoIncrToTarget(exec SqlExecutor, insertSql string, target interface{}, params ...interface{}) error {
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
func (d SnowflakeDialect) QuoteField(f string) string {
	if d.LowercaseFields {
		return `"` + strings.ToLower(f) + `"`
	}
	return `"` + f + `"`
}

// Returns quoted table name with schema support
func (d SnowflakeDialect) QuotedTableForQuery(schema string, table string) string {
	if strings.TrimSpace(schema) == "" {
		return d.QuoteField(table)
	}
	return schema + "." + d.QuoteField(table)
}

// Returns "if not exists"
func (d SnowflakeDialect) IfSchemaNotExists(command, schema string) string {
	return fmt.Sprintf("%s if not exists", command)
}

// Returns "if exists"
func (d SnowflakeDialect) IfTableExists(command, schema, table string) string {
	return fmt.Sprintf("%s if exists", command)
}

// Returns "if not exists"
func (d SnowflakeDialect) IfTableNotExists(command, schema, table string) string {
	return fmt.Sprintf("%s if not exists", command)
}
