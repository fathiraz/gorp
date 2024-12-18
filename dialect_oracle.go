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

// Implementation of Dialect for Oracle databases.
type OracleDialect struct{}

// Type returns the dialect type
func (d OracleDialect) Type() DialectType {
	return Oracle
}

// QuerySuffix adds a Suffix to any query, usually empty for Oracle
func (d OracleDialect) QuerySuffix() string { return "" }

// ToSqlType returns the SQL column type to use when creating a
// table of the given Go Type. maxsize can be used to switch based on
// size. For example, in Oracle []byte maps to BLOB
func (d OracleDialect) ToSqlType(val reflect.Type, opts ColumnOptions) string {
	switch val.Kind() {
	case reflect.Ptr:
		return d.ToSqlType(val.Elem(), opts)
	case reflect.Bool:
		return "NUMBER(1)"
	case reflect.Int8:
		return "NUMBER(3)"
	case reflect.Int16:
		return "NUMBER(5)"
	case reflect.Int32:
		if opts.IsAutoIncr {
			return "NUMBER GENERATED BY DEFAULT AS IDENTITY"
		}
		return "NUMBER(10)"
	case reflect.Int, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if opts.IsAutoIncr {
			return "NUMBER GENERATED BY DEFAULT AS IDENTITY"
		}
		return "NUMBER(19)"
	case reflect.Float32:
		return "BINARY_FLOAT"
	case reflect.Float64:
		return "BINARY_DOUBLE"
	case reflect.Slice:
		if val.Elem().Kind() == reflect.Uint8 {
			return "BLOB"
		}
	}

	switch val.Name() {
	case "NullInt64":
		return "NUMBER(19)"
	case "NullFloat64":
		return "BINARY_DOUBLE"
	case "NullBool":
		return "NUMBER(1)"
	case "Time", "NullTime":
		return "TIMESTAMP WITH TIME ZONE"
	}

	if opts.MaxSize > 0 {
		return fmt.Sprintf("VARCHAR2(%d)", opts.MaxSize)
	}
	return "CLOB"
}

// Returns empty string since Oracle uses GENERATED BY DEFAULT AS IDENTITY
func (d OracleDialect) AutoIncrStr() string {
	return ""
}

// Returns "NULL"
func (d OracleDialect) AutoIncrBindValue() string {
	return "NULL"
}

// Returns empty string
func (d OracleDialect) AutoIncrInsertSuffix(col *ColumnMap) string {
	return ""
}

// Returns empty string
func (d OracleDialect) CreateTableSuffix() string {
	return ""
}

// Returns empty string
func (d OracleDialect) CreateIndexSuffix() string {
	return ""
}

// Returns empty string
func (d OracleDialect) DropIndexSuffix() string {
	return ""
}

// Returns "truncate"
func (d OracleDialect) TruncateClause() string {
	return "truncate"
}

// Returns dbms_lock.sleep(s)
func (d OracleDialect) SleepClause(s time.Duration) string {
	return fmt.Sprintf("dbms_lock.sleep(%d)", s.Milliseconds()/1000)
}

// Returns ":n" where n = i+1
func (d OracleDialect) BindVar(i int) string {
	return fmt.Sprintf(":%d", i+1)
}

// After executing the insert uses the ColMap IdQuery to get the generated id
func (d OracleDialect) InsertQueryToTarget(exec SqlExecutor, insertSql, idSql string, target interface{}, params ...interface{}) error {
	_, err := exec.Exec(insertSql, params...)
	if err != nil {
		return err
	}
	id, err := exec.SelectInt(idSql)
	if err != nil {
		return err
	}
	switch target.(type) {
	case *int64:
		*(target.(*int64)) = id
	case *int32:
		*(target.(*int32)) = int32(id)
	case int:
		*(target.(*int)) = int(id)
	default:
		return fmt.Errorf("Id field can be int, int32 or int64")
	}
	return nil
}

// Returns quoted field name in uppercase
func (d OracleDialect) QuoteField(f string) string {
	return `"` + strings.ToUpper(f) + `"`
}

// Returns quoted table name with schema support
func (d OracleDialect) QuotedTableForQuery(schema string, table string) string {
	if strings.TrimSpace(schema) == "" {
		return d.QuoteField(table)
	}
	return schema + "." + d.QuoteField(table)
}

// Returns "if not exists"
func (d OracleDialect) IfSchemaNotExists(command, schema string) string {
	return fmt.Sprintf("%s if not exists", command)
}

// Returns "if exists"
func (d OracleDialect) IfTableExists(command, schema, table string) string {
	return fmt.Sprintf("%s if exists", command)
}

// Returns "if not exists"
func (d OracleDialect) IfTableNotExists(command, schema, table string) string {
	return fmt.Sprintf("%s if not exists", command)
}
