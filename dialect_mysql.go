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

// Implementation of Dialect for MySQL databases.
type MySQLDialect struct {
	// Engine is the storage engine to use "InnoDB" vs "MyISAM" for example
	Engine string

	// Encoding is the character encoding to use for created tables
	Encoding string
}

// Type returns the dialect type
func (d MySQLDialect) Type() DialectType {
	return MySQL
}

// QuerySuffix adds a Suffix to any query, usually ";"
func (d MySQLDialect) QuerySuffix() string { return ";" }

// ToSqlType returns the SQL column type to use when creating a
// table of the given Go Type. maxsize can be used to switch based on
// size.  For example, in MySQL []byte could map to BLOB, MEDIUMBLOB,
// or LONGBLOB depending on the maxsize
func (d MySQLDialect) ToSqlType(val reflect.Type, opts ColumnOptions) string {
	switch val.Kind() {
	case reflect.Ptr:
		return d.ToSqlType(val.Elem(), opts)
	case reflect.Bool:
		return "boolean"
	case reflect.Int8:
		return "tinyint"
	case reflect.Uint8:
		return "tinyint unsigned"
	case reflect.Int16:
		return "smallint"
	case reflect.Uint16:
		return "smallint unsigned"
	case reflect.Int, reflect.Int32:
		return "int"
	case reflect.Uint, reflect.Uint32:
		return "int unsigned"
	case reflect.Int64:
		return "bigint"
	case reflect.Uint64:
		return "bigint unsigned"
	case reflect.Float64, reflect.Float32:
		return "double"
	case reflect.Slice:
		if val.Elem().Kind() == reflect.Uint8 {
			return "mediumblob"
		}
	}

	switch val.Name() {
	case "NullInt64":
		return "bigint"
	case "NullFloat64":
		return "double"
	case "NullBool":
		return "tinyint"
	case "Time":
		return "datetime"
	}

	if opts.MaxSize < 1 {
		opts.MaxSize = 255
	}

	/* == About varchar(N) ==
	 * N is number of characters.
	 * A varchar column can store up to 65535 bytes.
	 * Remember that 1 character is 3 bytes in utf-8 charset.
	 * Also remember that each row can store up to 65535 bytes,
	 * and you have some overheads, so it's not possible for a
	 * varchar column to have 65535/3 characters really.
	 * So it would be better to use 'text' type in stead of
	 * large varchar type.
	 */
	if opts.MaxSize < 256 {
		return fmt.Sprintf("varchar(%d)", opts.MaxSize)
	} else {
		return "text"
	}
}

// Returns auto_increment
func (d MySQLDialect) AutoIncrStr() string {
	return "auto_increment"
}

// Returns NULL
func (d MySQLDialect) AutoIncrBindValue() string {
	return "null"
}

// Returns ""
func (d MySQLDialect) AutoIncrInsertSuffix(col *ColumnMap) string {
	return ""
}

// Returns engine=%s charset=%s  based on values stored on struct
func (d MySQLDialect) CreateTableSuffix() string {
	if d.Engine == "" || d.Encoding == "" {
		msg := "gorp - undefined"

		if d.Engine == "" {
			msg += " MySQLDialect.Engine"
		}
		if d.Engine == "" && d.Encoding == "" {
			msg += ","
		}
		if d.Encoding == "" {
			msg += " MySQLDialect.Encoding"
		}
		msg += ". Check that your MySQLDialect was correctly initialized when declared."
		panic(msg)
	}

	return fmt.Sprintf(" engine=%s charset=%s", d.Engine, d.Encoding)
}

// Returns using
func (d MySQLDialect) CreateIndexSuffix() string {
	return "using"
}

// Returns on
func (d MySQLDialect) DropIndexSuffix() string {
	return "on"
}

// Returns truncate
func (d MySQLDialect) TruncateClause() string {
	return "truncate"
}

// Returns sleep(s)
func (d MySQLDialect) SleepClause(s time.Duration) string {
	return fmt.Sprintf("sleep(%f)", s.Seconds())
}

// Returns "?"
func (d MySQLDialect) BindVar(i int) string {
	return "?"
}

// Returns "insert into %s %s values %s"
func (d MySQLDialect) InsertAutoIncr(exec SqlExecutor, insertSql string, params ...interface{}) (int64, error) {
	return standardInsertAutoIncr(exec, insertSql, params...)
}

// Returns "`%s`"
func (d MySQLDialect) QuoteField(f string) string {
	return "`" + f + "`"
}

// Returns "`%s`.`%s`"
func (d MySQLDialect) QuotedTableForQuery(schema string, table string) string {
	if strings.TrimSpace(schema) == "" {
		return d.QuoteField(table)
	}

	return schema + "." + d.QuoteField(table)
}

// Returns "if not exists"
func (d MySQLDialect) IfSchemaNotExists(command, schema string) string {
	return fmt.Sprintf("%s IF NOT EXISTS", command)
}

// Returns "if exists"
func (d MySQLDialect) IfTableExists(command, schema, table string) string {
	return fmt.Sprintf("%s IF EXISTS", command)
}

// Returns "if not exists"
func (d MySQLDialect) IfTableNotExists(command, schema, table string) string {
	return fmt.Sprintf("%s IF NOT EXISTS", command)
}

// Placeholder returns the placeholder for a column value
func (d MySQLDialect) Placeholder(i int) string {
	return "?"
}

// SupportsCascade returns whether MySQL supports CASCADE in DROP TABLE
func (d MySQLDialect) SupportsCascade() bool {
	return true
}

// SupportsMultipleSchema returns whether MySQL supports multiple schemas
func (d MySQLDialect) SupportsMultipleSchema() bool {
	return true
}

// SupportsLastInsertId returns whether MySQL supports LastInsertId
func (d MySQLDialect) SupportsLastInsertId() bool {
	return true
}
