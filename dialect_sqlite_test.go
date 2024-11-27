// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

//go:build !integration
// +build !integration

package gorp_test

import (
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/go-gorp/gorp/v3"
	"github.com/stretchr/testify/suite"
)

type SqliteDialectSuite struct {
	suite.Suite
	dialect gorp.SqliteDialect
}

func (s *SqliteDialectSuite) SetupTest() {
	s.dialect = gorp.SqliteDialect{}
}

func TestSqliteDialectSuite(t *testing.T) {
	suite.Run(t, new(SqliteDialectSuite))
}

func (s *SqliteDialectSuite) TestType() {
	s.Equal(gorp.SQLite, s.dialect.Type())
}

func (s *SqliteDialectSuite) TestQuerySuffix() {
	s.Equal(";", s.dialect.QuerySuffix())
}

func (s *SqliteDialectSuite) TestToSqlType() {
	tests := []struct {
		name     string
		value    interface{}
		maxSize  int
		autoIncr bool
		expected string
	}{
		{"bool", true, 0, false, "integer"},
		{"int8", int8(1), 0, false, "integer"},
		{"uint8", uint8(1), 0, false, "integer"},
		{"int16", int16(1), 0, false, "integer"},
		{"uint16", uint16(1), 0, false, "integer"},
		{"int32", int32(1), 0, false, "integer"},
		{"int (treated as int32)", int(1), 0, false, "integer"},
		{"uint32", uint32(1), 0, false, "integer"},
		{"uint (treated as uint32)", uint(1), 0, false, "integer"},
		{"int64", int64(1), 0, false, "integer"},
		{"uint64", uint64(1), 0, false, "integer"},
		{"float32", float32(1), 0, false, "real"},
		{"float64", float64(1), 0, false, "real"},
		{"[]uint8", []uint8{1}, 0, false, "blob"},
		{"NullInt64", sql.NullInt64{}, 0, false, "integer"},
		{"NullFloat64", sql.NullFloat64{}, 0, false, "real"},
		{"NullBool", sql.NullBool{}, 0, false, "integer"},
		{"Time", time.Time{}, 0, false, "datetime"},
		{"default-size string", "", 0, false, "varchar(255)"},
		{"sized string", "", 50, false, "varchar(50)"},
		{"large string", "", 1024, false, "varchar(1024)"},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			typ := reflect.TypeOf(tt.value)
			sqlType := s.dialect.ToSqlType(typ, gorp.ColumnOptions{
				MaxSize:    tt.maxSize,
				IsAutoIncr: tt.autoIncr,
			})
			s.Equal(tt.expected, sqlType)
		})
	}

	// Test pointer types
	s.Run("pointer types", func() {
		var i int
		sqlType := s.dialect.ToSqlType(reflect.TypeOf(&i), gorp.ColumnOptions{})
		s.Equal("integer", sqlType)
	})
}

func (s *SqliteDialectSuite) TestAutoIncrStr() {
	s.Equal("autoincrement", s.dialect.AutoIncrStr())
}

func (s *SqliteDialectSuite) TestAutoIncrBindValue() {
	s.Equal("null", s.dialect.AutoIncrBindValue())
}

func (s *SqliteDialectSuite) TestAutoIncrInsertSuffix() {
	s.Equal("", s.dialect.AutoIncrInsertSuffix(nil))
}

func (s *SqliteDialectSuite) TestCreateTableSuffix() {
	s.Equal("", s.dialect.CreateTableSuffix())

	// Test with custom Suffix
	dialectWithSuffix := gorp.SqliteDialect{Suffix: "WITHOUT ROWID"}
	s.Equal("WITHOUT ROWID", dialectWithSuffix.CreateTableSuffix())
}

func (s *SqliteDialectSuite) TestCreateIndexSuffix() {
	s.Equal("", s.dialect.CreateIndexSuffix())
}

func (s *SqliteDialectSuite) TestDropIndexSuffix() {
	s.Equal("", s.dialect.DropIndexSuffix())
}

func (s *SqliteDialectSuite) TestTruncateClause() {
	s.Equal("delete from", s.dialect.TruncateClause())
}

func (s *SqliteDialectSuite) TestSleepClause() {
	s.Equal("select sleep(1000)", s.dialect.SleepClause(1*time.Second))
	s.Equal("select sleep(100)", s.dialect.SleepClause(100*time.Millisecond))
}

func (s *SqliteDialectSuite) TestBindVar() {
	s.Equal("?", s.dialect.BindVar(0))
	s.Equal("?", s.dialect.BindVar(1))
}

func (s *SqliteDialectSuite) TestQuoteField() {
	s.Equal(`"field"`, s.dialect.QuoteField("field"))
}

func (s *SqliteDialectSuite) TestQuotedTableForQuery() {
	// SQLite doesn't support schemas, so schema parameter is ignored
	s.Equal(`"mytable"`, s.dialect.QuotedTableForQuery("myschema", "mytable"))
	s.Equal(`"table"`, s.dialect.QuotedTableForQuery("", "table"))
}

func (s *SqliteDialectSuite) TestIfSchemaNotExists() {
	s.Equal("CREATE if not exists", s.dialect.IfSchemaNotExists("CREATE", "myschema"))
}

func (s *SqliteDialectSuite) TestIfTableExists() {
	s.Equal("DROP if exists", s.dialect.IfTableExists("DROP", "myschema", "mytable"))
}

func (s *SqliteDialectSuite) TestIfTableNotExists() {
	s.Equal("CREATE if not exists", s.dialect.IfTableNotExists("CREATE", "myschema", "mytable"))
}
