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

type SqlServerDialectSuite struct {
	suite.Suite
	dialect gorp.SqlServerDialect
}

func (s *SqlServerDialectSuite) SetupTest() {
	s.dialect = gorp.SqlServerDialect{
		Version: "2008",
	}
}

func TestSqlServerDialectSuite(t *testing.T) {
	suite.Run(t, new(SqlServerDialectSuite))
}

func (s *SqlServerDialectSuite) TestType() {
	s.Equal(gorp.SQLServer, s.dialect.Type())
}

func (s *SqlServerDialectSuite) TestQuerySuffix() {
	s.Equal(";", s.dialect.QuerySuffix())
}

func (s *SqlServerDialectSuite) TestToSqlType() {
	tests := []struct {
		name     string
		value    interface{}
		maxSize  int
		autoIncr bool
		expected string
	}{
		{"bool", true, 0, false, "bit"},
		{"int8", int8(1), 0, false, "tinyint"},
		{"uint8", uint8(1), 0, false, "smallint"},
		{"int16", int16(1), 0, false, "smallint"},
		{"uint16", uint16(1), 0, false, "int"},
		{"int32", int32(1), 0, false, "int"},
		{"int32 auto-increment", int32(1), 0, true, "int"},
		{"uint32", uint32(1), 0, false, "bigint"},
		{"int64", int64(1), 0, false, "bigint"},
		{"int64 auto-increment", int64(1), 0, true, "bigint"},
		{"uint64", uint64(1), 0, false, "numeric(20,0)"},
		{"float32", float32(1), 0, false, "float(24)"},
		{"float64", float64(1), 0, false, "float(53)"},
		{"[]uint8", []uint8{1}, 0, false, "varbinary"},
		{"NullInt64", sql.NullInt64{}, 0, false, "bigint"},
		{"NullFloat64", sql.NullFloat64{}, 0, false, "float(53)"},
		{"NullBool", sql.NullBool{}, 0, false, "bit"},
		{"Time", time.Time{}, 0, false, "datetime2"},
		{"default-size string", "", 0, false, "nvarchar(max)"},
		{"sized string", "", 50, false, "nvarchar(50)"},
		{"large string", "", 1024, false, "nvarchar(1024)"},
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
		s.Equal("int", sqlType)
	})
}

func (s *SqlServerDialectSuite) TestAutoIncrStr() {
	s.Equal("identity(0,1)", s.dialect.AutoIncrStr())
}

func (s *SqlServerDialectSuite) TestAutoIncrBindValue() {
	s.Equal("", s.dialect.AutoIncrBindValue())
}

func (s *SqlServerDialectSuite) TestAutoIncrInsertSuffix() {
	s.Equal("", s.dialect.AutoIncrInsertSuffix(nil))
}

func (s *SqlServerDialectSuite) TestCreateTableSuffix() {
	s.Equal(";", s.dialect.CreateTableSuffix())
}

func (s *SqlServerDialectSuite) TestCreateIndexSuffix() {
	s.Equal("", s.dialect.CreateIndexSuffix())
}

func (s *SqlServerDialectSuite) TestDropIndexSuffix() {
	s.Equal("", s.dialect.DropIndexSuffix())
}

func (s *SqlServerDialectSuite) TestTruncateClause() {
	s.Equal("truncate table", s.dialect.TruncateClause())
}

func (s *SqlServerDialectSuite) TestSleepClause() {
	s.Equal("waitfor delay '0:00:01.000'", s.dialect.SleepClause(1*time.Second))
	s.Equal("waitfor delay '0:00:00.100'", s.dialect.SleepClause(100*time.Millisecond))
}

func (s *SqlServerDialectSuite) TestBindVar() {
	s.Equal("?", s.dialect.BindVar(0))
	s.Equal("?", s.dialect.BindVar(1))
	s.Equal("?", s.dialect.BindVar(9))
}

func (s *SqlServerDialectSuite) TestQuoteField() {
	s.Equal("[foo]", s.dialect.QuoteField("foo"))
	s.Equal("[bar]", s.dialect.QuoteField("bar"))
	s.Equal("[mixed_case]", s.dialect.QuoteField("mixed_case"))
}

func (s *SqlServerDialectSuite) TestQuotedTableForQuery() {
	s.Run("using the default schema", func() {
		s.Equal("[foo]", s.dialect.QuotedTableForQuery("", "foo"))
	})

	s.Run("with a supplied schema", func() {
		s.Equal("[myschema].[bar]", s.dialect.QuotedTableForQuery("myschema", "bar"))
	})
}

func (s *SqlServerDialectSuite) TestIfSchemaNotExists() {
	s.Equal("if schema_id(N'myschema') is null CREATE",
		s.dialect.IfSchemaNotExists("CREATE", "myschema"))
}

func (s *SqlServerDialectSuite) TestIfTableExists() {
	s.Equal("if object_id('[myschema].[mytable]') is not null DROP",
		s.dialect.IfTableExists("DROP", "myschema", "mytable"))
}

func (s *SqlServerDialectSuite) TestIfTableNotExists() {
	s.Equal("if object_id('myschema.mytable') is null CREATE",
		s.dialect.IfTableNotExists("CREATE", "myschema", "mytable"))
}

func (s *SqlServerDialectSuite) TestEncoding() {
	s.Equal("2008", s.dialect.Version)
}
