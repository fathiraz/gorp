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

type SnowflakeDialectSuite struct {
	suite.Suite
	dialect gorp.SnowflakeDialect
}

func (s *SnowflakeDialectSuite) SetupTest() {
	s.dialect = gorp.SnowflakeDialect{
		LowercaseFields: false,
	}
}

func TestSnowflakeDialectSuite(t *testing.T) {
	suite.Run(t, new(SnowflakeDialectSuite))
}

func (s *SnowflakeDialectSuite) TestType() {
	s.Equal(gorp.Snowflake, s.dialect.Type())
}

func (s *SnowflakeDialectSuite) TestQuerySuffix() {
	s.Equal(";", s.dialect.QuerySuffix())
}

func (s *SnowflakeDialectSuite) TestToSqlType() {
	tests := []struct {
		name     string
		value    interface{}
		maxSize  int
		autoIncr bool
		expected string
	}{
		{"bool", true, 0, false, "boolean"},
		{"int8", int8(1), 0, false, "integer"},
		{"uint8", uint8(1), 0, false, "integer"},
		{"int16", int16(1), 0, false, "integer"},
		{"uint16", uint16(1), 0, false, "integer"},
		{"int32", int32(1), 0, false, "integer"},
		{"int (treated as int32)", int(1), 0, false, "integer"},
		{"uint32", uint32(1), 0, false, "integer"},
		{"uint (treated as uint32)", uint(1), 0, false, "integer"},
		{"int64", int64(1), 0, false, "bigint"},
		{"uint64", uint64(1), 0, false, "bigint"},
		{"float32", float32(1), 0, false, "real"},
		{"float64", float64(1), 0, false, "double precision"},
		{"[]uint8", []uint8{1}, 0, false, "binary"},
		{"NullInt64", sql.NullInt64{}, 0, false, "bigint"},
		{"NullFloat64", sql.NullFloat64{}, 0, false, "double precision"},
		{"NullBool", sql.NullBool{}, 0, false, "boolean"},
		{"Time", time.Time{}, 0, false, "timestamp with time zone"},
		{"default-size string", "", 0, false, "text"},
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
}

func (s *SnowflakeDialectSuite) TestAutoIncrStr() {
	s.Equal("", s.dialect.AutoIncrStr())
}

func (s *SnowflakeDialectSuite) TestAutoIncrBindValue() {
	s.Equal("default", s.dialect.AutoIncrBindValue())
}

func (s *SnowflakeDialectSuite) TestAutoIncrInsertSuffix() {
	s.Equal("", s.dialect.AutoIncrInsertSuffix(nil))
}

func (s *SnowflakeDialectSuite) TestCreateTableSuffix() {
	s.Equal("", s.dialect.CreateTableSuffix())
}

func (s *SnowflakeDialectSuite) TestCreateIndexSuffix() {
	s.Equal("", s.dialect.CreateIndexSuffix())
}

func (s *SnowflakeDialectSuite) TestDropIndexSuffix() {
	s.Equal("", s.dialect.DropIndexSuffix())
}

func (s *SnowflakeDialectSuite) TestTruncateClause() {
	s.Equal("truncate", s.dialect.TruncateClause())
}

func (s *SnowflakeDialectSuite) TestBindVar() {
	s.Equal("?", s.dialect.BindVar(0))
	s.Equal("?", s.dialect.BindVar(4))
}

func (s *SnowflakeDialectSuite) TestQuoteField() {
	s.Run("By default, case is preserved", func() {
		s.Equal(`"Foo"`, s.dialect.QuoteField("Foo"))
		s.Equal(`"bar"`, s.dialect.QuoteField("bar"))
	})

	s.Run("With LowercaseFields set to true", func() {
		s.dialect.LowercaseFields = true
		s.Equal(`"foo"`, s.dialect.QuoteField("Foo"))
	})
}

func (s *SnowflakeDialectSuite) TestQuotedTableForQuery() {
	s.Run("using the default schema", func() {
		s.Equal(`"foo"`, s.dialect.QuotedTableForQuery("", "foo"))
	})

	s.Run("with a supplied schema", func() {
		s.Equal(`foo."bar"`, s.dialect.QuotedTableForQuery("foo", "bar"))
	})
}

func (s *SnowflakeDialectSuite) TestIfSchemaNotExists() {
	s.Equal("foo if not exists", s.dialect.IfSchemaNotExists("foo", "bar"))
}

func (s *SnowflakeDialectSuite) TestIfTableExists() {
	s.Equal("foo if exists", s.dialect.IfTableExists("foo", "bar", "baz"))
}

func (s *SnowflakeDialectSuite) TestIfTableNotExists() {
	s.Equal("foo if not exists", s.dialect.IfTableNotExists("foo", "bar", "baz"))
}
