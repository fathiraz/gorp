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

type PostgresDialectSuite struct {
	suite.Suite
	dialect gorp.PostgresDialect
}

func (s *PostgresDialectSuite) SetupTest() {
	s.dialect = gorp.PostgresDialect{
		LowercaseFields: false,
	}
}

func (s *PostgresDialectSuite) TestType() {
	s.Equal(gorp.PostgreSQL, s.dialect.Type())
}

func (s *PostgresDialectSuite) TestQuerySuffix() {
	s.Equal(";", s.dialect.QuerySuffix())
}

func TestPostgresDialectSuite(t *testing.T) {
	suite.Run(t, new(PostgresDialectSuite))
}

func (s *PostgresDialectSuite) TestToSqlType() {
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
		{"[]uint8", []uint8{1}, 0, false, "bytea"},
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

func (s *PostgresDialectSuite) TestAutoIncrStr() {
	s.Equal("", s.dialect.AutoIncrStr())
}

func (s *PostgresDialectSuite) TestAutoIncrBindValue() {
	s.Equal("default", s.dialect.AutoIncrBindValue())
}

func (s *PostgresDialectSuite) TestAutoIncrInsertSuffix() {
	cm := gorp.ColumnMap{
		ColumnName: "foo",
	}
	s.Equal(` returning "foo"`, s.dialect.AutoIncrInsertSuffix(&cm))
}

func (s *PostgresDialectSuite) TestCreateTableSuffix() {
	s.Equal("", s.dialect.CreateTableSuffix())
}

func (s *PostgresDialectSuite) TestCreateIndexSuffix() {
	s.Equal("using", s.dialect.CreateIndexSuffix())
}

func (s *PostgresDialectSuite) TestDropIndexSuffix() {
	s.Equal("", s.dialect.DropIndexSuffix())
}

func (s *PostgresDialectSuite) TestTruncateClause() {
	s.Equal("truncate", s.dialect.TruncateClause())
}

func (s *PostgresDialectSuite) TestSleepClause() {
	s.Equal("pg_sleep(1.000000)", s.dialect.SleepClause(1*time.Second))
	s.Equal("pg_sleep(0.100000)", s.dialect.SleepClause(100*time.Millisecond))
}

func (s *PostgresDialectSuite) TestBindVar() {
	s.Equal("$1", s.dialect.BindVar(0))
	s.Equal("$5", s.dialect.BindVar(4))
}

func (s *PostgresDialectSuite) TestQuoteField() {
	s.Run("By default, case is preserved", func() {
		s.Equal(`"Foo"`, s.dialect.QuoteField("Foo"))
		s.Equal(`"bar"`, s.dialect.QuoteField("bar"))
	})

	s.Run("With LowercaseFields set to true", func() {
		s.dialect.LowercaseFields = true
		s.Equal(`"foo"`, s.dialect.QuoteField("Foo"))
	})
}

func (s *PostgresDialectSuite) TestQuotedTableForQuery() {
	s.Run("using the default schema", func() {
		s.Equal(`"foo"`, s.dialect.QuotedTableForQuery("", "foo"))
	})

	s.Run("with a supplied schema", func() {
		s.Equal(`foo."bar"`, s.dialect.QuotedTableForQuery("foo", "bar"))
	})
}

func (s *PostgresDialectSuite) TestIfSchemaNotExists() {
	s.Equal("foo if not exists", s.dialect.IfSchemaNotExists("foo", "bar"))
}

func (s *PostgresDialectSuite) TestIfTableExists() {
	s.Equal("foo if exists", s.dialect.IfTableExists("foo", "bar", "baz"))
}

func (s *PostgresDialectSuite) TestIfTableNotExists() {
	s.Equal("foo if not exists", s.dialect.IfTableNotExists("foo", "bar", "baz"))
}
