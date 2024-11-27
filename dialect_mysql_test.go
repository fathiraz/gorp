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

type MySQLDialectSuite struct {
	suite.Suite
	dialect gorp.MySQLDialect
}

func (s *MySQLDialectSuite) SetupTest() {
	s.dialect = gorp.MySQLDialect{
		Engine:   "foo",
		Encoding: "bar",
	}
}

func TestMySQLDialectSuite(t *testing.T) {
	suite.Run(t, new(MySQLDialectSuite))
}

func (s *MySQLDialectSuite) TestType() {
	s.Equal(gorp.MySQL, s.dialect.Type())
}

func (s *MySQLDialectSuite) TestQuerySuffix() {
	s.Equal(";", s.dialect.QuerySuffix())
}

func (s *MySQLDialectSuite) TestToSqlType() {
	tests := []struct {
		name     string
		value    interface{}
		maxSize  int
		autoIncr bool
		expected string
	}{
		{"bool", true, 0, false, "boolean"},
		{"int8", int8(1), 0, false, "tinyint"},
		{"uint8", uint8(1), 0, false, "tinyint unsigned"},
		{"int16", int16(1), 0, false, "smallint"},
		{"uint16", uint16(1), 0, false, "smallint unsigned"},
		{"int32", int32(1), 0, false, "int"},
		{"int (treated as int32)", int(1), 0, false, "int"},
		{"uint32", uint32(1), 0, false, "int unsigned"},
		{"uint (treated as uint32)", uint(1), 0, false, "int unsigned"},
		{"int64", int64(1), 0, false, "bigint"},
		{"uint64", uint64(1), 0, false, "bigint unsigned"},
		{"float32", float32(1), 0, false, "double"},
		{"float64", float64(1), 0, false, "double"},
		{"[]uint8", []uint8{1}, 0, false, "mediumblob"},
		{"NullInt64", sql.NullInt64{}, 0, false, "bigint"},
		{"NullFloat64", sql.NullFloat64{}, 0, false, "double"},
		{"NullBool", sql.NullBool{}, 0, false, "tinyint"},
		{"Time", time.Time{}, 0, false, "datetime"},
		{"default-size string", "", 0, false, "varchar(255)"},
		{"sized string", "", 50, false, "varchar(50)"},
		{"large string", "", 1024, false, "text"},
	}
	for _, t := range tests {
		s.Run(t.name, func() {
			typ := reflect.TypeOf(t.value)
			sqlType := s.dialect.ToSqlType(typ, gorp.ColumnOptions{
				MaxSize:    t.maxSize,
				IsAutoIncr: t.autoIncr,
			})
			s.Assert().Equal(t.expected, sqlType)
		})
	}
}

func (s *MySQLDialectSuite) TestAutoIncrStr() {
	s.Assert().Equal("auto_increment", s.dialect.AutoIncrStr())
}

func (s *MySQLDialectSuite) TestAutoIncrBindValue() {
	s.Assert().Equal("null", s.dialect.AutoIncrBindValue())
}

func (s *MySQLDialectSuite) TestAutoIncrInsertSuffix() {
	s.Assert().Equal("", s.dialect.AutoIncrInsertSuffix(nil))
}

func (s *MySQLDialectSuite) TestCreateTableSuffix() {
	s.Run("with an empty engine", func() {
		dialect := gorp.MySQLDialect{
			Encoding: "utf8",
		}
		s.Panics(func() { dialect.CreateTableSuffix() })
	})

	s.Run("with an empty encoding", func() {
		dialect := gorp.MySQLDialect{
			Engine: "InnoDB",
		}
		s.Panics(func() { dialect.CreateTableSuffix() })
	})

	s.Run("with an engine and an encoding", func() {
		dialect := gorp.MySQLDialect{
			Engine:   "InnoDB",
			Encoding: "utf8",
		}
		s.Equal(" engine=InnoDB charset=utf8", dialect.CreateTableSuffix())
	})
}

func (s *MySQLDialectSuite) TestCreateIndexSuffix() {
	s.Assert().Equal("using", s.dialect.CreateIndexSuffix())
}

func (s *MySQLDialectSuite) TestDropIndexSuffix() {
	s.Assert().Equal("on", s.dialect.DropIndexSuffix())
}

func (s *MySQLDialectSuite) TestTruncateClause() {
	s.Assert().Equal("truncate", s.dialect.TruncateClause())
}

func (s *MySQLDialectSuite) TestSleepClause() {
	s.Assert().Equal("sleep(1.000000)", s.dialect.SleepClause(1*time.Second))
	s.Assert().Equal("sleep(0.100000)", s.dialect.SleepClause(100*time.Millisecond))
}

func (s *MySQLDialectSuite) TestBindVar() {
	s.Assert().Equal("?", s.dialect.BindVar(0))
}

func (s *MySQLDialectSuite) TestQuoteField() {
	s.Assert().Equal("`foo`", s.dialect.QuoteField("foo"))
}

func (s *MySQLDialectSuite) TestQuotedTableForQuery() {
	s.Run("using the default schema", func() {
		s.Assert().Equal("`foo`", s.dialect.QuotedTableForQuery("", "foo"))
	})

	s.Run("with a supplied schema", func() {
		s.Assert().Equal("foo.`bar`", s.dialect.QuotedTableForQuery("foo", "bar"))
	})
}

func (s *MySQLDialectSuite) TestIfSchemaNotExists() {
	s.Assert().Equal("foo IF NOT EXISTS", s.dialect.IfSchemaNotExists("foo", "bar"))
}

func (s *MySQLDialectSuite) TestIfTableExists() {
	s.Assert().Equal("foo IF EXISTS", s.dialect.IfTableExists("foo", "bar", "baz"))
}

func (s *MySQLDialectSuite) TestIfTableNotExists() {
	s.Assert().Equal("foo IF NOT EXISTS", s.dialect.IfTableNotExists("foo", "bar", "baz"))
}

func (s *MySQLDialectSuite) TestPlaceHolder() {
	s.Assert().Equal("?", s.dialect.Placeholder(0))
}

func (s *MySQLDialectSuite) TestSupportsLastInsertId() {
	s.Assert().True(s.dialect.SupportsLastInsertId())
}

func (s *MySQLDialectSuite) TestSupportsMultipleSchema() {
	s.Assert().True(s.dialect.SupportsMultipleSchema())
}

func (s *MySQLDialectSuite) TestSupportsCascade() {
	s.Assert().True(s.dialect.SupportsCascade())
}
