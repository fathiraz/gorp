package gorp_test

import (
	"testing"

	"github.com/go-gorp/gorp/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type IndexTestSuite struct {
	suite.Suite
	idx *gorp.IndexMap
}

func (s *IndexTestSuite) SetupTest() {
	s.idx = gorp.NewIndex("test_idx")
}

func (s *IndexTestSuite) TestNewIndex() {
	// Test basic index creation
	idx := gorp.NewIndex("test_idx")
	s.NotNil(idx)
	s.Equal("test_idx", idx.IndexName)
	s.False(idx.IsUnique())
	s.Empty(idx.GetColumns())
	s.Empty(idx.GetType())
}

func (s *IndexTestSuite) TestRename() {
	// Test renaming an index
	s.idx.Rename("new_idx")
	s.Equal("new_idx", s.idx.IndexName)
}

func (s *IndexTestSuite) TestSetUnique() {
	// Test setting unique flag
	s.idx.SetUnique(true)
	s.True(s.idx.IsUnique())

	s.idx.SetUnique(false)
	s.False(s.idx.IsUnique())
}

func (s *IndexTestSuite) TestSetType() {
	testCases := []struct {
		name      string
		indexType gorp.IndexType
	}{
		{"BTree", gorp.IndexTypeBTree},
		{"Hash", gorp.IndexTypeHash},
		{"GiST", gorp.IndexTypeGiST},
		{"GIN", gorp.IndexTypeGIN},
		{"SQLite", gorp.IndexTypeSQLite},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			s.idx.SetType(tc.indexType)
			s.Equal(tc.indexType, s.idx.GetType())
		})
	}
}

func (s *IndexTestSuite) TestAddColumns() {
	// Test adding single column
	s.idx.AddColumns("col1")
	s.Equal([]string{"col1"}, s.idx.GetColumns())

	// Test adding multiple columns
	s.idx = gorp.NewIndex("test_idx")
	s.idx.AddColumns("col1", "col2", "col3")
	s.Equal([]string{"col1", "col2", "col3"}, s.idx.GetColumns())

	// Test adding columns incrementally
	s.idx = gorp.NewIndex("test_idx")
	s.idx.AddColumns("col1").AddColumns("col2")
	s.Equal([]string{"col1", "col2"}, s.idx.GetColumns())
}

func (s *IndexTestSuite) TestString() {
	// Test string representation of a simple index
	s.idx.SetType(gorp.IndexTypeBTree).AddColumns("col1")
	s.Contains(s.idx.String(), "test_idx")
	s.Contains(s.idx.String(), "BTREE")
	s.Contains(s.idx.String(), "col1")

	// Test string representation of a unique multi-column index
	s.idx = gorp.NewIndex("test_idx")
	s.idx.SetType(gorp.IndexTypeHash).
		SetUnique(true).
		AddColumns("col1", "col2")
	str := s.idx.String()
	s.Contains(str, "test_idx")
	s.Contains(str, "HASH")
	s.Contains(str, "UNIQUE")
	s.Contains(str, "col1")
	s.Contains(str, "col2")
}

func (s *IndexTestSuite) TestValidate() {
	// Test validation of a valid index
	s.idx.SetType(gorp.IndexTypeBTree).AddColumns("col1")
	s.NoError(s.idx.Validate())

	// Test validation with no columns
	s.idx = gorp.NewIndex("test_idx")
	s.idx.SetType(gorp.IndexTypeBTree)
	s.Error(s.idx.Validate())

	// Test validation with empty index name
	s.idx = gorp.NewIndex("")
	s.idx.SetType(gorp.IndexTypeBTree).AddColumns("col1")
	s.Error(s.idx.Validate())
}

func (s *IndexTestSuite) TestFluentInterface() {
	// Test fluent interface chaining
	s.idx.SetType(gorp.IndexTypeBTree).
		SetUnique(true).
		AddColumns("col1", "col2").
		Rename("new_idx")

	s.Equal("new_idx", s.idx.IndexName)
	s.Equal(gorp.IndexTypeBTree, s.idx.GetType())
	s.True(s.idx.IsUnique())
	s.Equal([]string{"col1", "col2"}, s.idx.GetColumns())
}

func (s *IndexTestSuite) TestIndexTypeConstants() {
	// Verify index type constants
	s.Equal(gorp.IndexType("BTREE"), gorp.IndexTypeBTree)
	s.Equal(gorp.IndexType("HASH"), gorp.IndexTypeHash)
	s.Equal(gorp.IndexType("GIST"), gorp.IndexTypeGiST)
	s.Equal(gorp.IndexType("GIN"), gorp.IndexTypeGIN)
	s.Equal(gorp.IndexType(""), gorp.IndexTypeSQLite)
}

func TestIndexSuite(t *testing.T) {
	suite.Run(t, new(IndexTestSuite))
}

// Example test to demonstrate usage
func TestIndexExample(t *testing.T) {
	// Create a new index for email field
	idx := gorp.NewIndex("user_email_idx").
		SetType(gorp.IndexTypeBTree).
		SetUnique(true).
		AddColumns("email")

	// Verify the configuration
	assert.Equal(t, "user_email_idx", idx.IndexName)
	assert.Equal(t, gorp.IndexTypeBTree, idx.GetType())
	assert.True(t, idx.IsUnique())
	assert.Equal(t, []string{"email"}, idx.GetColumns())
	assert.NoError(t, idx.Validate())
}
