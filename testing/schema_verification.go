// Database schema verification tools for GORP testing
package testing

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/fathiraz/gorp/db"
	"github.com/fathiraz/gorp/mapping"
)

// SchemaVerifier provides database schema verification capabilities
type SchemaVerifier struct {
	connection db.Connection
	dbType     db.ConnectionType
}

// NewSchemaVerifier creates a new schema verifier
func NewSchemaVerifier(connection db.Connection) *SchemaVerifier {
	return &SchemaVerifier{
		connection: connection,
		dbType:     connection.Type(),
	}
}

// TableInfo represents database table metadata
type TableInfo struct {
	Name        string
	Schema      string
	Columns     []ColumnInfo
	Indexes     []IndexInfo
	Constraints []ConstraintInfo
	Triggers    []TriggerInfo
}

// ColumnInfo represents database column metadata
type ColumnInfo struct {
	Name         string
	DataType     string
	IsNullable   bool
	DefaultValue *string
	MaxLength    *int
	Precision    *int
	Scale        *int
	IsIdentity   bool
	IsPrimaryKey bool
}

// IndexInfo represents database index metadata
type IndexInfo struct {
	Name      string
	TableName string
	Columns   []string
	IsUnique  bool
	IsPrimary bool
	Type      string
}

// ConstraintInfo represents database constraint metadata
type ConstraintInfo struct {
	Name       string
	Type       string
	TableName  string
	Columns    []string
	RefTable   *string
	RefColumns []string
}

// TriggerInfo represents database trigger metadata
type TriggerInfo struct {
	Name      string
	TableName string
	Event     string
	Timing    string
	Body      string
}

// ExpectedSchema represents the expected database schema
type ExpectedSchema struct {
	Tables      []ExpectedTable
	Views       []ExpectedView
	Functions   []ExpectedFunction
	Procedures  []ExpectedProcedure
	Sequences   []ExpectedSequence
	Extensions  []ExpectedExtension
}

// ExpectedTable represents an expected table structure
type ExpectedTable struct {
	Name        string
	Schema      string
	Columns     []ExpectedColumn
	Indexes     []ExpectedIndex
	Constraints []ExpectedConstraint
	Triggers    []ExpectedTrigger
}

// ExpectedColumn represents an expected column structure
type ExpectedColumn struct {
	Name       string
	DataType   string
	Nullable   bool
	Default    *string
	MaxLength  *int
	Precision  *int
	Scale      *int
	IsIdentity bool
	IsPrimary  bool
}

// ExpectedIndex represents an expected index structure
type ExpectedIndex struct {
	Name     string
	Columns  []string
	IsUnique bool
	Type     string
}

// ExpectedConstraint represents an expected constraint
type ExpectedConstraint struct {
	Name       string
	Type       string
	Columns    []string
	RefTable   *string
	RefColumns []string
}

// ExpectedTrigger represents an expected trigger
type ExpectedTrigger struct {
	Name   string
	Event  string
	Timing string
	Body   string
}

// ExpectedView represents an expected view
type ExpectedView struct {
	Name       string
	Schema     string
	Definition string
}

// ExpectedFunction represents an expected function
type ExpectedFunction struct {
	Name       string
	Schema     string
	Parameters []string
	Returns    string
	Body       string
}

// ExpectedProcedure represents an expected stored procedure
type ExpectedProcedure struct {
	Name       string
	Schema     string
	Parameters []string
	Body       string
}

// ExpectedSequence represents an expected sequence
type ExpectedSequence struct {
	Name      string
	Schema    string
	StartWith int64
	Increment int64
	MinValue  *int64
	MaxValue  *int64
}

// ExpectedExtension represents an expected PostgreSQL extension
type ExpectedExtension struct {
	Name    string
	Version string
	Schema  string
}

// GetTableInfo retrieves table information from the database
func (sv *SchemaVerifier) GetTableInfo(ctx context.Context, tableName string, schema ...string) (*TableInfo, error) {
	schemaName := "public"
	if len(schema) > 0 {
		schemaName = schema[0]
	}

	switch sv.dbType {
	case db.PostgreSQLConnectionType:
		return sv.getPostgreSQLTableInfo(ctx, tableName, schemaName)
	case db.MySQLConnectionType:
		return sv.getMySQLTableInfo(ctx, tableName, schemaName)
	case db.SQLServerConnectionType:
		return sv.getSQLServerTableInfo(ctx, tableName, schemaName)
	case db.SQLiteConnectionType:
		return sv.getSQLiteTableInfo(ctx, tableName)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", sv.dbType)
	}
}

// VerifyTable verifies that a table matches the expected structure
func (sv *SchemaVerifier) VerifyTable(t *testing.T, expected ExpectedTable) {
	t.Helper()

	ctx := context.Background()
	actual, err := sv.GetTableInfo(ctx, expected.Name, expected.Schema)
	require.NoError(t, err, "Failed to get table info for %s", expected.Name)

	// Verify table exists
	assert.NotNil(t, actual, "Table %s should exist", expected.Name)

	// Verify columns
	sv.verifyColumns(t, expected.Name, expected.Columns, actual.Columns)

	// Verify indexes
	sv.verifyIndexes(t, expected.Name, expected.Indexes, actual.Indexes)

	// Verify constraints
	sv.verifyConstraints(t, expected.Name, expected.Constraints, actual.Constraints)

	// Verify triggers
	sv.verifyTriggers(t, expected.Name, expected.Triggers, actual.Triggers)
}

// VerifySchema verifies the entire database schema
func (sv *SchemaVerifier) VerifySchema(t *testing.T, expected ExpectedSchema) {
	t.Helper()

	// Verify tables
	for _, expectedTable := range expected.Tables {
		t.Run(fmt.Sprintf("Table_%s", expectedTable.Name), func(t *testing.T) {
			sv.VerifyTable(t, expectedTable)
		})
	}

	// Verify views
	for _, expectedView := range expected.Views {
		t.Run(fmt.Sprintf("View_%s", expectedView.Name), func(t *testing.T) {
			sv.verifyView(t, expectedView)
		})
	}

	// Verify functions
	for _, expectedFunction := range expected.Functions {
		t.Run(fmt.Sprintf("Function_%s", expectedFunction.Name), func(t *testing.T) {
			sv.verifyFunction(t, expectedFunction)
		})
	}

	// Verify procedures
	for _, expectedProcedure := range expected.Procedures {
		t.Run(fmt.Sprintf("Procedure_%s", expectedProcedure.Name), func(t *testing.T) {
			sv.verifyProcedure(t, expectedProcedure)
		})
	}

	// Verify sequences
	for _, expectedSequence := range expected.Sequences {
		t.Run(fmt.Sprintf("Sequence_%s", expectedSequence.Name), func(t *testing.T) {
			sv.verifySequence(t, expectedSequence)
		})
	}

	// Verify extensions (PostgreSQL only)
	if sv.dbType == db.PostgreSQLConnectionType {
		for _, expectedExtension := range expected.Extensions {
			t.Run(fmt.Sprintf("Extension_%s", expectedExtension.Name), func(t *testing.T) {
				sv.verifyExtension(t, expectedExtension)
			})
		}
	}
}

// GenerateExpectedSchema generates expected schema from existing database
func (sv *SchemaVerifier) GenerateExpectedSchema(ctx context.Context, schemas ...string) (*ExpectedSchema, error) {
	schemaList := []string{"public"}
	if len(schemas) > 0 {
		schemaList = schemas
	}

	expected := &ExpectedSchema{}

	for _, schema := range schemaList {
		// Get tables
		tables, err := sv.getTables(ctx, schema)
		if err != nil {
			return nil, fmt.Errorf("failed to get tables for schema %s: %w", schema, err)
		}

		for _, tableName := range tables {
			tableInfo, err := sv.GetTableInfo(ctx, tableName, schema)
			if err != nil {
				return nil, fmt.Errorf("failed to get info for table %s: %w", tableName, err)
			}

			expectedTable := sv.convertToExpectedTable(tableInfo)
			expected.Tables = append(expected.Tables, expectedTable)
		}

		// Get views
		views, err := sv.getViews(ctx, schema)
		if err != nil {
			return nil, fmt.Errorf("failed to get views for schema %s: %w", schema, err)
		}
		expected.Views = append(expected.Views, views...)

		// Get functions
		functions, err := sv.getFunctions(ctx, schema)
		if err != nil {
			return nil, fmt.Errorf("failed to get functions for schema %s: %w", schema, err)
		}
		expected.Functions = append(expected.Functions, functions...)

		// Get procedures
		procedures, err := sv.getProcedures(ctx, schema)
		if err != nil {
			return nil, fmt.Errorf("failed to get procedures for schema %s: %w", schema, err)
		}
		expected.Procedures = append(expected.Procedures, procedures...)

		// Get sequences
		sequences, err := sv.getSequences(ctx, schema)
		if err != nil {
			return nil, fmt.Errorf("failed to get sequences for schema %s: %w", schema, err)
		}
		expected.Sequences = append(expected.Sequences, sequences...)
	}

	// Get extensions (PostgreSQL only)
	if sv.dbType == db.PostgreSQLConnectionType {
		extensions, err := sv.getExtensions(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get extensions: %w", err)
		}
		expected.Extensions = extensions
	}

	return expected, nil
}

// VerifyMappingConsistency verifies that GORP mappings match database schema
func (sv *SchemaVerifier) VerifyMappingConsistency[T mapping.Mappable](t *testing.T, mapper mapping.TableMapper[T]) {
	t.Helper()

	tableName := mapper.TableName()
	schema := mapper.Schema()

	// Get actual table info
	ctx := context.Background()
	actualTable, err := sv.GetTableInfo(ctx, tableName)
	require.NoError(t, err, "Failed to get table info for %s", tableName)

	// Verify column mapping
	columnMap := mapper.ColumnMap()
	actualColumns := make(map[string]ColumnInfo)
	for _, col := range actualTable.Columns {
		actualColumns[strings.ToLower(col.Name)] = col
	}

	// Check that all mapped columns exist in database
	for fieldName, columnName := range columnMap {
		actualCol, exists := actualColumns[strings.ToLower(columnName)]
		assert.True(t, exists, "Mapped column %s (field %s) does not exist in database table %s", columnName, fieldName, tableName)

		if exists {
			// Verify column properties match schema definition
			if schema != nil {
				if schemaCol := schema.GetColumn(columnName); schemaCol != nil {
					sv.verifyColumnMapping(t, fieldName, columnName, actualCol, *schemaCol)
				}
			}
		}
	}

	// Verify primary key mapping
	primaryKeys := mapper.PrimaryKey()
	actualPrimaryKeys := make([]string, 0)
	for _, col := range actualTable.Columns {
		if col.IsPrimaryKey {
			actualPrimaryKeys = append(actualPrimaryKeys, col.Name)
		}
	}

	sort.Strings(primaryKeys)
	sort.Strings(actualPrimaryKeys)
	assert.Equal(t, primaryKeys, actualPrimaryKeys, "Primary key mismatch for table %s", tableName)

	// Verify indexes
	expectedIndexes := mapper.Indexes()
	sv.verifyMappedIndexes(t, tableName, expectedIndexes, actualTable.Indexes)
}

// TableExists checks if a table exists in the database
func (sv *SchemaVerifier) TableExists(ctx context.Context, tableName string, schema ...string) (bool, error) {
	schemaName := "public"
	if len(schema) > 0 {
		schemaName = schema[0]
	}

	var query string
	var args []interface{}

	switch sv.dbType {
	case db.PostgreSQLConnectionType:
		query = `SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_schema = $1 AND table_name = $2
		)`
		args = []interface{}{schemaName, tableName}
	case db.MySQLConnectionType:
		query = `SELECT EXISTS (
			SELECT * FROM information_schema.tables
			WHERE table_schema = ? AND table_name = ?
		)`
		args = []interface{}{schemaName, tableName}
	case db.SQLServerConnectionType:
		query = `SELECT CASE WHEN EXISTS (
			SELECT * FROM sys.tables t
			JOIN sys.schemas s ON t.schema_id = s.schema_id
			WHERE s.name = ? AND t.name = ?
		) THEN 1 ELSE 0 END`
		args = []interface{}{schemaName, tableName}
	case db.SQLiteConnectionType:
		query = `SELECT EXISTS (
			SELECT name FROM sqlite_master
			WHERE type='table' AND name=?
		)`
		args = []interface{}{tableName}
	default:
		return false, fmt.Errorf("unsupported database type: %s", sv.dbType)
	}

	var exists bool
	var row *sql.Row

	if pgPool := sv.connection.PgxPool(); pgPool != nil {
		err := pgPool.QueryRow(ctx, query, args...).Scan(&exists)
		return exists, err
	} else if sqlxDB := sv.connection.SqlxDB(); sqlxDB != nil {
		row = sqlxDB.QueryRowContext(ctx, query, args...)
	} else if sqlDB := sv.connection.SQLDB(); sqlDB != nil {
		row = sqlDB.QueryRowContext(ctx, query, args...)
	} else {
		return false, fmt.Errorf("no available database connection")
	}

	err := row.Scan(&exists)
	return exists, err
}

// CreateTableFromMapping creates a table based on GORP mapping
func (sv *SchemaVerifier) CreateTableFromMapping[T mapping.Mappable](ctx context.Context, mapper mapping.TableMapper[T]) error {
	schema := mapper.Schema()
	if schema == nil {
		return fmt.Errorf("no schema available for table %s", mapper.TableName())
	}

	// Generate CREATE TABLE statement
	createSQL, err := sv.generateCreateTableSQL(mapper.TableName(), schema)
	if err != nil {
		return fmt.Errorf("failed to generate CREATE TABLE SQL: %w", err)
	}

	// Execute CREATE TABLE
	var execErr error
	if pgPool := sv.connection.PgxPool(); pgPool != nil {
		_, execErr = pgPool.Exec(ctx, createSQL)
	} else if sqlxDB := sv.connection.SqlxDB(); sqlxDB != nil {
		_, execErr = sqlxDB.ExecContext(ctx, createSQL)
	} else if sqlDB := sv.connection.SQLDB(); sqlDB != nil {
		_, execErr = sqlDB.ExecContext(ctx, createSQL)
	} else {
		return fmt.Errorf("no available database connection")
	}

	if execErr != nil {
		return fmt.Errorf("failed to create table %s: %w", mapper.TableName(), execErr)
	}

	// Create indexes
	indexes := mapper.Indexes()
	for _, index := range indexes {
		indexSQL, err := sv.generateCreateIndexSQL(mapper.TableName(), index)
		if err != nil {
			return fmt.Errorf("failed to generate CREATE INDEX SQL: %w", err)
		}

		if pgPool := sv.connection.PgxPool(); pgPool != nil {
			_, execErr = pgPool.Exec(ctx, indexSQL)
		} else if sqlxDB := sv.connection.SqlxDB(); sqlxDB != nil {
			_, execErr = sqlxDB.ExecContext(ctx, indexSQL)
		} else if sqlDB := sv.connection.SQLDB(); sqlDB != nil {
			_, execErr = sqlDB.ExecContext(ctx, indexSQL)
		}

		if execErr != nil {
			return fmt.Errorf("failed to create index %s: %w", index.Name, execErr)
		}
	}

	return nil
}

// DropTableIfExists drops a table if it exists
func (sv *SchemaVerifier) DropTableIfExists(ctx context.Context, tableName string, schema ...string) error {
	exists, err := sv.TableExists(ctx, tableName, schema...)
	if err != nil {
		return err
	}

	if !exists {
		return nil
	}

	schemaName := "public"
	if len(schema) > 0 {
		schemaName = schema[0]
	}

	var dropSQL string
	switch sv.dbType {
	case db.PostgreSQLConnectionType:
		if schemaName != "public" {
			dropSQL = fmt.Sprintf("DROP TABLE IF EXISTS %s.%s CASCADE", schemaName, tableName)
		} else {
			dropSQL = fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", tableName)
		}
	case db.MySQLConnectionType:
		if schemaName != "public" {
			dropSQL = fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", schemaName, tableName)
		} else {
			dropSQL = fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
		}
	case db.SQLServerConnectionType:
		if schemaName != "dbo" {
			dropSQL = fmt.Sprintf("IF OBJECT_ID('%s.%s', 'U') IS NOT NULL DROP TABLE %s.%s", schemaName, tableName, schemaName, tableName)
		} else {
			dropSQL = fmt.Sprintf("IF OBJECT_ID('%s', 'U') IS NOT NULL DROP TABLE %s", tableName, tableName)
		}
	case db.SQLiteConnectionType:
		dropSQL = fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	default:
		return fmt.Errorf("unsupported database type: %s", sv.dbType)
	}

	var execErr error
	if pgPool := sv.connection.PgxPool(); pgPool != nil {
		_, execErr = pgPool.Exec(ctx, dropSQL)
	} else if sqlxDB := sv.connection.SqlxDB(); sqlxDB != nil {
		_, execErr = sqlxDB.ExecContext(ctx, dropSQL)
	} else if sqlDB := sv.connection.SQLDB(); sqlDB != nil {
		_, execErr = sqlDB.ExecContext(ctx, dropSQL)
	} else {
		return fmt.Errorf("no available database connection")
	}

	return execErr
}

// Helper methods will be implemented in additional files due to length
// These include database-specific implementations for PostgreSQL, MySQL, SQL Server, and SQLite