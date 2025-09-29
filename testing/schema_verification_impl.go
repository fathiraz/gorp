// Database-specific schema verification implementations
package testing

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/fathiraz/gorp/mapping"
)

// PostgreSQL-specific implementations
func (sv *SchemaVerifier) getPostgreSQLTableInfo(ctx context.Context, tableName, schemaName string) (*TableInfo, error) {
	tableInfo := &TableInfo{
		Name:   tableName,
		Schema: schemaName,
	}

	// Get columns
	columns, err := sv.getPostgreSQLColumns(ctx, tableName, schemaName)
	if err != nil {
		return nil, err
	}
	tableInfo.Columns = columns

	// Get indexes
	indexes, err := sv.getPostgreSQLIndexes(ctx, tableName, schemaName)
	if err != nil {
		return nil, err
	}
	tableInfo.Indexes = indexes

	// Get constraints
	constraints, err := sv.getPostgreSQLConstraints(ctx, tableName, schemaName)
	if err != nil {
		return nil, err
	}
	tableInfo.Constraints = constraints

	// Get triggers
	triggers, err := sv.getPostgreSQLTriggers(ctx, tableName, schemaName)
	if err != nil {
		return nil, err
	}
	tableInfo.Triggers = triggers

	return tableInfo, nil
}

func (sv *SchemaVerifier) getPostgreSQLColumns(ctx context.Context, tableName, schemaName string) ([]ColumnInfo, error) {
	query := `
		SELECT
			c.column_name,
			c.data_type,
			c.is_nullable = 'YES' as is_nullable,
			c.column_default,
			c.character_maximum_length,
			c.numeric_precision,
			c.numeric_scale,
			c.is_identity = 'YES' as is_identity,
			COALESCE(pk.is_primary, false) as is_primary_key
		FROM information_schema.columns c
		LEFT JOIN (
			SELECT kcu.column_name, true as is_primary
			FROM information_schema.table_constraints tc
			JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
			WHERE tc.table_schema = $1 AND tc.table_name = $2 AND tc.constraint_type = 'PRIMARY KEY'
		) pk ON c.column_name = pk.column_name
		WHERE c.table_schema = $1 AND c.table_name = $2
		ORDER BY c.ordinal_position
	`

	pgPool := sv.connection.PgxPool()
	rows, err := pgPool.Query(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query PostgreSQL columns: %w", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		var defaultValue, maxLength, precision, scale sql.NullString

		err := rows.Scan(
			&col.Name,
			&col.DataType,
			&col.IsNullable,
			&defaultValue,
			&maxLength,
			&precision,
			&scale,
			&col.IsIdentity,
			&col.IsPrimaryKey,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan PostgreSQL column: %w", err)
		}

		if defaultValue.Valid {
			col.DefaultValue = &defaultValue.String
		}

		if maxLength.Valid {
			if length := parseInt(maxLength.String); length != nil {
				col.MaxLength = length
			}
		}

		if precision.Valid {
			if prec := parseInt(precision.String); prec != nil {
				col.Precision = prec
			}
		}

		if scale.Valid {
			if sc := parseInt(scale.String); sc != nil {
				col.Scale = sc
			}
		}

		columns = append(columns, col)
	}

	return columns, rows.Err()
}

func (sv *SchemaVerifier) getPostgreSQLIndexes(ctx context.Context, tableName, schemaName string) ([]IndexInfo, error) {
	query := `
		SELECT
			i.relname as index_name,
			t.relname as table_name,
			array_agg(a.attname ORDER BY c.ordinality) as columns,
			ix.indisunique,
			ix.indisprimary,
			am.amname as index_type
		FROM pg_class t
		JOIN pg_namespace n ON n.oid = t.relnamespace
		JOIN pg_index ix ON t.oid = ix.indrelid
		JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_am am ON i.relam = am.oid
		JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS c(attnum, ordinality) ON true
		JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = c.attnum
		WHERE n.nspname = $1 AND t.relname = $2
		GROUP BY i.relname, t.relname, ix.indisunique, ix.indisprimary, am.amname
		ORDER BY i.relname
	`

	pgPool := sv.connection.PgxPool()
	rows, err := pgPool.Query(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query PostgreSQL indexes: %w", err)
	}
	defer rows.Close()

	var indexes []IndexInfo
	for rows.Next() {
		var idx IndexInfo
		var columns []string

		err := rows.Scan(
			&idx.Name,
			&idx.TableName,
			&columns,
			&idx.IsUnique,
			&idx.IsPrimary,
			&idx.Type,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan PostgreSQL index: %w", err)
		}

		idx.Columns = columns
		indexes = append(indexes, idx)
	}

	return indexes, rows.Err()
}

func (sv *SchemaVerifier) getPostgreSQLConstraints(ctx context.Context, tableName, schemaName string) ([]ConstraintInfo, error) {
	query := `
		SELECT
			tc.constraint_name,
			tc.constraint_type,
			tc.table_name,
			array_agg(kcu.column_name ORDER BY kcu.ordinal_position) as columns,
			rc.unique_constraint_schema as ref_schema,
			rc.referenced_table_name as ref_table,
			array_agg(rcu.column_name ORDER BY rcu.ordinal_position) as ref_columns
		FROM information_schema.table_constraints tc
		LEFT JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
		LEFT JOIN information_schema.referential_constraints rc ON tc.constraint_name = rc.constraint_name
		LEFT JOIN information_schema.key_column_usage rcu ON rc.unique_constraint_name = rcu.constraint_name
		WHERE tc.table_schema = $1 AND tc.table_name = $2
		GROUP BY tc.constraint_name, tc.constraint_type, tc.table_name, rc.unique_constraint_schema, rc.referenced_table_name
		ORDER BY tc.constraint_name
	`

	pgPool := sv.connection.PgxPool()
	rows, err := pgPool.Query(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query PostgreSQL constraints: %w", err)
	}
	defer rows.Close()

	var constraints []ConstraintInfo
	for rows.Next() {
		var constraint ConstraintInfo
		var columns, refColumns []string
		var refTable sql.NullString

		err := rows.Scan(
			&constraint.Name,
			&constraint.Type,
			&constraint.TableName,
			&columns,
			&sql.NullString{}, // ref_schema (not used)
			&refTable,
			&refColumns,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan PostgreSQL constraint: %w", err)
		}

		constraint.Columns = columns
		if refTable.Valid {
			constraint.RefTable = &refTable.String
			constraint.RefColumns = refColumns
		}

		constraints = append(constraints, constraint)
	}

	return constraints, rows.Err()
}

func (sv *SchemaVerifier) getPostgreSQLTriggers(ctx context.Context, tableName, schemaName string) ([]TriggerInfo, error) {
	query := `
		SELECT
			trigger_name,
			event_manipulation,
			action_timing,
			action_statement
		FROM information_schema.triggers
		WHERE event_object_schema = $1 AND event_object_table = $2
		ORDER BY trigger_name
	`

	pgPool := sv.connection.PgxPool()
	rows, err := pgPool.Query(ctx, query, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query PostgreSQL triggers: %w", err)
	}
	defer rows.Close()

	var triggers []TriggerInfo
	for rows.Next() {
		var trigger TriggerInfo

		err := rows.Scan(
			&trigger.Name,
			&trigger.Event,
			&trigger.Timing,
			&trigger.Body,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan PostgreSQL trigger: %w", err)
		}

		trigger.TableName = tableName
		triggers = append(triggers, trigger)
	}

	return triggers, rows.Err()
}

// MySQL-specific implementations
func (sv *SchemaVerifier) getMySQLTableInfo(ctx context.Context, tableName, schemaName string) (*TableInfo, error) {
	tableInfo := &TableInfo{
		Name:   tableName,
		Schema: schemaName,
	}

	sqlxDB := sv.connection.SqlxDB()
	if sqlxDB == nil {
		return nil, fmt.Errorf("no sqlx connection available for MySQL")
	}

	// Get columns
	columnsQuery := `
		SELECT
			COLUMN_NAME,
			DATA_TYPE,
			IS_NULLABLE = 'YES' as is_nullable,
			COLUMN_DEFAULT,
			CHARACTER_MAXIMUM_LENGTH,
			NUMERIC_PRECISION,
			NUMERIC_SCALE,
			EXTRA LIKE '%auto_increment%' as is_identity,
			COLUMN_KEY = 'PRI' as is_primary_key
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`

	rows, err := sqlxDB.QueryContext(ctx, columnsQuery, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query MySQL columns: %w", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		var defaultValue, maxLength, precision, scale sql.NullString

		err := rows.Scan(
			&col.Name,
			&col.DataType,
			&col.IsNullable,
			&defaultValue,
			&maxLength,
			&precision,
			&scale,
			&col.IsIdentity,
			&col.IsPrimaryKey,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan MySQL column: %w", err)
		}

		if defaultValue.Valid {
			col.DefaultValue = &defaultValue.String
		}

		if maxLength.Valid {
			if length := parseInt(maxLength.String); length != nil {
				col.MaxLength = length
			}
		}

		if precision.Valid {
			if prec := parseInt(precision.String); prec != nil {
				col.Precision = prec
			}
		}

		if scale.Valid {
			if sc := parseInt(scale.String); sc != nil {
				col.Scale = sc
			}
		}

		columns = append(columns, col)
	}
	tableInfo.Columns = columns

	// Get indexes
	indexesQuery := `
		SELECT
			INDEX_NAME,
			GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) as columns,
			NON_UNIQUE = 0 as is_unique,
			INDEX_NAME = 'PRIMARY' as is_primary,
			INDEX_TYPE
		FROM INFORMATION_SCHEMA.STATISTICS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		GROUP BY INDEX_NAME, NON_UNIQUE, INDEX_TYPE
		ORDER BY INDEX_NAME
	`

	indexRows, err := sqlxDB.QueryContext(ctx, indexesQuery, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query MySQL indexes: %w", err)
	}
	defer indexRows.Close()

	var indexes []IndexInfo
	for indexRows.Next() {
		var idx IndexInfo
		var columnsStr string

		err := indexRows.Scan(
			&idx.Name,
			&columnsStr,
			&idx.IsUnique,
			&idx.IsPrimary,
			&idx.Type,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan MySQL index: %w", err)
		}

		idx.TableName = tableName
		idx.Columns = strings.Split(columnsStr, ",")
		indexes = append(indexes, idx)
	}
	tableInfo.Indexes = indexes

	return tableInfo, nil
}

// SQL Server-specific implementations
func (sv *SchemaVerifier) getSQLServerTableInfo(ctx context.Context, tableName, schemaName string) (*TableInfo, error) {
	tableInfo := &TableInfo{
		Name:   tableName,
		Schema: schemaName,
	}

	sqlxDB := sv.connection.SqlxDB()
	if sqlxDB == nil {
		return nil, fmt.Errorf("no sqlx connection available for SQL Server")
	}

	// Get columns
	columnsQuery := `
		SELECT
			c.COLUMN_NAME,
			c.DATA_TYPE,
			c.IS_NULLABLE = 'YES' as is_nullable,
			c.COLUMN_DEFAULT,
			c.CHARACTER_MAXIMUM_LENGTH,
			c.NUMERIC_PRECISION,
			c.NUMERIC_SCALE,
			COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') as is_identity,
			CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN 1 ELSE 0 END as is_primary_key
		FROM INFORMATION_SCHEMA.COLUMNS c
		LEFT JOIN (
			SELECT kcu.COLUMN_NAME
			FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
			JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
			WHERE tc.TABLE_SCHEMA = ? AND tc.TABLE_NAME = ? AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
		) pk ON c.COLUMN_NAME = pk.COLUMN_NAME
		WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
		ORDER BY c.ORDINAL_POSITION
	`

	rows, err := sqlxDB.QueryContext(ctx, columnsQuery, schemaName, tableName, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query SQL Server columns: %w", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		var defaultValue, maxLength, precision, scale sql.NullString

		err := rows.Scan(
			&col.Name,
			&col.DataType,
			&col.IsNullable,
			&defaultValue,
			&maxLength,
			&precision,
			&scale,
			&col.IsIdentity,
			&col.IsPrimaryKey,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan SQL Server column: %w", err)
		}

		if defaultValue.Valid {
			col.DefaultValue = &defaultValue.String
		}

		if maxLength.Valid {
			if length := parseInt(maxLength.String); length != nil {
				col.MaxLength = length
			}
		}

		if precision.Valid {
			if prec := parseInt(precision.String); prec != nil {
				col.Precision = prec
			}
		}

		if scale.Valid {
			if sc := parseInt(scale.String); sc != nil {
				col.Scale = sc
			}
		}

		columns = append(columns, col)
	}
	tableInfo.Columns = columns

	return tableInfo, nil
}

// SQLite-specific implementations
func (sv *SchemaVerifier) getSQLiteTableInfo(ctx context.Context, tableName string) (*TableInfo, error) {
	tableInfo := &TableInfo{
		Name:   tableName,
		Schema: "main",
	}

	sqlxDB := sv.connection.SqlxDB()
	if sqlxDB == nil {
		return nil, fmt.Errorf("no sqlx connection available for SQLite")
	}

	// Get table info using PRAGMA
	pragmaQuery := "PRAGMA table_info(" + tableName + ")"
	rows, err := sqlxDB.QueryContext(ctx, pragmaQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query SQLite table info: %w", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var cid int
		var col ColumnInfo
		var notNull int
		var defaultValue sql.NullString
		var pk int

		err := rows.Scan(
			&cid,
			&col.Name,
			&col.DataType,
			&notNull,
			&defaultValue,
			&pk,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan SQLite column: %w", err)
		}

		col.IsNullable = notNull == 0
		col.IsPrimaryKey = pk > 0

		if defaultValue.Valid {
			col.DefaultValue = &defaultValue.String
		}

		columns = append(columns, col)
	}
	tableInfo.Columns = columns

	return tableInfo, nil
}

// Helper functions
func parseInt(s string) *int {
	// Simple integer parsing - in production, use strconv.Atoi
	if s == "" {
		return nil
	}
	// Simplified implementation
	var result int = 0
	for _, r := range s {
		if r >= '0' && r <= '9' {
			result = result*10 + int(r-'0')
		} else {
			return nil
		}
	}
	return &result
}

// Verification helper methods
func (sv *SchemaVerifier) verifyColumns(t TestingT, tableName string, expected []ExpectedColumn, actual []ColumnInfo) {
	expectedMap := make(map[string]ExpectedColumn)
	for _, col := range expected {
		expectedMap[strings.ToLower(col.Name)] = col
	}

	actualMap := make(map[string]ColumnInfo)
	for _, col := range actual {
		actualMap[strings.ToLower(col.Name)] = col
	}

	// Check all expected columns exist
	for _, expectedCol := range expected {
		actualCol, exists := actualMap[strings.ToLower(expectedCol.Name)]
		if !exists {
			t.Errorf("Table %s: expected column %s does not exist", tableName, expectedCol.Name)
			continue
		}

		// Verify column properties
		if !sv.dataTypesMatch(expectedCol.DataType, actualCol.DataType) {
			t.Errorf("Table %s, column %s: data type mismatch - expected %s, got %s",
				tableName, expectedCol.Name, expectedCol.DataType, actualCol.DataType)
		}

		if expectedCol.Nullable != actualCol.IsNullable {
			t.Errorf("Table %s, column %s: nullable mismatch - expected %v, got %v",
				tableName, expectedCol.Name, expectedCol.Nullable, actualCol.IsNullable)
		}

		if expectedCol.IsPrimary != actualCol.IsPrimaryKey {
			t.Errorf("Table %s, column %s: primary key mismatch - expected %v, got %v",
				tableName, expectedCol.Name, expectedCol.IsPrimary, actualCol.IsPrimaryKey)
		}
	}

	// Check for unexpected columns
	for _, actualCol := range actual {
		if _, exists := expectedMap[strings.ToLower(actualCol.Name)]; !exists {
			t.Errorf("Table %s: unexpected column %s", tableName, actualCol.Name)
		}
	}
}

func (sv *SchemaVerifier) verifyIndexes(t TestingT, tableName string, expected []ExpectedIndex, actual []IndexInfo) {
	expectedMap := make(map[string]ExpectedIndex)
	for _, idx := range expected {
		expectedMap[strings.ToLower(idx.Name)] = idx
	}

	actualMap := make(map[string]IndexInfo)
	for _, idx := range actual {
		actualMap[strings.ToLower(idx.Name)] = idx
	}

	// Check all expected indexes exist
	for _, expectedIdx := range expected {
		actualIdx, exists := actualMap[strings.ToLower(expectedIdx.Name)]
		if !exists {
			t.Errorf("Table %s: expected index %s does not exist", tableName, expectedIdx.Name)
			continue
		}

		// Verify index properties
		if expectedIdx.IsUnique != actualIdx.IsUnique {
			t.Errorf("Table %s, index %s: unique mismatch - expected %v, got %v",
				tableName, expectedIdx.Name, expectedIdx.IsUnique, actualIdx.IsUnique)
		}

		// Verify columns
		if !equalStringSlices(expectedIdx.Columns, actualIdx.Columns) {
			t.Errorf("Table %s, index %s: column mismatch - expected %v, got %v",
				tableName, expectedIdx.Name, expectedIdx.Columns, actualIdx.Columns)
		}
	}
}

func (sv *SchemaVerifier) verifyConstraints(t TestingT, tableName string, expected []ExpectedConstraint, actual []ConstraintInfo) {
	// Similar implementation to verifyIndexes
}

func (sv *SchemaVerifier) verifyTriggers(t TestingT, tableName string, expected []ExpectedTrigger, actual []TriggerInfo) {
	// Similar implementation to verifyIndexes
}

func (sv *SchemaVerifier) dataTypesMatch(expected, actual string) bool {
	// Normalize data types for comparison
	expected = strings.ToLower(strings.TrimSpace(expected))
	actual = strings.ToLower(strings.TrimSpace(actual))

	// Handle common type variations
	typeMapping := map[string][]string{
		"integer": {"int", "int4", "integer"},
		"bigint":  {"int8", "bigint"},
		"varchar": {"character varying", "varchar"},
		"text":    {"text", "longtext"},
	}

	for canonical, variants := range typeMapping {
		for _, variant := range variants {
			if (expected == canonical || expected == variant) &&
			   (actual == canonical || actual == variant) {
				return true
			}
		}
	}

	return expected == actual
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestingT is a subset of testing.T for error reporting
type TestingT interface {
	Errorf(format string, args ...interface{})
}

// Additional helper methods for schema generation and other operations
func (sv *SchemaVerifier) getTables(ctx context.Context, schema string) ([]string, error) {
	// Database-specific implementation to get table list
	return nil, fmt.Errorf("not implemented")
}

func (sv *SchemaVerifier) getViews(ctx context.Context, schema string) ([]ExpectedView, error) {
	// Database-specific implementation to get view list
	return nil, fmt.Errorf("not implemented")
}

func (sv *SchemaVerifier) getFunctions(ctx context.Context, schema string) ([]ExpectedFunction, error) {
	// Database-specific implementation to get function list
	return nil, fmt.Errorf("not implemented")
}

func (sv *SchemaVerifier) getProcedures(ctx context.Context, schema string) ([]ExpectedProcedure, error) {
	// Database-specific implementation to get procedure list
	return nil, fmt.Errorf("not implemented")
}

func (sv *SchemaVerifier) getSequences(ctx context.Context, schema string) ([]ExpectedSequence, error) {
	// Database-specific implementation to get sequence list
	return nil, fmt.Errorf("not implemented")
}

func (sv *SchemaVerifier) getExtensions(ctx context.Context) ([]ExpectedExtension, error) {
	// PostgreSQL-specific implementation to get extension list
	return nil, fmt.Errorf("not implemented")
}

func (sv *SchemaVerifier) convertToExpectedTable(tableInfo *TableInfo) ExpectedTable {
	// Convert TableInfo to ExpectedTable
	return ExpectedTable{}
}

func (sv *SchemaVerifier) verifyView(t TestingT, expected ExpectedView) {
	// Verify view implementation
}

func (sv *SchemaVerifier) verifyFunction(t TestingT, expected ExpectedFunction) {
	// Verify function implementation
}

func (sv *SchemaVerifier) verifyProcedure(t TestingT, expected ExpectedProcedure) {
	// Verify procedure implementation
}

func (sv *SchemaVerifier) verifySequence(t TestingT, expected ExpectedSequence) {
	// Verify sequence implementation
}

func (sv *SchemaVerifier) verifyExtension(t TestingT, expected ExpectedExtension) {
	// Verify extension implementation
}

func (sv *SchemaVerifier) verifyColumnMapping(t TestingT, fieldName, columnName string, actual ColumnInfo, schema mapping.ColumnDefinition) {
	// Verify column mapping implementation
}

func (sv *SchemaVerifier) verifyMappedIndexes(t TestingT, tableName string, expected []mapping.IndexDefinition, actual []IndexInfo) {
	// Verify mapped indexes implementation
}

func (sv *SchemaVerifier) generateCreateTableSQL(tableName string, schema *mapping.TableSchema) (string, error) {
	// Generate CREATE TABLE SQL
	return "", fmt.Errorf("not implemented")
}

func (sv *SchemaVerifier) generateCreateIndexSQL(tableName string, index mapping.IndexDefinition) (string, error) {
	// Generate CREATE INDEX SQL
	return "", fmt.Errorf("not implemented")
}