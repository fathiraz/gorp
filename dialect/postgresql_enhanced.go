package dialect

import (
	"context"
	"fmt"
	"strings"
)

// EnhancedPostgreSQLDialect provides advanced PostgreSQL-specific features
type EnhancedPostgreSQLDialect struct {
	*BaseDialect
	// Version represents the PostgreSQL version (e.g., "14", "13", "12")
	Version string
	// LowercaseFields controls field name case handling
	LowercaseFields bool
}

// NewEnhancedPostgreSQLDialect creates a new enhanced PostgreSQL dialect
func NewEnhancedPostgreSQLDialect(version string, lowercaseFields bool) *EnhancedPostgreSQLDialect {
	return &EnhancedPostgreSQLDialect{
		BaseDialect:     NewBaseDialect("postgresql"),
		Version:         version,
		LowercaseFields: lowercaseFields,
	}
}

// GetVersion returns the PostgreSQL version
func (d *EnhancedPostgreSQLDialect) GetVersion() string {
	return d.Version
}

// GetCapabilities returns PostgreSQL-specific capabilities
func (d *EnhancedPostgreSQLDialect) GetCapabilities() DialectCapabilities {
	return DialectCapabilities{
		// JSON support (PostgreSQL 9.2+, JSONB in 9.4+)
		SupportsJSON:         true,
		JSONType:             "JSONB",
		JSONQueryFunction:    "jsonb_extract_path",
		JSONModifyFunction:   "jsonb_set",
		SupportsJSONSchema:   true,

		// Arrays (native support)
		SupportsArrays:       true,
		ArrayType:            "ARRAY",
		ArrayQueryFunction:   "array_length",

		// Advanced indexing
		SupportsPartialIndex: true,
		SupportsExpressionIndex: true,
		SupportedIndexTypes:  []IndexType{IndexTypeBTree, IndexTypeHash, IndexTypeGIN, IndexTypeGiST, IndexTypeBRIN},
		SupportsIndexInclude: d.supportsVersion("11"), // INCLUDE indexes in 11+

		// Partitioning
		SupportsPartitioning: d.supportsVersion("10"), // Native partitioning in 10+
		PartitioningTypes:    []PartitionType{PartitionTypeRange, PartitionTypeList, PartitionTypeHash},
		SupportsSubpartitions: false,

		// Transaction features
		SupportsNestedTransactions: false,
		SupportsSavepoints:         true,
		MaxNestedLevel:            0,

		// Advanced data types
		SupportsUUID:           true,
		SupportsHStore:         true, // via extension
		SupportsEnums:          true,
		SupportsGeneratedColumns: d.supportsVersion("12"), // Generated columns in 12+
		SupportsStoredGenerated:  d.supportsVersion("12"),
		SupportsVirtualGenerated: false, // PostgreSQL only has stored generated columns

		// Performance features
		SupportsBulkInsert:     true,
		SupportsUpsert:         true,
		UpsertSyntax:          UpsertOnConflict,
		SupportsBatchOperations: true,
		SupportsAsyncOperations: true, // via async queries

		// Connection features
		SupportsConnectionPooling: true,
		SupportsReadReplicas:      true,
		SupportsSharding:          false, // Not native, requires external solutions

		// Security features
		SupportsRLS:              d.supportsVersion("9.5"), // Row Level Security in 9.5+
		SupportsTablespaceEncryption: false, // No native tablespace encryption
		SupportsColumnEncryption: false, // No native column encryption
		SupportedAuthMethods:     []AuthMethod{AuthPassword, AuthCertificate, AuthKerberos, AuthLDAP},

		// Full-text search
		SupportsFTS:          true,
		FTSType:             FTSNative,
		FTSQueryLanguage:    "postgresql",
		SupportsLanguages:   []string{"english", "german", "french", "spanish", "russian"},

		// Optimization features
		SupportsQueryPlan:    true,
		SupportsHints:        false, // PostgreSQL doesn't support query hints
		SupportsAnalyze:      true,
		SupportsVacuum:       true,
	}
}

// SupportsFeature checks if a specific feature is supported
func (d *EnhancedPostgreSQLDialect) SupportsFeature(feature Feature) bool {
	capabilities := d.GetCapabilities()

	switch feature {
	case FeatureJSON:
		return capabilities.SupportsJSON
	case FeatureJSONB:
		return true // PostgreSQL has native JSONB support
	case FeatureArrays:
		return capabilities.SupportsArrays
	case FeaturePartitioning:
		return capabilities.SupportsPartitioning
	case FeatureUpsert:
		return capabilities.SupportsUpsert
	case FeatureGeneratedColumns:
		return capabilities.SupportsGeneratedColumns
	case FeatureFTS:
		return capabilities.SupportsFTS
	case FeatureHStore:
		return capabilities.SupportsHStore
	case FeatureUUID:
		return capabilities.SupportsUUID
	case FeatureRLS:
		return capabilities.SupportsRLS
	case FeatureBulkInsert:
		return capabilities.SupportsBulkInsert
	case FeaturePartialIndex:
		return capabilities.SupportsPartialIndex
	case FeatureExpressionIndex:
		return capabilities.SupportsExpressionIndex
	case FeatureNestedTransactions:
		return capabilities.SupportsNestedTransactions
	case FeatureAsyncOperations:
		return capabilities.SupportsAsyncOperations
	default:
		return false
	}
}

// supportsVersion checks if the current version supports a feature introduced in minVersion
func (d *EnhancedPostgreSQLDialect) supportsVersion(minVersion string) bool {
	// Simplified version comparison - in production, use proper semver comparison
	return d.Version >= minVersion
}

// Override base methods for PostgreSQL-specific behavior
func (d *EnhancedPostgreSQLDialect) QuoteIdentifier(identifier string) string {
	if d.LowercaseFields {
		return `"` + strings.ToLower(identifier) + `"`
	}
	return `"` + identifier + `"`
}

func (d *EnhancedPostgreSQLDialect) Placeholder(index int) string {
	return fmt.Sprintf("$%d", index)
}

func (d *EnhancedPostgreSQLDialect) SupportsReturning() bool {
	return true
}

func (d *EnhancedPostgreSQLDialect) LimitClause(limit, offset int) string {
	if limit > 0 && offset > 0 {
		return fmt.Sprintf(" LIMIT %d OFFSET %d", limit, offset)
	} else if limit > 0 {
		return fmt.Sprintf(" LIMIT %d", limit)
	} else if offset > 0 {
		return fmt.Sprintf(" OFFSET %d", offset)
	}
	return ""
}

// JSON operations for PostgreSQL
func (d *EnhancedPostgreSQLDialect) CreateJSONColumn(name string, constraints string) string {
	sql := d.QuoteIdentifier(name) + " JSONB"
	if constraints != "" {
		sql += " " + constraints
	}
	return sql
}

func (d *EnhancedPostgreSQLDialect) JSONExtract(column string, path string) string {
	return fmt.Sprintf("%s->%s", column, path)
}

func (d *EnhancedPostgreSQLDialect) JSONSet(column string, path string, value string) string {
	return fmt.Sprintf("jsonb_set(%s, %s, %s)", column, path, value)
}

func (d *EnhancedPostgreSQLDialect) JSONArrayAppend(column string, path string, value string) string {
	return fmt.Sprintf("jsonb_set(%s, %s, (%s #> %s) || %s)", column, path, column, path, value)
}

func (d *EnhancedPostgreSQLDialect) JSONArrayInsert(column string, path string, value string) string {
	return fmt.Sprintf("jsonb_insert(%s, %s, %s)", column, path, value)
}

func (d *EnhancedPostgreSQLDialect) JSONRemove(column string, path string) string {
	return fmt.Sprintf("%s #- %s", column, path)
}

func (d *EnhancedPostgreSQLDialect) JSONType(column string, path string) string {
	return fmt.Sprintf("jsonb_typeof(%s #> %s)", column, path)
}

func (d *EnhancedPostgreSQLDialect) JSONValid(value string) string {
	// PostgreSQL automatically validates JSON on insert
	return fmt.Sprintf("%s::jsonb IS NOT NULL", value)
}

func (d *EnhancedPostgreSQLDialect) JSONSearch(column string, oneOrAll string, searchStr string, escapeChar string, path string) string {
	// PostgreSQL uses different syntax for JSON search
	if path != "" {
		return fmt.Sprintf("%s #> %s ? %s", column, path, searchStr)
	}
	return fmt.Sprintf("%s ? %s", column, searchStr)
}

// Array operations for PostgreSQL
func (d *EnhancedPostgreSQLDialect) CreateArrayColumn(elementType string, dimensions int) string {
	if dimensions > 1 {
		brackets := strings.Repeat("[]", dimensions)
		return elementType + brackets
	}
	return elementType + "[]"
}

func (d *EnhancedPostgreSQLDialect) ArrayAppend(array string, element string) string {
	return fmt.Sprintf("array_append(%s, %s)", array, element)
}

func (d *EnhancedPostgreSQLDialect) ArrayPrepend(element string, array string) string {
	return fmt.Sprintf("array_prepend(%s, %s)", element, array)
}

func (d *EnhancedPostgreSQLDialect) ArrayConcat(array1 string, array2 string) string {
	return fmt.Sprintf("%s || %s", array1, array2)
}

func (d *EnhancedPostgreSQLDialect) ArrayLength(array string, dimension int) string {
	return fmt.Sprintf("array_length(%s, %d)", array, dimension)
}

func (d *EnhancedPostgreSQLDialect) ArrayPosition(array string, element string) string {
	return fmt.Sprintf("array_position(%s, %s)", array, element)
}

func (d *EnhancedPostgreSQLDialect) ArrayRemove(array string, element string) string {
	return fmt.Sprintf("array_remove(%s, %s)", array, element)
}

func (d *EnhancedPostgreSQLDialect) ArrayReplace(array string, from string, to string) string {
	return fmt.Sprintf("array_replace(%s, %s, %s)", array, from, to)
}

func (d *EnhancedPostgreSQLDialect) ArrayToString(array string, delimiter string) string {
	return fmt.Sprintf("array_to_string(%s, %s)", array, delimiter)
}

func (d *EnhancedPostgreSQLDialect) StringToArray(str string, delimiter string) string {
	return fmt.Sprintf("string_to_array(%s, %s)", str, delimiter)
}

// Upsert operations for PostgreSQL
func (d *EnhancedPostgreSQLDialect) BuildUpsert(table string, columns []string, conflictColumns []string, updateColumns []string) string {
	var sql strings.Builder

	// Build base INSERT
	sql.WriteString("INSERT INTO ")
	sql.WriteString(d.QuoteIdentifier(table))
	sql.WriteString(" (")

	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = d.QuoteIdentifier(col)
	}
	sql.WriteString(strings.Join(quotedColumns, ", "))
	sql.WriteString(") VALUES (")

	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = d.Placeholder(i + 1)
	}
	sql.WriteString(strings.Join(placeholders, ", "))
	sql.WriteString(")")

	// Add ON CONFLICT clause
	if len(conflictColumns) > 0 {
		sql.WriteString(" ON CONFLICT (")
		quotedConflictCols := make([]string, len(conflictColumns))
		for i, col := range conflictColumns {
			quotedConflictCols[i] = d.QuoteIdentifier(col)
		}
		sql.WriteString(strings.Join(quotedConflictCols, ", "))
		sql.WriteString(")")

		if len(updateColumns) > 0 {
			sql.WriteString(" DO UPDATE SET ")
			updates := make([]string, len(updateColumns))
			for i, col := range updateColumns {
				updates[i] = d.QuoteIdentifier(col) + " = EXCLUDED." + d.QuoteIdentifier(col)
			}
			sql.WriteString(strings.Join(updates, ", "))
		} else {
			sql.WriteString(" DO NOTHING")
		}
	}

	return sql.String()
}

func (d *EnhancedPostgreSQLDialect) BuildInsertIgnore(table string, columns []string) string {
	// PostgreSQL doesn't have INSERT IGNORE, use ON CONFLICT DO NOTHING
	var sql strings.Builder
	sql.WriteString("INSERT INTO ")
	sql.WriteString(d.QuoteIdentifier(table))
	sql.WriteString(" (")

	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = d.QuoteIdentifier(col)
	}
	sql.WriteString(strings.Join(quotedColumns, ", "))
	sql.WriteString(") VALUES (")

	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = d.Placeholder(i + 1)
	}
	sql.WriteString(strings.Join(placeholders, ", "))
	sql.WriteString(") ON CONFLICT DO NOTHING")

	return sql.String()
}

func (d *EnhancedPostgreSQLDialect) BuildReplace(table string, columns []string) string {
	// PostgreSQL doesn't have REPLACE, use upsert pattern
	return d.BuildUpsert(table, columns, []string{}, columns)
}

// Generated column operations for PostgreSQL
func (d *EnhancedPostgreSQLDialect) CreateStoredGeneratedColumn(name string, expression string, dataType string) string {
	if !d.supportsVersion("12") {
		return ""
	}
	return fmt.Sprintf("%s %s GENERATED ALWAYS AS (%s) STORED",
		d.QuoteIdentifier(name), dataType, expression)
}

func (d *EnhancedPostgreSQLDialect) CreateVirtualGeneratedColumn(name string, expression string, dataType string) string {
	// PostgreSQL doesn't support virtual generated columns
	return ""
}

func (d *EnhancedPostgreSQLDialect) AlterAddGeneratedColumn(table string, column string, expression string, dataType string, stored bool) string {
	if !d.supportsVersion("12") {
		return ""
	}
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s GENERATED ALWAYS AS (%s) STORED",
		d.QuoteIdentifier(table), d.QuoteIdentifier(column), dataType, expression)
}

func (d *EnhancedPostgreSQLDialect) DropGeneratedColumn(table string, column string) string {
	return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", d.QuoteIdentifier(table), d.QuoteIdentifier(column))
}

// Advanced indexing operations for PostgreSQL
func (d *EnhancedPostgreSQLDialect) CreatePartialIndex(name string, table string, columns []string, condition string) string {
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = d.QuoteIdentifier(col)
	}
	return fmt.Sprintf("CREATE INDEX %s ON %s (%s) WHERE %s",
		d.QuoteIdentifier(name), d.QuoteIdentifier(table), strings.Join(quotedColumns, ", "), condition)
}

func (d *EnhancedPostgreSQLDialect) CreateExpressionIndex(name string, table string, expression string) string {
	return fmt.Sprintf("CREATE INDEX %s ON %s ((%s))",
		d.QuoteIdentifier(name), d.QuoteIdentifier(table), expression)
}

func (d *EnhancedPostgreSQLDialect) CreateCoveringIndex(name string, table string, columns []string, includedColumns []string) string {
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = d.QuoteIdentifier(col)
	}

	sql := fmt.Sprintf("CREATE INDEX %s ON %s (%s)",
		d.QuoteIdentifier(name), d.QuoteIdentifier(table), strings.Join(quotedColumns, ", "))

	if len(includedColumns) > 0 && d.supportsVersion("11") {
		quotedIncluded := make([]string, len(includedColumns))
		for i, col := range includedColumns {
			quotedIncluded[i] = d.QuoteIdentifier(col)
		}
		sql += " INCLUDE (" + strings.Join(quotedIncluded, ", ") + ")"
	}

	return sql
}

func (d *EnhancedPostgreSQLDialect) CreateFunctionalIndex(name string, table string, function string) string {
	return fmt.Sprintf("CREATE INDEX %s ON %s ((%s))",
		d.QuoteIdentifier(name), d.QuoteIdentifier(table), function)
}

func (d *EnhancedPostgreSQLDialect) GetIndexUsageStats(indexName string) string {
	return fmt.Sprintf(`
		SELECT
			schemaname,
			tablename,
			indexname,
			idx_tup_read,
			idx_tup_fetch,
			idx_scan
		FROM pg_stat_user_indexes
		WHERE indexname = $1
	`)
}

func (d *EnhancedPostgreSQLDialect) GetIndexSizeInfo(indexName string) string {
	return fmt.Sprintf(`
		SELECT
			schemaname,
			tablename,
			indexname,
			pg_size_pretty(pg_relation_size(indexrelid)) as size
		FROM pg_stat_user_indexes
		WHERE indexname = $1
	`)
}

// Partitioning operations for PostgreSQL
func (d *EnhancedPostgreSQLDialect) CreatePartitionedTable(table string, partitionType PartitionType, partitionKey string) string {
	if !d.supportsVersion("10") {
		return ""
	}

	var partitionClause string
	switch partitionType {
	case PartitionTypeRange:
		partitionClause = fmt.Sprintf("PARTITION BY RANGE (%s)", partitionKey)
	case PartitionTypeList:
		partitionClause = fmt.Sprintf("PARTITION BY LIST (%s)", partitionKey)
	case PartitionTypeHash:
		partitionClause = fmt.Sprintf("PARTITION BY HASH (%s)", partitionKey)
	default:
		return ""
	}

	return fmt.Sprintf("CREATE TABLE %s (...) %s", d.QuoteIdentifier(table), partitionClause)
}

func (d *EnhancedPostgreSQLDialect) CreatePartition(parentTable string, partitionName string, values string) string {
	return fmt.Sprintf("CREATE TABLE %s PARTITION OF %s FOR VALUES %s",
		d.QuoteIdentifier(partitionName), d.QuoteIdentifier(parentTable), values)
}

func (d *EnhancedPostgreSQLDialect) DropPartition(table string, partitionName string) string {
	return fmt.Sprintf("DROP TABLE %s", d.QuoteIdentifier(partitionName))
}

func (d *EnhancedPostgreSQLDialect) AttachPartition(parentTable string, partitionTable string, values string) string {
	return fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION %s FOR VALUES %s",
		d.QuoteIdentifier(parentTable), d.QuoteIdentifier(partitionTable), values)
}

func (d *EnhancedPostgreSQLDialect) DetachPartition(parentTable string, partitionName string) string {
	return fmt.Sprintf("ALTER TABLE %s DETACH PARTITION %s",
		d.QuoteIdentifier(parentTable), d.QuoteIdentifier(partitionName))
}

func (d *EnhancedPostgreSQLDialect) ListPartitions(table string) string {
	return fmt.Sprintf(`
		SELECT
			schemaname,
			tablename as partition_name,
			tableowner,
			pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
		FROM pg_tables
		WHERE tablename LIKE %s || '%%'
	`, d.Placeholder(1))
}

func (d *EnhancedPostgreSQLDialect) PartitionPruningInfo(table string) string {
	return fmt.Sprintf(`
		SELECT
			schemaname,
			tablename,
			attname,
			n_distinct,
			correlation
		FROM pg_stats
		WHERE tablename = %s
	`, d.Placeholder(1))
}

// Bulk operations for PostgreSQL
func (d *EnhancedPostgreSQLDialect) BulkInsert(table string, columns []string, rows [][]interface{}) (string, error) {
	// PostgreSQL supports COPY for bulk inserts
	var sql strings.Builder
	sql.WriteString("COPY ")
	sql.WriteString(d.QuoteIdentifier(table))
	sql.WriteString(" (")

	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = d.QuoteIdentifier(col)
	}
	sql.WriteString(strings.Join(quotedColumns, ", "))
	sql.WriteString(") FROM STDIN")

	return sql.String(), nil
}

func (d *EnhancedPostgreSQLDialect) BulkUpdate(table string, updates []BulkUpdate) (string, error) {
	if len(updates) == 0 {
		return "", fmt.Errorf("no updates provided")
	}

	var sql strings.Builder
	sql.WriteString("UPDATE ")
	sql.WriteString(d.QuoteIdentifier(table))
	sql.WriteString(" SET ")

	// Use CASE statements for bulk updates
	setColumns := updates[0].SetColumns
	for i, col := range setColumns {
		if i > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString(d.QuoteIdentifier(col))
		sql.WriteString(" = CASE ")

		for j, update := range updates {
			sql.WriteString("WHEN ")
			sql.WriteString(update.Condition)
			sql.WriteString(" THEN ")
			sql.WriteString(d.Placeholder(j*len(setColumns) + i + 1))
			sql.WriteString(" ")
		}
		sql.WriteString("ELSE ")
		sql.WriteString(d.QuoteIdentifier(col))
		sql.WriteString(" END")
	}

	return sql.String(), nil
}

func (d *EnhancedPostgreSQLDialect) BulkDelete(table string, conditions []string) (string, error) {
	if len(conditions) == 0 {
		return "", fmt.Errorf("no conditions provided")
	}

	var sql strings.Builder
	sql.WriteString("DELETE FROM ")
	sql.WriteString(d.QuoteIdentifier(table))
	sql.WriteString(" WHERE ")
	sql.WriteString(strings.Join(conditions, " OR "))

	return sql.String(), nil
}

func (d *EnhancedPostgreSQLDialect) StreamingInsert(ctx context.Context, table string, columns []string, rows <-chan []interface{}) error {
	// This would require a database connection to implement properly
	// For now, return a placeholder implementation
	return fmt.Errorf("streaming insert requires database connection")
}

// Register the enhanced PostgreSQL dialect
func init() {
	RegisterExtendedDialect("postgresql", func() ExtendedDialect {
		return NewEnhancedPostgreSQLDialect("14", false)
	})
}