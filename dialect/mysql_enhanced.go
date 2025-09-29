package dialect

import (
	"fmt"
	"strings"
)

// EnhancedMySQLDialect provides advanced MySQL-specific features
type EnhancedMySQLDialect struct {
	*BaseDialect
	// Engine is the storage engine to use "InnoDB" vs "MyISAM"
	Engine string
	// Encoding is the character encoding to use for created tables
	Encoding string
	// Version represents the MySQL version (e.g., "8.0", "5.7")
	Version string
}

// NewEnhancedMySQLDialect creates a new enhanced MySQL dialect
func NewEnhancedMySQLDialect(engine, encoding, version string) *EnhancedMySQLDialect {
	return &EnhancedMySQLDialect{
		BaseDialect: NewBaseDialect("mysql"),
		Engine:      engine,
		Encoding:    encoding,
		Version:     version,
	}
}

// GetVersion returns the MySQL version
func (d *EnhancedMySQLDialect) GetVersion() string {
	return d.Version
}

// GetCapabilities returns MySQL-specific capabilities
func (d *EnhancedMySQLDialect) GetCapabilities() DialectCapabilities {
	return DialectCapabilities{
		// JSON support (MySQL 5.7+)
		SupportsJSON:         d.supportsVersion("5.7"),
		JSONType:             "JSON",
		JSONQueryFunction:    "JSON_EXTRACT",
		JSONModifyFunction:   "JSON_SET",
		SupportsJSONSchema:   d.supportsVersion("8.0"),

		// Arrays (not natively supported, but can use JSON arrays)
		SupportsArrays:       false,
		ArrayType:            "",
		ArrayQueryFunction:   "",

		// Advanced indexing
		SupportsPartialIndex: false, // MySQL doesn't support partial indexes
		SupportsExpressionIndex: d.supportsVersion("8.0"), // Functional indexes in 8.0+
		SupportedIndexTypes:  []IndexType{IndexTypeBTree, IndexTypeHash, IndexTypeFulltext, IndexTypeSpatial},
		SupportsIndexInclude: false,

		// Partitioning
		SupportsPartitioning: true,
		PartitioningTypes:    []PartitionType{PartitionTypeRange, PartitionTypeList, PartitionTypeHash, PartitionTypeKey},
		SupportsSubpartitions: true,

		// Transaction features
		SupportsNestedTransactions: false,
		SupportsSavepoints:         true,
		MaxNestedLevel:            0,

		// Advanced data types
		SupportsUUID:           false, // No native UUID type, use CHAR(36) or BINARY(16)
		SupportsHStore:         false,
		SupportsEnums:          true,
		SupportsGeneratedColumns: d.supportsVersion("5.7"),
		SupportsStoredGenerated:  d.supportsVersion("5.7"),
		SupportsVirtualGenerated: d.supportsVersion("5.7"),

		// Performance features
		SupportsBulkInsert:     true,
		SupportsUpsert:         true,
		UpsertSyntax:          UpsertOnDuplicateKey,
		SupportsBatchOperations: true,
		SupportsAsyncOperations: false,

		// Connection features
		SupportsConnectionPooling: true,
		SupportsReadReplicas:      true,
		SupportsSharding:          false, // Not native, requires external solutions

		// Security features
		SupportsRLS:              false, // No native RLS support
		SupportsTablespaceEncryption: d.supportsVersion("5.7"),
		SupportsColumnEncryption: false,
		SupportedAuthMethods:     []AuthMethod{AuthPassword, AuthCertificate, AuthLDAP},

		// Full-text search
		SupportsFTS:          true,
		FTSType:             FTSNative,
		FTSQueryLanguage:    "mysql",
		SupportsLanguages:   []string{"english", "german", "french", "spanish"},

		// Optimization features
		SupportsQueryPlan:    true,
		SupportsHints:        true,
		SupportsAnalyze:      true,
		SupportsVacuum:       false, // MySQL uses OPTIMIZE TABLE instead
	}
}

// SupportsFeature checks if a specific feature is supported
func (d *EnhancedMySQLDialect) SupportsFeature(feature Feature) bool {
	capabilities := d.GetCapabilities()

	switch feature {
	case FeatureJSON:
		return capabilities.SupportsJSON
	case FeatureJSONB:
		return false // MySQL doesn't have JSONB
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
func (d *EnhancedMySQLDialect) supportsVersion(minVersion string) bool {
	// Simplified version comparison - in production, use proper semver comparison
	return d.Version >= minVersion
}

// Override base methods for MySQL-specific behavior
func (d *EnhancedMySQLDialect) QuoteIdentifier(identifier string) string {
	return "`" + identifier + "`"
}

func (d *EnhancedMySQLDialect) Placeholder(index int) string {
	return "?"
}

func (d *EnhancedMySQLDialect) CreateTableSQL(table string, columns []ColumnDefinition) string {
	var sql strings.Builder
	sql.WriteString("CREATE TABLE ")
	sql.WriteString(d.QuoteIdentifier(table))
	sql.WriteString(" (")

	columnDefs := make([]string, len(columns))
	for i, col := range columns {
		columnDefs[i] = d.formatMySQLColumnDefinition(col)
	}
	sql.WriteString(strings.Join(columnDefs, ", "))
	sql.WriteString(")")

	// Add MySQL-specific table options
	if d.Engine != "" || d.Encoding != "" {
		sql.WriteString(" ENGINE=")
		sql.WriteString(d.Engine)
		sql.WriteString(" DEFAULT CHARSET=")
		sql.WriteString(d.Encoding)
	}

	return sql.String()
}

func (d *EnhancedMySQLDialect) formatMySQLColumnDefinition(col ColumnDefinition) string {
	var def strings.Builder
	def.WriteString(d.QuoteIdentifier(col.Name))
	def.WriteString(" ")
	def.WriteString(col.Type)

	if col.NotNull {
		def.WriteString(" NOT NULL")
	}

	if col.AutoIncrement {
		def.WriteString(" AUTO_INCREMENT")
	}

	if col.Default != nil {
		def.WriteString(" DEFAULT ")
		def.WriteString(*col.Default)
	}

	if col.PrimaryKey {
		def.WriteString(" PRIMARY KEY")
	}

	if col.Unique && !col.PrimaryKey {
		def.WriteString(" UNIQUE")
	}

	return def.String()
}

// JSON operations for MySQL
func (d *EnhancedMySQLDialect) CreateJSONColumn(name string, constraints string) string {
	sql := d.QuoteIdentifier(name) + " JSON"
	if constraints != "" {
		sql += " " + constraints
	}
	return sql
}

func (d *EnhancedMySQLDialect) JSONExtract(column string, path string) string {
	return fmt.Sprintf("JSON_EXTRACT(%s, %s)", column, path)
}

func (d *EnhancedMySQLDialect) JSONSet(column string, path string, value string) string {
	return fmt.Sprintf("JSON_SET(%s, %s, %s)", column, path, value)
}

func (d *EnhancedMySQLDialect) JSONArrayAppend(column string, path string, value string) string {
	return fmt.Sprintf("JSON_ARRAY_APPEND(%s, %s, %s)", column, path, value)
}

func (d *EnhancedMySQLDialect) JSONArrayInsert(column string, path string, value string) string {
	return fmt.Sprintf("JSON_ARRAY_INSERT(%s, %s, %s)", column, path, value)
}

func (d *EnhancedMySQLDialect) JSONRemove(column string, path string) string {
	return fmt.Sprintf("JSON_REMOVE(%s, %s)", column, path)
}

func (d *EnhancedMySQLDialect) JSONType(column string, path string) string {
	return fmt.Sprintf("JSON_TYPE(JSON_EXTRACT(%s, %s))", column, path)
}

func (d *EnhancedMySQLDialect) JSONValid(value string) string {
	return fmt.Sprintf("JSON_VALID(%s)", value)
}

func (d *EnhancedMySQLDialect) JSONSearch(column string, oneOrAll string, searchStr string, escapeChar string, path string) string {
	if path != "" {
		return fmt.Sprintf("JSON_SEARCH(%s, %s, %s, %s, %s)", column, oneOrAll, searchStr, escapeChar, path)
	}
	return fmt.Sprintf("JSON_SEARCH(%s, %s, %s, %s)", column, oneOrAll, searchStr, escapeChar)
}

// Upsert operations for MySQL
func (d *EnhancedMySQLDialect) BuildUpsert(table string, columns []string, conflictColumns []string, updateColumns []string) string {
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

	// Add ON DUPLICATE KEY UPDATE clause
	if len(updateColumns) > 0 {
		sql.WriteString(" ON DUPLICATE KEY UPDATE ")
		updates := make([]string, len(updateColumns))
		for i, col := range updateColumns {
			updates[i] = d.QuoteIdentifier(col) + " = VALUES(" + d.QuoteIdentifier(col) + ")"
		}
		sql.WriteString(strings.Join(updates, ", "))
	}

	return sql.String()
}

func (d *EnhancedMySQLDialect) BuildInsertIgnore(table string, columns []string) string {
	var sql strings.Builder
	sql.WriteString("INSERT IGNORE INTO ")
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

	return sql.String()
}

func (d *EnhancedMySQLDialect) BuildReplace(table string, columns []string) string {
	var sql strings.Builder
	sql.WriteString("REPLACE INTO ")
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

	return sql.String()
}

// Generated column operations for MySQL
func (d *EnhancedMySQLDialect) CreateStoredGeneratedColumn(name string, expression string, dataType string) string {
	return fmt.Sprintf("%s %s GENERATED ALWAYS AS (%s) STORED",
		d.QuoteIdentifier(name), dataType, expression)
}

func (d *EnhancedMySQLDialect) CreateVirtualGeneratedColumn(name string, expression string, dataType string) string {
	return fmt.Sprintf("%s %s GENERATED ALWAYS AS (%s) VIRTUAL",
		d.QuoteIdentifier(name), dataType, expression)
}

func (d *EnhancedMySQLDialect) AlterAddGeneratedColumn(table string, column string, expression string, dataType string, stored bool) string {
	columnType := "VIRTUAL"
	if stored {
		columnType = "STORED"
	}
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s GENERATED ALWAYS AS (%s) %s",
		d.QuoteIdentifier(table), d.QuoteIdentifier(column), dataType, expression, columnType)
}

func (d *EnhancedMySQLDialect) DropGeneratedColumn(table string, column string) string {
	return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", d.QuoteIdentifier(table), d.QuoteIdentifier(column))
}

// Advanced indexing operations for MySQL
func (d *EnhancedMySQLDialect) CreatePartialIndex(name string, table string, columns []string, condition string) string {
	// MySQL doesn't support partial indexes, return empty string
	return ""
}

func (d *EnhancedMySQLDialect) CreateExpressionIndex(name string, table string, expression string) string {
	if !d.supportsVersion("8.0") {
		return ""
	}
	return fmt.Sprintf("CREATE INDEX %s ON %s ((%s))",
		d.QuoteIdentifier(name), d.QuoteIdentifier(table), expression)
}

func (d *EnhancedMySQLDialect) CreateCoveringIndex(name string, table string, columns []string, includedColumns []string) string {
	// MySQL doesn't have INCLUDE syntax, just create regular composite index
	allColumns := append(columns, includedColumns...)
	quotedColumns := make([]string, len(allColumns))
	for i, col := range allColumns {
		quotedColumns[i] = d.QuoteIdentifier(col)
	}
	return fmt.Sprintf("CREATE INDEX %s ON %s (%s)",
		d.QuoteIdentifier(name), d.QuoteIdentifier(table), strings.Join(quotedColumns, ", "))
}

func (d *EnhancedMySQLDialect) CreateFunctionalIndex(name string, table string, function string) string {
	if !d.supportsVersion("8.0") {
		return ""
	}
	return fmt.Sprintf("CREATE INDEX %s ON %s ((%s))",
		d.QuoteIdentifier(name), d.QuoteIdentifier(table), function)
}

func (d *EnhancedMySQLDialect) GetIndexUsageStats(indexName string) string {
	return fmt.Sprintf(`
		SELECT
			table_schema,
			table_name,
			index_name,
			cardinality,
			sub_part,
			packed,
			nullable,
			index_type
		FROM information_schema.statistics
		WHERE index_name = %s
	`, d.Placeholder(1))
}

func (d *EnhancedMySQLDialect) GetIndexSizeInfo(indexName string) string {
	return fmt.Sprintf(`
		SELECT
			table_schema,
			table_name,
			index_name,
			round(stat_value * @@innodb_page_size / 1024 / 1024, 2) as size_mb
		FROM mysql.innodb_index_stats
		WHERE index_name = %s AND stat_name = 'size'
	`, d.Placeholder(1))
}

// Partitioning operations for MySQL
func (d *EnhancedMySQLDialect) CreatePartitionedTable(table string, partitionType PartitionType, partitionKey string) string {
	var partitionClause string
	switch partitionType {
	case PartitionTypeRange:
		partitionClause = fmt.Sprintf("PARTITION BY RANGE (%s)", partitionKey)
	case PartitionTypeList:
		partitionClause = fmt.Sprintf("PARTITION BY LIST (%s)", partitionKey)
	case PartitionTypeHash:
		partitionClause = fmt.Sprintf("PARTITION BY HASH (%s)", partitionKey)
	case PartitionTypeKey:
		partitionClause = fmt.Sprintf("PARTITION BY KEY (%s)", partitionKey)
	default:
		return ""
	}

	return fmt.Sprintf("CREATE TABLE %s (...) %s", d.QuoteIdentifier(table), partitionClause)
}

func (d *EnhancedMySQLDialect) CreatePartition(parentTable string, partitionName string, values string) string {
	return fmt.Sprintf("ALTER TABLE %s ADD PARTITION (PARTITION %s VALUES %s)",
		d.QuoteIdentifier(parentTable), d.QuoteIdentifier(partitionName), values)
}

func (d *EnhancedMySQLDialect) DropPartition(table string, partitionName string) string {
	return fmt.Sprintf("ALTER TABLE %s DROP PARTITION %s",
		d.QuoteIdentifier(table), d.QuoteIdentifier(partitionName))
}

func (d *EnhancedMySQLDialect) AttachPartition(parentTable string, partitionTable string, values string) string {
	// MySQL doesn't have ATTACH PARTITION like PostgreSQL
	return ""
}

func (d *EnhancedMySQLDialect) DetachPartition(parentTable string, partitionName string) string {
	// MySQL doesn't have DETACH PARTITION like PostgreSQL
	return ""
}

func (d *EnhancedMySQLDialect) ListPartitions(table string) string {
	return fmt.Sprintf(`
		SELECT
			partition_name,
			partition_expression,
			partition_description,
			table_rows,
			avg_row_length,
			data_length
		FROM information_schema.partitions
		WHERE table_name = %s AND partition_name IS NOT NULL
	`, d.Placeholder(1))
}

func (d *EnhancedMySQLDialect) PartitionPruningInfo(table string) string {
	return fmt.Sprintf(`
		SELECT
			partition_name,
			partition_expression,
			partition_description
		FROM information_schema.partitions
		WHERE table_name = %s AND partition_name IS NOT NULL
	`, d.Placeholder(1))
}

// Register the enhanced MySQL dialect
func init() {
	RegisterExtendedDialect("mysql", func() ExtendedDialect {
		return NewEnhancedMySQLDialect("InnoDB", "utf8mb4", "8.0")
	})
}