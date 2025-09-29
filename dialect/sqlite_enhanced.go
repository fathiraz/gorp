package dialect

import (
	"context"
	"fmt"
	"strings"
)

// EnhancedSQLiteDialect provides advanced SQLite-specific features
type EnhancedSQLiteDialect struct {
	*BaseDialect
	// Version represents the SQLite version (e.g., "3.38", "3.37")
	Version string
	// WALMode enables WAL (Write-Ahead Logging) mode
	WALMode bool
	// ForeignKeys enables foreign key constraints
	ForeignKeys bool
	// JournalMode specifies the journal mode
	JournalMode string
}

// SQLitePragmaSettings holds SQLite PRAGMA configuration
type SQLitePragmaSettings struct {
	JournalMode     string // DELETE, WAL, MEMORY, etc.
	Synchronous     string // OFF, NORMAL, FULL, EXTRA
	CacheSize       int    // Page cache size
	TempStore       string // DEFAULT, FILE, MEMORY
	MmapSize        int64  // Memory-mapped I/O size
	PageSize        int    // Database page size
	ForeignKeys     bool   // Enable foreign key constraints
	AutoVacuum      string // NONE, FULL, INCREMENTAL
	BusyTimeout     int    // Busy timeout in milliseconds
	JournalSizeLimit int64 // Journal size limit
}

// NewEnhancedSQLiteDialect creates a new enhanced SQLite dialect
func NewEnhancedSQLiteDialect(version string) *EnhancedSQLiteDialect {
	return &EnhancedSQLiteDialect{
		BaseDialect: NewBaseDialect("sqlite"),
		Version:     version,
		WALMode:     false,
		ForeignKeys: true,
		JournalMode: "DELETE",
	}
}

// GetVersion returns the SQLite version
func (d *EnhancedSQLiteDialect) GetVersion() string {
	return d.Version
}

// GetCapabilities returns SQLite-specific capabilities
func (d *EnhancedSQLiteDialect) GetCapabilities() DialectCapabilities {
	return DialectCapabilities{
		// JSON support (SQLite 3.38+)
		SupportsJSON:         d.supportsVersion("3.38"),
		JSONType:             "TEXT", // SQLite stores JSON as TEXT with validation
		JSONQueryFunction:    "json_extract",
		JSONModifyFunction:   "json_set",
		SupportsJSONSchema:   false,

		// Arrays (not natively supported, but can use JSON arrays)
		SupportsArrays:       false,
		ArrayType:            "",
		ArrayQueryFunction:   "",

		// Advanced indexing
		SupportsPartialIndex: true,
		SupportsExpressionIndex: true,
		SupportedIndexTypes:  []IndexType{IndexTypeBTree}, // SQLite only supports B-tree indexes
		SupportsIndexInclude: false,

		// Partitioning
		SupportsPartitioning: false, // SQLite doesn't support table partitioning
		PartitioningTypes:    []PartitionType{},
		SupportsSubpartitions: false,

		// Transaction features
		SupportsNestedTransactions: false,
		SupportsSavepoints:         true,
		MaxNestedLevel:            0,

		// Advanced data types
		SupportsUUID:           false, // No native UUID type, use TEXT
		SupportsHStore:         false,
		SupportsEnums:          false, // No native enum type
		SupportsGeneratedColumns: d.supportsVersion("3.31"), // Generated columns in 3.31+
		SupportsStoredGenerated:  d.supportsVersion("3.31"),
		SupportsVirtualGenerated: d.supportsVersion("3.31"),

		// Performance features
		SupportsBulkInsert:     true,
		SupportsUpsert:         d.supportsVersion("3.24"), // UPSERT in 3.24+
		UpsertSyntax:          UpsertOnConflict,
		SupportsBatchOperations: true,
		SupportsAsyncOperations: false,

		// Connection features
		SupportsConnectionPooling: false, // SQLite is embedded, no connection pooling
		SupportsReadReplicas:      false,
		SupportsSharding:          false,

		// Security features
		SupportsRLS:              false, // No row-level security
		SupportsTablespaceEncryption: true, // SQLite supports database encryption via extensions
		SupportsColumnEncryption: false,
		SupportedAuthMethods:     []AuthMethod{}, // No authentication in embedded mode

		// Full-text search
		SupportsFTS:          true,
		FTSType:             FTSExtension, // Via FTS5 extension
		FTSQueryLanguage:    "sqlite",
		SupportsLanguages:   []string{"english", "porter"},

		// Optimization features
		SupportsQueryPlan:    true,
		SupportsHints:        false,
		SupportsAnalyze:      true,
		SupportsVacuum:       true,
	}
}

// SupportsFeature checks if a specific feature is supported
func (d *EnhancedSQLiteDialect) SupportsFeature(feature Feature) bool {
	capabilities := d.GetCapabilities()

	switch feature {
	case FeatureJSON:
		return capabilities.SupportsJSON
	case FeatureJSONB:
		return false // SQLite doesn't have JSONB
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
func (d *EnhancedSQLiteDialect) supportsVersion(minVersion string) bool {
	// Simplified version comparison - in production, use proper semver comparison
	return d.Version >= minVersion
}

// Override base methods for SQLite-specific behavior
func (d *EnhancedSQLiteDialect) QuoteIdentifier(identifier string) string {
	return `"` + identifier + `"`
}

func (d *EnhancedSQLiteDialect) Placeholder(index int) string {
	return "?"
}

// PRAGMA management for SQLite
func (d *EnhancedSQLiteDialect) GetDefaultPragmaSettings() SQLitePragmaSettings {
	return SQLitePragmaSettings{
		JournalMode:     "WAL",
		Synchronous:     "NORMAL",
		CacheSize:       -64000, // 64MB
		TempStore:       "MEMORY",
		MmapSize:        268435456, // 256MB
		PageSize:        4096,
		ForeignKeys:     true,
		AutoVacuum:      "INCREMENTAL",
		BusyTimeout:     5000, // 5 seconds
		JournalSizeLimit: 67108864, // 64MB
	}
}

func (d *EnhancedSQLiteDialect) ApplyPragmaSettings(settings SQLitePragmaSettings) []string {
	var pragmas []string

	if settings.JournalMode != "" {
		pragmas = append(pragmas, fmt.Sprintf("PRAGMA journal_mode = %s", settings.JournalMode))
	}

	if settings.Synchronous != "" {
		pragmas = append(pragmas, fmt.Sprintf("PRAGMA synchronous = %s", settings.Synchronous))
	}

	if settings.CacheSize != 0 {
		pragmas = append(pragmas, fmt.Sprintf("PRAGMA cache_size = %d", settings.CacheSize))
	}

	if settings.TempStore != "" {
		pragmas = append(pragmas, fmt.Sprintf("PRAGMA temp_store = %s", settings.TempStore))
	}

	if settings.MmapSize > 0 {
		pragmas = append(pragmas, fmt.Sprintf("PRAGMA mmap_size = %d", settings.MmapSize))
	}

	if settings.PageSize > 0 {
		pragmas = append(pragmas, fmt.Sprintf("PRAGMA page_size = %d", settings.PageSize))
	}

	pragmas = append(pragmas, fmt.Sprintf("PRAGMA foreign_keys = %t", settings.ForeignKeys))

	if settings.AutoVacuum != "" {
		pragmas = append(pragmas, fmt.Sprintf("PRAGMA auto_vacuum = %s", settings.AutoVacuum))
	}

	if settings.BusyTimeout > 0 {
		pragmas = append(pragmas, fmt.Sprintf("PRAGMA busy_timeout = %d", settings.BusyTimeout))
	}

	if settings.JournalSizeLimit > 0 {
		pragmas = append(pragmas, fmt.Sprintf("PRAGMA journal_size_limit = %d", settings.JournalSizeLimit))
	}

	return pragmas
}

func (d *EnhancedSQLiteDialect) OptimizeForWAL() []string {
	return []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
		"PRAGMA cache_size = -64000", // 64MB cache
		"PRAGMA temp_store = MEMORY",
		"PRAGMA mmap_size = 268435456", // 256MB mmap
		"PRAGMA wal_autocheckpoint = 1000",
		"PRAGMA wal_checkpoint(TRUNCATE)",
	}
}

func (d *EnhancedSQLiteDialect) OptimizeForConcurrency() []string {
	return []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
		"PRAGMA busy_timeout = 10000", // 10 seconds
		"PRAGMA cache_size = -128000", // 128MB cache
		"PRAGMA temp_store = MEMORY",
		"PRAGMA mmap_size = 536870912", // 512MB mmap
	}
}

func (d *EnhancedSQLiteDialect) OptimizeForPerformance() []string {
	return []string{
		"PRAGMA journal_mode = MEMORY",
		"PRAGMA synchronous = OFF",
		"PRAGMA cache_size = -256000", // 256MB cache
		"PRAGMA temp_store = MEMORY",
		"PRAGMA mmap_size = 1073741824", // 1GB mmap
		"PRAGMA locking_mode = EXCLUSIVE",
	}
}

// JSON operations for SQLite
func (d *EnhancedSQLiteDialect) CreateJSONColumn(name string, constraints string) string {
	sql := d.QuoteIdentifier(name) + " TEXT"
	if d.supportsVersion("3.38") {
		sql += " CHECK(json_valid(" + d.QuoteIdentifier(name) + "))"
	}
	if constraints != "" {
		sql += " " + constraints
	}
	return sql
}

func (d *EnhancedSQLiteDialect) JSONExtract(column string, path string) string {
	return fmt.Sprintf("json_extract(%s, %s)", column, path)
}

func (d *EnhancedSQLiteDialect) JSONSet(column string, path string, value string) string {
	return fmt.Sprintf("json_set(%s, %s, %s)", column, path, value)
}

func (d *EnhancedSQLiteDialect) JSONArrayAppend(column string, path string, value string) string {
	// SQLite doesn't have json_array_append, use json_set with array manipulation
	return fmt.Sprintf("json_set(%s, %s || '[#]', %s)", column, path, value)
}

func (d *EnhancedSQLiteDialect) JSONArrayInsert(column string, path string, value string) string {
	return fmt.Sprintf("json_insert(%s, %s, %s)", column, path, value)
}

func (d *EnhancedSQLiteDialect) JSONRemove(column string, path string) string {
	return fmt.Sprintf("json_remove(%s, %s)", column, path)
}

func (d *EnhancedSQLiteDialect) JSONType(column string, path string) string {
	return fmt.Sprintf("json_type(%s, %s)", column, path)
}

func (d *EnhancedSQLiteDialect) JSONValid(value string) string {
	return fmt.Sprintf("json_valid(%s)", value)
}

func (d *EnhancedSQLiteDialect) JSONSearch(column string, oneOrAll string, searchStr string, escapeChar string, path string) string {
	// SQLite doesn't have a direct json_search function, use alternative approach
	if path != "" {
		return fmt.Sprintf("json_extract(%s, %s) LIKE %s", column, path, searchStr)
	}
	return fmt.Sprintf("%s LIKE %s", column, searchStr)
}

// Upsert operations for SQLite
func (d *EnhancedSQLiteDialect) BuildUpsert(table string, columns []string, conflictColumns []string, updateColumns []string) string {
	if !d.supportsVersion("3.24") {
		// Fallback to INSERT OR REPLACE for older versions
		return d.BuildReplace(table, columns)
	}

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
				updates[i] = d.QuoteIdentifier(col) + " = excluded." + d.QuoteIdentifier(col)
			}
			sql.WriteString(strings.Join(updates, ", "))
		} else {
			sql.WriteString(" DO NOTHING")
		}
	}

	return sql.String()
}

func (d *EnhancedSQLiteDialect) BuildInsertIgnore(table string, columns []string) string {
	var sql strings.Builder
	sql.WriteString("INSERT OR IGNORE INTO ")
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

func (d *EnhancedSQLiteDialect) BuildReplace(table string, columns []string) string {
	var sql strings.Builder
	sql.WriteString("INSERT OR REPLACE INTO ")
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

// Generated column operations for SQLite
func (d *EnhancedSQLiteDialect) CreateStoredGeneratedColumn(name string, expression string, dataType string) string {
	if !d.supportsVersion("3.31") {
		return ""
	}
	return fmt.Sprintf("%s %s GENERATED ALWAYS AS (%s) STORED",
		d.QuoteIdentifier(name), dataType, expression)
}

func (d *EnhancedSQLiteDialect) CreateVirtualGeneratedColumn(name string, expression string, dataType string) string {
	if !d.supportsVersion("3.31") {
		return ""
	}
	return fmt.Sprintf("%s %s GENERATED ALWAYS AS (%s) VIRTUAL",
		d.QuoteIdentifier(name), dataType, expression)
}

func (d *EnhancedSQLiteDialect) AlterAddGeneratedColumn(table string, column string, expression string, dataType string, stored bool) string {
	if !d.supportsVersion("3.31") {
		return ""
	}
	columnType := "VIRTUAL"
	if stored {
		columnType = "STORED"
	}
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s GENERATED ALWAYS AS (%s) %s",
		d.QuoteIdentifier(table), d.QuoteIdentifier(column), dataType, expression, columnType)
}

func (d *EnhancedSQLiteDialect) DropGeneratedColumn(table string, column string) string {
	return fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", d.QuoteIdentifier(table), d.QuoteIdentifier(column))
}

// Advanced indexing operations for SQLite
func (d *EnhancedSQLiteDialect) CreatePartialIndex(name string, table string, columns []string, condition string) string {
	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = d.QuoteIdentifier(col)
	}
	return fmt.Sprintf("CREATE INDEX %s ON %s (%s) WHERE %s",
		d.QuoteIdentifier(name), d.QuoteIdentifier(table), strings.Join(quotedColumns, ", "), condition)
}

func (d *EnhancedSQLiteDialect) CreateExpressionIndex(name string, table string, expression string) string {
	return fmt.Sprintf("CREATE INDEX %s ON %s (%s)",
		d.QuoteIdentifier(name), d.QuoteIdentifier(table), expression)
}

func (d *EnhancedSQLiteDialect) CreateCoveringIndex(name string, table string, columns []string, includedColumns []string) string {
	// SQLite doesn't have INCLUDE syntax, create composite index
	allColumns := append(columns, includedColumns...)
	quotedColumns := make([]string, len(allColumns))
	for i, col := range allColumns {
		quotedColumns[i] = d.QuoteIdentifier(col)
	}
	return fmt.Sprintf("CREATE INDEX %s ON %s (%s)",
		d.QuoteIdentifier(name), d.QuoteIdentifier(table), strings.Join(quotedColumns, ", "))
}

func (d *EnhancedSQLiteDialect) CreateFunctionalIndex(name string, table string, function string) string {
	return fmt.Sprintf("CREATE INDEX %s ON %s (%s)",
		d.QuoteIdentifier(name), d.QuoteIdentifier(table), function)
}

func (d *EnhancedSQLiteDialect) GetIndexUsageStats(indexName string) string {
	return "SELECT name, sql FROM sqlite_master WHERE type='index' AND name = ?"
}

func (d *EnhancedSQLiteDialect) GetIndexSizeInfo(indexName string) string {
	return "SELECT name, sql FROM sqlite_master WHERE type='index' AND name = ?"
}

// FTS operations for SQLite
func (d *EnhancedSQLiteDialect) CreateFTSTable(table string, columns []string, ftsVersion string) string {
	if ftsVersion == "" {
		ftsVersion = "fts5"
	}

	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = d.QuoteIdentifier(col)
	}

	return fmt.Sprintf("CREATE VIRTUAL TABLE %s USING %s(%s)",
		d.QuoteIdentifier(table), ftsVersion, strings.Join(quotedColumns, ", "))
}

func (d *EnhancedSQLiteDialect) FTSQuery(table string, query string) string {
	return fmt.Sprintf("SELECT * FROM %s WHERE %s MATCH ?",
		d.QuoteIdentifier(table), d.QuoteIdentifier(table))
}

func (d *EnhancedSQLiteDialect) FTSRank(table string, query string, rankFunction string) string {
	if rankFunction == "" {
		rankFunction = "bm25"
	}
	return fmt.Sprintf("SELECT *, %s(%s) as rank FROM %s WHERE %s MATCH ? ORDER BY rank",
		rankFunction, d.QuoteIdentifier(table), d.QuoteIdentifier(table), d.QuoteIdentifier(table))
}

// Bulk operations for SQLite
func (d *EnhancedSQLiteDialect) BulkInsert(table string, columns []string, rows [][]interface{}) (string, error) {
	// SQLite supports multi-row inserts
	var sql strings.Builder
	sql.WriteString("INSERT INTO ")
	sql.WriteString(d.QuoteIdentifier(table))
	sql.WriteString(" (")

	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = d.QuoteIdentifier(col)
	}
	sql.WriteString(strings.Join(quotedColumns, ", "))
	sql.WriteString(") VALUES ")

	valueSets := make([]string, len(rows))
	for i := range rows {
		placeholders := make([]string, len(columns))
		for j := range placeholders {
			placeholders[j] = "?"
		}
		valueSets[i] = "(" + strings.Join(placeholders, ", ") + ")"
	}
	sql.WriteString(strings.Join(valueSets, ", "))

	return sql.String(), nil
}

func (d *EnhancedSQLiteDialect) BulkUpdate(table string, updates []BulkUpdate) (string, error) {
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

		for _, update := range updates {
			sql.WriteString("WHEN ")
			sql.WriteString(update.Condition)
			sql.WriteString(" THEN ? ")
		}
		sql.WriteString("ELSE ")
		sql.WriteString(d.QuoteIdentifier(col))
		sql.WriteString(" END")
	}

	return sql.String(), nil
}

func (d *EnhancedSQLiteDialect) BulkDelete(table string, conditions []string) (string, error) {
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

func (d *EnhancedSQLiteDialect) StreamingInsert(ctx context.Context, table string, columns []string, rows <-chan []interface{}) error {
	// This would require a database connection to implement properly
	return fmt.Errorf("streaming insert requires database connection")
}

// Register the enhanced SQLite dialect
func init() {
	RegisterExtendedDialect("sqlite", func() ExtendedDialect {
		return NewEnhancedSQLiteDialect("3.38")
	})
}