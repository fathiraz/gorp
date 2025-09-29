package dialect

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"time"
)

// ExtendedDialect extends the base Dialect interface with advanced database-specific features
type ExtendedDialect interface {
	Dialect // Embed the base dialect interface

	// Enhanced features
	GetCapabilities() DialectCapabilities
	GetVersion() string
	SupportsFeature(feature Feature) bool
}

// DialectCapabilities defines what features a database dialect supports
type DialectCapabilities struct {
	// JSON support
	SupportsJSON         bool
	JSONType             string
	JSONQueryFunction    string
	JSONModifyFunction   string
	SupportsJSONSchema   bool

	// Array support
	SupportsArrays       bool
	ArrayType            string
	ArrayQueryFunction   string

	// Advanced indexing
	SupportsPartialIndex bool
	SupportsExpressionIndex bool
	SupportedIndexTypes  []IndexType
	SupportsIndexInclude bool

	// Partitioning
	SupportsPartitioning bool
	PartitioningTypes    []PartitionType
	SupportsSubpartitions bool

	// Transaction features
	SupportsNestedTransactions bool
	SupportsSavepoints         bool
	MaxNestedLevel            int

	// Advanced data types
	SupportsUUID           bool
	SupportsHStore         bool
	SupportsEnums          bool
	SupportsGeneratedColumns bool
	SupportsStoredGenerated  bool
	SupportsVirtualGenerated bool

	// Performance features
	SupportsBulkInsert     bool
	SupportsUpsert         bool
	UpsertSyntax          UpsertType
	SupportsBatchOperations bool
	SupportsAsyncOperations bool

	// Connection features
	SupportsConnectionPooling bool
	SupportsReadReplicas      bool
	SupportsSharding          bool

	// Security features
	SupportsRLS              bool // Row Level Security
	SupportsTablespaceEncryption bool
	SupportsColumnEncryption bool
	SupportedAuthMethods     []AuthMethod

	// Full-text search
	SupportsFTS          bool
	FTSType             FTSType
	FTSQueryLanguage    string
	SupportsLanguages   []string

	// Optimization features
	SupportsQueryPlan    bool
	SupportsHints        bool
	SupportsAnalyze      bool
	SupportsVacuum       bool
}

// Feature represents a database feature
type Feature string

const (
	FeatureJSON              Feature = "json"
	FeatureJSONB             Feature = "jsonb"
	FeatureArrays            Feature = "arrays"
	FeaturePartitioning      Feature = "partitioning"
	FeatureUpsert            Feature = "upsert"
	FeatureGeneratedColumns  Feature = "generated_columns"
	FeatureFTS               Feature = "full_text_search"
	FeatureHStore            Feature = "hstore"
	FeatureUUID              Feature = "uuid"
	FeatureRLS               Feature = "row_level_security"
	FeatureBulkInsert        Feature = "bulk_insert"
	FeaturePartialIndex      Feature = "partial_index"
	FeatureExpressionIndex   Feature = "expression_index"
	FeatureNestedTransactions Feature = "nested_transactions"
	FeatureAsyncOperations   Feature = "async_operations"
)

// IndexType represents different types of database indexes
type IndexType string

const (
	IndexTypeBTree   IndexType = "btree"
	IndexTypeHash    IndexType = "hash"
	IndexTypeGIN     IndexType = "gin"
	IndexTypeGiST    IndexType = "gist"
	IndexTypeBRIN    IndexType = "brin"
	IndexTypeSpatial IndexType = "spatial"
	IndexTypeFulltext IndexType = "fulltext"
	IndexTypeInverted IndexType = "inverted"
	IndexTypeColumnstore IndexType = "columnstore"
)

// PartitionType represents different partitioning strategies
type PartitionType string

const (
	PartitionTypeRange PartitionType = "range"
	PartitionTypeList  PartitionType = "list"
	PartitionTypeHash  PartitionType = "hash"
	PartitionTypeKey   PartitionType = "key"
)

// UpsertType represents different upsert syntaxes
type UpsertType string

const (
	UpsertOnDuplicateKey UpsertType = "on_duplicate_key"
	UpsertOnConflict     UpsertType = "on_conflict"
	UpsertMerge          UpsertType = "merge"
	UpsertReplace        UpsertType = "replace"
)

// AuthMethod represents authentication methods
type AuthMethod string

const (
	AuthPassword     AuthMethod = "password"
	AuthCertificate  AuthMethod = "certificate"
	AuthOAuth        AuthMethod = "oauth"
	AuthKerberos     AuthMethod = "kerberos"
	AuthLDAP         AuthMethod = "ldap"
	AuthActiveDirectory AuthMethod = "active_directory"
)

// FTSType represents full-text search implementations
type FTSType string

const (
	FTSNative     FTSType = "native"
	FTSPlugin     FTSType = "plugin"
	FTSExtension  FTSType = "extension"
	FTSIntegrated FTSType = "integrated"
)

// JSONDialect provides JSON operations for databases that support it
type JSONDialect interface {
	// JSON column operations
	CreateJSONColumn(name string, constraints string) string
	JSONExtract(column string, path string) string
	JSONSet(column string, path string, value string) string
	JSONArrayAppend(column string, path string, value string) string
	JSONArrayInsert(column string, path string, value string) string
	JSONRemove(column string, path string) string
	JSONType(column string, path string) string
	JSONValid(value string) string
	JSONSearch(column string, oneOrAll string, searchStr string, escapeChar string, path string) string
}

// ArrayDialect provides array operations for databases that support them
type ArrayDialect interface {
	// Array operations
	CreateArrayColumn(elementType string, dimensions int) string
	ArrayAppend(array string, element string) string
	ArrayPrepend(element string, array string) string
	ArrayConcat(array1 string, array2 string) string
	ArrayLength(array string, dimension int) string
	ArrayPosition(array string, element string) string
	ArrayRemove(array string, element string) string
	ArrayReplace(array string, from string, to string) string
	ArrayToString(array string, delimiter string) string
	StringToArray(str string, delimiter string) string
}

// PartitioningDialect provides partitioning operations
type PartitioningDialect interface {
	// Partitioning operations
	CreatePartitionedTable(table string, partitionType PartitionType, partitionKey string) string
	CreatePartition(parentTable string, partitionName string, values string) string
	DropPartition(table string, partitionName string) string
	AttachPartition(parentTable string, partitionTable string, values string) string
	DetachPartition(parentTable string, partitionName string) string
	ListPartitions(table string) string
	PartitionPruningInfo(table string) string
}

// UpsertDialect provides upsert (insert or update) operations
type UpsertDialect interface {
	// Upsert operations
	BuildUpsert(table string, columns []string, conflictColumns []string, updateColumns []string) string
	BuildInsertIgnore(table string, columns []string) string
	BuildReplace(table string, columns []string) string
}

// AdvancedIndexDialect provides advanced indexing operations
type AdvancedIndexDialect interface {
	// Advanced indexing
	CreatePartialIndex(name string, table string, columns []string, condition string) string
	CreateExpressionIndex(name string, table string, expression string) string
	CreateCoveringIndex(name string, table string, columns []string, includedColumns []string) string
	CreateFunctionalIndex(name string, table string, function string) string
	GetIndexUsageStats(indexName string) string
	GetIndexSizeInfo(indexName string) string
}

// BulkOperationDialect provides bulk operation support
type BulkOperationDialect interface {
	// Bulk operations
	BulkInsert(table string, columns []string, rows [][]interface{}) (string, error)
	BulkUpdate(table string, updates []BulkUpdate) (string, error)
	BulkDelete(table string, conditions []string) (string, error)
	StreamingInsert(ctx context.Context, table string, columns []string, rows <-chan []interface{}) error
}

// BulkUpdate represents a bulk update operation
type BulkUpdate struct {
	SetColumns []string
	SetValues  []interface{}
	Condition  string
}

// GeneratedColumnDialect provides generated column support
type GeneratedColumnDialect interface {
	// Generated columns
	CreateStoredGeneratedColumn(name string, expression string, dataType string) string
	CreateVirtualGeneratedColumn(name string, expression string, dataType string) string
	AlterAddGeneratedColumn(table string, column string, expression string, dataType string, stored bool) string
	DropGeneratedColumn(table string, column string) string
}

// ExtendedDialectRegistry manages extended dialects
type ExtendedDialectRegistry struct {
	dialects map[string]func() ExtendedDialect
}

// NewExtendedDialectRegistry creates a new extended dialect registry
func NewExtendedDialectRegistry() *ExtendedDialectRegistry {
	return &ExtendedDialectRegistry{
		dialects: make(map[string]func() ExtendedDialect),
	}
}

// Register registers a new extended dialect
func (r *ExtendedDialectRegistry) Register(name string, factory func() ExtendedDialect) {
	r.dialects[name] = factory
}

// Get returns an extended dialect by name
func (r *ExtendedDialectRegistry) Get(name string) (ExtendedDialect, error) {
	factory, exists := r.dialects[name]
	if !exists {
		return nil, fmt.Errorf("extended dialect '%s' not found", name)
	}
	return factory(), nil
}

// List returns all registered extended dialect names
func (r *ExtendedDialectRegistry) List() []string {
	names := make([]string, 0, len(r.dialects))
	for name := range r.dialects {
		names = append(names, name)
	}
	return names
}

// EnhancedRegistry is the global extended dialect registry
var EnhancedRegistry = NewExtendedDialectRegistry()

// GetExtendedDialect returns an extended dialect from the enhanced registry
func GetExtendedDialect(name string) (ExtendedDialect, error) {
	return EnhancedRegistry.Get(name)
}

// RegisterExtendedDialect registers an extended dialect in the enhanced registry
func RegisterExtendedDialect(name string, factory func() ExtendedDialect) {
	EnhancedRegistry.Register(name, factory)
}

// DialectFeatureMatrix provides a feature comparison matrix
type DialectFeatureMatrix map[string]map[Feature]bool

// GetFeatureMatrix returns the feature support matrix for all registered extended dialects
func GetFeatureMatrix() DialectFeatureMatrix {
	matrix := make(DialectFeatureMatrix)

	for _, name := range EnhancedRegistry.List() {
		dialect, err := EnhancedRegistry.Get(name)
		if err != nil {
			continue
		}

		features := make(map[Feature]bool)
		allFeatures := []Feature{
			FeatureJSON, FeatureJSONB, FeatureArrays, FeaturePartitioning,
			FeatureUpsert, FeatureGeneratedColumns, FeatureFTS, FeatureHStore,
			FeatureUUID, FeatureRLS, FeatureBulkInsert, FeaturePartialIndex,
			FeatureExpressionIndex, FeatureNestedTransactions, FeatureAsyncOperations,
		}

		for _, feature := range allFeatures {
			features[feature] = dialect.SupportsFeature(feature)
		}

		matrix[name] = features
	}

	return matrix
}

// PerformanceMetrics tracks dialect-specific performance metrics
type PerformanceMetrics struct {
	DialectName        string
	QueryExecutionTime time.Duration
	ConnectionTime     time.Duration
	BulkInsertRate     float64 // rows per second
	IndexSeekTime      time.Duration
	MemoryUsage        int64
	Timestamp          time.Time
}

// DialectBenchmark provides benchmarking capabilities for dialects
type DialectBenchmark struct {
	dialect ExtendedDialect
	db      *sql.DB
	metrics []PerformanceMetrics
}

// NewDialectBenchmark creates a new benchmark instance
func NewDialectBenchmark(dialect ExtendedDialect, db *sql.DB) *DialectBenchmark {
	return &DialectBenchmark{
		dialect: dialect,
		db:      db,
		metrics: make([]PerformanceMetrics, 0),
	}
}

// BenchmarkQuery benchmarks query execution
func (b *DialectBenchmark) BenchmarkQuery(query string, args ...interface{}) (*PerformanceMetrics, error) {
	start := time.Now()

	rows, err := b.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Consume all rows
	for rows.Next() {
		// Do nothing, just consume
	}

	duration := time.Since(start)

	metric := &PerformanceMetrics{
		DialectName:        b.dialect.Name(),
		QueryExecutionTime: duration,
		Timestamp:          time.Now(),
	}

	b.metrics = append(b.metrics, *metric)
	return metric, rows.Err()
}

// GetMetrics returns all collected metrics
func (b *DialectBenchmark) GetMetrics() []PerformanceMetrics {
	return b.metrics
}

// ClearMetrics clears all collected metrics
func (b *DialectBenchmark) ClearMetrics() {
	b.metrics = make([]PerformanceMetrics, 0)
}

// Legacy interface compatibility helpers
type ColumnMap struct {
	ColumnName string
	Transient  bool
}

// LegacyDialectAdapter adapts old dialect interface to new system
type LegacyDialectAdapter interface {
	QuerySuffix() string
	ToSqlType(val reflect.Type, maxsize int, isAutoIncr bool) string
	AutoIncrStr() string
	AutoIncrBindValue() string
	AutoIncrInsertSuffix(col *ColumnMap) string
	CreateTableSuffix() string
	CreateIndexSuffix() string
	DropIndexSuffix() string
	TruncateClause() string
	BindVar(i int) string
	QuoteField(field string) string
	QuotedTableForQuery(schema string, table string) string
	IfSchemaNotExists(command, schema string) string
	IfTableExists(command, schema, table string) string
	IfTableNotExists(command, schema, table string) string
}