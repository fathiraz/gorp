package db

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgreSQLDialect provides PostgreSQL-specific enhancements and optimizations
type PostgreSQLDialect struct {
	conn         *PostgreSQLEnhancedConnection
	errorHandler *PostgreSQLErrorHandler
	typeMapper   *PostgreSQLTypeMapper
	config       *PostgreSQLDialectConfig
}

// PostgreSQLDialectConfig holds PostgreSQL dialect configuration
type PostgreSQLDialectConfig struct {
	EnableQueryPlan        bool
	StatementTimeout       time.Duration
	LockTimeout           time.Duration
	IdleInTransactionTimeout time.Duration
	WorkMem               string
	MaintenanceWorkMem    string
	EffectiveCacheSize    string
	RandomPageCost        float64
	SeqPageCost          float64
	CPUTupleCoSt         float64
	CPUIndexTupleCoSt    float64
	CPUOperatorCoSt      float64
	EnableHashJoin        bool
	EnableMergeJoin       bool
	EnableNeStLoopJoin    bool
	EnableParallelQuery   bool
	MaxParallelWorkers    int
	MaxParallelWorkersPerGather int
}

// DefaultPostgreSQLDialectConfig returns sensible defaults
func DefaultPostgreSQLDialectConfig() *PostgreSQLDialectConfig {
	return &PostgreSQLDialectConfig{
		EnableQueryPlan:              false,
		StatementTimeout:             30 * time.Second,
		LockTimeout:                  10 * time.Second,
		IdleInTransactionTimeout:     60 * time.Second,
		WorkMem:                      "4MB",
		MaintenanceWorkMem:           "64MB",
		EffectiveCacheSize:          "1GB",
		RandomPageCost:              4.0,
		SeqPageCost:                 1.0,
		CPUTupleCoSt:                0.01,
		CPUIndexTupleCoSt:           0.005,
		CPUOperatorCoSt:             0.0025,
		EnableHashJoin:              true,
		EnableMergeJoin:             true,
		EnableNeStLoopJoin:          true,
		EnableParallelQuery:         true,
		MaxParallelWorkers:          8,
		MaxParallelWorkersPerGather: 2,
	}
}

// NewPostgreSQLDialect creates a new PostgreSQL dialect
func NewPostgreSQLDialect(conn *PostgreSQLEnhancedConnection, config *PostgreSQLDialectConfig) *PostgreSQLDialect {
	if config == nil {
		config = DefaultPostgreSQLDialectConfig()
	}

	return &PostgreSQLDialect{
		conn:         conn,
		errorHandler: NewPostgreSQLErrorHandler(nil),
		typeMapper:   NewPostgreSQLTypeMapper(),
		config:       config,
	}
}

// UpsertQuery builds a PostgreSQL UPSERT query using ON CONFLICT
func (d *PostgreSQLDialect) UpsertQuery(table string, columns []string, conflictColumns []string, updateColumns []string) string {
	var query strings.Builder

	query.WriteString("INSERT INTO ")
	query.WriteString(table)
	query.WriteString(" (")
	query.WriteString(strings.Join(columns, ", "))
	query.WriteString(") VALUES (")

	// Add placeholders
	placeholders := make([]string, len(columns))
	for i := range columns {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	query.WriteString(strings.Join(placeholders, ", "))
	query.WriteString(")")

	// Add ON CONFLICT clause
	query.WriteString(" ON CONFLICT (")
	query.WriteString(strings.Join(conflictColumns, ", "))
	query.WriteString(")")

	if len(updateColumns) > 0 {
		query.WriteString(" DO UPDATE SET ")
		updateClauses := make([]string, len(updateColumns))
		for i, col := range updateColumns {
			updateClauses[i] = fmt.Sprintf("%s = EXCLUDED.%s", col, col)
		}
		query.WriteString(strings.Join(updateClauses, ", "))
	} else {
		query.WriteString(" DO NOTHING")
	}

	return query.String()
}

// BulkUpsert performs bulk upsert operations using PostgreSQL's ON CONFLICT
func (d *PostgreSQLDialect) BulkUpsert(ctx context.Context, table string, columns []string, conflictColumns []string, updateColumns []string, data [][]interface{}) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}

	// Build the base query
	baseQuery := d.UpsertQuery(table, columns, conflictColumns, updateColumns)

	return d.errorHandler.RetryOperation(ctx, func() error {
		return d.conn.ExecuteInTransaction(ctx, func(tx pgx.Tx) error {
			var totalAffected int64

			// Use prepared statement for better performance
			for _, row := range data {
				result, err := tx.Exec(ctx, baseQuery, row...)
				if err != nil {
					return d.errorHandler.WrapError(err, "bulk_upsert", fmt.Sprintf("table=%s", table))
				}
				totalAffected += result.RowsAffected()
			}

			return nil
		})
	}), nil
}

// OptimizedQuery represents a query with PostgreSQL-specific optimizations
type OptimizedQuery struct {
	SQL           string
	Args          []interface{}
	Hints         []string
	ExpectedRows  int64
	UseIndex      string
	ParallelSafe  bool
	CostThreshold float64
}

// OptimizeQuery applies PostgreSQL-specific query optimizations
func (d *PostgreSQLDialect) OptimizeQuery(query *OptimizedQuery) string {
	var optimized strings.Builder

	// Add query hints if any
	if len(query.Hints) > 0 {
		optimized.WriteString("/* ")
		optimized.WriteString(strings.Join(query.Hints, ", "))
		optimized.WriteString(" */ ")
	}

	// Set query-specific parameters if needed
	if query.ExpectedRows > 0 {
		optimized.WriteString("SET LOCAL work_mem = ")
		if query.ExpectedRows > 100000 {
			optimized.WriteString("'16MB'; ")
		} else if query.ExpectedRows > 10000 {
			optimized.WriteString("'8MB'; ")
		} else {
			optimized.WriteString("'4MB'; ")
		}
	}

	// Enable parallel query if marked as safe and expected to be expensive
	if query.ParallelSafe && query.ExpectedRows > 10000 {
		optimized.WriteString("SET LOCAL max_parallel_workers_per_gather = 4; ")
		optimized.WriteString("SET LOCAL parallel_tuple_cost = 0.1; ")
	}

	// Add the actual query
	optimized.WriteString(query.SQL)

	return optimized.String()
}

// ExplainQuery returns the query execution plan
func (d *PostgreSQLDialect) ExplainQuery(ctx context.Context, query string, args ...interface{}) (*QueryPlan, error) {
	explainSQL := "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + query

	var planJSON string
	err := d.errorHandler.RetryOperation(ctx, func() error {
		return d.conn.Get(ctx, &planJSON, explainSQL, args...)
	})

	if err != nil {
		return nil, d.errorHandler.WrapError(err, "explain_query", fmt.Sprintf("query=%s", query))
	}

	return parseQueryPlan(planJSON)
}

// QueryPlan represents a PostgreSQL query execution plan
type QueryPlan struct {
	PlanningTime   float64        `json:"planning_time"`
	ExecutionTime  float64        `json:"execution_time"`
	TotalCost      float64        `json:"total_cost"`
	ActualRows     int64          `json:"actual_rows"`
	PlanRows       int64          `json:"plan_rows"`
	Nodes          []PlanNode     `json:"nodes"`
	BufferStats    BufferStats    `json:"buffer_stats"`
	IOTimings      IOTimings      `json:"io_timings"`
}

// PlanNode represents a node in the execution plan
type PlanNode struct {
	NodeType         string      `json:"node_type"`
	RelationName     string      `json:"relation_name,omitempty"`
	IndexName        string      `json:"index_name,omitempty"`
	StartupCost      float64     `json:"startup_cost"`
	TotalCost        float64     `json:"total_cost"`
	PlanRows         int64       `json:"plan_rows"`
	PlanWidth        int         `json:"plan_width"`
	ActualStartupTime float64    `json:"actual_startup_time"`
	ActualTotalTime  float64     `json:"actual_total_time"`
	ActualRows       int64       `json:"actual_rows"`
	ActualLoops      int         `json:"actual_loops"`
	SharedHitBlocks  int64       `json:"shared_hit_blocks"`
	SharedReadBlocks int64       `json:"shared_read_blocks"`
	TempReadBlocks   int64       `json:"temp_read_blocks"`
	TempWrittenBlocks int64      `json:"temp_written_blocks"`
	Children         []PlanNode  `json:"children,omitempty"`
}

// BufferStats represents buffer usage statistics
type BufferStats struct {
	SharedHit      int64 `json:"shared_hit"`
	SharedRead     int64 `json:"shared_read"`
	SharedDirtied  int64 `json:"shared_dirtied"`
	SharedWritten  int64 `json:"shared_written"`
	LocalHit       int64 `json:"local_hit"`
	LocalRead      int64 `json:"local_read"`
	LocalDirtied   int64 `json:"local_dirtied"`
	LocalWritten   int64 `json:"local_written"`
	TempRead       int64 `json:"temp_read"`
	TempWritten    int64 `json:"temp_written"`
}

// IOTimings represents I/O timing statistics
type IOTimings struct {
	ReadTime  float64 `json:"read_time"`
	WriteTime float64 `json:"write_time"`
}

// CreateConstraint creates various types of PostgreSQL constraints
func (d *PostgreSQLDialect) CreateConstraint(ctx context.Context, constraint ConstraintSpec) error {
	query := d.buildConstraintQuery(constraint)

	return d.errorHandler.RetryOperation(ctx, func() error {
		_, err := d.conn.Exec(ctx, query)
		return d.errorHandler.WrapError(err, "create_constraint", fmt.Sprintf("table=%s, type=%s", constraint.Table, constraint.Type))
	})
}

// ConstraintSpec represents a PostgreSQL constraint specification
type ConstraintSpec struct {
	Name       string
	Table      string
	Type       ConstraintType
	Columns    []string
	RefTable   string
	RefColumns []string
	OnDelete   ReferentialAction
	OnUpdate   ReferentialAction
	Expression string
	Deferrable bool
	Initially  DeferMode
}

// ConstraintType represents types of PostgreSQL constraints
type ConstraintType string

const (
	PrimaryKeyConstraint ConstraintType = "PRIMARY KEY"
	UniqueConstraint     ConstraintType = "UNIQUE"
	ForeignKeyConstraint ConstraintType = "FOREIGN KEY"
	CheckConstraint      ConstraintType = "CHECK"
	ExcludeConstraint    ConstraintType = "EXCLUDE"
)

// ReferentialAction represents referential actions
type ReferentialAction string

const (
	NoAction   ReferentialAction = "NO ACTION"
	Restrict   ReferentialAction = "RESTRICT"
	Cascade    ReferentialAction = "CASCADE"
	SetNull    ReferentialAction = "SET NULL"
	SetDefault ReferentialAction = "SET DEFAULT"
)

// DeferMode represents deferrable constraint modes
type DeferMode string

const (
	Immediate DeferMode = "IMMEDIATE"
	Deferred  DeferMode = "DEFERRED"
)

// buildConstraintQuery builds a constraint creation query
func (d *PostgreSQLDialect) buildConstraintQuery(spec ConstraintSpec) string {
	var query strings.Builder

	query.WriteString("ALTER TABLE ")
	query.WriteString(spec.Table)
	query.WriteString(" ADD CONSTRAINT ")
	query.WriteString(spec.Name)
	query.WriteString(" ")

	switch spec.Type {
	case PrimaryKeyConstraint:
		query.WriteString("PRIMARY KEY (")
		query.WriteString(strings.Join(spec.Columns, ", "))
		query.WriteString(")")

	case UniqueConstraint:
		query.WriteString("UNIQUE (")
		query.WriteString(strings.Join(spec.Columns, ", "))
		query.WriteString(")")

	case ForeignKeyConstraint:
		query.WriteString("FOREIGN KEY (")
		query.WriteString(strings.Join(spec.Columns, ", "))
		query.WriteString(") REFERENCES ")
		query.WriteString(spec.RefTable)
		query.WriteString(" (")
		query.WriteString(strings.Join(spec.RefColumns, ", "))
		query.WriteString(")")

		if spec.OnDelete != "" {
			query.WriteString(" ON DELETE ")
			query.WriteString(string(spec.OnDelete))
		}

		if spec.OnUpdate != "" {
			query.WriteString(" ON UPDATE ")
			query.WriteString(string(spec.OnUpdate))
		}

	case CheckConstraint:
		query.WriteString("CHECK (")
		query.WriteString(spec.Expression)
		query.WriteString(")")
	}

	if spec.Deferrable {
		query.WriteString(" DEFERRABLE")
		if spec.Initially != "" {
			query.WriteString(" INITIALLY ")
			query.WriteString(string(spec.Initially))
		}
	}

	return query.String()
}

// VacuumAnalyze performs VACUUM and ANALYZE operations
func (d *PostgreSQLDialect) VacuumAnalyze(ctx context.Context, options VacuumOptions) error {
	return d.errorHandler.RetryOperation(ctx, func() error {
		query := d.buildVacuumQuery(options)
		_, err := d.conn.Exec(ctx, query)
		return d.errorHandler.WrapError(err, "vacuum_analyze", fmt.Sprintf("table=%s", options.Table))
	})
}

// VacuumOptions represents VACUUM operation options
type VacuumOptions struct {
	Table          string
	Full           bool
	Freeze         bool
	Verbose        bool
	Analyze        bool
	DisablePageSkipping bool
	SkipLocked     bool
	IndexCleanup   bool
	TruncAte       bool
	Parallel       int
}

// buildVacuumQuery builds a VACUUM command
func (d *PostgreSQLDialect) buildVacuumQuery(options VacuumOptions) string {
	var parts []string
	var optionsList []string

	if options.Full {
		optionsList = append(optionsList, "FULL")
	}
	if options.Freeze {
		optionsList = append(optionsList, "FREEZE")
	}
	if options.Verbose {
		optionsList = append(optionsList, "VERBOSE")
	}
	if options.Analyze {
		optionsList = append(optionsList, "ANALYZE")
	}
	if options.DisablePageSkipping {
		optionsList = append(optionsList, "DISABLE_PAGE_SKIPPING")
	}
	if options.SkipLocked {
		optionsList = append(optionsList, "SKIP_LOCKED")
	}
	if !options.IndexCleanup {
		optionsList = append(optionsList, "INDEX_CLEANUP FALSE")
	}
	if !options.TruncAte {
		optionsList = append(optionsList, "TRUNCATE FALSE")
	}
	if options.Parallel > 0 {
		optionsList = append(optionsList, fmt.Sprintf("PARALLEL %d", options.Parallel))
	}

	parts = append(parts, "VACUUM")

	if len(optionsList) > 0 {
		parts = append(parts, fmt.Sprintf("(%s)", strings.Join(optionsList, ", ")))
	}

	if options.Table != "" {
		parts = append(parts, options.Table)
	}

	return strings.Join(parts, " ")
}

// GetStatistics returns table and index statistics
func (d *PostgreSQLDialect) GetStatistics(ctx context.Context, schema, table string) (*TableStatistics, error) {
	var stats TableStatistics

	query := `
		SELECT
			schemaname,
			tablename,
			n_tup_ins,
			n_tup_upd,
			n_tup_del,
			n_live_tup,
			n_dead_tup,
			last_vacuum,
			last_autovacuum,
			last_analyze,
			last_autoanalyze,
			vacuum_count,
			autovacuum_count,
			analyze_count,
			autoanalyze_count
		FROM pg_stat_user_tables
		WHERE schemaname = $1 AND tablename = $2
	`

	err := d.errorHandler.RetryOperation(ctx, func() error {
		return d.conn.Get(ctx, &stats, query, schema, table)
	})

	if err != nil {
		return nil, d.errorHandler.WrapError(err, "get_statistics", fmt.Sprintf("schema=%s, table=%s", schema, table))
	}

	// Get index statistics
	indexQuery := `
		SELECT
			indexname,
			idx_tup_read,
			idx_tup_fetch,
			idx_scan
		FROM pg_stat_user_indexes
		WHERE schemaname = $1 AND tablename = $2
	`

	rows, err := d.conn.Query(ctx, indexQuery, schema, table)
	if err != nil {
		return &stats, d.errorHandler.WrapError(err, "get_index_statistics", fmt.Sprintf("schema=%s, table=%s", schema, table))
	}
	defer rows.Close()

	for rows.Next() {
		var indexStat IndexStatistics
		err := rows.StructScan(&indexStat)
		if err != nil {
			continue
		}
		stats.IndexStats = append(stats.IndexStats, indexStat)
	}

	return &stats, nil
}

// TableStatistics represents PostgreSQL table statistics
type TableStatistics struct {
	SchemaName       string              `db:"schemaname"`
	TableName        string              `db:"tablename"`
	TupleInserts     int64               `db:"n_tup_ins"`
	TupleUpdates     int64               `db:"n_tup_upd"`
	TupleDeletes     int64               `db:"n_tup_del"`
	LiveTuples       int64               `db:"n_live_tup"`
	DeadTuples       int64               `db:"n_dead_tup"`
	LastVacuum       *time.Time          `db:"last_vacuum"`
	LastAutoVacuum   *time.Time          `db:"last_autovacuum"`
	LastAnalyze      *time.Time          `db:"last_analyze"`
	LastAutoAnalyze  *time.Time          `db:"last_autoanalyze"`
	VacuumCount      int64               `db:"vacuum_count"`
	AutoVacuumCount  int64               `db:"autovacuum_count"`
	AnalyzeCount     int64               `db:"analyze_count"`
	AutoAnalyzeCount int64               `db:"autoanalyze_count"`
	IndexStats       []IndexStatistics   `json:"index_stats"`
}

// IndexStatistics represents PostgreSQL index statistics
type IndexStatistics struct {
	IndexName    string `db:"indexname"`
	TupleRead    int64  `db:"idx_tup_read"`
	TupleFetch   int64  `db:"idx_tup_fetch"`
	Scans        int64  `db:"idx_scan"`
}

// Helper function to parse JSON query plan (simplified version)
func parseQueryPlan(planJSON string) (*QueryPlan, error) {
	// In a real implementation, you would parse the JSON response
	// For now, returning a placeholder
	return &QueryPlan{
		PlanningTime:  0,
		ExecutionTime: 0,
		TotalCost:     0,
		ActualRows:    0,
		PlanRows:      0,
	}, nil
}

// GeneratePartitionedTable creates a partitioned table with specified strategy
func (d *PostgreSQLDialect) GeneratePartitionedTable(ctx context.Context, spec PartitionSpec) error {
	query := d.buildPartitionedTableQuery(spec)

	return d.errorHandler.RetryOperation(ctx, func() error {
		_, err := d.conn.Exec(ctx, query)
		return d.errorHandler.WrapError(err, "create_partitioned_table", fmt.Sprintf("table=%s", spec.TableName))
	})
}

// PartitionSpec represents partitioned table specification
type PartitionSpec struct {
	TableName        string
	Columns          []ColumnSpec
	PartitionBy      PartitionStrategy
	PartitionColumns []string
	PartitionExpression string
}

// PartitionStrategy represents partitioning strategies
type PartitionStrategy string

const (
	RangePartition PartitionStrategy = "RANGE"
	ListPartition  PartitionStrategy = "LIST"
	HashPartition  PartitionStrategy = "HASH"
)

// ColumnSpec represents a table column specification
type ColumnSpec struct {
	Name         string
	Type         string
	NotNull      bool
	Default      string
	PrimaryKey   bool
	Unique       bool
	References   string
	CheckClause  string
}

// buildPartitionedTableQuery builds a partitioned table creation query
func (d *PostgreSQLDialect) buildPartitionedTableQuery(spec PartitionSpec) string {
	var query strings.Builder

	query.WriteString("CREATE TABLE ")
	query.WriteString(spec.TableName)
	query.WriteString(" (")

	// Add columns
	columnDefs := make([]string, len(spec.Columns))
	for i, col := range spec.Columns {
		columnDefs[i] = d.buildColumnDefinition(col)
	}
	query.WriteString(strings.Join(columnDefs, ", "))

	query.WriteString(") PARTITION BY ")
	query.WriteString(string(spec.PartitionBy))

	if len(spec.PartitionColumns) > 0 {
		query.WriteString(" (")
		query.WriteString(strings.Join(spec.PartitionColumns, ", "))
		query.WriteString(")")
	} else if spec.PartitionExpression != "" {
		query.WriteString(" (")
		query.WriteString(spec.PartitionExpression)
		query.WriteString(")")
	}

	return query.String()
}

// buildColumnDefinition builds a column definition string
func (d *PostgreSQLDialect) buildColumnDefinition(col ColumnSpec) string {
	var parts []string

	parts = append(parts, col.Name, col.Type)

	if col.NotNull {
		parts = append(parts, "NOT NULL")
	}

	if col.Default != "" {
		parts = append(parts, "DEFAULT", col.Default)
	}

	if col.PrimaryKey {
		parts = append(parts, "PRIMARY KEY")
	}

	if col.Unique {
		parts = append(parts, "UNIQUE")
	}

	if col.References != "" {
		parts = append(parts, "REFERENCES", col.References)
	}

	if col.CheckClause != "" {
		parts = append(parts, "CHECK", "("+col.CheckClause+")")
	}

	return strings.Join(parts, " ")
}