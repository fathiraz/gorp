package instrumentation

import (
	"context"
	"strings"
	"sync"
	"time"
)

// DatabaseMetrics contains standard database operation metrics
type DatabaseMetrics struct {
	// Query metrics
	QueriesTotal           string
	QueryDuration          string
	QueryErrors            string
	SlowQueries            string

	// Connection metrics
	ConnectionsActive      string
	ConnectionsIdle        string
	ConnectionsTotal       string
	ConnectionsMaxOpen     string
	ConnectionAcquireDuration string
	ConnectionErrors       string

	// Transaction metrics
	TransactionsTotal      string
	TransactionDuration    string
	TransactionErrors      string
	TransactionRollbacks   string

	// Database-specific metrics
	PostgreSQLMetrics      *PostgreSQLSpecificMetrics
	MySQLMetrics          *MySQLSpecificMetrics
	SQLiteMetrics         *SQLiteSpecificMetrics
	SQLServerMetrics      *SQLServerSpecificMetrics
}

// PostgreSQLSpecificMetrics contains PostgreSQL-specific metrics
type PostgreSQLSpecificMetrics struct {
	DeadlockCount         string
	TempFilesCreated      string
	TempBytesWritten      string
	WALFilesCount         string
	ReplicationLag        string
	CacheHitRatio         string
	IndexHitRatio         string
	VacuumCount           string
	AnalyzeCount          string
}

// MySQLSpecificMetrics contains MySQL-specific metrics
type MySQLSpecificMetrics struct {
	QueryCacheHitRate     string
	QueryCacheSize        string
	InnoDBBufferPoolHitRate string
	InnoDBBufferPoolSize  string
	SlowQueryCount        string
	TableLockWaits        string
	InnoDBRowLockWaits    string
	BinlogSize            string
}

// SQLiteSpecificMetrics contains SQLite-specific metrics
type SQLiteSpecificMetrics struct {
	PageCacheHitRatio     string
	PageCacheSize         string
	SchemaVersion         string
	WALSize               string
	CheckpointCount       string
	VacuumCount           string
}

// SQLServerSpecificMetrics contains SQL Server-specific metrics
type SQLServerSpecificMetrics struct {
	BufferCacheHitRatio   string
	PageLifeExpectancy    string
	BatchRequestsPerSec   string
	ConnectionCount       string
	LockWaits             string
	DeadlockCount         string
	UserConnections       string
	TempDBUsage           string
}

// DefaultDatabaseMetrics returns standard database metric names
func DefaultDatabaseMetrics() *DatabaseMetrics {
	return &DatabaseMetrics{
		QueriesTotal:              "gorp_queries_total",
		QueryDuration:             "gorp_query_duration_seconds",
		QueryErrors:               "gorp_query_errors_total",
		SlowQueries:               "gorp_slow_queries_total",
		ConnectionsActive:         "gorp_connections_active",
		ConnectionsIdle:           "gorp_connections_idle",
		ConnectionsTotal:          "gorp_connections_total",
		ConnectionsMaxOpen:        "gorp_connections_max_open",
		ConnectionAcquireDuration: "gorp_connection_acquire_duration_seconds",
		ConnectionErrors:          "gorp_connection_errors_total",
		TransactionsTotal:         "gorp_transactions_total",
		TransactionDuration:       "gorp_transaction_duration_seconds",
		TransactionErrors:         "gorp_transaction_errors_total",
		TransactionRollbacks:      "gorp_transaction_rollbacks_total",
		PostgreSQLMetrics:         DefaultPostgreSQLMetrics(),
		MySQLMetrics:             DefaultMySQLMetrics(),
		SQLiteMetrics:            DefaultSQLiteMetrics(),
		SQLServerMetrics:         DefaultSQLServerMetrics(),
	}
}

// DefaultPostgreSQLMetrics returns default PostgreSQL-specific metrics
func DefaultPostgreSQLMetrics() *PostgreSQLSpecificMetrics {
	return &PostgreSQLSpecificMetrics{
		DeadlockCount:         "gorp_postgresql_deadlocks_total",
		TempFilesCreated:      "gorp_postgresql_temp_files_total",
		TempBytesWritten:      "gorp_postgresql_temp_bytes_total",
		WALFilesCount:         "gorp_postgresql_wal_files_total",
		ReplicationLag:        "gorp_postgresql_replication_lag_seconds",
		CacheHitRatio:         "gorp_postgresql_cache_hit_ratio",
		IndexHitRatio:         "gorp_postgresql_index_hit_ratio",
		VacuumCount:           "gorp_postgresql_vacuum_total",
		AnalyzeCount:          "gorp_postgresql_analyze_total",
	}
}

// DefaultMySQLMetrics returns default MySQL-specific metrics
func DefaultMySQLMetrics() *MySQLSpecificMetrics {
	return &MySQLSpecificMetrics{
		QueryCacheHitRate:         "gorp_mysql_query_cache_hit_rate",
		QueryCacheSize:            "gorp_mysql_query_cache_size_bytes",
		InnoDBBufferPoolHitRate:   "gorp_mysql_innodb_buffer_pool_hit_rate",
		InnoDBBufferPoolSize:      "gorp_mysql_innodb_buffer_pool_size_bytes",
		SlowQueryCount:            "gorp_mysql_slow_queries_total",
		TableLockWaits:            "gorp_mysql_table_lock_waits_total",
		InnoDBRowLockWaits:        "gorp_mysql_innodb_row_lock_waits_total",
		BinlogSize:                "gorp_mysql_binlog_size_bytes",
	}
}

// DefaultSQLiteMetrics returns default SQLite-specific metrics
func DefaultSQLiteMetrics() *SQLiteSpecificMetrics {
	return &SQLiteSpecificMetrics{
		PageCacheHitRatio:         "gorp_sqlite_page_cache_hit_ratio",
		PageCacheSize:             "gorp_sqlite_page_cache_size_bytes",
		SchemaVersion:             "gorp_sqlite_schema_version",
		WALSize:                   "gorp_sqlite_wal_size_bytes",
		CheckpointCount:           "gorp_sqlite_checkpoint_total",
		VacuumCount:               "gorp_sqlite_vacuum_total",
	}
}

// DefaultSQLServerMetrics returns default SQL Server-specific metrics
func DefaultSQLServerMetrics() *SQLServerSpecificMetrics {
	return &SQLServerSpecificMetrics{
		BufferCacheHitRatio:       "gorp_sqlserver_buffer_cache_hit_ratio",
		PageLifeExpectancy:        "gorp_sqlserver_page_life_expectancy_seconds",
		BatchRequestsPerSec:       "gorp_sqlserver_batch_requests_per_second",
		ConnectionCount:           "gorp_sqlserver_connections_total",
		LockWaits:                 "gorp_sqlserver_lock_waits_total",
		DeadlockCount:             "gorp_sqlserver_deadlocks_total",
		UserConnections:           "gorp_sqlserver_user_connections",
		TempDBUsage:               "gorp_sqlserver_tempdb_usage_bytes",
	}
}

// MetricsConfig holds configuration for metrics collection
type MetricsConfig struct {
	Enabled            bool
	CollectionInterval time.Duration
	FlushInterval      time.Duration
	MetricsPrefix      string
	Labels             map[string]string
	EnabledMetrics     []string
	SampleRate         float64
	MaxMetricsCount    int
	RetentionPeriod    time.Duration
}

// DefaultMetricsConfig returns sensible defaults for metrics collection
func DefaultMetricsConfig() *MetricsConfig {
	return &MetricsConfig{
		Enabled:            true,
		CollectionInterval: 30 * time.Second,
		FlushInterval:      60 * time.Second,
		MetricsPrefix:      "gorp",
		Labels:             make(map[string]string),
		EnabledMetrics:     []string{"*"}, // Enable all metrics by default
		SampleRate:         1.0,           // Collect all metrics
		MaxMetricsCount:    10000,
		RetentionPeriod:    24 * time.Hour,
	}
}

// MetricsManager manages multiple metrics collectors and aggregation
type MetricsManager struct {
	collectors map[string]MetricsCollector
	config     *MetricsConfig
	metrics    *DatabaseMetrics
	mu         sync.RWMutex
	started    bool
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewMetricsManager creates a new metrics manager
func NewMetricsManager(config *MetricsConfig) *MetricsManager {
	if config == nil {
		config = DefaultMetricsConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &MetricsManager{
		collectors: make(map[string]MetricsCollector),
		config:     config,
		metrics:    DefaultDatabaseMetrics(),
		ctx:        ctx,
		cancel:     cancel,
	}
}

// RegisterCollector registers a metrics collector
func (mm *MetricsManager) RegisterCollector(collectorName string, collector MetricsCollector) error {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	if _, exists := mm.collectors[collectorName]; exists {
		return ErrCollectorAlreadyRegistered{Name: collectorName}
	}

	mm.collectors[collectorName] = collector
	return nil
}

// UnregisterCollector unregisters a metrics collector
func (mm *MetricsManager) UnregisterCollector(collectorName string) error {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	if _, exists := mm.collectors[collectorName]; !exists {
		return ErrCollectorNotFound{Name: collectorName}
	}

	delete(mm.collectors, collectorName)
	return nil
}

// Start starts metrics collection
func (mm *MetricsManager) Start(ctx context.Context) error {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	if mm.started {
		return ErrMetricsManagerAlreadyStarted{}
	}

	// Start all collectors
	for collectorName, collector := range mm.collectors {
		if err := collector.Start(ctx); err != nil {
			return ErrCollectorStartFailed{Name: collectorName, Err: err}
		}
	}

	mm.started = true

	// Start background collection routine
	go mm.collectMetrics()

	return nil
}

// Stop stops metrics collection
func (mm *MetricsManager) Stop(ctx context.Context) error {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	if !mm.started {
		return nil
	}

	// Cancel background collection
	mm.cancel()

	// Stop all collectors
	for _, collector := range mm.collectors {
		if err := collector.Stop(ctx); err != nil {
			// Log error but continue stopping other collectors
			continue
		}
	}

	mm.started = false
	return nil
}

// collectMetrics runs the background metrics collection loop
func (mm *MetricsManager) collectMetrics() {
	ticker := time.NewTicker(mm.config.CollectionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-mm.ctx.Done():
			return
		case <-ticker.C:
			mm.collectAndEmit()
		}
	}
}

// collectAndEmit collects metrics from all sources and emits them
func (mm *MetricsManager) collectAndEmit() {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	// This would collect metrics from various sources and emit to collectors
	// Implementation would depend on specific database connections and operations
	// For now, this is a placeholder
}

// IncrementCounter increments a counter across all collectors
func (mm *MetricsManager) IncrementCounter(name string, labels map[string]string) {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	if !mm.isMetricEnabled(name) {
		return
	}

	// Add default labels
	allLabels := mm.mergeLabels(labels)

	for _, collector := range mm.collectors {
		collector.IncrementCounter(name, allLabels)
	}
}

// RecordDuration records a duration across all collectors
func (mm *MetricsManager) RecordDuration(name string, duration time.Duration, labels map[string]string) {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	if !mm.isMetricEnabled(name) {
		return
	}

	allLabels := mm.mergeLabels(labels)

	for _, collector := range mm.collectors {
		collector.RecordDuration(name, duration, allLabels)
	}
}

// SetGauge sets a gauge value across all collectors
func (mm *MetricsManager) SetGauge(name string, value float64, labels map[string]string) {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	if !mm.isMetricEnabled(name) {
		return
	}

	allLabels := mm.mergeLabels(labels)

	for _, collector := range mm.collectors {
		collector.SetGauge(name, value, allLabels)
	}
}

// isMetricEnabled checks if a metric is enabled in configuration
func (mm *MetricsManager) isMetricEnabled(name string) bool {
	if !mm.config.Enabled {
		return false
	}

	// If sample rate is less than 1.0, probabilistically sample
	if mm.config.SampleRate < 1.0 {
		// Simple random sampling - in production you'd want better sampling
		if time.Now().UnixNano()%1000 >= int64(mm.config.SampleRate*1000) {
			return false
		}
	}

	// Check enabled metrics list
	for _, enabledMetric := range mm.config.EnabledMetrics {
		if enabledMetric == "*" || enabledMetric == name {
			return true
		}
	}

	return false
}

// mergeLabels merges default labels with provided labels
func (mm *MetricsManager) mergeLabels(labels map[string]string) map[string]string {
	merged := make(map[string]string)

	// Add default labels
	for k, v := range mm.config.Labels {
		merged[k] = v
	}

	// Add provided labels (override defaults)
	for k, v := range labels {
		merged[k] = v
	}

	return merged
}

// GetMetrics returns current metrics snapshot
func (mm *MetricsManager) GetMetrics() map[string]interface{} {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	// Return basic metrics info - could be enhanced to aggregate from collectors
	return map[string]interface{}{
		"enabled":    mm.config.Enabled,
		"collectors": len(mm.collectors),
		"started":    mm.started,
	}
}

// DatabaseMetricsCollector provides convenience methods for database metrics
type DatabaseMetricsCollector struct {
	manager *MetricsManager
	metrics *DatabaseMetrics
}

// NewDatabaseMetricsCollector creates a new database metrics collector
func NewDatabaseMetricsCollector(manager *MetricsManager) *DatabaseMetricsCollector {
	return &DatabaseMetricsCollector{
		manager: manager,
		metrics: manager.metrics,
	}
}

// RecordQuery records a database query execution
func (dmc *DatabaseMetricsCollector) RecordQuery(operation, table string, duration time.Duration, err error) {
	labels := map[string]string{
		"operation": operation,
		"table":     table,
	}

	// Record query count
	dmc.manager.IncrementCounter(dmc.metrics.QueriesTotal, labels)

	// Record query duration
	dmc.manager.RecordDuration(dmc.metrics.QueryDuration, duration, labels)

	// Record errors
	if err != nil {
		errorLabels := make(map[string]string)
		for k, v := range labels {
			errorLabels[k] = v
		}
		errorLabels["error_type"] = classifyError(err)
		dmc.manager.IncrementCounter(dmc.metrics.QueryErrors, errorLabels)
	}

	// Record slow queries
	if duration > 1*time.Second { // Configurable threshold
		dmc.manager.IncrementCounter(dmc.metrics.SlowQueries, labels)
	}
}

// RecordConnection records connection pool metrics
func (dmc *DatabaseMetricsCollector) RecordConnection(active, idle, maxOpen int) {
	labels := map[string]string{}

	dmc.manager.SetGauge(dmc.metrics.ConnectionsActive, float64(active), labels)
	dmc.manager.SetGauge(dmc.metrics.ConnectionsIdle, float64(idle), labels)
	dmc.manager.SetGauge(dmc.metrics.ConnectionsMaxOpen, float64(maxOpen), labels)
	dmc.manager.SetGauge(dmc.metrics.ConnectionsTotal, float64(active+idle), labels)
}

// RecordTransaction records transaction metrics
func (dmc *DatabaseMetricsCollector) RecordTransaction(duration time.Duration, committed bool, err error) {
	labels := map[string]string{
		"outcome": func() string {
			if err != nil {
				return "error"
			}
			if committed {
				return "commit"
			}
			return "rollback"
		}(),
	}

	dmc.manager.IncrementCounter(dmc.metrics.TransactionsTotal, labels)
	dmc.manager.RecordDuration(dmc.metrics.TransactionDuration, duration, labels)

	if err != nil {
		dmc.manager.IncrementCounter(dmc.metrics.TransactionErrors, labels)
	}

	if !committed && err == nil {
		dmc.manager.IncrementCounter(dmc.metrics.TransactionRollbacks, labels)
	}
}

// classifyError classifies database errors into categories
func classifyError(err error) string {
	if err == nil {
		return "none"
	}

	errStr := err.Error()
	switch {
	case strings.Contains(errStr, "timeout"):
		return "timeout"
	case strings.Contains(errStr, "connection"):
		return "connection"
	case strings.Contains(errStr, "syntax"):
		return "syntax"
	case strings.Contains(errStr, "constraint"):
		return "constraint"
	case strings.Contains(errStr, "deadlock"):
		return "deadlock"
	default:
		return "other"
	}
}

// Custom errors
type ErrCollectorAlreadyRegistered struct {
	Name string
}

func (e ErrCollectorAlreadyRegistered) Error() string {
	return "metrics collector already registered: " + e.Name
}

type ErrCollectorNotFound struct {
	Name string
}

func (e ErrCollectorNotFound) Error() string {
	return "metrics collector not found: " + e.Name
}

type ErrMetricsManagerAlreadyStarted struct{}

func (e ErrMetricsManagerAlreadyStarted) Error() string {
	return "metrics manager already started"
}

type ErrCollectorStartFailed struct {
	Name string
	Err  error
}

func (e ErrCollectorStartFailed) Error() string {
	return "failed to start collector " + e.Name + ": " + e.Err.Error()
}