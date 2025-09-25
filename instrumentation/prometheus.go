package instrumentation

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// PrometheusCollector implements MetricsCollector for Prometheus
type PrometheusCollector struct {
	registry prometheus.Registerer

	// Standard database metrics
	queryTotal          *prometheus.CounterVec
	queryDuration       *prometheus.HistogramVec
	connectionTotal     prometheus.Counter
	connectionActive    prometheus.Gauge
	connectionIdle      prometheus.Gauge
	connectionLifetime  prometheus.Histogram
	transactionTotal    *prometheus.CounterVec
	transactionDuration prometheus.Histogram
	errorTotal          *prometheus.CounterVec

	// Database-specific metrics
	postgresMetrics *PostgreSQLPrometheusMetrics
	mysqlMetrics    *MySQLPrometheusMetrics
	sqliteMetrics   *SQLitePrometheusMetrics
	sqlserverMetrics *SQLServerPrometheusMetrics

	// Custom metrics storage
	customCounters   map[string]*prometheus.CounterVec
	customGauges     map[string]*prometheus.GaugeVec
	customHistograms map[string]*prometheus.HistogramVec
	customTimers     map[string]*prometheus.HistogramVec
	metricsLock      sync.RWMutex

	namespace string
}

// NewPrometheusCollector creates a new Prometheus metrics collector
func NewPrometheusCollector(namespace string, registry prometheus.Registerer) *PrometheusCollector {
	if registry == nil {
		registry = prometheus.DefaultRegisterer
	}
	if namespace == "" {
		namespace = "gorp"
	}

	pc := &PrometheusCollector{
		registry:         registry,
		namespace:        namespace,
		customCounters:   make(map[string]*prometheus.CounterVec),
		customGauges:     make(map[string]*prometheus.GaugeVec),
		customHistograms: make(map[string]*prometheus.HistogramVec),
		customTimers:     make(map[string]*prometheus.HistogramVec),
	}

	pc.initStandardMetrics()
	pc.initDatabaseSpecificMetrics()

	return pc
}

// initStandardMetrics initializes standard database metrics
func (pc *PrometheusCollector) initStandardMetrics() {
	pc.queryTotal = promauto.With(pc.registry).NewCounterVec(
		prometheus.CounterOpts{
			Namespace: pc.namespace,
			Name:      "queries_total",
			Help:      "Total number of database queries executed",
		},
		[]string{"operation", "table", "status"},
	)

	pc.queryDuration = promauto.With(pc.registry).NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: pc.namespace,
			Name:      "query_duration_seconds",
			Help:      "Duration of database queries in seconds",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
		},
		[]string{"operation", "table"},
	)

	pc.connectionTotal = promauto.With(pc.registry).NewCounter(
		prometheus.CounterOpts{
			Namespace: pc.namespace,
			Name:      "connections_opened_total",
			Help:      "Total number of database connections opened",
		},
	)

	pc.connectionActive = promauto.With(pc.registry).NewGauge(
		prometheus.GaugeOpts{
			Namespace: pc.namespace,
			Name:      "connections_active",
			Help:      "Number of active database connections",
		},
	)

	pc.connectionIdle = promauto.With(pc.registry).NewGauge(
		prometheus.GaugeOpts{
			Namespace: pc.namespace,
			Name:      "connections_idle",
			Help:      "Number of idle database connections",
		},
	)

	pc.connectionLifetime = promauto.With(pc.registry).NewHistogram(
		prometheus.HistogramOpts{
			Namespace: pc.namespace,
			Name:      "connection_lifetime_seconds",
			Help:      "Lifetime of database connections in seconds",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 15), // 1s to ~9h
		},
	)

	pc.transactionTotal = promauto.With(pc.registry).NewCounterVec(
		prometheus.CounterOpts{
			Namespace: pc.namespace,
			Name:      "transactions_total",
			Help:      "Total number of database transactions",
		},
		[]string{"status"},
	)

	pc.transactionDuration = promauto.With(pc.registry).NewHistogram(
		prometheus.HistogramOpts{
			Namespace: pc.namespace,
			Name:      "transaction_duration_seconds",
			Help:      "Duration of database transactions in seconds",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30},
		},
	)

	pc.errorTotal = promauto.With(pc.registry).NewCounterVec(
		prometheus.CounterOpts{
			Namespace: pc.namespace,
			Name:      "errors_total",
			Help:      "Total number of database errors",
		},
		[]string{"type", "operation"},
	)
}

// RecordQuery implements MetricsCollector
func (pc *PrometheusCollector) RecordQuery(operation, table string, duration time.Duration, err error) {
	status := "success"
	if err != nil {
		status = "error"
		pc.errorTotal.WithLabelValues("query", operation).Inc()
	}

	pc.queryTotal.WithLabelValues(operation, table, status).Inc()
	pc.queryDuration.WithLabelValues(operation, table).Observe(duration.Seconds())
}

// RecordConnection implements MetricsCollector
func (pc *PrometheusCollector) RecordConnection(opened bool) {
	if opened {
		pc.connectionTotal.Inc()
		pc.connectionActive.Inc()
	} else {
		pc.connectionActive.Dec()
	}
}

// RecordConnectionStats implements MetricsCollector
func (pc *PrometheusCollector) RecordConnectionStats(active, idle int, avgLifetime time.Duration) {
	pc.connectionActive.Set(float64(active))
	pc.connectionIdle.Set(float64(idle))
	if avgLifetime > 0 {
		pc.connectionLifetime.Observe(avgLifetime.Seconds())
	}
}

// RecordTransaction implements MetricsCollector
func (pc *PrometheusCollector) RecordTransaction(duration time.Duration, committed bool) {
	status := "committed"
	if !committed {
		status = "rolled_back"
	}

	pc.transactionTotal.WithLabelValues(status).Inc()
	pc.transactionDuration.Observe(duration.Seconds())
}

// RecordError implements MetricsCollector
func (pc *PrometheusCollector) RecordError(errorType, operation string) {
	pc.errorTotal.WithLabelValues(errorType, operation).Inc()
}

// Counter implements MetricsCollector
func (pc *PrometheusCollector) Counter(name string, labels map[string]string) {
	pc.metricsLock.Lock()
	defer pc.metricsLock.Unlock()

	key := pc.metricKey(name, labels)
	counter, exists := pc.customCounters[key]

	if !exists {
		labelNames := make([]string, 0, len(labels))
		for k := range labels {
			labelNames = append(labelNames, k)
		}

		counter = promauto.With(pc.registry).NewCounterVec(
			prometheus.CounterOpts{
				Namespace: pc.namespace,
				Name:      name,
				Help:      fmt.Sprintf("Custom counter metric: %s", name),
			},
			labelNames,
		)
		pc.customCounters[key] = counter
	}

	labelValues := make([]string, 0, len(labels))
	for _, v := range labels {
		labelValues = append(labelValues, v)
	}

	counter.WithLabelValues(labelValues...).Inc()
}

// Gauge implements MetricsCollector
func (pc *PrometheusCollector) Gauge(name string, value float64, labels map[string]string) {
	pc.metricsLock.Lock()
	defer pc.metricsLock.Unlock()

	key := pc.metricKey(name, labels)
	gauge, exists := pc.customGauges[key]

	if !exists {
		labelNames := make([]string, 0, len(labels))
		for k := range labels {
			labelNames = append(labelNames, k)
		}

		gauge = promauto.With(pc.registry).NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: pc.namespace,
				Name:      name,
				Help:      fmt.Sprintf("Custom gauge metric: %s", name),
			},
			labelNames,
		)
		pc.customGauges[key] = gauge
	}

	labelValues := make([]string, 0, len(labels))
	for _, v := range labels {
		labelValues = append(labelValues, v)
	}

	gauge.WithLabelValues(labelValues...).Set(value)
}

// Histogram implements MetricsCollector
func (pc *PrometheusCollector) Histogram(name string, value float64, labels map[string]string) {
	pc.metricsLock.Lock()
	defer pc.metricsLock.Unlock()

	key := pc.metricKey(name, labels)
	histogram, exists := pc.customHistograms[key]

	if !exists {
		labelNames := make([]string, 0, len(labels))
		for k := range labels {
			labelNames = append(labelNames, k)
		}

		histogram = promauto.With(pc.registry).NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: pc.namespace,
				Name:      name,
				Help:      fmt.Sprintf("Custom histogram metric: %s", name),
				Buckets:   prometheus.DefBuckets,
			},
			labelNames,
		)
		pc.customHistograms[key] = histogram
	}

	labelValues := make([]string, 0, len(labels))
	for _, v := range labels {
		labelValues = append(labelValues, v)
	}

	histogram.WithLabelValues(labelValues...).Observe(value)
}

// Timer implements MetricsCollector
func (pc *PrometheusCollector) Timer(name string, duration time.Duration, labels map[string]string) {
	pc.metricsLock.Lock()
	defer pc.metricsLock.Unlock()

	key := pc.metricKey(name, labels)
	timer, exists := pc.customTimers[key]

	if !exists {
		labelNames := make([]string, 0, len(labels))
		for k := range labels {
			labelNames = append(labelNames, k)
		}

		timer = promauto.With(pc.registry).NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: pc.namespace,
				Name:      name + "_duration_seconds",
				Help:      fmt.Sprintf("Custom timer metric: %s", name),
				Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
			},
			labelNames,
		)
		pc.customTimers[key] = timer
	}

	labelValues := make([]string, 0, len(labels))
	for _, v := range labels {
		labelValues = append(labelValues, v)
	}

	timer.WithLabelValues(labelValues...).Observe(duration.Seconds())
}

// metricKey generates a unique key for metric storage
func (pc *PrometheusCollector) metricKey(name string, labels map[string]string) string {
	key := name
	for k, v := range labels {
		key += "_" + k + "_" + v
	}
	return key
}

// Database-specific metrics implementations

// PostgreSQLPrometheusMetrics holds PostgreSQL-specific Prometheus metrics
type PostgreSQLPrometheusMetrics struct {
	replicationLag     prometheus.Gauge
	locksHeld          *prometheus.GaugeVec
	bgWriterStats      *prometheus.CounterVec
	checkpointStats    *prometheus.CounterVec
	vacuumStats        *prometheus.CounterVec
	indexUsage         *prometheus.GaugeVec
	tableSize          *prometheus.GaugeVec
	connectionsByState *prometheus.GaugeVec
}

// MySQLPrometheusMetrics holds MySQL-specific Prometheus metrics
type MySQLPrometheusMetrics struct {
	innodbStats        *prometheus.GaugeVec
	replicationDelay   prometheus.Gauge
	slowQueries        prometheus.Counter
	tableSize          *prometheus.GaugeVec
	indexUsage         *prometheus.GaugeVec
	connectionsByState *prometheus.GaugeVec
	bufferPoolStats    *prometheus.GaugeVec
}

// SQLitePrometheusMetrics holds SQLite-specific Prometheus metrics
type SQLitePrometheusMetrics struct {
	fileSize           prometheus.Gauge
	pageCount          prometheus.Gauge
	cacheStats         *prometheus.GaugeVec
	walStats           *prometheus.GaugeVec
	pragmaSettings     *prometheus.GaugeVec
}

// SQLServerPrometheusMetrics holds SQL Server-specific Prometheus metrics
type SQLServerPrometheusMetrics struct {
	bufferCacheHitRatio prometheus.Gauge
	pageLifeExpectancy  prometheus.Gauge
	waitStats           *prometheus.CounterVec
	lockStats           *prometheus.GaugeVec
	indexFragmentation  *prometheus.GaugeVec
	dbSize              *prometheus.GaugeVec
}

// initDatabaseSpecificMetrics initializes database-specific metrics
func (pc *PrometheusCollector) initDatabaseSpecificMetrics() {
	pc.initPostgreSQLMetrics()
	pc.initMySQLMetrics()
	pc.initSQLiteMetrics()
	pc.initSQLServerMetrics()
}

// initPostgreSQLMetrics initializes PostgreSQL-specific metrics
func (pc *PrometheusCollector) initPostgreSQLMetrics() {
	pc.postgresMetrics = &PostgreSQLPrometheusMetrics{
		replicationLag: promauto.With(pc.registry).NewGauge(
			prometheus.GaugeOpts{
				Namespace: pc.namespace,
				Subsystem: "postgresql",
				Name:      "replication_lag_seconds",
				Help:      "PostgreSQL replication lag in seconds",
			},
		),

		locksHeld: promauto.With(pc.registry).NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: pc.namespace,
				Subsystem: "postgresql",
				Name:      "locks_held",
				Help:      "Number of PostgreSQL locks held by type",
			},
			[]string{"lock_type", "mode"},
		),

		bgWriterStats: promauto.With(pc.registry).NewCounterVec(
			prometheus.CounterOpts{
				Namespace: pc.namespace,
				Subsystem: "postgresql",
				Name:      "bgwriter_stats_total",
				Help:      "PostgreSQL background writer statistics",
			},
			[]string{"stat_type"},
		),

		checkpointStats: promauto.With(pc.registry).NewCounterVec(
			prometheus.CounterOpts{
				Namespace: pc.namespace,
				Subsystem: "postgresql",
				Name:      "checkpoint_stats_total",
				Help:      "PostgreSQL checkpoint statistics",
			},
			[]string{"stat_type"},
		),

		vacuumStats: promauto.With(pc.registry).NewCounterVec(
			prometheus.CounterOpts{
				Namespace: pc.namespace,
				Subsystem: "postgresql",
				Name:      "vacuum_stats_total",
				Help:      "PostgreSQL vacuum statistics",
			},
			[]string{"operation", "table"},
		),

		indexUsage: promauto.With(pc.registry).NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: pc.namespace,
				Subsystem: "postgresql",
				Name:      "index_usage_ratio",
				Help:      "PostgreSQL index usage ratio",
			},
			[]string{"database", "table", "index"},
		),

		tableSize: promauto.With(pc.registry).NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: pc.namespace,
				Subsystem: "postgresql",
				Name:      "table_size_bytes",
				Help:      "PostgreSQL table size in bytes",
			},
			[]string{"database", "table"},
		),

		connectionsByState: promauto.With(pc.registry).NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: pc.namespace,
				Subsystem: "postgresql",
				Name:      "connections_by_state",
				Help:      "PostgreSQL connections grouped by state",
			},
			[]string{"state", "database"},
		),
	}
}

// initMySQLMetrics initializes MySQL-specific metrics
func (pc *PrometheusCollector) initMySQLMetrics() {
	pc.mysqlMetrics = &MySQLPrometheusMetrics{
		innodbStats: promauto.With(pc.registry).NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: pc.namespace,
				Subsystem: "mysql",
				Name:      "innodb_stats",
				Help:      "MySQL InnoDB statistics",
			},
			[]string{"stat_name"},
		),

		replicationDelay: promauto.With(pc.registry).NewGauge(
			prometheus.GaugeOpts{
				Namespace: pc.namespace,
				Subsystem: "mysql",
				Name:      "replication_delay_seconds",
				Help:      "MySQL replication delay in seconds",
			},
		),

		slowQueries: promauto.With(pc.registry).NewCounter(
			prometheus.CounterOpts{
				Namespace: pc.namespace,
				Subsystem: "mysql",
				Name:      "slow_queries_total",
				Help:      "Total number of MySQL slow queries",
			},
		),

		tableSize: promauto.With(pc.registry).NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: pc.namespace,
				Subsystem: "mysql",
				Name:      "table_size_bytes",
				Help:      "MySQL table size in bytes",
			},
			[]string{"database", "table"},
		),

		indexUsage: promauto.With(pc.registry).NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: pc.namespace,
				Subsystem: "mysql",
				Name:      "index_usage_count",
				Help:      "MySQL index usage count",
			},
			[]string{"database", "table", "index"},
		),

		connectionsByState: promauto.With(pc.registry).NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: pc.namespace,
				Subsystem: "mysql",
				Name:      "connections_by_state",
				Help:      "MySQL connections grouped by state",
			},
			[]string{"state"},
		),

		bufferPoolStats: promauto.With(pc.registry).NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: pc.namespace,
				Subsystem: "mysql",
				Name:      "buffer_pool_stats",
				Help:      "MySQL InnoDB buffer pool statistics",
			},
			[]string{"stat_type"},
		),
	}
}

// initSQLiteMetrics initializes SQLite-specific metrics
func (pc *PrometheusCollector) initSQLiteMetrics() {
	pc.sqliteMetrics = &SQLitePrometheusMetrics{
		fileSize: promauto.With(pc.registry).NewGauge(
			prometheus.GaugeOpts{
				Namespace: pc.namespace,
				Subsystem: "sqlite",
				Name:      "file_size_bytes",
				Help:      "SQLite database file size in bytes",
			},
		),

		pageCount: promauto.With(pc.registry).NewGauge(
			prometheus.GaugeOpts{
				Namespace: pc.namespace,
				Subsystem: "sqlite",
				Name:      "page_count",
				Help:      "SQLite database page count",
			},
		),

		cacheStats: promauto.With(pc.registry).NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: pc.namespace,
				Subsystem: "sqlite",
				Name:      "cache_stats",
				Help:      "SQLite cache statistics",
			},
			[]string{"stat_type"},
		),

		walStats: promauto.With(pc.registry).NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: pc.namespace,
				Subsystem: "sqlite",
				Name:      "wal_stats",
				Help:      "SQLite WAL statistics",
			},
			[]string{"stat_type"},
		),

		pragmaSettings: promauto.With(pc.registry).NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: pc.namespace,
				Subsystem: "sqlite",
				Name:      "pragma_settings",
				Help:      "SQLite PRAGMA settings",
			},
			[]string{"pragma_name"},
		),
	}
}

// initSQLServerMetrics initializes SQL Server-specific metrics
func (pc *PrometheusCollector) initSQLServerMetrics() {
	pc.sqlserverMetrics = &SQLServerPrometheusMetrics{
		bufferCacheHitRatio: promauto.With(pc.registry).NewGauge(
			prometheus.GaugeOpts{
				Namespace: pc.namespace,
				Subsystem: "sqlserver",
				Name:      "buffer_cache_hit_ratio",
				Help:      "SQL Server buffer cache hit ratio",
			},
		),

		pageLifeExpectancy: promauto.With(pc.registry).NewGauge(
			prometheus.GaugeOpts{
				Namespace: pc.namespace,
				Subsystem: "sqlserver",
				Name:      "page_life_expectancy_seconds",
				Help:      "SQL Server page life expectancy in seconds",
			},
		),

		waitStats: promauto.With(pc.registry).NewCounterVec(
			prometheus.CounterOpts{
				Namespace: pc.namespace,
				Subsystem: "sqlserver",
				Name:      "wait_stats_total",
				Help:      "SQL Server wait statistics",
			},
			[]string{"wait_type"},
		),

		lockStats: promauto.With(pc.registry).NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: pc.namespace,
				Subsystem: "sqlserver",
				Name:      "lock_stats",
				Help:      "SQL Server lock statistics",
			},
			[]string{"lock_type", "mode"},
		),

		indexFragmentation: promauto.With(pc.registry).NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: pc.namespace,
				Subsystem: "sqlserver",
				Name:      "index_fragmentation_percent",
				Help:      "SQL Server index fragmentation percentage",
			},
			[]string{"database", "table", "index"},
		),

		dbSize: promauto.With(pc.registry).NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: pc.namespace,
				Subsystem: "sqlserver",
				Name:      "database_size_bytes",
				Help:      "SQL Server database size in bytes",
			},
			[]string{"database", "file_type"},
		),
	}
}

// Database-specific metric recording methods

// RecordPostgreSQLReplicationLag records PostgreSQL replication lag
func (pc *PrometheusCollector) RecordPostgreSQLReplicationLag(lagSeconds float64) {
	if pc.postgresMetrics != nil {
		pc.postgresMetrics.replicationLag.Set(lagSeconds)
	}
}

// RecordPostgreSQLLocks records PostgreSQL lock information
func (pc *PrometheusCollector) RecordPostgreSQLLocks(lockType, mode string, count int) {
	if pc.postgresMetrics != nil {
		pc.postgresMetrics.locksHeld.WithLabelValues(lockType, mode).Set(float64(count))
	}
}

// RecordMySQLReplicationDelay records MySQL replication delay
func (pc *PrometheusCollector) RecordMySQLReplicationDelay(delaySeconds float64) {
	if pc.mysqlMetrics != nil {
		pc.mysqlMetrics.replicationDelay.Set(delaySeconds)
	}
}

// RecordMySQLSlowQuery records MySQL slow query
func (pc *PrometheusCollector) RecordMySQLSlowQuery() {
	if pc.mysqlMetrics != nil {
		pc.mysqlMetrics.slowQueries.Inc()
	}
}

// RecordSQLiteFileSize records SQLite file size
func (pc *PrometheusCollector) RecordSQLiteFileSize(sizeBytes int64) {
	if pc.sqliteMetrics != nil {
		pc.sqliteMetrics.fileSize.Set(float64(sizeBytes))
	}
}

// RecordSQLServerBufferCacheHitRatio records SQL Server buffer cache hit ratio
func (pc *PrometheusCollector) RecordSQLServerBufferCacheHitRatio(ratio float64) {
	if pc.sqlserverMetrics != nil {
		pc.sqlserverMetrics.bufferCacheHitRatio.Set(ratio)
	}
}

// GetRegistry returns the Prometheus registry used by this collector
func (pc *PrometheusCollector) GetRegistry() prometheus.Registerer {
	return pc.registry
}

// CreateDatabaseMetricsCollector creates a new DatabaseMetricsCollector with Prometheus backend
func (pc *PrometheusCollector) CreateDatabaseMetricsCollector() *DatabaseMetricsCollector {
	return NewDatabaseMetricsCollector(pc)
}