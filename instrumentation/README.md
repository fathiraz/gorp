# GORP Instrumentation Package

The instrumentation package provides comprehensive metrics collection, monitoring, and observability for GORP database operations. It supports pluggable backends with built-in support for Prometheus, custom business metrics, time-based aggregation, and database-specific monitoring.

## Features

- **Pluggable Metrics Backends**: Support for multiple metrics collectors (Prometheus, custom implementations)
- **Database-Specific Metrics**: Specialized collectors for PostgreSQL, MySQL, SQLite, and SQL Server
- **Business Logic Metrics**: Pre-built counters, gauges, histograms, and timers for common business operations
- **Time-Based Aggregation**: Configurable time windows with statistical calculations (mean, median, percentiles)
- **Grafana Dashboards**: Ready-to-use dashboard templates for visualization
- **Go 1.24 Generics**: Type-safe metric collection with zero reflection

## Quick Start

### Basic Setup

```go
package main

import (
    "database/sql"
    "time"

    "github.com/go-gorp/gorp/v3/instrumentation"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
    "net/http"
)

func main() {
    // Create Prometheus collector
    prometheusCollector := instrumentation.NewPrometheusCollector("myapp", prometheus.DefaultRegisterer)

    // Create metrics manager with Prometheus backend
    metricsManager := instrumentation.NewMetricsManager()
    metricsManager.AddCollector(prometheusCollector)

    // Create database metrics collector (assumes you have a database connection)
    dbCollector := metricsManager.CreateDatabaseMetricsCollector()

    // Record some metrics
    dbCollector.RecordQuery("SELECT", "users", 50*time.Millisecond, nil)
    dbCollector.RecordConnection(true)

    // Expose Prometheus metrics
    http.Handle("/metrics", promhttp.Handler())
    http.ListenAndServe(":8080", nil)
}
```

### Database-Specific Monitoring

```go
// PostgreSQL monitoring
if pool, ok := db.(*pgxpool.Pool); ok {
    dbSpecificCollector := instrumentation.NewDatabaseSpecificCollector(
        prometheusCollector,
        "postgresql",
        pool,
        30*time.Second, // collection interval
    )
    defer dbSpecificCollector.Close()
}

// MySQL monitoring
if sqlxDB, ok := db.(*sqlx.DB); ok && driverName == "mysql" {
    dbSpecificCollector := instrumentation.NewDatabaseSpecificCollector(
        prometheusCollector,
        "mysql",
        sqlxDB,
        30*time.Second,
    )
    defer dbSpecificCollector.Close()
}
```

### Business Metrics

```go
// Get default business metrics collector
businessMetrics := instrumentation.GetDefaultBusinessMetricsCollector()

// Add Prometheus collector to the registry
instrumentation.GetDefaultCustomMetricsRegistry().AddCollector(prometheusCollector)

// Record business events
businessMetrics.RecordUserRegistration("web")
businessMetrics.RecordUserLogin("oauth", true)
businessMetrics.RecordOrderCreated("mobile", 99.99)
businessMetrics.RecordAPIRequest("GET", "/api/users", 200, 150*time.Millisecond, 1024)

// Update live metrics
businessMetrics.UpdateActiveUsers(1250, "authenticated")
businessMetrics.UpdateQueueDepth("email_queue", 45)
```

### Custom Metrics

```go
// Create custom metrics registry
customRegistry := instrumentation.NewCustomMetricsRegistry()
customRegistry.AddCollector(prometheusCollector)

// Create named metrics
userActions := customRegistry.NewNamedCounter("user_actions", "Total user actions performed")
cacheHitRate := customRegistry.NewNamedGauge("cache_hit_rate", "Cache hit rate percentage")
requestLatency := customRegistry.NewNamedHistogram("request_latency", "Request latency distribution")

// Use the metrics
userActions.Inc(map[string]string{"action": "click", "page": "dashboard"})
cacheHitRate.Set(0.95, map[string]string{"cache": "redis"})
requestLatency.Observe(0.125, map[string]string{"endpoint": "/api/data"})
```

### Aggregation Windows

```go
// Create aggregation manager
aggregatorManager := instrumentation.NewMetricAggregatorManager()

// Create standard aggregators for a metric (minute, hourly, daily windows)
aggregatorManager.CreateStandardAggregators("api_response_time")

// Get specific aggregator and add values
minuteAggregator := aggregatorManager.GetAggregator("api_response_time_minute")
minuteAggregator.AddValue(0.125, map[string]string{"endpoint": "/users"})
minuteAggregator.AddValue(0.089, map[string]string{"endpoint": "/users"})

// Get aggregated data
timeSeriesData := minuteAggregator.GetAggregatedData(map[string]string{"endpoint": "/users"})
if timeSeriesData != nil {
    for _, window := range timeSeriesData.Windows {
        fmt.Printf("Window %s - %s: Mean=%.3f, P95=%.3f, Count=%d\n",
            window.WindowStart.Format(time.RFC3339),
            window.WindowEnd.Format(time.RFC3339),
            window.Mean,
            window.P95,
            window.Count,
        )
    }
}
```

## Configuration

### Prometheus Configuration

```go
// Custom Prometheus registry
customRegistry := prometheus.NewRegistry()
prometheusCollector := instrumentation.NewPrometheusCollector("myapp", customRegistry)

// Custom namespace and labels
prometheusCollector := instrumentation.NewPrometheusCollector("custom_namespace", prometheus.DefaultRegisterer)
```

### Aggregation Windows

```go
// Pre-defined windows
windows := instrumentation.CommonAggregationWindows
// Available: "realtime", "minute", "hourly", "daily", "weekly", "monthly"

// Custom window
customWindow := instrumentation.AggregationWindow{
    Duration: 5 * time.Minute,  // 5-minute windows
    Size:     288,              // Keep 288 windows (24 hours)
    Interval: 30 * time.Second, // Aggregate every 30 seconds
}

aggregator := instrumentation.NewMetricAggregator("custom_metric", customWindow)
```

## Metrics Reference

### Standard Database Metrics

| Metric | Type | Description | Labels |
|--------|------|-------------|---------|
| `gorp_queries_total` | Counter | Total database queries | `operation`, `table`, `status` |
| `gorp_query_duration_seconds` | Histogram | Query execution time | `operation`, `table` |
| `gorp_connections_opened_total` | Counter | Total connections opened | - |
| `gorp_connections_active` | Gauge | Active connections | - |
| `gorp_connections_idle` | Gauge | Idle connections | - |
| `gorp_connection_lifetime_seconds` | Histogram | Connection lifetime | - |
| `gorp_transactions_total` | Counter | Total transactions | `status` |
| `gorp_transaction_duration_seconds` | Histogram | Transaction duration | - |
| `gorp_errors_total` | Counter | Total errors | `type`, `operation` |

### Business Metrics

| Metric | Type | Description | Labels |
|--------|------|-------------|---------|
| `gorp_user_registrations_total` | Counter | User registrations | `source` |
| `gorp_user_logins_total` | Counter | User logins | `method`, `success` |
| `gorp_orders_created_total` | Counter | Orders created | `channel` |
| `gorp_orders_completed_total` | Counter | Orders completed | `channel` |
| `gorp_payments_processed_total` | Counter | Payments processed | `method`, `status` |
| `gorp_active_users` | Gauge | Currently active users | `type` |
| `gorp_queue_depth` | Gauge | Queue depth | `queue` |
| `gorp_api_requests_total` | Counter | API requests | `method`, `endpoint`, `status_code` |

### PostgreSQL-Specific Metrics

| Metric | Type | Description | Labels |
|--------|------|-------------|---------|
| `gorp_postgresql_replication_lag_seconds` | Gauge | Replication lag | - |
| `gorp_postgresql_connections_by_state` | Gauge | Connections by state | `state`, `database` |
| `gorp_postgresql_locks_held` | Gauge | Locks held | `lock_type`, `mode` |
| `gorp_postgresql_table_size_bytes` | Gauge | Table sizes | `database`, `table` |
| `gorp_postgresql_index_usage_ratio` | Gauge | Index usage ratio | `database`, `table`, `index` |

### MySQL-Specific Metrics

| Metric | Type | Description | Labels |
|--------|------|-------------|---------|
| `gorp_mysql_replication_delay_seconds` | Gauge | Replication delay | - |
| `gorp_mysql_slow_queries_total` | Counter | Slow queries | - |
| `gorp_mysql_innodb_stats` | Gauge | InnoDB statistics | `stat_name` |
| `gorp_mysql_buffer_pool_stats` | Gauge | Buffer pool stats | `stat_type` |

### SQLite-Specific Metrics

| Metric | Type | Description | Labels |
|--------|------|-------------|---------|
| `gorp_sqlite_file_size_bytes` | Gauge | Database file size | - |
| `gorp_sqlite_page_count` | Gauge | Page count | - |
| `gorp_sqlite_cache_stats` | Gauge | Cache statistics | `stat_type` |
| `gorp_sqlite_wal_stats` | Gauge | WAL statistics | `stat_type` |

## Grafana Dashboards

The package includes three pre-built Grafana dashboard templates:

### 1. GORP Overview Dashboard (`grafana/gorp-overview.json`)
- Query rate and duration percentiles
- Connection pool status
- Error rates
- Transaction performance

### 2. PostgreSQL-Specific Dashboard (`grafana/postgresql-specific.json`)
- Replication lag monitoring
- Connection state breakdown
- Lock statistics
- Table and index metrics
- Background writer stats
- Vacuum/analyze statistics

### 3. Business Metrics Dashboard (`grafana/business-metrics.json`)
- User registration and login rates
- Order processing metrics
- Payment processing stats
- API performance metrics
- Queue depth and active users

### Installing Dashboards

1. Open Grafana
2. Go to Dashboards > Import
3. Upload the JSON files from `instrumentation/grafana/`
4. Configure your Prometheus datasource

## Integration Patterns

### With Database Connections

```go
// PostgreSQL with pgxpool
func setupPostgreSQLMetrics(pool *pgxpool.Pool, collector MetricsCollector) {
    dbCollector := NewDatabaseSpecificCollector(collector, "postgresql", pool, 30*time.Second)

    // Hook into connection lifecycle
    pool.Config().AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
        collector.RecordConnection(true)
        return nil
    }

    pool.Config().BeforeClose = func(conn *pgx.Conn) {
        collector.RecordConnection(false)
    }
}

// SQLX with middleware
func setupSQLXMetrics(db *sqlx.DB, collector MetricsCollector) *sqlx.DB {
    // Wrap queries with metrics
    original := db.QueryxContext
    db.QueryxContext = func(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error) {
        start := time.Now()
        rows, err := original(ctx, query, args...)

        operation := "SELECT" // Parse from query
        table := "unknown"    // Parse from query
        collector.RecordQuery(operation, table, time.Since(start), err)

        return rows, err
    }

    return db
}
```

### Middleware Integration

```go
// HTTP middleware for API metrics
func MetricsMiddleware(businessCollector *BusinessMetricsCollector) func(http.Handler) http.Handler {
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            start := time.Now()

            // Wrap response writer to capture status code
            wrapper := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

            next.ServeHTTP(wrapper, r)

            // Record metrics
            duration := time.Since(start)
            businessCollector.RecordAPIRequest(
                r.Method,
                r.URL.Path,
                wrapper.statusCode,
                duration,
                wrapper.size,
            )
        })
    }
}

type responseWriter struct {
    http.ResponseWriter
    statusCode int
    size       int64
}

func (rw *responseWriter) WriteHeader(code int) {
    rw.statusCode = code
    rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
    size, err := rw.ResponseWriter.Write(b)
    rw.size += int64(size)
    return size, err
}
```

### Context-Aware Metrics

```go
// Using context for distributed metrics
func ProcessOrder(ctx context.Context, order Order) error {
    // Create metrics context
    metricsCtx := instrumentation.NewBusinessMetricsContext(
        ctx,
        businessCollector,
        map[string]string{
            "user_id": order.UserID,
            "channel": order.Channel,
        },
    )

    // Process order with metrics
    return businessCollector.TimeOperation("order_processing", func() {
        // Business logic here

        // Record events with context
        metricsCtx.RecordOrderCreated()

        if err := processPayment(order); err != nil {
            metricsCtx.RecordPaymentFailure()
            return
        }

        metricsCtx.RecordOrderCompleted()
    })
}
```

## Performance Considerations

### Memory Usage
- Aggregators maintain raw values for percentile calculation
- Configure appropriate window sizes to balance accuracy vs memory
- Use `AggregatingMetricsCollector` wrapper for automatic cleanup

### Collection Intervals
- Database-specific metrics: 30-60 seconds
- Real-time business metrics: 1-5 seconds
- Aggregation intervals: 10-30 seconds

### Cardinality
- Avoid high-cardinality labels (user IDs, session IDs)
- Use sampling for detailed tracing
- Consider label aggregation for similar values

## Troubleshooting

### Common Issues

**High Memory Usage**
```go
// Reduce aggregation window sizes
customWindow := instrumentation.AggregationWindow{
    Duration: 1 * time.Minute,
    Size:     60,  // Reduced from default
    Interval: 10 * time.Second,
}
```

**Missing Database Metrics**
```go
// Check database connection type and permissions
if err := db.Ping(); err != nil {
    log.Printf("Database connection issue: %v", err)
}

// Verify database user has required permissions for system tables
```

**Prometheus Metrics Not Appearing**
```go
// Ensure metrics are being recorded
collector.Counter("test_metric", map[string]string{"test": "value"})

// Check Prometheus configuration
if registry, ok := collector.(*instrumentation.PrometheusCollector); ok {
    gatherer := prometheus.Gatherers{registry.GetRegistry()}
    // Test gathering
}
```

## Examples

See the `examples/` directory for complete integration examples:
- `examples/postgresql/` - PostgreSQL monitoring setup
- `examples/mysql/` - MySQL monitoring setup
- `examples/business/` - Business metrics integration
- `examples/aggregation/` - Time-based aggregation patterns

## Contributing

When adding new metrics:

1. Follow naming conventions: `gorp_[subsystem_]metric_name_unit`
2. Add appropriate labels for filtering
3. Update dashboard templates
4. Include tests for new collectors
5. Document new metrics in this README

## License

This package is part of the GORP project and follows the same license terms.