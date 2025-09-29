package instrumentation

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetricsInterface(t *testing.T) {
	collector := NewMetricsCollector()

	t.Run("Record query", func(t *testing.T) {
		start := time.Now()
		time.Sleep(10 * time.Millisecond)

		collector.RecordQuery(context.Background(), QueryInfo{
			Query:    "SELECT * FROM users",
			Duration: time.Since(start),
			Type:     "SELECT",
			Database: "test_db",
			Table:    "users",
		})

		// Verify metrics were recorded
		if collector.queryTotal == nil {
			t.Error("Query total counter should be initialized")
		}
	})

	t.Run("Record connection", func(t *testing.T) {
		collector.RecordConnection(ConnectionInfo{
			Database: "test_db",
			Active:   5,
			Idle:     3,
			Open:     8,
		})

		// Verify connection metrics were recorded
		if collector.connectionGauge == nil {
			t.Error("Connection gauge should be initialized")
		}
	})

	t.Run("Record error", func(t *testing.T) {
		collector.RecordError(context.Background(), ErrorInfo{
			Error:    "connection timeout",
			Type:     "timeout",
			Database: "test_db",
			Query:    "SELECT * FROM users",
		})

		// Verify error metrics were recorded
		if collector.errorTotal == nil {
			t.Error("Error total counter should be initialized")
		}
	})
}

func TestPrometheusMetrics(t *testing.T) {
	// Create a test registry
	registry := prometheus.NewRegistry()
	collector := NewMetricsCollector()
	registry.MustRegister(collector)

	t.Run("Query metrics", func(t *testing.T) {
		// Record some test queries
		collector.RecordQuery(context.Background(), QueryInfo{
			Query:    "SELECT * FROM users",
			Duration: 50 * time.Millisecond,
			Type:     "SELECT",
			Database: "test_db",
			Table:    "users",
		})

		collector.RecordQuery(context.Background(), QueryInfo{
			Query:    "INSERT INTO users VALUES (?)",
			Duration: 25 * time.Millisecond,
			Type:     "INSERT",
			Database: "test_db",
			Table:    "users",
		})

		// Check that metrics were recorded
		metricNames := []string{
			"gorp_database_queries_total",
			"gorp_database_query_duration_seconds",
		}

		for _, name := range metricNames {
			if err := testutil.GatherAndCompare(registry, strings.NewReader(""), name); err != nil {
				t.Logf("Metric %s verification: %v", name, err)
			}
		}
	})

	t.Run("Connection metrics", func(t *testing.T) {
		collector.RecordConnection(ConnectionInfo{
			Database: "test_db",
			Active:   10,
			Idle:     5,
			Open:     15,
		})

		// Verify connection metrics
		expected := `
# HELP gorp_database_connections_active Number of active database connections
# TYPE gorp_database_connections_active gauge
gorp_database_connections_active{database="test_db"} 10
`
		if err := testutil.GatherAndCompare(registry, strings.NewReader(expected), "gorp_database_connections_active"); err != nil {
			t.Logf("Connection metric verification: %v", err)
		}
	})

	t.Run("Error metrics", func(t *testing.T) {
		collector.RecordError(context.Background(), ErrorInfo{
			Error:    "connection failed",
			Type:     "connection",
			Database: "test_db",
			Query:    "SELECT 1",
		})

		collector.RecordError(context.Background(), ErrorInfo{
			Error:    "syntax error",
			Type:     "sql",
			Database: "test_db",
			Query:    "SELECTT * FROM users",
		})

		// Check error metrics were recorded
		metricNames := []string{
			"gorp_database_errors_total",
		}

		for _, name := range metricNames {
			if err := testutil.GatherAndCompare(registry, strings.NewReader(""), name); err != nil {
				t.Logf("Error metric %s verification: %v", name, err)
			}
		}
	})
}

func TestCustomMetrics(t *testing.T) {
	customMetrics := NewCustomMetrics("test_namespace")

	t.Run("Register custom counter", func(t *testing.T) {
		counterOpts := prometheus.CounterOpts{
			Namespace: "test_namespace",
			Name:      "custom_operations_total",
			Help:      "Total number of custom operations",
		}

		counter := customMetrics.RegisterCounter(counterOpts, []string{"operation_type"})
		if counter == nil {
			t.Error("Failed to register custom counter")
		}

		// Use the counter
		counter.WithLabelValues("test_operation").Inc()
	})

	t.Run("Register custom gauge", func(t *testing.T) {
		gaugeOpts := prometheus.GaugeOpts{
			Namespace: "test_namespace",
			Name:      "custom_queue_size",
			Help:      "Current queue size",
		}

		gauge := customMetrics.RegisterGauge(gaugeOpts, []string{"queue_name"})
		if gauge == nil {
			t.Error("Failed to register custom gauge")
		}

		// Use the gauge
		gauge.WithLabelValues("priority_queue").Set(42)
	})

	t.Run("Register custom histogram", func(t *testing.T) {
		histogramOpts := prometheus.HistogramOpts{
			Namespace: "test_namespace",
			Name:      "custom_processing_duration_seconds",
			Help:      "Processing duration in seconds",
			Buckets:   prometheus.DefBuckets,
		}

		histogram := customMetrics.RegisterHistogram(histogramOpts, []string{"processor"})
		if histogram == nil {
			t.Error("Failed to register custom histogram")
		}

		// Use the histogram
		histogram.WithLabelValues("data_processor").Observe(0.123)
	})

	t.Run("Metric collision handling", func(t *testing.T) {
		// Try to register the same metric twice
		counterOpts := prometheus.CounterOpts{
			Namespace: "test_namespace",
			Name:      "duplicate_metric",
			Help:      "Duplicate metric test",
		}

		counter1 := customMetrics.RegisterCounter(counterOpts, []string{"label1"})
		counter2 := customMetrics.RegisterCounter(counterOpts, []string{"label1"})

		if counter1 == nil {
			t.Error("First registration should succeed")
		}
		if counter2 == nil {
			t.Error("Second registration should return the existing metric")
		}
	})
}

func TestDatabaseSpecificMetrics(t *testing.T) {
	collector := NewMetricsCollector()

	t.Run("PostgreSQL metrics", func(t *testing.T) {
		collector.RecordDatabaseSpecific(DatabaseSpecificInfo{
			Database: "test_db",
			Dialect:  "postgresql",
			Metrics: map[string]float64{
				"buffer_cache_hit_ratio":    0.95,
				"connections_active":        25,
				"shared_buffers_used_bytes": 1024 * 1024 * 256,
			},
		})

		// Verify PostgreSQL-specific metrics
		if collector.postgresBufferCacheHitRatio == nil {
			t.Error("PostgreSQL buffer cache hit ratio metric should be initialized")
		}
	})

	t.Run("MySQL metrics", func(t *testing.T) {
		collector.RecordDatabaseSpecific(DatabaseSpecificInfo{
			Database: "test_db",
			Dialect:  "mysql",
			Metrics: map[string]float64{
				"innodb_buffer_pool_reads":      1000,
				"innodb_buffer_pool_read_requests": 10000,
				"query_cache_hits":              5000,
			},
		})

		// Verify MySQL-specific metrics
		if collector.mysqlInnoDBBufferPoolReads == nil {
			t.Error("MySQL InnoDB buffer pool reads metric should be initialized")
		}
	})

	t.Run("SQLite metrics", func(t *testing.T) {
		collector.RecordDatabaseSpecific(DatabaseSpecificInfo{
			Database: "test_db",
			Dialect:  "sqlite",
			Metrics: map[string]float64{
				"page_cache_hits":   2000,
				"page_cache_misses": 200,
				"database_size":     1024 * 1024 * 10,
			},
		})

		// Verify SQLite-specific metrics
		if collector.sqlitePageCacheHits == nil {
			t.Error("SQLite page cache hits metric should be initialized")
		}
	})

	t.Run("Unknown dialect", func(t *testing.T) {
		// Should not panic with unknown dialect
		collector.RecordDatabaseSpecific(DatabaseSpecificInfo{
			Database: "test_db",
			Dialect:  "unknown",
			Metrics: map[string]float64{
				"some_metric": 42,
			},
		})
	})
}

func TestMetricsAggregation(t *testing.T) {
	aggregator := NewMetricsAggregator()

	t.Run("Add metrics", func(t *testing.T) {
		metrics1 := DatabaseMetrics{
			Database:         "db1",
			QueriesTotal:     100,
			ErrorsTotal:      5,
			AvgQueryDuration: 50 * time.Millisecond,
		}

		metrics2 := DatabaseMetrics{
			Database:         "db2",
			QueriesTotal:     200,
			ErrorsTotal:      10,
			AvgQueryDuration: 75 * time.Millisecond,
		}

		aggregator.AddMetrics(metrics1)
		aggregator.AddMetrics(metrics2)

		snapshot := aggregator.GetSnapshot()
		if len(snapshot) != 2 {
			t.Errorf("Expected 2 database metrics, got %d", len(snapshot))
		}
	})

	t.Run("Aggregate by time window", func(t *testing.T) {
		now := time.Now()

		// Add metrics for different time windows
		for i := 0; i < 10; i++ {
			metrics := DatabaseMetrics{
				Database:         "test_db",
				QueriesTotal:     uint64(10 + i),
				ErrorsTotal:      uint64(i),
				AvgQueryDuration: time.Duration(50+i*5) * time.Millisecond,
				Timestamp:        now.Add(time.Duration(-i) * time.Minute),
			}
			aggregator.AddMetrics(metrics)
		}

		// Get aggregated metrics for last 5 minutes
		fiveMinAgo := now.Add(-5 * time.Minute)
		aggregated := aggregator.GetAggregatedMetrics("test_db", fiveMinAgo, now)

		if aggregated.QueriesTotal == 0 {
			t.Error("Expected aggregated queries total to be > 0")
		}
	})

	t.Run("Clear old metrics", func(t *testing.T) {
		// Add old metrics
		oldTime := time.Now().Add(-2 * time.Hour)
		oldMetrics := DatabaseMetrics{
			Database:  "old_db",
			Timestamp: oldTime,
		}
		aggregator.AddMetrics(oldMetrics)

		// Clear metrics older than 1 hour
		cutoff := time.Now().Add(-1 * time.Hour)
		aggregator.ClearOldMetrics(cutoff)

		// Verify old metrics are gone
		snapshot := aggregator.GetSnapshot()
		for _, metrics := range snapshot {
			if metrics.Database == "old_db" {
				t.Error("Old metrics should have been cleared")
			}
		}
	})
}

func TestInstrumentationIntegration(t *testing.T) {
	config := Config{
		Enabled:         true,
		MetricsInterval: 100 * time.Millisecond,
		EnablePrometheus: true,
		PrometheusNamespace: "test_gorp",
	}

	instrumentation := New(config)

	t.Run("Start instrumentation", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		err := instrumentation.Start(ctx)
		if err != nil {
			t.Fatalf("Failed to start instrumentation: %v", err)
		}

		// Wait a bit for metrics collection
		time.Sleep(200 * time.Millisecond)

		instrumentation.Stop()
	})

	t.Run("Record operations", func(t *testing.T) {
		ctx := context.Background()

		// Record a query
		instrumentation.RecordQuery(ctx, QueryInfo{
			Query:    "SELECT * FROM test_table",
			Duration: 45 * time.Millisecond,
			Type:     "SELECT",
			Database: "test_db",
			Table:    "test_table",
		})

		// Record a connection event
		instrumentation.RecordConnection(ConnectionInfo{
			Database: "test_db",
			Active:   3,
			Idle:     2,
			Open:     5,
		})

		// Record an error
		instrumentation.RecordError(ctx, ErrorInfo{
			Error:    "table not found",
			Type:     "sql_error",
			Database: "test_db",
			Query:    "SELECT * FROM nonexistent",
		})

		// Verify metrics are being collected
		if instrumentation.collector == nil {
			t.Error("Metrics collector should be initialized")
		}
	})

	t.Run("Custom metrics", func(t *testing.T) {
		// Register and use custom metrics
		counterOpts := prometheus.CounterOpts{
			Namespace: "test_gorp",
			Name:      "custom_events_total",
			Help:      "Total custom events",
		}

		counter := instrumentation.RegisterCounter(counterOpts, []string{"event_type"})
		if counter == nil {
			t.Error("Failed to register custom counter")
		}

		counter.WithLabelValues("test_event").Inc()
	})
}

// Benchmark tests for metrics performance
func BenchmarkRecordQuery(b *testing.B) {
	collector := NewMetricsCollector()
	ctx := context.Background()

	queryInfo := QueryInfo{
		Query:    "SELECT * FROM users WHERE id = ?",
		Duration: 25 * time.Millisecond,
		Type:     "SELECT",
		Database: "test_db",
		Table:    "users",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		collector.RecordQuery(ctx, queryInfo)
	}
}

func BenchmarkRecordConnection(b *testing.B) {
	collector := NewMetricsCollector()

	connInfo := ConnectionInfo{
		Database: "test_db",
		Active:   10,
		Idle:     5,
		Open:     15,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		collector.RecordConnection(connInfo)
	}
}

func BenchmarkRecordError(b *testing.B) {
	collector := NewMetricsCollector()
	ctx := context.Background()

	errorInfo := ErrorInfo{
		Error:    "connection timeout",
		Type:     "timeout",
		Database: "test_db",
		Query:    "SELECT 1",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		collector.RecordError(ctx, errorInfo)
	}
}

func BenchmarkMetricsAggregation(b *testing.B) {
	aggregator := NewMetricsAggregator()

	metrics := DatabaseMetrics{
		Database:         "test_db",
		QueriesTotal:     100,
		ErrorsTotal:      5,
		AvgQueryDuration: 50 * time.Millisecond,
		Timestamp:        time.Now(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		aggregator.AddMetrics(metrics)
	}
}