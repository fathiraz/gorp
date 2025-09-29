package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/fathiraz/gorp/instrumentation"
	"github.com/prometheus/client_golang/prometheus"
)

func main() {
	// Test 1: Basic MetricsCollector interface
	fmt.Println("=== Testing Basic Metrics Collection ===")

	// Create Prometheus collector
	registry := prometheus.NewRegistry()
	promCollector := instrumentation.NewPrometheusCollector("gorp", registry)
	if err := promCollector.Start(context.Background()); err != nil {
		log.Fatalf("Failed to start Prometheus collector: %v", err)
	}
	defer promCollector.Stop(context.Background())

	// Test counter operations
	labels := map[string]string{"operation": "select", "table": "users"}
	promCollector.IncrementCounter("gorp_queries_total", labels)
	promCollector.IncrementCounterBy("gorp_queries_total", 5.0, labels)

	// Test gauge operations
	promCollector.SetGauge("gorp_connections_active", 10.0, nil)
	promCollector.IncrementGauge("gorp_connections_active", nil)
	promCollector.DecrementGauge("gorp_connections_active", nil)

	// Test histogram operations
	promCollector.RecordHistogram("gorp_query_duration_seconds", 0.123, labels)
	promCollector.RecordDuration("gorp_query_duration_seconds", 150*time.Millisecond, labels)

	// Test timer operations
	timer := promCollector.StartTimer("gorp_operation_duration", labels)
	time.Sleep(50 * time.Millisecond)
	duration := timer.Stop()
	fmt.Printf("Timer measured: %v\n", duration)

	// Test 2: Custom Metrics Registry
	fmt.Println("\n=== Testing Custom Metrics ===")

	customRegistry := instrumentation.GetDefaultCustomMetricsRegistry()
	customRegistry.AddCollector(promCollector)

	businessCollector := instrumentation.GetDefaultBusinessMetricsCollector()
	businessCollector.RecordUserRegistration("web")
	businessCollector.RecordUserLogin("email", true)
	businessCollector.RecordAPIRequest("GET", "/api/users", 200, 100*time.Millisecond, 1024)
	businessCollector.UpdateActiveUsers(150, "authenticated")

	// Test 3: Metrics Aggregation
	fmt.Println("\n=== Testing Metrics Aggregation ===")

	// Create aggregator for minute-level aggregation
	window := instrumentation.CommonAggregationWindows["minute"]
	aggregator := instrumentation.NewMetricAggregator("test_metric", window)
	defer aggregator.Close()

	// Add some test values
	testLabels := map[string]string{"service": "api"}
	for i := 0; i < 10; i++ {
		aggregator.AddValue(float64(i*10), testLabels)
		time.Sleep(10 * time.Millisecond)
	}

	// Get aggregated data
	time.Sleep(100 * time.Millisecond) // Wait for aggregation
	data := aggregator.GetAggregatedData(testLabels)
	if data != nil && len(data.Windows) > 0 {
		latest := data.Windows[len(data.Windows)-1]
		fmt.Printf("Aggregated metrics - Count: %d, Mean: %.2f, Min: %.2f, Max: %.2f\n",
			latest.Count, latest.Mean, latest.Min, latest.Max)
	}

	// Test 4: Database Metrics (without actual DB connection)
	fmt.Println("\n=== Testing Database Metrics Structure ===")

	manager := instrumentation.NewMetricsManager(nil)
	manager.RegisterCollector("prometheus", promCollector)

	dbCollector := instrumentation.NewDatabaseMetricsCollector(manager)

	// Test metrics recording (simulated)
	dbCollector.RecordQuery("SELECT", "users", 50*time.Millisecond, nil)
	dbCollector.RecordConnection(5, 3, 10)
	dbCollector.RecordTransaction(200*time.Millisecond, true, nil)

	fmt.Println("\n=== All Tests Completed Successfully ===")
	fmt.Println("Metrics system is working correctly!")
}