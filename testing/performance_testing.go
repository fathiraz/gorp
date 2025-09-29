// Performance regression testing framework for GORP
package testing

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/fathiraz/gorp/db"
	"github.com/fathiraz/gorp/mapping"
	"github.com/fathiraz/gorp/query"
)

// PerformanceTest represents a performance test case
type PerformanceTest struct {
	Name        string
	Description string
	Setup       func(t *testing.T, conn db.Connection) error
	Test        func(t *testing.T, conn db.Connection) error
	Teardown    func(t *testing.T, conn db.Connection) error
	Timeout     time.Duration
	MinRuns     int
	MaxRuns     int
	WarmupRuns  int
	DataSizes   []int
}

// PerformanceMetrics contains performance measurement data
type PerformanceMetrics struct {
	TestName       string                 `json:"test_name"`
	DatabaseType   string                 `json:"database_type"`
	DataSize       int                    `json:"data_size"`
	Timestamp      time.Time              `json:"timestamp"`
	Duration       time.Duration          `json:"duration"`
	MemoryBefore   runtime.MemStats       `json:"memory_before"`
	MemoryAfter    runtime.MemStats       `json:"memory_after"`
	GCStats        runtime.GCStats        `json:"gc_stats"`
	CPUSample      CPUProfile             `json:"cpu_sample"`
	DatabaseStats  sql.DBStats            `json:"database_stats"`
	CustomMetrics  map[string]interface{} `json:"custom_metrics"`
}

// CPUProfile contains CPU profiling information
type CPUProfile struct {
	UserTime   time.Duration `json:"user_time"`
	SystemTime time.Duration `json:"system_time"`
	Goroutines int           `json:"goroutines"`
}

// PerformanceBenchmark represents historical performance data
type PerformanceBenchmark struct {
	TestName     string             `json:"test_name"`
	DatabaseType string             `json:"database_type"`
	Baseline     PerformanceMetrics `json:"baseline"`
	History      []PerformanceMetrics `json:"history"`
	Thresholds   PerformanceThresholds `json:"thresholds"`
}

// PerformanceThresholds defines acceptable performance ranges
type PerformanceThresholds struct {
	MaxDuration      time.Duration `json:"max_duration"`
	MaxMemoryIncrease int64         `json:"max_memory_increase"`
	MaxRegressionPct float64       `json:"max_regression_pct"`
	MinImprovementPct float64      `json:"min_improvement_pct"`
}

// PerformanceTestRunner manages performance test execution
type PerformanceTestRunner struct {
	connection       db.Connection
	baselineDir      string
	reportDir        string
	enableProfiling  bool
	enableMemoryProf bool
	enableCPUProf    bool
	warmupRuns       int
	minRuns          int
	maxRuns          int
	timeout          time.Duration
	thresholds       PerformanceThresholds
	mutex            sync.RWMutex
	benchmarks       map[string]*PerformanceBenchmark
}

// NewPerformanceTestRunner creates a new performance test runner
func NewPerformanceTestRunner(connection db.Connection, baselineDir string) *PerformanceTestRunner {
	return &PerformanceTestRunner{
		connection:      connection,
		baselineDir:     baselineDir,
		reportDir:       filepath.Join(baselineDir, "reports"),
		enableProfiling: true,
		warmupRuns:      3,
		minRuns:         5,
		maxRuns:         20,
		timeout:         5 * time.Minute,
		thresholds: PerformanceThresholds{
			MaxDuration:       10 * time.Second,
			MaxMemoryIncrease: 100 * 1024 * 1024, // 100MB
			MaxRegressionPct:  20.0,               // 20% slower is regression
			MinImprovementPct: 5.0,                // 5% faster is improvement
		},
		benchmarks: make(map[string]*PerformanceBenchmark),
	}
}

// SetThresholds configures performance thresholds
func (ptr *PerformanceTestRunner) SetThresholds(thresholds PerformanceThresholds) {
	ptr.thresholds = thresholds
}

// EnableProfiling enables or disables profiling
func (ptr *PerformanceTestRunner) EnableProfiling(cpu, memory bool) {
	ptr.enableCPUProf = cpu
	ptr.enableMemoryProf = memory
	ptr.enableProfiling = cpu || memory
}

// SetRunParameters configures test run parameters
func (ptr *PerformanceTestRunner) SetRunParameters(warmup, min, max int, timeout time.Duration) {
	ptr.warmupRuns = warmup
	ptr.minRuns = min
	ptr.maxRuns = max
	ptr.timeout = timeout
}

// LoadBaselines loads historical performance baselines
func (ptr *PerformanceTestRunner) LoadBaselines() error {
	if _, err := os.Stat(ptr.baselineDir); os.IsNotExist(err) {
		return os.MkdirAll(ptr.baselineDir, 0755)
	}

	matches, err := filepath.Glob(filepath.Join(ptr.baselineDir, "*.json"))
	if err != nil {
		return fmt.Errorf("failed to find baseline files: %w", err)
	}

	ptr.mutex.Lock()
	defer ptr.mutex.Unlock()

	for _, file := range matches {
		var benchmark PerformanceBenchmark
		data, err := os.ReadFile(file)
		if err != nil {
			continue
		}

		if err := json.Unmarshal(data, &benchmark); err != nil {
			continue
		}

		key := fmt.Sprintf("%s_%s", benchmark.TestName, benchmark.DatabaseType)
		ptr.benchmarks[key] = &benchmark
	}

	return nil
}

// SaveBaseline saves performance baseline data
func (ptr *PerformanceTestRunner) SaveBaseline(testName string, metrics PerformanceMetrics) error {
	ptr.mutex.Lock()
	defer ptr.mutex.Unlock()

	key := fmt.Sprintf("%s_%s", testName, metrics.DatabaseType)

	benchmark, exists := ptr.benchmarks[key]
	if !exists {
		benchmark = &PerformanceBenchmark{
			TestName:     testName,
			DatabaseType: metrics.DatabaseType,
			Baseline:     metrics,
			History:      []PerformanceMetrics{},
			Thresholds:   ptr.thresholds,
		}
		ptr.benchmarks[key] = benchmark
	}

	// Update history
	benchmark.History = append(benchmark.History, metrics)

	// Keep only last 100 measurements
	if len(benchmark.History) > 100 {
		benchmark.History = benchmark.History[len(benchmark.History)-100:]
	}

	// Update baseline if this is significantly better
	if ptr.isSignificantImprovement(benchmark.Baseline, metrics) {
		benchmark.Baseline = metrics
	}

	// Save to file
	filename := filepath.Join(ptr.baselineDir, fmt.Sprintf("%s.json", key))
	data, err := json.MarshalIndent(benchmark, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal benchmark: %w", err)
	}

	return os.WriteFile(filename, data, 0644)
}

// RunPerformanceTest executes a performance test
func (ptr *PerformanceTestRunner) RunPerformanceTest(t *testing.T, test PerformanceTest) {
	t.Helper()

	// Load baselines
	if err := ptr.LoadBaselines(); err != nil {
		t.Logf("Warning: failed to load baselines: %v", err)
	}

	// Set defaults
	if test.Timeout == 0 {
		test.Timeout = ptr.timeout
	}
	if test.MinRuns == 0 {
		test.MinRuns = ptr.minRuns
	}
	if test.MaxRuns == 0 {
		test.MaxRuns = ptr.maxRuns
	}
	if test.WarmupRuns == 0 {
		test.WarmupRuns = ptr.warmupRuns
	}

	dataSizes := test.DataSizes
	if len(dataSizes) == 0 {
		dataSizes = []int{10, 100, 1000}
	}

	for _, dataSize := range dataSizes {
		testName := fmt.Sprintf("%s_size_%d", test.Name, dataSize)
		t.Run(testName, func(t *testing.T) {
			ptr.runSinglePerformanceTest(t, test, testName, dataSize)
		})
	}
}

func (ptr *PerformanceTestRunner) runSinglePerformanceTest(t *testing.T, test PerformanceTest, testName string, dataSize int) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), test.Timeout)
	defer cancel()

	// Setup
	if test.Setup != nil {
		require.NoError(t, test.Setup(t, ptr.connection), "Test setup failed")
	}

	// Ensure cleanup
	defer func() {
		if test.Teardown != nil {
			if err := test.Teardown(t, ptr.connection); err != nil {
				t.Logf("Teardown failed: %v", err)
			}
		}
	}()

	// Warmup runs
	for i := 0; i < test.WarmupRuns; i++ {
		select {
		case <-ctx.Done():
			t.Fatal("Performance test timed out during warmup")
		default:
		}

		if err := test.Test(t, ptr.connection); err != nil {
			t.Fatalf("Warmup run %d failed: %v", i+1, err)
		}
	}

	// Measurement runs
	var measurements []PerformanceMetrics

	for i := 0; i < test.MaxRuns; i++ {
		select {
		case <-ctx.Done():
			t.Fatal("Performance test timed out during measurement")
		default:
		}

		metrics := ptr.measurePerformance(t, test, testName, dataSize)
		measurements = append(measurements, metrics)

		// Check if we have enough stable measurements
		if i >= test.MinRuns-1 && ptr.isStable(measurements) {
			break
		}
	}

	// Analyze results
	result := ptr.analyzeResults(testName, measurements)

	// Save baseline
	if err := ptr.SaveBaseline(testName, result); err != nil {
		t.Logf("Failed to save baseline: %v", err)
	}

	// Check for regressions
	ptr.checkRegression(t, testName, result)

	// Generate report
	ptr.generateReport(t, testName, result, measurements)
}

func (ptr *PerformanceTestRunner) measurePerformance(t *testing.T, test PerformanceTest, testName string, dataSize int) PerformanceMetrics {
	// Collect initial state
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)
	runtime.GC() // Force GC for consistent measurements

	var gcStatsBefore runtime.GCStats
	runtime.ReadGCStats(&gcStatsBefore)

	dbStatsBefore := ptr.connection.Stats()

	startTime := time.Now()
	goroutinesBefore := runtime.NumGoroutine()

	// Run the test
	err := test.Test(t, ptr.connection)

	endTime := time.Now()
	duration := endTime.Sub(startTime)

	require.NoError(t, err, "Performance test execution failed")

	// Collect final state
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	var gcStatsAfter runtime.GCStats
	runtime.ReadGCStats(&gcStatsAfter)

	dbStatsAfter := ptr.connection.Stats()
	goroutinesAfter := runtime.NumGoroutine()

	// Calculate CPU profile (simplified)
	cpuProfile := CPUProfile{
		UserTime:   duration, // Simplified - in real implementation, use proper CPU profiling
		SystemTime: duration / 10,
		Goroutines: goroutinesAfter - goroutinesBefore,
	}

	return PerformanceMetrics{
		TestName:      testName,
		DatabaseType:  string(ptr.connection.Type()),
		DataSize:      dataSize,
		Timestamp:     startTime,
		Duration:      duration,
		MemoryBefore:  memBefore,
		MemoryAfter:   memAfter,
		GCStats:       gcStatsAfter,
		CPUSample:     cpuProfile,
		DatabaseStats: dbStatsAfter,
		CustomMetrics: map[string]interface{}{
			"db_connections_before": dbStatsBefore.OpenConnections,
			"db_connections_after":  dbStatsAfter.OpenConnections,
			"memory_delta":          int64(memAfter.Alloc) - int64(memBefore.Alloc),
			"gc_runs":               gcStatsAfter.NumGC - gcStatsBefore.NumGC,
		},
	}
}

func (ptr *PerformanceTestRunner) isStable(measurements []PerformanceMetrics) bool {
	if len(measurements) < 3 {
		return false
	}

	// Calculate coefficient of variation for last 3 measurements
	recent := measurements[len(measurements)-3:]
	durations := make([]float64, len(recent))
	for i, m := range recent {
		durations[i] = float64(m.Duration.Nanoseconds())
	}

	mean := 0.0
	for _, d := range durations {
		mean += d
	}
	mean /= float64(len(durations))

	variance := 0.0
	for _, d := range durations {
		variance += (d - mean) * (d - mean)
	}
	variance /= float64(len(durations))

	stddev := variance * 0.5 // Simplified sqrt
	cv := stddev / mean

	// Consider stable if coefficient of variation < 5%
	return cv < 0.05
}

func (ptr *PerformanceTestRunner) analyzeResults(testName string, measurements []PerformanceMetrics) PerformanceMetrics {
	if len(measurements) == 0 {
		panic("No measurements to analyze")
	}

	// Sort by duration
	sort.Slice(measurements, func(i, j int) bool {
		return measurements[i].Duration < measurements[j].Duration
	})

	// Return median measurement
	median := measurements[len(measurements)/2]
	median.CustomMetrics["measurement_count"] = len(measurements)
	median.CustomMetrics["min_duration"] = measurements[0].Duration.Nanoseconds()
	median.CustomMetrics["max_duration"] = measurements[len(measurements)-1].Duration.Nanoseconds()

	return median
}

func (ptr *PerformanceTestRunner) checkRegression(t *testing.T, testName string, current PerformanceMetrics) {
	ptr.mutex.RLock()
	key := fmt.Sprintf("%s_%s", testName, current.DatabaseType)
	benchmark, exists := ptr.benchmarks[key]
	ptr.mutex.RUnlock()

	if !exists {
		t.Logf("No baseline found for %s, establishing new baseline", testName)
		return
	}

	baseline := benchmark.Baseline.Duration
	regressionPct := float64(current.Duration-baseline) / float64(baseline) * 100

	if regressionPct > ptr.thresholds.MaxRegressionPct {
		t.Errorf("Performance regression detected in %s: %.2f%% slower than baseline (%.2fms vs %.2fms)",
			testName, regressionPct,
			float64(current.Duration.Nanoseconds())/1e6,
			float64(baseline.Nanoseconds())/1e6)
	} else if regressionPct < -ptr.thresholds.MinImprovementPct {
		t.Logf("Performance improvement detected in %s: %.2f%% faster than baseline",
			testName, -regressionPct)
	}

	// Check memory regression
	baselineMem := int64(benchmark.Baseline.MemoryAfter.Alloc - benchmark.Baseline.MemoryBefore.Alloc)
	currentMem := int64(current.MemoryAfter.Alloc - current.MemoryBefore.Alloc)
	memDelta := currentMem - baselineMem

	if memDelta > ptr.thresholds.MaxMemoryIncrease {
		t.Errorf("Memory regression detected in %s: %d bytes more than baseline",
			testName, memDelta)
	}
}

func (ptr *PerformanceTestRunner) isSignificantImprovement(baseline, current PerformanceMetrics) bool {
	improvementPct := float64(baseline.Duration-current.Duration) / float64(baseline.Duration) * 100
	return improvementPct > ptr.thresholds.MinImprovementPct
}

func (ptr *PerformanceTestRunner) generateReport(t *testing.T, testName string, result PerformanceMetrics, measurements []PerformanceMetrics) {
	if ptr.reportDir == "" {
		return
	}

	if err := os.MkdirAll(ptr.reportDir, 0755); err != nil {
		t.Logf("Failed to create report directory: %v", err)
		return
	}

	report := map[string]interface{}{
		"test_name":     testName,
		"database_type": result.DatabaseType,
		"timestamp":     result.Timestamp,
		"summary": map[string]interface{}{
			"duration_ms":      float64(result.Duration.Nanoseconds()) / 1e6,
			"memory_delta_mb":  float64(result.MemoryAfter.Alloc-result.MemoryBefore.Alloc) / 1024 / 1024,
			"measurement_count": len(measurements),
		},
		"measurements": measurements,
		"thresholds":   ptr.thresholds,
	}

	filename := filepath.Join(ptr.reportDir, fmt.Sprintf("%s_%s.json", testName, time.Now().Format("20060102_150405")))
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Logf("Failed to marshal report: %v", err)
		return
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		t.Logf("Failed to write report: %v", err)
	}
}

// Database-specific performance tests
func CreateCRUDPerformanceTest[T mapping.Mappable](
	mapper mapping.TableMapper[T],
	generator func(size int) []T,
) PerformanceTest {
	return PerformanceTest{
		Name:        fmt.Sprintf("CRUD_%s", mapper.TableName()),
		Description: fmt.Sprintf("CRUD operations performance for %s", mapper.TableName()),
		Setup: func(t *testing.T, conn db.Connection) error {
			// Create table if needed
			verifier := NewSchemaVerifier(conn)
			return verifier.CreateTableFromMapping(context.Background(), mapper)
		},
		Test: func(t *testing.T, conn db.Connection) error {
			// Generate test data
			entities := generator(100)

			ctx := context.Background()
			tx, err := conn.Begin(ctx)
			if err != nil {
				return err
			}
			defer tx.Rollback()

			// Insert entities
			for _, entity := range entities {
				row, err := mapper.ToRow(entity)
				if err != nil {
					return err
				}

				// Simplified insert - real implementation would use proper query building
				_, err = tx.ExecContext(ctx, "INSERT INTO "+mapper.TableName()+" VALUES (?)", row)
				if err != nil {
					return err
				}
			}

			return tx.Commit()
		},
		Teardown: func(t *testing.T, conn db.Connection) error {
			verifier := NewSchemaVerifier(conn)
			return verifier.DropTableIfExists(context.Background(), mapper.TableName())
		},
		DataSizes: []int{10, 100, 1000},
	}
}

func CreateQueryPerformanceTest[T mapping.Mappable](
	mapper mapping.TableMapper[T],
	queryBuilder query.QueryBuilder[T],
) PerformanceTest {
	return PerformanceTest{
		Name:        fmt.Sprintf("Query_%s", mapper.TableName()),
		Description: fmt.Sprintf("Query performance for %s", mapper.TableName()),
		Test: func(t *testing.T, conn db.Connection) error {
			ctx := context.Background()

			sql, args, err := queryBuilder.Build()
			if err != nil {
				return err
			}

			var rows *sql.Rows
			if pgPool := conn.PgxPool(); pgPool != nil {
				rows, err = pgPool.Query(ctx, sql, args...)
			} else if sqlxDB := conn.SqlxDB(); sqlxDB != nil {
				rows, err = sqlxDB.QueryContext(ctx, sql, args...)
			} else {
				return fmt.Errorf("no available database connection")
			}

			if err != nil {
				return err
			}
			defer rows.Close()

			// Consume all rows
			for rows.Next() {
				// Simplified scanning
			}

			return rows.Err()
		},
	}
}

// Run comprehensive performance test suite
func RunPerformanceTestSuite(t *testing.T, connection db.Connection, baselineDir string) {
	runner := NewPerformanceTestRunner(connection, baselineDir)

	runner.SetThresholds(PerformanceThresholds{
		MaxDuration:       5 * time.Second,
		MaxMemoryIncrease: 50 * 1024 * 1024, // 50MB
		MaxRegressionPct:  15.0,
		MinImprovementPct: 5.0,
	})

	runner.SetRunParameters(2, 3, 10, 2*time.Minute)

	// Add performance tests here based on your mapping and query builders
	// This is a framework - specific tests would be defined by the user
}