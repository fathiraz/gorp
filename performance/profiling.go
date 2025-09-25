package performance

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-gorp/gorp/v3/db"
)

// ProfilerHook provides performance profiling capabilities for database operations
type ProfilerHook struct {
	enabled       bool
	tracer        trace.Tracer
	meter         metric.Meter
	queryCounter  metric.Int64Counter
	queryDuration metric.Float64Histogram
	errorCounter  metric.Int64Counter
	mu            sync.RWMutex
	hooks         map[string][]ProfileHookFunc
}

// ProfileHookFunc defines the signature for profile hooks
type ProfileHookFunc func(ctx context.Context, event *ProfileEvent)

// ProfileEvent contains information about a database operation
type ProfileEvent struct {
	Operation    string
	Query        string
	Args         []interface{}
	Duration     time.Duration
	Error        error
	RowsAffected int64
	Timestamp    time.Time
	ConnectionType db.ConnectionType
	Metadata     map[string]interface{}
}

// ProfilingConfig holds configuration for performance profiling
type ProfilingConfig struct {
	Enabled           bool
	EnableTracing     bool
	EnableMetrics     bool
	TracerName        string
	MeterName         string
	SlowQueryThreshold time.Duration
	MaxQueryLength    int
}

// DefaultProfilingConfig returns sensible defaults
func DefaultProfilingConfig() *ProfilingConfig {
	return &ProfilingConfig{
		Enabled:           true,
		EnableTracing:     true,
		EnableMetrics:     true,
		TracerName:        "gorp.profiler",
		MeterName:         "gorp.profiler",
		SlowQueryThreshold: 1 * time.Second,
		MaxQueryLength:    1000,
	}
}

// NewProfilerHook creates a new profiler hook
func NewProfilerHook(config *ProfilingConfig) (*ProfilerHook, error) {
	if config == nil {
		config = DefaultProfilingConfig()
	}

	profiler := &ProfilerHook{
		enabled: config.Enabled,
		hooks:   make(map[string][]ProfileHookFunc),
	}

	if !config.Enabled {
		return profiler, nil
	}

	// Initialize tracing
	if config.EnableTracing {
		profiler.tracer = otel.Tracer(config.TracerName)
	}

	// Initialize metrics
	if config.EnableMetrics {
		meter := otel.Meter(config.MeterName)

		queryCounter, err := meter.Int64Counter(
			"db_queries_total",
			metric.WithDescription("Total number of database queries"),
			metric.WithUnit("{query}"),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create query counter: %v", err)
		}
		profiler.queryCounter = queryCounter

		queryDuration, err := meter.Float64Histogram(
			"db_query_duration",
			metric.WithDescription("Database query duration"),
			metric.WithUnit("s"),
			metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create query duration histogram: %v", err)
		}
		profiler.queryDuration = queryDuration

		errorCounter, err := meter.Int64Counter(
			"db_errors_total",
			metric.WithDescription("Total number of database errors"),
			metric.WithUnit("{error}"),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create error counter: %v", err)
		}
		profiler.errorCounter = errorCounter

		profiler.meter = meter
	}

	return profiler, nil
}

// StartOperation begins profiling a database operation
func (ph *ProfilerHook) StartOperation(ctx context.Context, operation, query string, args []interface{}, connType db.ConnectionType) (context.Context, *ProfileContext) {
	if !ph.enabled {
		return ctx, nil
	}

	profileCtx := &ProfileContext{
		Operation:    operation,
		Query:        query,
		Args:         args,
		StartTime:    time.Now(),
		ConnectionType: connType,
	}

	// Start tracing span
	if ph.tracer != nil {
		var span trace.Span
		ctx, span = ph.tracer.Start(ctx, fmt.Sprintf("db.%s", operation),
			trace.WithAttributes(
				attribute.String("db.operation", operation),
				attribute.String("db.statement", truncateQuery(query, 100)),
				attribute.String("db.connection_type", string(connType)),
				attribute.Int("db.args_count", len(args)),
			))
		profileCtx.span = span
	}

	return ctx, profileCtx
}

// FinishOperation completes profiling of a database operation
func (ph *ProfilerHook) FinishOperation(ctx context.Context, profileCtx *ProfileContext, err error, rowsAffected int64) {
	if !ph.enabled || profileCtx == nil {
		return
	}

	duration := time.Since(profileCtx.StartTime)
	profileCtx.Duration = duration
	profileCtx.Error = err
	profileCtx.RowsAffected = rowsAffected

	// Finish tracing span
	if profileCtx.span != nil {
		if err != nil {
			profileCtx.span.RecordError(err)
		}

		profileCtx.span.SetAttributes(
			attribute.Int64("db.rows_affected", rowsAffected),
			attribute.Float64("db.duration_ms", float64(duration.Nanoseconds())/1e6),
			attribute.Bool("db.success", err == nil),
		)

		profileCtx.span.End()
	}

	// Record metrics
	if ph.meter != nil {
		attributes := []attribute.KeyValue{
			attribute.String("operation", profileCtx.Operation),
			attribute.String("connection_type", string(profileCtx.ConnectionType)),
			attribute.Bool("success", err == nil),
		}

		ph.queryCounter.Add(ctx, 1, metric.WithAttributes(attributes...))
		ph.queryDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(attributes...))

		if err != nil {
			ph.errorCounter.Add(ctx, 1, metric.WithAttributes(attributes...))
		}
	}

	// Execute hooks
	event := &ProfileEvent{
		Operation:      profileCtx.Operation,
		Query:          profileCtx.Query,
		Args:           profileCtx.Args,
		Duration:       duration,
		Error:          err,
		RowsAffected:   rowsAffected,
		Timestamp:      profileCtx.StartTime,
		ConnectionType: profileCtx.ConnectionType,
		Metadata:       make(map[string]interface{}),
	}

	ph.executeHooks(ctx, event)
}

// ProfileContext holds context for ongoing profiling operation
type ProfileContext struct {
	Operation      string
	Query          string
	Args           []interface{}
	StartTime      time.Time
	Duration       time.Duration
	Error          error
	RowsAffected   int64
	ConnectionType db.ConnectionType
	span           trace.Span
}

// AddHook registers a hook function for specific operations
func (ph *ProfilerHook) AddHook(operation string, hook ProfileHookFunc) {
	ph.mu.Lock()
	defer ph.mu.Unlock()

	ph.hooks[operation] = append(ph.hooks[operation], hook)
}

// RemoveHooks removes all hooks for a specific operation
func (ph *ProfilerHook) RemoveHooks(operation string) {
	ph.mu.Lock()
	defer ph.mu.Unlock()

	delete(ph.hooks, operation)
}

// executeHooks runs all registered hooks for an operation
func (ph *ProfilerHook) executeHooks(ctx context.Context, event *ProfileEvent) {
	ph.mu.RLock()
	hooks := append(ph.hooks["*"], ph.hooks[event.Operation]...)
	ph.mu.RUnlock()

	for _, hook := range hooks {
		go func(h ProfileHookFunc) {
			defer func() {
				if r := recover(); r != nil {
					// Log panic but don't crash
				}
			}()
			h(ctx, event)
		}(hook)
	}
}

// Built-in Hook Functions

// SlowQueryHook logs queries that exceed a threshold
func SlowQueryHook(threshold time.Duration) ProfileHookFunc {
	return func(ctx context.Context, event *ProfileEvent) {
		if event.Duration > threshold {
			fmt.Printf("SLOW QUERY [%v]: %s (args: %v)\n",
				event.Duration,
				truncateQuery(event.Query, 200),
				event.Args)
		}
	}
}

// ErrorHook logs query errors
func ErrorHook() ProfileHookFunc {
	return func(ctx context.Context, event *ProfileEvent) {
		if event.Error != nil {
			fmt.Printf("QUERY ERROR [%v]: %s - %v\n",
				event.Timestamp.Format(time.RFC3339),
				truncateQuery(event.Query, 100),
				event.Error)
		}
	}
}

// StatsHook collects operation statistics
type StatsHook struct {
	stats map[string]*OperationStats
	mu    sync.RWMutex
}

type OperationStats struct {
	Count        int64
	TotalTime    time.Duration
	AverageTime  time.Duration
	MinTime      time.Duration
	MaxTime      time.Duration
	ErrorCount   int64
	RowsAffected int64
}

func NewStatsHook() *StatsHook {
	return &StatsHook{
		stats: make(map[string]*OperationStats),
	}
}

func (sh *StatsHook) Hook() ProfileHookFunc {
	return func(ctx context.Context, event *ProfileEvent) {
		sh.mu.Lock()
		defer sh.mu.Unlock()

		key := fmt.Sprintf("%s:%s", event.ConnectionType, event.Operation)
		stats, exists := sh.stats[key]
		if !exists {
			stats = &OperationStats{
				MinTime: event.Duration,
				MaxTime: event.Duration,
			}
			sh.stats[key] = stats
		}

		stats.Count++
		stats.TotalTime += event.Duration
		stats.AverageTime = time.Duration(int64(stats.TotalTime) / stats.Count)
		stats.RowsAffected += event.RowsAffected

		if event.Duration < stats.MinTime {
			stats.MinTime = event.Duration
		}
		if event.Duration > stats.MaxTime {
			stats.MaxTime = event.Duration
		}

		if event.Error != nil {
			stats.ErrorCount++
		}
	}
}

func (sh *StatsHook) GetStats() map[string]*OperationStats {
	sh.mu.RLock()
	defer sh.mu.RUnlock()

	result := make(map[string]*OperationStats)
	for key, stats := range sh.stats {
		// Create a copy to avoid race conditions
		result[key] = &OperationStats{
			Count:        stats.Count,
			TotalTime:    stats.TotalTime,
			AverageTime:  stats.AverageTime,
			MinTime:      stats.MinTime,
			MaxTime:      stats.MaxTime,
			ErrorCount:   stats.ErrorCount,
			RowsAffected: stats.RowsAffected,
		}
	}
	return result
}

func (sh *StatsHook) Reset() {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	sh.stats = make(map[string]*OperationStats)
}

// ProfiledConnection wraps a connection with profiling
type ProfiledConnection struct {
	conn     db.Connection
	profiler *ProfilerHook
}

// NewProfiledConnection creates a connection with profiling enabled
func NewProfiledConnection(conn db.Connection, profiler *ProfilerHook) *ProfiledConnection {
	return &ProfiledConnection{
		conn:     conn,
		profiler: profiler,
	}
}

// Query executes a query with profiling
func (pc *ProfiledConnection) Query(ctx context.Context, query string, args ...interface{}) (db.Rows, error) {
	profCtx, profileCtx := pc.profiler.StartOperation(ctx, "query", query, args, pc.conn.Type())

	rows, err := pc.conn.Query(profCtx, query, args...)

	// For queries, we can't get row count until rows are consumed
	pc.profiler.FinishOperation(profCtx, profileCtx, err, -1)

	return rows, err
}

// Exec executes a statement with profiling
func (pc *ProfiledConnection) Exec(ctx context.Context, query string, args ...interface{}) (db.Result, error) {
	profCtx, profileCtx := pc.profiler.StartOperation(ctx, "exec", query, args, pc.conn.Type())

	result, err := pc.conn.Exec(profCtx, query, args...)

	var rowsAffected int64
	if err == nil && result != nil {
		rowsAffected, _ = result.RowsAffected()
	}

	pc.profiler.FinishOperation(profCtx, profileCtx, err, rowsAffected)

	return result, err
}

// Delegate other methods to underlying connection
func (pc *ProfiledConnection) Type() db.ConnectionType { return pc.conn.Type() }
func (pc *ProfiledConnection) Close() error           { return pc.conn.Close() }
func (pc *ProfiledConnection) IsHealthy() bool        { return pc.conn.IsHealthy() }

// ProfileManager coordinates profiling across multiple connections
type ProfileManager struct {
	profilers map[string]*ProfilerHook
	mu        sync.RWMutex
	statsHook *StatsHook
}

// NewProfileManager creates a new profile manager
func NewProfileManager() *ProfileManager {
	return &ProfileManager{
		profilers: make(map[string]*ProfilerHook),
		statsHook: NewStatsHook(),
	}
}

// GetProfiler returns or creates a profiler for a specific name
func (pm *ProfileManager) GetProfiler(name string, config *ProfilingConfig) (*ProfilerHook, error) {
	pm.mu.RLock()
	if profiler, exists := pm.profilers[name]; exists {
		pm.mu.RUnlock()
		return profiler, nil
	}
	pm.mu.RUnlock()

	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Double-check pattern
	if profiler, exists := pm.profilers[name]; exists {
		return profiler, nil
	}

	// Create new profiler
	profiler, err := NewProfilerHook(config)
	if err != nil {
		return nil, err
	}

	// Add global stats hook
	profiler.AddHook("*", pm.statsHook.Hook())

	pm.profilers[name] = profiler
	return profiler, nil
}

// GetGlobalStats returns aggregated statistics across all profilers
func (pm *ProfileManager) GetGlobalStats() map[string]*OperationStats {
	return pm.statsHook.GetStats()
}

// ResetGlobalStats resets all collected statistics
func (pm *ProfileManager) ResetGlobalStats() {
	pm.statsHook.Reset()
}

// Utility functions

// truncateQuery limits query length for logging
func truncateQuery(query string, maxLen int) string {
	if len(query) <= maxLen {
		return query
	}
	return query[:maxLen-3] + "..."
}

// FormatDuration formats duration for human readability
func FormatDuration(d time.Duration) string {
	if d < time.Microsecond {
		return fmt.Sprintf("%dns", d.Nanoseconds())
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%.2fμs", float64(d.Nanoseconds())/1000)
	}
	if d < time.Second {
		return fmt.Sprintf("%.2fms", float64(d.Nanoseconds())/1e6)
	}
	return fmt.Sprintf("%.3fs", d.Seconds())
}

// Performance monitoring utilities
type PerformanceMonitor struct {
	profiler     *ProfilerHook
	alertThresholds map[string]time.Duration
	mu           sync.RWMutex
}

// NewPerformanceMonitor creates a performance monitor with alerting
func NewPerformanceMonitor(profiler *ProfilerHook) *PerformanceMonitor {
	monitor := &PerformanceMonitor{
		profiler:        profiler,
		alertThresholds: make(map[string]time.Duration),
	}

	// Add default alert hook
	profiler.AddHook("*", monitor.alertHook)

	return monitor
}

// SetAlertThreshold sets an alert threshold for an operation
func (pm *PerformanceMonitor) SetAlertThreshold(operation string, threshold time.Duration) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.alertThresholds[operation] = threshold
}

// alertHook checks for performance alerts
func (pm *PerformanceMonitor) alertHook(ctx context.Context, event *ProfileEvent) {
	pm.mu.RLock()
	threshold, exists := pm.alertThresholds[event.Operation]
	if !exists {
		threshold = pm.alertThresholds["*"]
	}
	pm.mu.RUnlock()

	if exists && event.Duration > threshold {
		fmt.Printf("PERFORMANCE ALERT: %s operation took %v (threshold: %v)\n",
			event.Operation, FormatDuration(event.Duration), FormatDuration(threshold))
	}
}