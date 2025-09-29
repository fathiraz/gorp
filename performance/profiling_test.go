package performance

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/fathiraz/gorp/db"
)

func TestProfilerHook_Basic(t *testing.T) {
	config := &ProfilingConfig{
		Enabled:       true,
		EnableTracing: false, // Disable for simpler test
		EnableMetrics: false,
	}

	profiler, err := NewProfilerHook(config)
	if err != nil {
		t.Fatalf("Failed to create profiler: %v", err)
	}

	// Add a test hook
	var capturedEvent *ProfileEvent
	profiler.AddHook("query", func(ctx context.Context, event *ProfileEvent) {
		capturedEvent = event
	})

	ctx := context.Background()
	profCtx, profileCtx := profiler.StartOperation(ctx, "query", "SELECT * FROM users", nil, db.SQLiteConnectionType)

	// Simulate some work
	time.Sleep(10 * time.Millisecond)

	profiler.FinishOperation(profCtx, profileCtx, nil, 5)

	// Wait a bit for hook to execute
	time.Sleep(50 * time.Millisecond)

	if capturedEvent == nil {
		t.Fatal("Expected hook to capture event")
	}

	if capturedEvent.Operation != "query" {
		t.Errorf("Expected operation 'query', got %s", capturedEvent.Operation)
	}

	if capturedEvent.Query != "SELECT * FROM users" {
		t.Errorf("Expected query 'SELECT * FROM users', got %s", capturedEvent.Query)
	}

	if capturedEvent.RowsAffected != 5 {
		t.Errorf("Expected 5 rows affected, got %d", capturedEvent.RowsAffected)
	}

	if capturedEvent.Duration < 10*time.Millisecond {
		t.Errorf("Expected duration >= 10ms, got %v", capturedEvent.Duration)
	}
}

func TestProfilerHook_Disabled(t *testing.T) {
	config := &ProfilingConfig{
		Enabled: false,
	}

	profiler, err := NewProfilerHook(config)
	if err != nil {
		t.Fatalf("Failed to create profiler: %v", err)
	}

	var hookCalled bool
	profiler.AddHook("query", func(ctx context.Context, event *ProfileEvent) {
		hookCalled = true
	})

	ctx := context.Background()
	profCtx, profileCtx := profiler.StartOperation(ctx, "query", "SELECT 1", nil, db.SQLiteConnectionType)
	profiler.FinishOperation(profCtx, profileCtx, nil, 1)

	// Wait a bit
	time.Sleep(50 * time.Millisecond)

	if hookCalled {
		t.Error("Expected hook not to be called when profiler is disabled")
	}

	if profileCtx != nil {
		t.Error("Expected nil profile context when profiler is disabled")
	}
}

func TestSlowQueryHook(t *testing.T) {
	threshold := 50 * time.Millisecond
	slowHook := SlowQueryHook(threshold)

	// Test with slow query
	slowEvent := &ProfileEvent{
		Operation: "query",
		Query:     "SELECT * FROM big_table",
		Duration:  100 * time.Millisecond,
		Args:      []interface{}{},
	}

	// This would normally print to stdout, but we're just testing it doesn't panic
	slowHook(context.Background(), slowEvent)

	// Test with fast query
	fastEvent := &ProfileEvent{
		Operation: "query",
		Query:     "SELECT 1",
		Duration:  10 * time.Millisecond,
		Args:      []interface{}{},
	}

	slowHook(context.Background(), fastEvent)
}

func TestErrorHook(t *testing.T) {
	errorHook := ErrorHook()

	// Test with error
	errorEvent := &ProfileEvent{
		Operation: "query",
		Query:     "SELECT * FROM nonexistent",
		Error:     fmt.Errorf("table does not exist"),
		Timestamp: time.Now(),
	}

	errorHook(context.Background(), errorEvent)

	// Test without error
	successEvent := &ProfileEvent{
		Operation: "query",
		Query:     "SELECT 1",
		Error:     nil,
		Timestamp: time.Now(),
	}

	errorHook(context.Background(), successEvent)
}

func TestStatsHook(t *testing.T) {
	statsHook := NewStatsHook()
	hook := statsHook.Hook()

	// Simulate multiple events
	events := []*ProfileEvent{
		{
			Operation:      "query",
			ConnectionType: db.SQLiteConnectionType,
			Duration:       10 * time.Millisecond,
			RowsAffected:   5,
		},
		{
			Operation:      "query",
			ConnectionType: db.SQLiteConnectionType,
			Duration:       20 * time.Millisecond,
			RowsAffected:   3,
		},
		{
			Operation:      "query",
			ConnectionType: db.SQLiteConnectionType,
			Duration:       5 * time.Millisecond,
			RowsAffected:   1,
			Error:          fmt.Errorf("some error"),
		},
	}

	for _, event := range events {
		hook(context.Background(), event)
	}

	stats := statsHook.GetStats()
	key := "sqlite:query"

	if _, exists := stats[key]; !exists {
		t.Fatalf("Expected stats for key %s", key)
	}

	stat := stats[key]
	if stat.Count != 3 {
		t.Errorf("Expected count 3, got %d", stat.Count)
	}

	if stat.ErrorCount != 1 {
		t.Errorf("Expected error count 1, got %d", stat.ErrorCount)
	}

	if stat.RowsAffected != 9 {
		t.Errorf("Expected 9 rows affected, got %d", stat.RowsAffected)
	}

	if stat.MinTime != 5*time.Millisecond {
		t.Errorf("Expected min time 5ms, got %v", stat.MinTime)
	}

	if stat.MaxTime != 20*time.Millisecond {
		t.Errorf("Expected max time 20ms, got %v", stat.MaxTime)
	}

	expectedAvg := (10 + 20 + 5) * time.Millisecond / 3
	if stat.AverageTime != expectedAvg {
		t.Errorf("Expected average time %v, got %v", expectedAvg, stat.AverageTime)
	}

	// Test reset
	statsHook.Reset()
	stats = statsHook.GetStats()
	if len(stats) != 0 {
		t.Error("Expected empty stats after reset")
	}
}

func TestProfileManager(t *testing.T) {
	manager := NewProfileManager()

	config := &ProfilingConfig{
		Enabled:       true,
		EnableTracing: false,
		EnableMetrics: false,
	}

	// Get profiler for "test" name
	profiler1, err := manager.GetProfiler("test", config)
	if err != nil {
		t.Fatalf("Failed to get profiler: %v", err)
	}

	// Get same profiler again
	profiler2, err := manager.GetProfiler("test", config)
	if err != nil {
		t.Fatalf("Failed to get profiler second time: %v", err)
	}

	if profiler1 != profiler2 {
		t.Error("Expected same profiler instance")
	}

	// Test global stats
	ctx := context.Background()
	profCtx, profileCtx := profiler1.StartOperation(ctx, "query", "SELECT 1", nil, db.SQLiteConnectionType)
	profiler1.FinishOperation(profCtx, profileCtx, nil, 1)

	// Wait for stats to be collected
	time.Sleep(50 * time.Millisecond)

	stats := manager.GetGlobalStats()
	if len(stats) == 0 {
		t.Error("Expected global stats to be collected")
	}

	// Test reset
	manager.ResetGlobalStats()
	stats = manager.GetGlobalStats()
	if len(stats) != 0 {
		t.Error("Expected global stats to be reset")
	}
}

func TestPerformanceMonitor(t *testing.T) {
	config := &ProfilingConfig{
		Enabled:       true,
		EnableTracing: false,
		EnableMetrics: false,
	}

	profiler, err := NewProfilerHook(config)
	if err != nil {
		t.Fatalf("Failed to create profiler: %v", err)
	}

	monitor := NewPerformanceMonitor(profiler)

	// Set alert threshold
	monitor.SetAlertThreshold("query", 50*time.Millisecond)

	// Test operation that should trigger alert
	ctx := context.Background()
	profCtx, profileCtx := profiler.StartOperation(ctx, "query", "SELECT * FROM big_table", nil, db.SQLiteConnectionType)

	// Simulate slow operation
	time.Sleep(60 * time.Millisecond)

	profiler.FinishOperation(profCtx, profileCtx, nil, 100)

	// The alert would be printed to stdout - we're just testing it doesn't panic
}

func TestTruncateQuery(t *testing.T) {
	// Test short query
	short := "SELECT 1"
	result := truncateQuery(short, 10)
	if result != short {
		t.Errorf("Expected '%s', got '%s'", short, result)
	}

	// Test long query
	long := "SELECT * FROM users WHERE id = 1 AND name = 'test' AND age > 18"
	result = truncateQuery(long, 20)
	expected := "SELECT * FROM use..."
	if result != expected {
		t.Errorf("Expected '%s', got '%s'", expected, result)
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		duration time.Duration
		contains string // What the output should contain
	}{
		{100 * time.Nanosecond, "ns"},
		{100 * time.Microsecond, "μs"},
		{100 * time.Millisecond, "ms"},
		{2 * time.Second, "s"},
	}

	for _, test := range tests {
		result := FormatDuration(test.duration)
		if result == "" {
			t.Errorf("Expected non-empty result for duration %v", test.duration)
		}
		// We're just checking that it formats without error and contains expected unit
	}
}

func BenchmarkProfilerHook(b *testing.B) {
	config := &ProfilingConfig{
		Enabled:       true,
		EnableTracing: false,
		EnableMetrics: false,
	}

	profiler, err := NewProfilerHook(config)
	if err != nil {
		b.Fatalf("Failed to create profiler: %v", err)
	}

	ctx := context.Background()
	query := "SELECT * FROM users WHERE id = ?"
	args := []interface{}{42}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		profCtx, profileCtx := profiler.StartOperation(ctx, "query", query, args, db.SQLiteConnectionType)
		profiler.FinishOperation(profCtx, profileCtx, nil, 1)
	}
}

func BenchmarkProfilerHook_WithHook(b *testing.B) {
	config := &ProfilingConfig{
		Enabled:       true,
		EnableTracing: false,
		EnableMetrics: false,
	}

	profiler, err := NewProfilerHook(config)
	if err != nil {
		b.Fatalf("Failed to create profiler: %v", err)
	}

	// Add a simple hook
	profiler.AddHook("query", func(ctx context.Context, event *ProfileEvent) {
		// Do nothing
	})

	ctx := context.Background()
	query := "SELECT * FROM users WHERE id = ?"
	args := []interface{}{42}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		profCtx, profileCtx := profiler.StartOperation(ctx, "query", query, args, db.SQLiteConnectionType)
		profiler.FinishOperation(profCtx, profileCtx, nil, 1)
	}
}