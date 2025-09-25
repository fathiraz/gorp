package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)


func TestLogger_SetLevel(t *testing.T) {
	var buf bytes.Buffer
	config := &LoggerConfig{
		Level:  DEBUG,
		Format: "json",
		Output: &buf,
	}
	logger := NewStandardLogger(config)

	// Test setting level to INFO
	logger.SetLevel(INFO)

	// DEBUG message should not appear
	logger.Debug(context.Background(), "debug message")
	if buf.String() != "" {
		t.Error("DEBUG message should not appear when level is INFO")
	}

	// INFO message should appear
	buf.Reset()
	logger.Info(context.Background(), "info message")
	if buf.String() == "" {
		t.Error("INFO message should appear when level is INFO")
	}
}

func TestLogger_JSONFormat(t *testing.T) {
	var buf bytes.Buffer
	config := &LoggerConfig{
		Level:  INFO,
		Format: "json",
		Output: &buf,
	}
	logger := NewStandardLogger(config)

	logger.Info(context.Background(), "test message",
		String("key", "value"),
		Int("number", 42),
		Duration("duration", time.Second))

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse JSON log: %v", err)
	}

	if logEntry["level"] != "INFO" {
		t.Errorf("Expected level INFO, got %v", logEntry["level"])
	}

	if logEntry["message"] != "test message" {
		t.Errorf("Expected message 'test message', got %v", logEntry["message"])
	}

	if logEntry["key"] != "value" {
		t.Errorf("Expected key 'value', got %v", logEntry["key"])
	}

	if logEntry["number"] != float64(42) {
		t.Errorf("Expected number 42, got %v", logEntry["number"])
	}
}

func TestLogger_TextFormat(t *testing.T) {
	var buf bytes.Buffer
	config := &LoggerConfig{
		Level:  INFO,
		Format: "text",
		Output: &buf,
	}
	logger := NewStandardLogger(config)

	logger.Info(context.Background(), "test message", String("key", "value"))

	output := buf.String()
	if !strings.Contains(output, "INFO") {
		t.Error("Text output should contain level INFO")
	}
	if !strings.Contains(output, "test message") {
		t.Error("Text output should contain the message")
	}
	if !strings.Contains(output, "key=value") {
		t.Error("Text output should contain the field")
	}
}

func TestLogger_QueryLogging(t *testing.T) {
	var buf bytes.Buffer
	config := &LoggerConfig{
		Level:  DEBUG,
		Format: "json",
		Output: &buf,
	}
	logger := NewStandardLogger(config)

	query := "SELECT * FROM users WHERE id = ? AND name = ?"
	args := []interface{}{123, "sensitive_data"}
	duration := 50 * time.Millisecond

	logger.LogQuery(context.Background(), query, args, duration, nil)

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse JSON log: %v", err)
	}

	if logEntry["query"] != query {
		t.Errorf("Expected query to be logged")
	}

	// Check parameter sanitization
	argsStr := logEntry["args"].(string)
	if strings.Contains(argsStr, "sensitive_data") {
		t.Error("Sensitive data should be sanitized")
	}
	if !strings.Contains(argsStr, "[REDACTED]") {
		t.Error("Should contain [REDACTED] for sanitized parameters")
	}

	if logEntry["duration_ms"] != float64(50) {
		t.Errorf("Expected duration 50ms, got %v", logEntry["duration_ms"])
	}
}

func TestLogger_SlowQueryDetection(t *testing.T) {
	var buf bytes.Buffer
	config := &LoggerConfig{
		Level:  INFO,
		Format: "json",
		Output: &buf,
	}
	logger := NewStandardLogger(config)

	query := "SELECT * FROM large_table"
	args := []interface{}{}
	duration := 2 * time.Second
	threshold := 1 * time.Second

	logger.LogSlowQuery(context.Background(), query, args, duration, threshold)

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse JSON log: %v", err)
	}

	if logEntry["level"] != "WARN" {
		t.Error("Slow queries should be logged at WARN level")
	}

	if !strings.Contains(logEntry["message"].(string), "SLOW QUERY") {
		t.Error("Should indicate this is a slow query")
	}

	if logEntry["duration_ms"] != float64(2000) {
		t.Errorf("Expected duration 2000ms, got %v", logEntry["duration_ms"])
	}

	if logEntry["threshold_ms"] != float64(1000) {
		t.Errorf("Expected threshold 1000ms, got %v", logEntry["threshold_ms"])
	}
}

func TestLogger_RequestIDCorrelation(t *testing.T) {
	var buf bytes.Buffer
	config := &LoggerConfig{
		Level:  INFO,
		Format: "json",
		Output: &buf,
	}
	logger := NewStandardLogger(config)

	requestID := "req-123-456"
	ctx := logger.WithRequestID(context.Background(), requestID)

	logger.Info(ctx, "test message with request ID")

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse JSON log: %v", err)
	}

	if logEntry["request_id"] != requestID {
		t.Errorf("Expected request_id %s, got %v", requestID, logEntry["request_id"])
	}
}

func TestLogger_ErrorLogging(t *testing.T) {
	var buf bytes.Buffer
	config := &LoggerConfig{
		Level:  ERROR,
		Format: "json",
		Output: &buf,
	}
	logger := NewStandardLogger(config)

	query := "INSERT INTO users (name) VALUES (?)"
	args := []interface{}{"test"}
	duration := 10 * time.Millisecond
	err := fmt.Errorf("duplicate key error")

	logger.LogQuery(context.Background(), query, args, duration, err)

	var logEntry map[string]interface{}
	if jsonErr := json.Unmarshal(buf.Bytes(), &logEntry); jsonErr != nil {
		t.Fatalf("Failed to parse JSON log: %v", jsonErr)
	}

	if logEntry["level"] != "ERROR" {
		t.Error("Query errors should be logged at ERROR level")
	}

	if logEntry["error"] == nil {
		t.Error("Error should be included in log entry")
	}
}

func TestLogger_Sampling(t *testing.T) {
	var buf bytes.Buffer
	config := &LoggerConfig{
		Level:        DEBUG,
		Format:       "json",
		Output:       &buf,
		SamplingRate: 0.1,
	}
	logger := NewStandardLogger(config)

	messageCount := 0
	iterations := 1000

	for i := 0; i < iterations; i++ {
		buf.Reset()
		logger.Debug(context.Background(), "sampled message")
		if buf.Len() > 0 {
			messageCount++
		}
	}

	// With 0.1 sampling rate, we should get roughly 100 messages (+/- some variance)
	if messageCount < 50 || messageCount > 150 {
		t.Errorf("Expected roughly 100 messages with 0.1 sampling, got %d", messageCount)
	}
}

func TestLogger_PerformanceMetrics(t *testing.T) {
	var buf bytes.Buffer
	config := &LoggerConfig{
		Level:  INFO,
		Format: "json",
		Output: &buf,
	}
	logger := NewStandardLogger(config)

	// Test performance logging through structured fields
	logger.Info(context.Background(), "connection pool stats",
		Int("active_connections", 5),
		Int("idle_connections", 10),
		Int("max_connections", 20),
		Int("total_acquired", 100),
		Int("total_released", 95),
		Duration("average_wait_time", 50*time.Millisecond))

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse JSON log: %v", err)
	}

	if logEntry["active_connections"] != float64(5) {
		t.Errorf("Expected active_connections 5, got %v", logEntry["active_connections"])
	}

	if logEntry["idle_connections"] != float64(10) {
		t.Errorf("Expected idle_connections 10, got %v", logEntry["idle_connections"])
	}

	if logEntry["average_wait_time"] != float64(50000000) { // Duration in nanoseconds
		t.Errorf("Expected average_wait_time 50000000ns, got %v", logEntry["average_wait_time"])
	}
}

func TestParameterSanitization(t *testing.T) {
	var buf bytes.Buffer
	config := &LoggerConfig{
		Level:          DEBUG,
		Format:         "json",
		Output:         &buf,
		SanitizeParams: true,
	}
	logger := NewStandardLogger(config)

	// Test that sensitive string parameters are sanitized
	query := "SELECT * FROM users WHERE email = ? AND password = ?"
	args := []interface{}{"user@example.com", "password123"}
	duration := 10 * time.Millisecond

	logger.LogQuery(context.Background(), query, args, duration, nil)

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse JSON log: %v", err)
	}

	// Check that the args were logged and sanitized (contains redacted info)
	argsStr := logEntry["args"].(string)
	if !strings.Contains(argsStr, "[REDACTED]") && !strings.Contains(argsStr, "***") {
		t.Error("Sensitive parameters should be sanitized in logs")
	}
}

func BenchmarkLogger_Info(b *testing.B) {
	var buf bytes.Buffer
	config := &LoggerConfig{
		Level:  INFO,
		Format: "json",
		Output: &buf,
	}
	logger := NewStandardLogger(config)

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		logger.Info(ctx, "benchmark message", String("iteration", string(rune(i))))
	}
}

func BenchmarkLogger_QueryLogging(b *testing.B) {
	var buf bytes.Buffer
	config := &LoggerConfig{
		Level:  DEBUG,
		Format: "json",
		Output: &buf,
	}
	logger := NewStandardLogger(config)

	ctx := context.Background()
	query := "SELECT * FROM users WHERE id = ? AND name = ?"
	args := []interface{}{123, "test"}
	duration := 10 * time.Millisecond

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		logger.LogQuery(ctx, query, args, duration, nil)
	}
}

func BenchmarkLogger_WithSampling(b *testing.B) {
	var buf bytes.Buffer
	config := &LoggerConfig{
		Level:        DEBUG,
		Format:       "json",
		Output:       &buf,
		SamplingRate: 0.1,
	}
	logger := NewStandardLogger(config)

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		logger.Debug(ctx, "sampled message")
	}
}