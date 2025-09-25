package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)


func TestLogrusAdapter(t *testing.T) {
	var buf bytes.Buffer
	logrusLogger := logrus.New()
	logrusLogger.SetOutput(&buf)
	logrusLogger.SetFormatter(&logrus.JSONFormatter{})
	logrusLogger.SetLevel(logrus.InfoLevel)

	adapter := NewLogrusAdapter(logrusLogger)
	adapter.Info(context.Background(), "test message", String("key", "value"))

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse JSON log: %v", err)
	}

	if logEntry["level"] != "info" {
		t.Errorf("Expected level info, got %v", logEntry["level"])
	}

	if logEntry["msg"] != "test message" {
		t.Errorf("Expected msg 'test message', got %v", logEntry["msg"])
	}

	if logEntry["key"] != "value" {
		t.Errorf("Expected key 'value', got %v", logEntry["key"])
	}
}

func TestLogrusAdapter_QueryLogging(t *testing.T) {
	var buf bytes.Buffer
	logrusLogger := logrus.New()
	logrusLogger.SetOutput(&buf)
	logrusLogger.SetFormatter(&logrus.JSONFormatter{})
	logrusLogger.SetLevel(logrus.DebugLevel)

	adapter := NewLogrusAdapter(logrusLogger)

	query := "SELECT * FROM users WHERE id = ?"
	args := []interface{}{123}
	duration := 25 * time.Millisecond

	adapter.LogQuery(context.Background(), query, args, duration, nil)

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse JSON log: %v", err)
	}

	if logEntry["query"] != query {
		t.Errorf("Expected query to be logged")
	}

	if logEntry["duration_ms"] != float64(25) {
		t.Errorf("Expected duration 25ms, got %v", logEntry["duration_ms"])
	}
}

func TestZapAdapter(t *testing.T) {
	var buf bytes.Buffer
	config := zap.NewDevelopmentConfig()
	config.OutputPaths = []string{"stdout"}

	// Create a custom sink for testing
	_, err := config.Build()
	if err != nil {
		t.Fatalf("Failed to create zap logger: %v", err)
	}

	// Create a buffer logger for testing
	encoderCfg := zapcore.EncoderConfig{
		MessageKey:  "msg",
		LevelKey:    "level",
		TimeKey:     "timestamp",
		CallerKey:   "caller",
		EncodeLevel: zapcore.LowercaseLevelEncoder,
		EncodeTime:  zapcore.ISO8601TimeEncoder,
	}

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderCfg),
		zapcore.AddSync(&buf),
		zapcore.InfoLevel,
	)

	testLogger := zap.New(core)
	defer testLogger.Sync()

	adapter := NewZapAdapter(testLogger)
	adapter.Info(context.Background(), "test message", String("key", "value"))

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse JSON log: %v", err)
	}

	if logEntry["level"] != "info" {
		t.Errorf("Expected level info, got %v", logEntry["level"])
	}

	if logEntry["msg"] != "test message" {
		t.Errorf("Expected msg 'test message', got %v", logEntry["msg"])
	}

	if logEntry["key"] != "value" {
		t.Errorf("Expected key 'value', got %v", logEntry["key"])
	}
}

func TestZapAdapter_SlowQuery(t *testing.T) {
	var buf bytes.Buffer
	encoderCfg := zapcore.EncoderConfig{
		MessageKey:  "msg",
		LevelKey:    "level",
		TimeKey:     "timestamp",
		EncodeLevel: zapcore.LowercaseLevelEncoder,
		EncodeTime:  zapcore.ISO8601TimeEncoder,
	}

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderCfg),
		zapcore.AddSync(&buf),
		zapcore.WarnLevel,
	)

	testLogger := zap.New(core)
	defer testLogger.Sync()

	adapter := NewZapAdapter(testLogger)

	query := "SELECT * FROM large_table"
	args := []interface{}{}
	duration := 2 * time.Second
	threshold := 1 * time.Second

	adapter.LogSlowQuery(context.Background(), query, args, duration, threshold)

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse JSON log: %v", err)
	}

	if logEntry["level"] != "warn" {
		t.Error("Slow queries should be logged at warn level")
	}

	if !strings.Contains(logEntry["msg"].(string), "SLOW QUERY") {
		t.Error("Should indicate this is a slow query")
	}
}

func TestSlogAdapter(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	slogLogger := slog.New(handler)

	adapter := NewSlogAdapter(slogLogger)
	adapter.Info(context.Background(), "test message", String("key", "value"))

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse JSON log: %v", err)
	}

	if logEntry["level"] != "INFO" {
		t.Errorf("Expected level INFO, got %v", logEntry["level"])
	}

	if logEntry["msg"] != "test message" {
		t.Errorf("Expected msg 'test message', got %v", logEntry["msg"])
	}

	if logEntry["key"] != "value" {
		t.Errorf("Expected key 'value', got %v", logEntry["key"])
	}
}

func TestSlogAdapter_RequestIDCorrelation(t *testing.T) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	slogLogger := slog.New(handler)

	adapter := NewSlogAdapter(slogLogger)

	requestID := "req-789-012"
	ctx := adapter.WithRequestID(context.Background(), requestID)
	adapter.Info(ctx, "test message with request ID")

	var logEntry map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &logEntry); err != nil {
		t.Fatalf("Failed to parse JSON log: %v", err)
	}

	if logEntry["request_id"] != requestID {
		t.Errorf("Expected request_id %s, got %v", requestID, logEntry["request_id"])
	}
}

func TestLoggerFactory(t *testing.T) {
	tests := []struct {
		name        string
		loggerType  string
		expectError bool
	}{
		{
			name:        "standard logger",
			loggerType:  "standard",
			expectError: false,
		},
		{
			name:        "logrus logger",
			loggerType:  "logrus",
			expectError: false,
		},
		{
			name:        "zap logger",
			loggerType:  "zap",
			expectError: false,
		},
		{
			name:        "slog logger",
			loggerType:  "slog",
			expectError: false,
		},
		{
			name:        "unknown logger",
			loggerType:  "unknown",
			expectError: true,
		},
	}

	factory := NewLoggerFactory()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := map[string]interface{}{
				"level":  INFO,
				"format": "json",
			}

			logger, err := factory.CreateLogger(tt.loggerType, config)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if logger == nil {
				t.Error("Expected logger but got nil")
			}

			// Test that the logger works
			var buf bytes.Buffer
			ctx := context.Background()

			switch tt.loggerType {
			case "standard":
				// For standard logger, we need to redirect output
				standardLogger := logger.(*StandardLogger)
				standardLogger.output = &buf
			}

			logger.Info(ctx, "test message")
			// Don't assert output format since different loggers have different formats
			// Just ensure no panic occurred
		})
	}
}

func TestAdapter_LevelMapping(t *testing.T) {
	// Test that all adapters properly map log levels
	var buf bytes.Buffer

	// Test Logrus adapter
	logrusLogger := logrus.New()
	logrusLogger.SetOutput(&buf)
	logrusLogger.SetLevel(logrus.ErrorLevel)
	logrusAdapter := NewLogrusAdapter(logrusLogger)

	// INFO should be filtered out
	logrusAdapter.Info(context.Background(), "info message")
	if buf.Len() > 0 {
		t.Error("INFO message should be filtered out when level is ERROR")
	}

	// ERROR should pass through
	buf.Reset()
	logrusAdapter.Error(context.Background(), "error message", nil)
	if buf.Len() == 0 {
		t.Error("ERROR message should pass through when level is ERROR")
	}

	// Test Zap adapter
	buf.Reset()
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(zapcore.EncoderConfig{
			MessageKey: "msg",
			LevelKey:   "level",
		}),
		zapcore.AddSync(&buf),
		zapcore.ErrorLevel,
	)

	testLogger := zap.New(core)
	zapAdapter := NewZapAdapter(testLogger)

	// INFO should be filtered out
	zapAdapter.Info(context.Background(), "info message")
	if buf.Len() > 0 {
		t.Error("INFO message should be filtered out when level is ERROR")
	}

	// ERROR should pass through
	buf.Reset()
	zapAdapter.Error(context.Background(), "error message", nil)
	if buf.Len() == 0 {
		t.Error("ERROR message should pass through when level is ERROR")
	}

	// Test Slog adapter
	buf.Reset()
	handler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelError,
	})
	slogLogger := slog.New(handler)
	slogAdapter := NewSlogAdapter(slogLogger)

	// INFO should be filtered out
	slogAdapter.Info(context.Background(), "info message")
	if buf.Len() > 0 {
		t.Error("INFO message should be filtered out when level is ERROR")
	}

	// ERROR should pass through
	buf.Reset()
	slogAdapter.Error(context.Background(), "error message", nil)
	if buf.Len() == 0 {
		t.Error("ERROR message should pass through when level is ERROR")
	}
}

func BenchmarkLogrusAdapter(b *testing.B) {
	var buf bytes.Buffer
	logrusLogger := logrus.New()
	logrusLogger.SetOutput(&buf)
	logrusLogger.SetFormatter(&logrus.JSONFormatter{})

	adapter := NewLogrusAdapter(logrusLogger)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		adapter.Info(ctx, "benchmark message", String("iteration", string(rune(i))))
	}
}

func BenchmarkZapAdapter(b *testing.B) {
	var buf bytes.Buffer
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(zapcore.EncoderConfig{
			MessageKey: "msg",
			LevelKey:   "level",
		}),
		zapcore.AddSync(&buf),
		zapcore.InfoLevel,
	)

	testLogger := zap.New(core)
	adapter := NewZapAdapter(testLogger)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		adapter.Info(ctx, "benchmark message", String("iteration", string(rune(i))))
	}
}

func BenchmarkSlogAdapter(b *testing.B) {
	var buf bytes.Buffer
	handler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	slogLogger := slog.New(handler)

	adapter := NewSlogAdapter(slogLogger)
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		adapter.Info(ctx, "benchmark message", String("iteration", string(rune(i))))
	}
}