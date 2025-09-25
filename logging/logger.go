package logging

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

// LogLevel represents logging verbosity levels
type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
	FATAL
)

var levelNames = map[LogLevel]string{
	DEBUG: "DEBUG",
	INFO:  "INFO",
	WARN:  "WARN",
	ERROR: "ERROR",
	FATAL: "FATAL",
}

func (l LogLevel) String() string {
	if name, exists := levelNames[l]; exists {
		return name
	}
	return "UNKNOWN"
}

// Logger interface defines the contract for GORP logging
type Logger interface {
	// Basic logging methods
	Debug(ctx context.Context, msg string, fields ...Field)
	Info(ctx context.Context, msg string, fields ...Field)
	Warn(ctx context.Context, msg string, fields ...Field)
	Error(ctx context.Context, msg string, err error, fields ...Field)
	Fatal(ctx context.Context, msg string, err error, fields ...Field)

	// Query-specific logging
	LogQuery(ctx context.Context, query string, args []interface{}, duration time.Duration, err error)
	LogSlowQuery(ctx context.Context, query string, args []interface{}, duration time.Duration, threshold time.Duration)

	// Transaction logging
	LogTransaction(ctx context.Context, event TransactionEvent, fields ...Field)

	// Connection logging
	LogConnection(ctx context.Context, event ConnectionEvent, fields ...Field)

	// Performance metrics
	LogMetrics(ctx context.Context, metrics *PerformanceMetrics)

	// Configuration
	SetLevel(level LogLevel)
	GetLevel() LogLevel
	IsEnabled(level LogLevel) bool

	// Context utilities
	WithRequestID(ctx context.Context, requestID string) context.Context
	WithFields(fields ...Field) Logger
}

// Field represents a structured logging field
type Field struct {
	Key   string
	Value interface{}
}

// Convenience functions for creating fields
func String(key, value string) Field       { return Field{Key: key, Value: value} }
func Int(key string, value int) Field      { return Field{Key: key, Value: value} }
func Int64(key string, value int64) Field  { return Field{Key: key, Value: value} }
func Float64(key string, value float64) Field { return Field{Key: key, Value: value} }
func Bool(key string, value bool) Field    { return Field{Key: key, Value: value} }
func Duration(key string, value time.Duration) Field { return Field{Key: key, Value: value} }
func Time(key string, value time.Time) Field { return Field{Key: key, Value: value} }
func Error(err error) Field                 { return Field{Key: "error", Value: err} }
func Any(key string, value interface{}) Field { return Field{Key: key, Value: value} }

// Events for structured logging
type TransactionEvent string

const (
	TransactionBegin    TransactionEvent = "begin"
	TransactionCommit   TransactionEvent = "commit"
	TransactionRollback TransactionEvent = "rollback"
)

type ConnectionEvent string

const (
	ConnectionOpen  ConnectionEvent = "open"
	ConnectionClose ConnectionEvent = "close"
	ConnectionError ConnectionEvent = "error"
)

// PerformanceMetrics holds performance statistics
type PerformanceMetrics struct {
	QueryCount       int64         `json:"query_count"`
	AverageLatency   time.Duration `json:"average_latency"`
	ErrorRate        float64       `json:"error_rate"`
	SlowQueryCount   int64         `json:"slow_query_count"`
	ConnectionsActive int          `json:"connections_active"`
	ConnectionsIdle   int          `json:"connections_idle"`
	Timestamp        time.Time     `json:"timestamp"`
}

// LoggerConfig holds logger configuration
type LoggerConfig struct {
	Level              LogLevel      `json:"level"`
	Format             string        `json:"format"` // "text" or "json"
	Output             io.Writer     `json:"-"`
	EnableQueryLogging bool          `json:"enable_query_logging"`
	SlowQueryThreshold time.Duration `json:"slow_query_threshold"`
	SanitizeParams     bool          `json:"sanitize_params"`
	MaxParamLength     int           `json:"max_param_length"`
	SamplingRate       float64       `json:"sampling_rate"` // 0.0 to 1.0
	IncludeStackTrace  bool          `json:"include_stack_trace"`
	RequestIDHeader    string        `json:"request_id_header"`
}

// DefaultLoggerConfig returns sensible defaults
func DefaultLoggerConfig() *LoggerConfig {
	return &LoggerConfig{
		Level:              INFO,
		Format:             "text",
		Output:             os.Stdout,
		EnableQueryLogging: true,
		SlowQueryThreshold: 1 * time.Second,
		SanitizeParams:     true,
		MaxParamLength:     100,
		SamplingRate:       1.0,
		IncludeStackTrace:  false,
		RequestIDHeader:    "X-Request-ID",
	}
}

// StandardLogger is the default implementation of Logger
type StandardLogger struct {
	config    *LoggerConfig
	output    io.Writer
	mu        sync.RWMutex
	fields    []Field
	sampler   *LogSampler
}

// NewStandardLogger creates a new standard logger
func NewStandardLogger(config *LoggerConfig) *StandardLogger {
	if config == nil {
		config = DefaultLoggerConfig()
	}

	if config.Output == nil {
		config.Output = os.Stdout
	}

	logger := &StandardLogger{
		config:  config,
		output:  config.Output,
		sampler: NewLogSampler(config.SamplingRate),
	}

	return logger
}

// Debug logs a debug message
func (l *StandardLogger) Debug(ctx context.Context, msg string, fields ...Field) {
	l.log(ctx, DEBUG, msg, nil, fields...)
}

// Info logs an info message
func (l *StandardLogger) Info(ctx context.Context, msg string, fields ...Field) {
	l.log(ctx, INFO, msg, nil, fields...)
}

// Warn logs a warning message
func (l *StandardLogger) Warn(ctx context.Context, msg string, fields ...Field) {
	l.log(ctx, WARN, msg, nil, fields...)
}

// Error logs an error message
func (l *StandardLogger) Error(ctx context.Context, msg string, err error, fields ...Field) {
	l.log(ctx, ERROR, msg, err, fields...)
}

// Fatal logs a fatal message and exits
func (l *StandardLogger) Fatal(ctx context.Context, msg string, err error, fields ...Field) {
	l.log(ctx, FATAL, msg, err, fields...)
	os.Exit(1)
}

// LogQuery logs database query execution
func (l *StandardLogger) LogQuery(ctx context.Context, query string, args []interface{}, duration time.Duration, err error) {
	if !l.config.EnableQueryLogging || !l.IsEnabled(DEBUG) {
		return
	}

	fields := []Field{
		String("query", l.sanitizeQuery(query)),
		Duration("duration", duration),
		Int("args_count", len(args)),
	}

	if l.config.SanitizeParams {
		fields = append(fields, String("args", l.sanitizeArgs(args)))
	} else {
		fields = append(fields, Any("args", args))
	}

	if err != nil {
		l.Error(ctx, "Query failed", err, fields...)
	} else {
		l.Debug(ctx, "Query executed", fields...)
	}
}

// LogSlowQuery logs slow query detection
func (l *StandardLogger) LogSlowQuery(ctx context.Context, query string, args []interface{}, duration time.Duration, threshold time.Duration) {
	if duration < threshold {
		return
	}

	fields := []Field{
		String("query", l.sanitizeQuery(query)),
		Duration("duration", duration),
		Duration("threshold", threshold),
		Float64("slowness_ratio", float64(duration)/float64(threshold)),
	}

	if l.config.SanitizeParams {
		fields = append(fields, String("args", l.sanitizeArgs(args)))
	} else {
		fields = append(fields, Any("args", args))
	}

	l.Warn(ctx, "Slow query detected", fields...)
}

// LogTransaction logs transaction events
func (l *StandardLogger) LogTransaction(ctx context.Context, event TransactionEvent, fields ...Field) {
	if !l.IsEnabled(DEBUG) {
		return
	}

	allFields := append([]Field{String("event", string(event))}, fields...)
	l.Debug(ctx, "Transaction event", allFields...)
}

// LogConnection logs connection events
func (l *StandardLogger) LogConnection(ctx context.Context, event ConnectionEvent, fields ...Field) {
	level := INFO
	if event == ConnectionError {
		level = ERROR
	}

	allFields := append([]Field{String("event", string(event))}, fields...)

	switch level {
	case ERROR:
		// Extract error from fields if present
		var err error
		for _, field := range fields {
			if field.Key == "error" {
				if e, ok := field.Value.(error); ok {
					err = e
					break
				}
			}
		}
		l.Error(ctx, "Connection event", err, allFields...)
	default:
		l.Info(ctx, "Connection event", allFields...)
	}
}

// LogMetrics logs performance metrics
func (l *StandardLogger) LogMetrics(ctx context.Context, metrics *PerformanceMetrics) {
	if !l.IsEnabled(INFO) {
		return
	}

	fields := []Field{
		Int64("query_count", metrics.QueryCount),
		Duration("average_latency", metrics.AverageLatency),
		Float64("error_rate", metrics.ErrorRate),
		Int64("slow_query_count", metrics.SlowQueryCount),
		Int("connections_active", metrics.ConnectionsActive),
		Int("connections_idle", metrics.ConnectionsIdle),
		Time("timestamp", metrics.Timestamp),
	}

	l.Info(ctx, "Performance metrics", fields...)
}

// SetLevel sets the logging level
func (l *StandardLogger) SetLevel(level LogLevel) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.config.Level = level
}

// GetLevel returns the current logging level
func (l *StandardLogger) GetLevel() LogLevel {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.config.Level
}

// IsEnabled checks if a log level is enabled
func (l *StandardLogger) IsEnabled(level LogLevel) bool {
	return level >= l.GetLevel()
}

// WithRequestID adds a request ID to the context
func (l *StandardLogger) WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, "request_id", requestID)
}

// WithFields returns a new logger with additional fields
func (l *StandardLogger) WithFields(fields ...Field) Logger {
	newLogger := &StandardLogger{
		config:  l.config,
		output:  l.output,
		sampler: l.sampler,
		fields:  append(l.fields, fields...),
	}
	return newLogger
}

// log is the internal logging implementation
func (l *StandardLogger) log(ctx context.Context, level LogLevel, msg string, err error, fields ...Field) {
	if !l.IsEnabled(level) {
		return
	}

	// Apply sampling
	if !l.sampler.ShouldLog() {
		return
	}

	// Combine all fields
	allFields := append(l.fields, fields...)

	// Add request ID from context
	if requestID := l.getRequestID(ctx); requestID != "" {
		allFields = append(allFields, String("request_id", requestID))
	}

	// Add error field if present
	if err != nil {
		allFields = append(allFields, Field{Key: "error", Value: err.Error()})
	}

	// Create log entry
	entry := &LogEntry{
		Timestamp: time.Now(),
		Level:     level,
		Message:   msg,
		Fields:    allFields,
	}

	// Format and write
	formatted := l.formatEntry(entry)
	l.mu.Lock()
	fmt.Fprint(l.output, formatted)
	l.mu.Unlock()
}

// LogEntry represents a single log entry
type LogEntry struct {
	Timestamp time.Time `json:"timestamp"`
	Level     LogLevel  `json:"level"`
	Message   string    `json:"message"`
	Fields    []Field   `json:"fields"`
}

// formatEntry formats a log entry based on configuration
func (l *StandardLogger) formatEntry(entry *LogEntry) string {
	switch strings.ToLower(l.config.Format) {
	case "json":
		return l.formatJSON(entry)
	default:
		return l.formatText(entry)
	}
}

// formatText formats log entry as human-readable text
func (l *StandardLogger) formatText(entry *LogEntry) string {
	var builder strings.Builder

	// Timestamp and level
	builder.WriteString(entry.Timestamp.Format("2006-01-02T15:04:05.000Z07:00"))
	builder.WriteString(" [")
	builder.WriteString(entry.Level.String())
	builder.WriteString("] ")
	builder.WriteString(entry.Message)

	// Add fields
	if len(entry.Fields) > 0 {
		builder.WriteString(" |")
		for _, field := range entry.Fields {
			builder.WriteString(" ")
			builder.WriteString(field.Key)
			builder.WriteString("=")
			builder.WriteString(formatValue(field.Value))
		}
	}

	builder.WriteString("\n")
	return builder.String()
}

// formatJSON formats log entry as JSON
func (l *StandardLogger) formatJSON(entry *LogEntry) string {
	data := map[string]interface{}{
		"timestamp": entry.Timestamp.Format(time.RFC3339Nano),
		"level":     entry.Level.String(),
		"message":   entry.Message,
	}

	// Add fields to the data map
	for _, field := range entry.Fields {
		data[field.Key] = field.Value
	}

	jsonBytes, err := json.Marshal(data)
	if err != nil {
		// Fallback to text format
		return l.formatText(entry)
	}

	return string(jsonBytes) + "\n"
}

// formatValue converts a field value to string representation
func formatValue(value interface{}) string {
	if value == nil {
		return "<nil>"
	}

	switch v := value.(type) {
	case string:
		return fmt.Sprintf(`"%s"`, v)
	case time.Duration:
		return v.String()
	case time.Time:
		return v.Format(time.RFC3339)
	case error:
		return fmt.Sprintf(`"%s"`, v.Error())
	default:
		return fmt.Sprintf("%v", v)
	}
}

// getRequestID extracts request ID from context
func (l *StandardLogger) getRequestID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}

	if requestID, ok := ctx.Value("request_id").(string); ok {
		return requestID
	}

	return ""
}

// Query and parameter sanitization
var (
	passwordRegex = regexp.MustCompile(`(?i)(password|pwd|secret|token|key)\s*[=:]\s*['"]?([^'",\s]+)`)
	emailRegex    = regexp.MustCompile(`\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b`)
	numberRegex   = regexp.MustCompile(`\b\d{4,}\b`) // Numbers with 4+ digits (potential IDs, SSNs, etc.)
)

// sanitizeQuery sanitizes sensitive information from queries
func (l *StandardLogger) sanitizeQuery(query string) string {
	if !l.config.SanitizeParams {
		return query
	}

	// Remove passwords and secrets
	sanitized := passwordRegex.ReplaceAllString(query, "${1}=***")

	// Optionally sanitize emails and long numbers
	sanitized = emailRegex.ReplaceAllString(sanitized, "***@***.***")
	sanitized = numberRegex.ReplaceAllString(sanitized, "***")

	return sanitized
}

// sanitizeArgs sanitizes query arguments for safe logging
func (l *StandardLogger) sanitizeArgs(args []interface{}) string {
	if len(args) == 0 {
		return "[]"
	}

	var sanitized []string
	for i, arg := range args {
		str := l.sanitizeValue(arg)
		if len(str) > l.config.MaxParamLength {
			str = str[:l.config.MaxParamLength-3] + "..."
		}
		sanitized = append(sanitized, fmt.Sprintf("[%d]=%s", i, str))
	}

	return "[" + strings.Join(sanitized, ", ") + "]"
}

// sanitizeValue sanitizes a single parameter value
func (l *StandardLogger) sanitizeValue(value interface{}) string {
	if value == nil {
		return "<nil>"
	}

	str := fmt.Sprintf("%v", value)

	if !l.config.SanitizeParams {
		return str
	}

	// Check if value looks like sensitive data
	if l.isSensitiveValue(str) {
		return "***"
	}

	return str
}

// isSensitiveValue checks if a value looks sensitive
func (l *StandardLogger) isSensitiveValue(value string) bool {
	lower := strings.ToLower(value)

	// Check for common password patterns
	if strings.Contains(lower, "password") ||
	   strings.Contains(lower, "secret") ||
	   strings.Contains(lower, "token") ||
	   len(value) > 20 { // Long strings might be sensitive
		return true
	}

	// Check for email pattern
	if emailRegex.MatchString(value) {
		return true
	}

	return false
}

// LogSampler handles log sampling to reduce overhead
type LogSampler struct {
	rate    float64
	counter int64
	mu      sync.Mutex
}

// NewLogSampler creates a new log sampler
func NewLogSampler(rate float64) *LogSampler {
	if rate < 0 {
		rate = 0
	}
	if rate > 1 {
		rate = 1
	}

	return &LogSampler{
		rate: rate,
	}
}

// ShouldLog determines if a log message should be written
func (ls *LogSampler) ShouldLog() bool {
	if ls.rate >= 1.0 {
		return true
	}

	if ls.rate <= 0.0 {
		return false
	}

	ls.mu.Lock()
	defer ls.mu.Unlock()

	ls.counter++
	if float64(ls.counter)*ls.rate >= 1.0 {
		ls.counter = 0
		return true
	}
	return false
}

// NoOpLogger is a logger that does nothing (for testing or disabling logging)
type NoOpLogger struct{}

func NewNoOpLogger() *NoOpLogger {
	return &NoOpLogger{}
}

func (n *NoOpLogger) Debug(ctx context.Context, msg string, fields ...Field)                                                    {}
func (n *NoOpLogger) Info(ctx context.Context, msg string, fields ...Field)                                                     {}
func (n *NoOpLogger) Warn(ctx context.Context, msg string, fields ...Field)                                                     {}
func (n *NoOpLogger) Error(ctx context.Context, msg string, err error, fields ...Field)                                       {}
func (n *NoOpLogger) Fatal(ctx context.Context, msg string, err error, fields ...Field)                                       {}
func (n *NoOpLogger) LogQuery(ctx context.Context, query string, args []interface{}, duration time.Duration, err error)       {}
func (n *NoOpLogger) LogSlowQuery(ctx context.Context, query string, args []interface{}, duration time.Duration, threshold time.Duration) {}
func (n *NoOpLogger) LogTransaction(ctx context.Context, event TransactionEvent, fields ...Field)                              {}
func (n *NoOpLogger) LogConnection(ctx context.Context, event ConnectionEvent, fields ...Field)                                {}
func (n *NoOpLogger) LogMetrics(ctx context.Context, metrics *PerformanceMetrics)                                             {}
func (n *NoOpLogger) SetLevel(level LogLevel)                                                                                  {}
func (n *NoOpLogger) GetLevel() LogLevel                                                                                       { return FATAL }
func (n *NoOpLogger) IsEnabled(level LogLevel) bool                                                                           { return false }
func (n *NoOpLogger) WithRequestID(ctx context.Context, requestID string) context.Context                                     { return ctx }
func (n *NoOpLogger) WithFields(fields ...Field) Logger                                                                       { return n }

// Global logger instance
var (
	globalLogger Logger = NewStandardLogger(DefaultLoggerConfig())
	globalMu     sync.RWMutex
)

// SetGlobalLogger sets the global logger instance
func SetGlobalLogger(logger Logger) {
	globalMu.Lock()
	defer globalMu.Unlock()
	globalLogger = logger
}

// GetGlobalLogger returns the global logger instance
func GetGlobalLogger() Logger {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return globalLogger
}

// Convenience functions using global logger
func Debug(ctx context.Context, msg string, fields ...Field) {
	GetGlobalLogger().Debug(ctx, msg, fields...)
}

func Info(ctx context.Context, msg string, fields ...Field) {
	GetGlobalLogger().Info(ctx, msg, fields...)
}

func Warn(ctx context.Context, msg string, fields ...Field) {
	GetGlobalLogger().Warn(ctx, msg, fields...)
}

func LogError(ctx context.Context, msg string, err error, fields ...Field) {
	GetGlobalLogger().Error(ctx, msg, err, fields...)
}

func Fatal(ctx context.Context, msg string, err error, fields ...Field) {
	GetGlobalLogger().Fatal(ctx, msg, err, fields...)
}