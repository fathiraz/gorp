// Package instrumentation provides debug logging, metrics collection, and OpenTelemetry hooks
package instrumentation

import (
	"context"
	"log"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Logger defines the interface for logging operations
type Logger interface {
	Debug(msg string, args ...interface{})
	Info(msg string, args ...interface{})
	Warn(msg string, args ...interface{})
	Error(msg string, args ...interface{})
}

// LogLevel represents the logging level
type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarn
	LogLevelError
)

// DefaultLogger provides a default logger implementation
type DefaultLogger struct {
	level LogLevel
}

// NewDefaultLogger creates a new default logger
func NewDefaultLogger(level LogLevel) *DefaultLogger {
	return &DefaultLogger{level: level}
}

func (l *DefaultLogger) Debug(msg string, args ...interface{}) {
	if l.level <= LogLevelDebug {
		log.Printf("[DEBUG] "+msg, args...)
	}
}

func (l *DefaultLogger) Info(msg string, args ...interface{}) {
	if l.level <= LogLevelInfo {
		log.Printf("[INFO] "+msg, args...)
	}
}

func (l *DefaultLogger) Warn(msg string, args ...interface{}) {
	if l.level <= LogLevelWarn {
		log.Printf("[WARN] "+msg, args...)
	}
}

func (l *DefaultLogger) Error(msg string, args ...interface{}) {
	if l.level <= LogLevelError {
		log.Printf("[ERROR] "+msg, args...)
	}
}

// MetricsCollector defines the interface for collecting metrics
type MetricsCollector interface {
	// Counter operations
	IncrementCounter(name string, labels map[string]string)
	IncrementCounterBy(name string, value float64, labels map[string]string)

	// Gauge operations
	SetGauge(name string, value float64, labels map[string]string)
	IncrementGauge(name string, labels map[string]string)
	DecrementGauge(name string, labels map[string]string)

	// Histogram operations
	RecordHistogram(name string, value float64, labels map[string]string)
	RecordDuration(name string, duration time.Duration, labels map[string]string)

	// Timer operations
	StartTimer(name string, labels map[string]string) Timer
	RecordTimer(name string, labels map[string]string) func()

	// Custom metrics
	RegisterCustomMetric(name, help string, metricType MetricType, labels []string) error
	RecordCustomMetric(name string, value float64, labels map[string]string)

	// Lifecycle
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Flush(ctx context.Context) error
}

// Timer represents a running timer for measuring durations
type Timer interface {
	Stop() time.Duration
	Elapsed() time.Duration
}

// MetricType represents the type of metric
type MetricType string

const (
	MetricTypeCounter   MetricType = "counter"
	MetricTypeGauge     MetricType = "gauge"
	MetricTypeHistogram MetricType = "histogram"
	MetricTypeSummary   MetricType = "summary"
)

// NoOpMetricsCollector provides a no-op implementation of MetricsCollector
type NoOpMetricsCollector struct{}

func (NoOpMetricsCollector) IncrementCounter(name string, labels map[string]string)                      {}
func (NoOpMetricsCollector) IncrementCounterBy(name string, value float64, labels map[string]string)    {}
func (NoOpMetricsCollector) SetGauge(name string, value float64, labels map[string]string)              {}
func (NoOpMetricsCollector) IncrementGauge(name string, labels map[string]string)                       {}
func (NoOpMetricsCollector) DecrementGauge(name string, labels map[string]string)                       {}
func (NoOpMetricsCollector) RecordHistogram(name string, value float64, labels map[string]string)       {}
func (NoOpMetricsCollector) RecordDuration(name string, duration time.Duration, labels map[string]string) {}
func (NoOpMetricsCollector) StartTimer(name string, labels map[string]string) Timer                     { return &NoOpTimer{} }
func (NoOpMetricsCollector) RecordTimer(name string, labels map[string]string) func()                   { return func() {} }
func (NoOpMetricsCollector) RegisterCustomMetric(name, help string, metricType MetricType, labels []string) error { return nil }
func (NoOpMetricsCollector) RecordCustomMetric(name string, value float64, labels map[string]string)    {}
func (NoOpMetricsCollector) Start(ctx context.Context) error                                            { return nil }
func (NoOpMetricsCollector) Stop(ctx context.Context) error                                             { return nil }
func (NoOpMetricsCollector) Flush(ctx context.Context) error                                            { return nil }

// NoOpTimer provides a no-op implementation of Timer
type NoOpTimer struct {
	start time.Time
}

func (t *NoOpTimer) Stop() time.Duration {
	if t.start.IsZero() {
		t.start = time.Now()
	}
	return time.Since(t.start)
}

func (t *NoOpTimer) Elapsed() time.Duration {
	if t.start.IsZero() {
		t.start = time.Now()
	}
	return time.Since(t.start)
}

// Instrumentation provides instrumentation capabilities
type Instrumentation struct {
	logger           Logger
	metricsCollector MetricsCollector
	tracer           trace.Tracer
	enabled          bool
}

// Config holds instrumentation configuration
type Config struct {
	Logger           Logger
	MetricsCollector MetricsCollector
	TracerName       string
	Enabled          bool
}

// New creates a new Instrumentation instance
func New(config Config) *Instrumentation {
	logger := config.Logger
	if logger == nil {
		logger = NewDefaultLogger(LogLevelInfo)
	}

	metricsCollector := config.MetricsCollector
	if metricsCollector == nil {
		metricsCollector = &NoOpMetricsCollector{}
	}

	tracerName := config.TracerName
	if tracerName == "" {
		tracerName = "github.com/fathiraz/gorp"
	}

	return &Instrumentation{
		logger:           logger,
		metricsCollector: metricsCollector,
		tracer:           otel.Tracer(tracerName),
		enabled:          config.Enabled,
	}
}

// Logger returns the logger
func (i *Instrumentation) Logger() Logger {
	return i.logger
}

// MetricsCollector returns the metrics collector
func (i *Instrumentation) MetricsCollector() MetricsCollector {
	return i.metricsCollector
}

// IsEnabled returns whether instrumentation is enabled
func (i *Instrumentation) IsEnabled() bool {
	return i.enabled
}

// StartSpan starts a new OpenTelemetry span
func (i *Instrumentation) StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if !i.enabled {
		return ctx, trace.SpanFromContext(ctx)
	}
	return i.tracer.Start(ctx, name, opts...)
}

// LogQuery logs a database query with timing
func (i *Instrumentation) LogQuery(ctx context.Context, query string, args []interface{}, duration time.Duration, err error) {
	if !i.enabled {
		return
	}

	labels := map[string]string{
		"operation": "query",
	}

	if err != nil {
		labels["status"] = "error"
		i.logger.Error("Query failed: %s, args: %v, duration: %v, error: %v", query, args, duration, err)
		i.metricsCollector.IncrementCounter("gorp_query_errors_total", labels)
	} else {
		labels["status"] = "success"
		i.logger.Debug("Query executed: %s, args: %v, duration: %v", query, args, duration)
		i.metricsCollector.IncrementCounter("gorp_queries_total", labels)
	}

	i.metricsCollector.RecordDuration("gorp_query_duration_seconds", duration, labels)
}

// LogTransaction logs a database transaction
func (i *Instrumentation) LogTransaction(ctx context.Context, operation string, duration time.Duration, err error) {
	if !i.enabled {
		return
	}

	labels := map[string]string{
		"operation": operation,
	}

	if err != nil {
		labels["status"] = "error"
		i.logger.Error("Transaction %s failed: duration: %v, error: %v", operation, duration, err)
		i.metricsCollector.IncrementCounter("gorp_transaction_errors_total", labels)
	} else {
		labels["status"] = "success"
		i.logger.Debug("Transaction %s completed: duration: %v", operation, duration)
		i.metricsCollector.IncrementCounter("gorp_transactions_total", labels)
	}

	i.metricsCollector.RecordDuration("gorp_transaction_duration_seconds", duration, labels)
}

// WithSpanAttributes adds attributes to the current span
func (i *Instrumentation) WithSpanAttributes(ctx context.Context, attrs ...attribute.KeyValue) {
	if !i.enabled {
		return
	}

	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attrs...)
}

// WithSpanError records an error on the current span
func (i *Instrumentation) WithSpanError(ctx context.Context, err error) {
	if !i.enabled || err == nil {
		return
	}

	span := trace.SpanFromContext(ctx)
	span.RecordError(err)
	span.SetAttributes(attribute.Bool("error", true))
}

// QueryContext provides context for query operations
type QueryContext struct {
	ctx             context.Context
	instrumentation *Instrumentation
	span            trace.Span
	startTime       time.Time
}

// NewQueryContext creates a new query context
func NewQueryContext(ctx context.Context, instrumentation *Instrumentation, operation string) *QueryContext {
	newCtx, span := instrumentation.StartSpan(ctx, operation)

	return &QueryContext{
		ctx:             newCtx,
		instrumentation: instrumentation,
		span:            span,
		startTime:       time.Now(),
	}
}

// Context returns the underlying context
func (qc *QueryContext) Context() context.Context {
	return qc.ctx
}

// Finish completes the query context and logs metrics
func (qc *QueryContext) Finish(query string, args []interface{}, err error) {
	duration := time.Since(qc.startTime)

	qc.instrumentation.LogQuery(qc.ctx, query, args, duration, err)

	if err != nil {
		qc.instrumentation.WithSpanError(qc.ctx, err)
	}

	qc.instrumentation.WithSpanAttributes(qc.ctx,
		attribute.String("db.statement", query),
		attribute.Int("db.args_count", len(args)),
		attribute.Float64("db.duration_seconds", duration.Seconds()),
	)

	qc.span.End()
}

// Default instrumentation instance
var Default = New(Config{
	Logger:           NewDefaultLogger(LogLevelInfo),
	MetricsCollector: &NoOpMetricsCollector{},
	TracerName:       "github.com/fathiraz/gorp",
	Enabled:          false,
})