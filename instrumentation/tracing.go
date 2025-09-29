package instrumentation

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	// TracingInstrumentationName is the name for GORP tracing instrumentation
	TracingInstrumentationName = "github.com/fathiraz/gorp/instrumentation"
	// TracingInstrumentationVersion is the version of GORP tracing instrumentation
	TracingInstrumentationVersion = "v1.0.0"
)

// TracingConfig configures OpenTelemetry tracing for GORP
type TracingConfig struct {
	// ServiceName identifies this service in traces
	ServiceName string
	// ServiceVersion is the version of this service
	ServiceVersion string
	// Environment specifies the deployment environment (dev, staging, prod)
	Environment string
	// SamplingRatio controls trace sampling (0.0 = none, 1.0 = all)
	SamplingRatio float64
	// BatchTimeout controls trace export batch timeout
	BatchTimeout time.Duration
	// MaxBatchSize controls maximum trace batch size
	MaxBatchSize int
	// EnableMetrics enables OpenTelemetry metrics export
	EnableMetrics bool
	// DatabaseName is the default database name for traces
	DatabaseName string
	// DisableQuerySanitization disables query parameter sanitization in traces
	DisableQuerySanitization bool
	// MaxQueryLength limits the length of queries stored in spans
	MaxQueryLength int
}

// DefaultTracingConfig returns a default tracing configuration
func DefaultTracingConfig() TracingConfig {
	return TracingConfig{
		ServiceName:                  "gorp-application",
		ServiceVersion:               "unknown",
		Environment:                  "development",
		SamplingRatio:                0.1,
		BatchTimeout:                 5 * time.Second,
		MaxBatchSize:                 512,
		EnableMetrics:                true,
		DatabaseName:                 "default",
		DisableQuerySanitization:     false,
		MaxQueryLength:               1000,
	}
}

// TracingInstrumentation provides OpenTelemetry tracing for GORP
type TracingInstrumentation struct {
	config   TracingConfig
	tracer   trace.Tracer
	meter    metric.Meter
	provider *sdktrace.TracerProvider

	// Built-in metrics
	queryDuration    metric.Float64Histogram
	connectionCount  metric.Int64UpDownCounter
	transactionCount metric.Int64Counter
	errorCount       metric.Int64Counter
}

// NewTracingInstrumentation creates a new tracing instrumentation instance
func NewTracingInstrumentation(config TracingConfig) (*TracingInstrumentation, error) {
	// Create resource with service information
	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			semconv.ServiceName(config.ServiceName),
			semconv.ServiceVersion(config.ServiceVersion),
			semconv.DeploymentEnvironment(config.Environment),
			attribute.String("instrumentation.name", TracingInstrumentationName),
			attribute.String("instrumentation.version", TracingInstrumentationVersion),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Create trace provider with sampling
	sampler := sdktrace.ParentBased(sdktrace.TraceIDRatioBased(config.SamplingRatio))
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)

	// Set global trace provider
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// Create tracer and meter
	tracer := provider.Tracer(TracingInstrumentationName,
		trace.WithInstrumentationVersion(TracingInstrumentationVersion))
	meter := otel.Meter(TracingInstrumentationName,
		metric.WithInstrumentationVersion(TracingInstrumentationVersion))

	ti := &TracingInstrumentation{
		config:   config,
		tracer:   tracer,
		meter:    meter,
		provider: provider,
	}

	// Initialize metrics
	if err := ti.initializeMetrics(); err != nil {
		return nil, fmt.Errorf("failed to initialize metrics: %w", err)
	}

	return ti, nil
}

// initializeMetrics creates OpenTelemetry metrics instruments
func (ti *TracingInstrumentation) initializeMetrics() error {
	var err error

	ti.queryDuration, err = ti.meter.Float64Histogram(
		"gorp.query.duration",
		metric.WithDescription("Duration of database queries"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return fmt.Errorf("failed to create query duration histogram: %w", err)
	}

	ti.connectionCount, err = ti.meter.Int64UpDownCounter(
		"gorp.connections.active",
		metric.WithDescription("Number of active database connections"),
	)
	if err != nil {
		return fmt.Errorf("failed to create connection count gauge: %w", err)
	}

	ti.transactionCount, err = ti.meter.Int64Counter(
		"gorp.transactions.total",
		metric.WithDescription("Total number of database transactions"),
	)
	if err != nil {
		return fmt.Errorf("failed to create transaction counter: %w", err)
	}

	ti.errorCount, err = ti.meter.Int64Counter(
		"gorp.errors.total",
		metric.WithDescription("Total number of database errors"),
	)
	if err != nil {
		return fmt.Errorf("failed to create error counter: %w", err)
	}

	return nil
}

// WrapDatabase wraps a database with OpenTelemetry instrumentation
func (ti *TracingInstrumentation) WrapDatabase(driverName string, db *sql.DB) *TracedDB {
	return &TracedDB{
		DB:     db,
		tracer: ti,
		driver: driverName,
	}
}

// StartSpan creates a new span for a database operation
func (ti *TracingInstrumentation) StartSpan(ctx context.Context, operationName string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	// Add standard attributes
	standardAttrs := []attribute.KeyValue{
		semconv.DBSystemKey.String("other"), // Will be overridden by specific database type
		attribute.String("db.operation", operationName),
		attribute.String("db.name", ti.config.DatabaseName),
	}
	standardAttrs = append(standardAttrs, attrs...)

	return ti.tracer.Start(ctx, operationName, trace.WithAttributes(standardAttrs...))
}

// RecordQuery records a database query with tracing and metrics
func (ti *TracingInstrumentation) RecordQuery(ctx context.Context, query string, duration time.Duration, err error, attrs ...attribute.KeyValue) {
	// Create span for the query
	spanCtx, span := ti.StartSpan(ctx, "db.query", attrs...)
	defer span.End()

	// Add query-specific attributes
	if !ti.config.DisableQuerySanitization {
		span.SetAttributes(attribute.String("db.statement", ti.sanitizeQuery(query)))
	} else {
		span.SetAttributes(attribute.String("db.statement", query))
	}
	span.SetAttributes(attribute.Float64("db.duration_ms", float64(duration.Nanoseconds())/1000000))

	// Record error if present
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		ti.errorCount.Add(spanCtx, 1, metric.WithAttributes(attrs...))
	} else {
		span.SetStatus(codes.Ok, "")
	}

	// Record metrics
	ti.queryDuration.Record(spanCtx, float64(duration.Nanoseconds())/1000000,
		metric.WithAttributes(attrs...))
}

// RecordTransaction records a database transaction with tracing and metrics
func (ti *TracingInstrumentation) RecordTransaction(ctx context.Context, operationType string, duration time.Duration, err error, attrs ...attribute.KeyValue) {
	// Create span for the transaction
	spanCtx, span := ti.StartSpan(ctx, "db.transaction", attrs...)
	defer span.End()

	// Add transaction-specific attributes
	span.SetAttributes(
		attribute.String("db.transaction.type", operationType),
		attribute.Float64("db.duration_ms", float64(duration.Nanoseconds())/1000000),
	)

	// Record error if present
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		ti.errorCount.Add(spanCtx, 1, metric.WithAttributes(attrs...))
	} else {
		span.SetStatus(codes.Ok, "")
		ti.transactionCount.Add(spanCtx, 1, metric.WithAttributes(attrs...))
	}
}

// RecordConnection records connection metrics
func (ti *TracingInstrumentation) RecordConnection(ctx context.Context, activeConnections int64, attrs ...attribute.KeyValue) {
	ti.connectionCount.Add(ctx, activeConnections, metric.WithAttributes(attrs...))
}

// PropagateContext extracts trace context from incoming requests/operations
func (ti *TracingInstrumentation) PropagateContext(ctx context.Context, carrier propagation.TextMapCarrier) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// InjectContext injects trace context into outgoing requests/operations
func (ti *TracingInstrumentation) InjectContext(ctx context.Context, carrier propagation.TextMapCarrier) {
	otel.GetTextMapPropagator().Inject(ctx, carrier)
}

// Shutdown gracefully shuts down the tracing instrumentation
func (ti *TracingInstrumentation) Shutdown(ctx context.Context) error {
	if ti.provider != nil {
		return ti.provider.Shutdown(ctx)
	}
	return nil
}

// GetTracer returns the OpenTelemetry tracer
func (ti *TracingInstrumentation) GetTracer() trace.Tracer {
	return ti.tracer
}

// GetMeter returns the OpenTelemetry meter
func (ti *TracingInstrumentation) GetMeter() metric.Meter {
	return ti.meter
}

// sanitizeQuery removes sensitive data from SQL queries for tracing
func (ti *TracingInstrumentation) sanitizeQuery(query string) string {
	// Truncate if too long
	if len(query) > ti.config.MaxQueryLength {
		return query[:ti.config.MaxQueryLength] + "... [truncated]"
	}

	// Basic sanitization - replace values in common patterns
	sanitized := strings.ReplaceAll(query, "'", "?")
	sanitized = strings.ReplaceAll(sanitized, "\"", "?")

	return sanitized
}

// TracedDB wraps sql.DB with OpenTelemetry tracing
type TracedDB struct {
	*sql.DB
	tracer *TracingInstrumentation
	driver string
}

// QueryContext executes a query with tracing
func (db *TracedDB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	start := time.Now()

	ctx, span := db.tracer.StartSpan(ctx, "db.query",
		DatabaseTypeFromDriver(db.driver),
		attribute.String("db.operation.name", "query"),
	)
	defer span.End()

	rows, err := db.DB.QueryContext(ctx, query, args...)
	duration := time.Since(start)

	db.tracer.RecordQuery(ctx, query, duration, err,
		DatabaseTypeFromDriver(db.driver),
		attribute.String("db.operation.name", "query"),
	)

	return rows, err
}

// ExecContext executes a query with tracing
func (db *TracedDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	start := time.Now()

	ctx, span := db.tracer.StartSpan(ctx, "db.exec",
		DatabaseTypeFromDriver(db.driver),
		attribute.String("db.operation.name", "exec"),
	)
	defer span.End()

	result, err := db.DB.ExecContext(ctx, query, args...)
	duration := time.Since(start)

	// Add rows affected if available
	attrs := []attribute.KeyValue{
		DatabaseTypeFromDriver(db.driver),
		attribute.String("db.operation.name", "exec"),
	}
	if result != nil {
		if affected, affectedErr := result.RowsAffected(); affectedErr == nil {
			attrs = append(attrs, attribute.Int64("db.rows_affected", affected))
		}
	}

	db.tracer.RecordQuery(ctx, query, duration, err, attrs...)

	return result, err
}

// BeginTx starts a transaction with tracing
func (db *TracedDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*TracedTx, error) {
	start := time.Now()

	ctx, span := db.tracer.StartSpan(ctx, "db.begin",
		DatabaseTypeFromDriver(db.driver),
		attribute.String("db.operation.name", "begin"),
	)
	defer span.End()

	tx, err := db.DB.BeginTx(ctx, opts)
	duration := time.Since(start)

	db.tracer.RecordTransaction(ctx, "begin", duration, err,
		DatabaseTypeFromDriver(db.driver),
	)

	if err != nil {
		return nil, err
	}

	return &TracedTx{
		Tx:     tx,
		tracer: db.tracer,
		driver: db.driver,
		ctx:    ctx,
	}, nil
}

// TracedTx wraps sql.Tx with OpenTelemetry tracing
type TracedTx struct {
	*sql.Tx
	tracer *TracingInstrumentation
	driver string
	ctx    context.Context
}

// Commit commits the transaction with tracing
func (tx *TracedTx) Commit() error {
	start := time.Now()

	ctx, span := tx.tracer.StartSpan(tx.ctx, "db.commit",
		DatabaseTypeFromDriver(tx.driver),
		attribute.String("db.operation.name", "commit"),
	)
	defer span.End()

	err := tx.Tx.Commit()
	duration := time.Since(start)

	tx.tracer.RecordTransaction(ctx, "commit", duration, err,
		DatabaseTypeFromDriver(tx.driver),
	)

	return err
}

// Rollback rolls back the transaction with tracing
func (tx *TracedTx) Rollback() error {
	start := time.Now()

	ctx, span := tx.tracer.StartSpan(tx.ctx, "db.rollback",
		DatabaseTypeFromDriver(tx.driver),
		attribute.String("db.operation.name", "rollback"),
	)
	defer span.End()

	err := tx.Tx.Rollback()
	duration := time.Since(start)

	tx.tracer.RecordTransaction(ctx, "rollback", duration, err,
		DatabaseTypeFromDriver(tx.driver),
	)

	return err
}

// Custom span attributes for GORP operations
var (
	// DatabaseTypeAttribute identifies the database type (postgres, mysql, sqlite, etc.)
	DatabaseTypeAttribute = attribute.Key("db.type")
	// TableNameAttribute identifies the table being operated on
	TableNameAttribute = attribute.Key("db.table")
	// RowsAffectedAttribute records the number of affected rows
	RowsAffectedAttribute = attribute.Key("db.rows_affected")
	// ConnectionPoolAttribute identifies the connection pool
	ConnectionPoolAttribute = attribute.Key("db.connection_pool")
	// CacheHitAttribute indicates if a query was served from cache
	CacheHitAttribute = attribute.Key("db.cache_hit")
	// BatchSizeAttribute records the size of batch operations
	BatchSizeAttribute = attribute.Key("db.batch_size")
)

// DatabaseTypeFromDriver maps driver names to database types
func DatabaseTypeFromDriver(driverName string) attribute.KeyValue {
	switch driverName {
	case "postgres", "pgx":
		return semconv.DBSystemPostgreSQL
	case "mysql":
		return semconv.DBSystemMySQL
	case "sqlite3", "sqlite":
		return semconv.DBSystemSqlite
	case "mssql", "sqlserver":
		return semconv.DBSystemMSSQL
	default:
		return semconv.DBSystemKey.String("other")
	}
}

// SpanFromContext extracts the current span from context
func SpanFromContext(ctx context.Context) trace.Span {
	return trace.SpanFromContext(ctx)
}

// ContextWithSpan returns a context with the given span
func ContextWithSpan(ctx context.Context, span trace.Span) context.Context {
	return trace.ContextWithSpan(ctx, span)
}

// InstrumentationMiddleware provides middleware for HTTP services to propagate trace context
type InstrumentationMiddleware struct {
	tracer *TracingInstrumentation
}

// NewInstrumentationMiddleware creates middleware for trace context propagation
func NewInstrumentationMiddleware(tracer *TracingInstrumentation) *InstrumentationMiddleware {
	return &InstrumentationMiddleware{tracer: tracer}
}

// HTTPHeaderCarrier implements propagation.TextMapCarrier for HTTP headers
type HTTPHeaderCarrier map[string]string

// Get returns the value for a key
func (c HTTPHeaderCarrier) Get(key string) string {
	return c[key]
}

// Set stores a key-value pair
func (c HTTPHeaderCarrier) Set(key, value string) {
	c[key] = value
}

// Keys returns all keys
func (c HTTPHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// DistributedTransactionManager manages trace context across distributed transactions
type DistributedTransactionManager struct {
	tracer *TracingInstrumentation
}

// NewDistributedTransactionManager creates a new distributed transaction manager
func NewDistributedTransactionManager(tracer *TracingInstrumentation) *DistributedTransactionManager {
	return &DistributedTransactionManager{tracer: tracer}
}

// BeginDistributedTransaction starts a distributed transaction with proper trace context
func (dtm *DistributedTransactionManager) BeginDistributedTransaction(ctx context.Context, db *TracedDB, opts *sql.TxOptions, transactionID string) (*TracedTx, error) {
	// Create a span for the distributed transaction
	spanCtx, span := dtm.tracer.StartSpan(ctx, "db.distributed_transaction",
		attribute.String("transaction.id", transactionID),
		attribute.String("transaction.type", "distributed"),
		DatabaseTypeFromDriver(db.driver),
	)

	// Begin the actual transaction
	tx, err := db.BeginTx(spanCtx, opts)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.End()
		return nil, err
	}

	// Enhance the TracedTx with distributed transaction metadata
	tx.ctx = trace.ContextWithSpan(tx.ctx, span)

	// Record transaction start metric
	dtm.tracer.transactionCount.Add(spanCtx, 1,
		metric.WithAttributes(
			attribute.String("transaction.type", "distributed"),
			attribute.String("transaction.id", transactionID),
		))

	return tx, nil
}

// PropagateTransactionContext extracts and injects trace context for distributed operations
func (dtm *DistributedTransactionManager) PropagateTransactionContext(ctx context.Context, operation string) (context.Context, map[string]string) {
	// Create carrier for trace context
	carrier := make(HTTPHeaderCarrier)

	// Inject current context into carrier
	dtm.tracer.InjectContext(ctx, carrier)

	// Create new span for the propagated operation
	newCtx, span := dtm.tracer.StartSpan(ctx, operation,
		attribute.String("operation.type", "distributed"),
		attribute.String("operation.propagated", "true"),
	)
	defer span.End()

	return newCtx, map[string]string(carrier)
}

// ReceiveTransactionContext receives trace context from a distributed operation
func (dtm *DistributedTransactionManager) ReceiveTransactionContext(ctx context.Context, headers map[string]string) context.Context {
	// Create carrier from headers
	carrier := HTTPHeaderCarrier(headers)

	// Extract context from carrier
	return dtm.tracer.PropagateContext(ctx, carrier)
}

// Advanced instrumentation hooks for business logic tracing
type InstrumentationHooks struct {
	tracer *TracingInstrumentation
}

// NewInstrumentationHooks creates new instrumentation hooks
func NewInstrumentationHooks(tracer *TracingInstrumentation) *InstrumentationHooks {
	return &InstrumentationHooks{tracer: tracer}
}

// TraceBusinessOperation creates a span for business logic operations
func (ih *InstrumentationHooks) TraceBusinessOperation(ctx context.Context, operationName string, attrs ...attribute.KeyValue) (context.Context, func()) {
	// Add business operation attributes
	businessAttrs := []attribute.KeyValue{
		attribute.String("operation.layer", "business"),
		attribute.String("operation.name", operationName),
	}
	businessAttrs = append(businessAttrs, attrs...)

	spanCtx, span := ih.tracer.StartSpan(ctx, "business."+operationName, businessAttrs...)

	// Return context and cleanup function
	return spanCtx, func() {
		span.End()
	}
}

// TraceDataAccess creates spans for data access layer operations
func (ih *InstrumentationHooks) TraceDataAccess(ctx context.Context, entity string, operation string, attrs ...attribute.KeyValue) (context.Context, func(error)) {
	// Add data access attributes
	dataAttrs := []attribute.KeyValue{
		attribute.String("operation.layer", "data"),
		attribute.String("data.entity", entity),
		attribute.String("data.operation", operation),
	}
	dataAttrs = append(dataAttrs, attrs...)

	spanCtx, span := ih.tracer.StartSpan(ctx, "data."+entity+"."+operation, dataAttrs...)

	// Return context and cleanup function with error handling
	return spanCtx, func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Ok, "")
		}
		span.End()
	}
}

// TraceCacheOperation creates spans for cache operations
func (ih *InstrumentationHooks) TraceCacheOperation(ctx context.Context, cacheType string, operation string, key string, hit bool) (context.Context, func()) {
	attrs := []attribute.KeyValue{
		attribute.String("operation.layer", "cache"),
		attribute.String("cache.type", cacheType),
		attribute.String("cache.operation", operation),
		attribute.String("cache.key", key),
		attribute.Bool("cache.hit", hit),
	}

	spanCtx, span := ih.tracer.StartSpan(ctx, "cache."+operation, attrs...)

	return spanCtx, func() {
		span.End()
	}
}

// Resource detection and service metadata injection
type ResourceDetector struct {
	serviceName    string
	serviceVersion string
	environment    string
}

// NewResourceDetector creates a new resource detector
func NewResourceDetector(serviceName, serviceVersion, environment string) *ResourceDetector {
	return &ResourceDetector{
		serviceName:    serviceName,
		serviceVersion: serviceVersion,
		environment:    environment,
	}
}

// DetectResource detects and returns resource attributes for the service
func (rd *ResourceDetector) DetectResource(ctx context.Context) (*resource.Resource, error) {
	// Base service attributes
	attrs := []attribute.KeyValue{
		semconv.ServiceName(rd.serviceName),
		semconv.ServiceVersion(rd.serviceVersion),
		semconv.DeploymentEnvironment(rd.environment),
	}

	// Add GORP-specific attributes
	attrs = append(attrs,
		attribute.String("library.name", "gorp"),
		attribute.String("library.version", "v3.0.0"),
		attribute.String("instrumentation.provider", "gorp-instrumentation"),
	)

	// Try to detect additional environment attributes
	if hostname, err := detectHostname(); err == nil {
		attrs = append(attrs, semconv.HostName(hostname))
	}

	if pid := detectProcessID(); pid > 0 {
		attrs = append(attrs, semconv.ProcessPID(pid))
	}

	return resource.New(ctx, resource.WithAttributes(attrs...))
}

// detectHostname detects the hostname
func detectHostname() (string, error) {
	return os.Hostname()
}

// detectProcessID detects the process ID
func detectProcessID() int {
	return os.Getpid()
}

// Enhanced sampling configuration
type SamplingConfig struct {
	// BaseSamplingRatio is the default sampling ratio
	BaseSamplingRatio float64
	// HighThroughputRatio is used when request rate is high
	HighThroughputRatio float64
	// ErrorSamplingRatio is used for error traces (usually 1.0)
	ErrorSamplingRatio float64
	// RequestRateThreshold defines when to switch to high throughput sampling
	RequestRateThreshold float64
}

// AdaptiveSampler provides adaptive sampling based on request rate
type AdaptiveSampler struct {
	config SamplingConfig
	// Add rate tracking here if needed
}

// NewAdaptiveSampler creates a new adaptive sampler
func NewAdaptiveSampler(config SamplingConfig) *AdaptiveSampler {
	return &AdaptiveSampler{config: config}
}

// GetSamplingRatio returns the appropriate sampling ratio based on current conditions
func (as *AdaptiveSampler) GetSamplingRatio(isError bool, requestRate float64) float64 {
	if isError {
		return as.config.ErrorSamplingRatio
	}

	if requestRate > as.config.RequestRateThreshold {
		return as.config.HighThroughputRatio
	}

	return as.config.BaseSamplingRatio
}