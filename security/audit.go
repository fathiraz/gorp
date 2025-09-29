package security

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// AuditLogger provides comprehensive audit logging
type AuditLogger struct {
	config       *AuditConfig
	writer       io.Writer
	file         *os.File
	tracer       trace.Tracer
	mu           sync.RWMutex
	eventBuffer  []AuditEvent
	bufferSize   int
	flushTicker  *time.Ticker
	stopChan     chan struct{}
}

// AuditEvent represents a security audit event
type AuditEvent struct {
	ID           string                 `json:"id"`
	Timestamp    time.Time             `json:"timestamp"`
	Type         AuditEventType        `json:"type"`
	Severity     SeverityLevel         `json:"severity"`
	UserID       string                `json:"user_id,omitempty"`
	SessionID    string                `json:"session_id,omitempty"`
	IPAddress    string                `json:"ip_address,omitempty"`
	UserAgent    string                `json:"user_agent,omitempty"`
	Operation    string                `json:"operation"`
	Resource     string                `json:"resource,omitempty"`
	Query        string                `json:"query,omitempty"`
	Parameters   map[string]interface{} `json:"parameters,omitempty"`
	Result       AuditResult           `json:"result"`
	Error        string                `json:"error,omitempty"`
	Duration     time.Duration         `json:"duration,omitempty"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
	TraceID      string                `json:"trace_id,omitempty"`
	SpanID       string                `json:"span_id,omitempty"`
}

// AuditEventType defines types of audit events
type AuditEventType string

const (
	AuthenticationEvent    AuditEventType = "authentication"
	AuthorizationEvent     AuditEventType = "authorization"
	DatabaseQueryEvent     AuditEventType = "database_query"
	DataAccessEvent        AuditEventType = "data_access"
	DataModificationEvent  AuditEventType = "data_modification"
	SecurityViolationEvent AuditEventType = "security_violation"
	ConfigurationEvent     AuditEventType = "configuration"
	SystemEvent           AuditEventType = "system"
	ErrorEvent            AuditEventType = "error"
)

// AuditResult defines the result of an audited operation
type AuditResult string

const (
	AuditSuccess  AuditResult = "success"
	AuditFailure  AuditResult = "failure"
	AuditBlocked  AuditResult = "blocked"
	AuditWarning  AuditResult = "warning"
)

// SecurityEventSQLInjectionAttempt is a security event type
const SecurityEventSQLInjectionAttempt = "sql_injection_attempt"

// NewAuditLogger creates a new audit logger
func NewAuditLogger(config *AuditConfig) *AuditLogger {
	if config == nil {
		config = DefaultAuditConfig()
	}

	logger := &AuditLogger{
		config:      config,
		bufferSize:  1000, // Default buffer size
		eventBuffer: make([]AuditEvent, 0, 1000),
		stopChan:    make(chan struct{}),
		tracer:      otel.Tracer("gorp.security.audit"),
	}

	if err := logger.initialize(); err != nil {
		fmt.Printf("Warning: Failed to initialize audit logger: %v\n", err)
	}

	return logger
}

// initialize sets up the audit logger
func (al *AuditLogger) initialize() error {
	if !al.config.Enabled {
		al.writer = io.Discard
		return nil
	}

	// Create log directory if it doesn't exist
	logDir := filepath.Dir("audit.log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}

	// Open log file
	file, err := os.OpenFile("audit.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open audit log file: %w", err)
	}

	al.file = file
	al.writer = file

	// Start buffer flusher
	if al.config.LogRotation {
		al.flushTicker = time.NewTicker(time.Minute) // Flush every minute
		go al.flushRoutine()
	}

	return nil
}

// flushRoutine periodically flushes the event buffer
func (al *AuditLogger) flushRoutine() {
	for {
		select {
		case <-al.flushTicker.C:
			al.flushBuffer()
		case <-al.stopChan:
			al.flushBuffer()
			return
		}
	}
}

// LogOperation logs a database operation
func (al *AuditLogger) LogOperation(ctx context.Context, operation string, entity interface{}, result interface{}) {
	if !al.config.Enabled {
		return
	}

	event := AuditEvent{
		ID:        al.generateEventID(),
		Timestamp: time.Now(),
		Type:      DatabaseQueryEvent,
		Severity:  InfoSeverity,
		Operation: operation,
		Result:    AuditSuccess,
		Metadata: map[string]interface{}{
			"entity_type": fmt.Sprintf("%T", entity),
		},
	}

	// Extract context information
	al.extractContextInfo(ctx, &event)

	// Add result information if not sensitive
	if result != nil && !al.config.LogSensitiveData {
		event.Metadata["result_type"] = fmt.Sprintf("%T", result)
	}

	al.logEvent(ctx, event)
}

// LogSecurityEvent logs a security-related event
func (al *AuditLogger) LogSecurityEvent(ctx context.Context, eventType string, query string, err error) {
	if !al.config.Enabled {
		return
	}

	event := AuditEvent{
		ID:        al.generateEventID(),
		Timestamp: time.Now(),
		Type:      SecurityViolationEvent,
		Severity:  CriticalSeverity,
		Operation: eventType,
		Query:     al.sanitizeQuery(query),
		Result:    AuditBlocked,
		Error:     err.Error(),
	}

	al.extractContextInfo(ctx, &event)
	al.logEvent(ctx, event)
}

// LogSecurityViolations logs multiple security violations
func (al *AuditLogger) LogSecurityViolations(ctx context.Context, violations []SecurityViolation) {
	if !al.config.Enabled {
		return
	}

	for _, violation := range violations {
		event := AuditEvent{
			ID:        al.generateEventID(),
			Timestamp: time.Now(),
			Type:      SecurityViolationEvent,
			Severity:  al.mapSeverity(violation.Severity),
			Operation: string(violation.Type),
			Result:    AuditWarning,
			Metadata: map[string]interface{}{
				"violation_type": violation.Type,
				"pattern":        violation.Pattern,
				"description":    violation.Description,
				"suggestion":     violation.Suggestion,
			},
		}

		al.extractContextInfo(ctx, &event)
		al.logEvent(ctx, event)
	}
}

// LogQuery logs a database query execution
func (al *AuditLogger) LogQuery(ctx context.Context, query string, args []interface{}, duration time.Duration, err error) {
	if !al.config.Enabled || !al.config.LogQueries {
		return
	}

	event := AuditEvent{
		ID:        al.generateEventID(),
		Timestamp: time.Now(),
		Type:      DatabaseQueryEvent,
		Severity:  InfoSeverity,
		Operation: "query_execution",
		Query:     al.sanitizeQuery(query),
		Duration:  duration,
		Result:    AuditSuccess,
	}

	if err != nil {
		event.Result = AuditFailure
		event.Error = err.Error()
		event.Severity = MediumSeverity
	}

	// Add sanitized parameters
	if len(args) > 0 && al.config.LogSensitiveData {
		sanitizer := NewParameterSanitizer()
		paramMap := make(map[string]interface{})
		for i, arg := range args {
			paramMap[fmt.Sprintf("param_%d", i)] = arg
		}
		event.Parameters = sanitizer.SanitizeParameters(paramMap)
	}

	al.extractContextInfo(ctx, &event)
	al.logEvent(ctx, event)
}

// LogConnection logs database connection events
func (al *AuditLogger) LogConnection(ctx context.Context, operation string, result AuditResult, metadata map[string]interface{}) {
	if !al.config.Enabled || !al.config.LogConnections {
		return
	}

	event := AuditEvent{
		ID:        al.generateEventID(),
		Timestamp: time.Now(),
		Type:      DatabaseQueryEvent,
		Severity:  InfoSeverity,
		Operation: operation,
		Result:    result,
		Metadata:  metadata,
	}

	al.extractContextInfo(ctx, &event)
	al.logEvent(ctx, event)
}

// LogTransaction logs transaction events
func (al *AuditLogger) LogTransaction(ctx context.Context, operation string, transactionID string, result AuditResult) {
	if !al.config.Enabled || !al.config.LogTransactions {
		return
	}

	event := AuditEvent{
		ID:        al.generateEventID(),
		Timestamp: time.Now(),
		Type:      DatabaseQueryEvent,
		Severity:  InfoSeverity,
		Operation: operation,
		Result:    result,
		Metadata: map[string]interface{}{
			"transaction_id": transactionID,
		},
	}

	al.extractContextInfo(ctx, &event)
	al.logEvent(ctx, event)
}

// LogAuthentication logs authentication events
func (al *AuditLogger) LogAuthentication(ctx context.Context, userID string, result AuditResult, metadata map[string]interface{}) {
	if !al.config.Enabled {
		return
	}

	event := AuditEvent{
		ID:        al.generateEventID(),
		Timestamp: time.Now(),
		Type:      AuthenticationEvent,
		Severity:  InfoSeverity,
		UserID:    userID,
		Operation: "authentication",
		Result:    result,
		Metadata:  metadata,
	}

	if result == AuditFailure {
		event.Severity = MediumSeverity
	}

	al.extractContextInfo(ctx, &event)
	al.logEvent(ctx, event)
}

// LogAuthorization logs authorization events
func (al *AuditLogger) LogAuthorization(ctx context.Context, userID string, resource string, operation string, result AuditResult) {
	if !al.config.Enabled {
		return
	}

	event := AuditEvent{
		ID:        al.generateEventID(),
		Timestamp: time.Now(),
		Type:      AuthorizationEvent,
		Severity:  InfoSeverity,
		UserID:    userID,
		Operation: operation,
		Resource:  resource,
		Result:    result,
	}

	if result == AuditFailure || result == AuditBlocked {
		event.Severity = MediumSeverity
	}

	al.extractContextInfo(ctx, &event)
	al.logEvent(ctx, event)
}

// logEvent writes an audit event
func (al *AuditLogger) logEvent(ctx context.Context, event AuditEvent) {
	// Start tracing span
	ctx, span := al.tracer.Start(ctx, "audit.log_event")
	defer span.End()

	span.SetAttributes(
		attribute.String("audit.event_type", string(event.Type)),
		attribute.String("audit.severity", string(event.Severity)),
		attribute.String("audit.operation", event.Operation),
		attribute.String("audit.result", string(event.Result)),
	)

	// Add to buffer or write directly
	al.mu.Lock()
	defer al.mu.Unlock()

	if len(al.eventBuffer) >= al.bufferSize {
		al.flushBufferUnsafe()
	}

	al.eventBuffer = append(al.eventBuffer, event)

	// Write immediately for high-severity events
	if event.Severity == CriticalSeverity || event.Severity == HighSeverity {
		al.flushBufferUnsafe()
	}

	span.SetStatus(codes.Ok, "Event logged successfully")
}

// flushBuffer writes buffered events to storage
func (al *AuditLogger) flushBuffer() {
	al.mu.Lock()
	defer al.mu.Unlock()
	al.flushBufferUnsafe()
}

// flushBufferUnsafe writes buffered events without locking
func (al *AuditLogger) flushBufferUnsafe() {
	if len(al.eventBuffer) == 0 {
		return
	}

	for _, event := range al.eventBuffer {
		if jsonData, err := json.Marshal(event); err == nil {
			al.writer.Write(jsonData)
			al.writer.Write([]byte("\n"))
		}
	}

	// Clear buffer
	al.eventBuffer = al.eventBuffer[:0]

	// Sync file if available
	if al.file != nil {
		al.file.Sync()
	}
}

// extractContextInfo extracts audit information from context
func (al *AuditLogger) extractContextInfo(ctx context.Context, event *AuditEvent) {
	// Extract security context if available
	if secCtx, ok := ctx.Value("security_context").(*SecurityContext); ok {
		event.UserID = secCtx.UserID
		event.SessionID = secCtx.SessionID
		event.IPAddress = secCtx.IPAddress
		event.UserAgent = secCtx.UserAgent
	}

	// Extract tracing information
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() {
		event.TraceID = span.SpanContext().TraceID().String()
		event.SpanID = span.SpanContext().SpanID().String()
	}
}

// sanitizeQuery sanitizes SQL query for logging
func (al *AuditLogger) sanitizeQuery(query string) string {
	if !al.config.LogSensitiveData {
		sanitizer := NewParameterSanitizer()
		return sanitizer.SanitizeQuery(query)
	}
	return query
}

// mapSeverity maps security severity to audit severity
func (al *AuditLogger) mapSeverity(severity SeverityLevel) SeverityLevel {
	return severity // Direct mapping for now
}

// generateEventID generates a unique event ID
func (al *AuditLogger) generateEventID() string {
	return fmt.Sprintf("audit_%d_%d", time.Now().UnixNano(), al.getSequence())
}

var eventSequence int64 = 0

func (al *AuditLogger) getSequence() int64 {
	// Simple sequence generation - in production use atomic operations
	eventSequence++
	return eventSequence
}

// Close closes the audit logger and flushes remaining events
func (al *AuditLogger) Close() error {
	if al.flushTicker != nil {
		al.flushTicker.Stop()
	}

	if al.stopChan != nil {
		close(al.stopChan)
	}

	al.flushBuffer()

	if al.file != nil {
		return al.file.Close()
	}

	return nil
}

// SetWriter sets a custom writer for audit logs
func (al *AuditLogger) SetWriter(writer io.Writer) {
	al.mu.Lock()
	defer al.mu.Unlock()
	al.writer = writer
}

// GetEventCount returns the number of events in the buffer
func (al *AuditLogger) GetEventCount() int {
	al.mu.RLock()
	defer al.mu.RUnlock()
	return len(al.eventBuffer)
}

// GetConfig returns the current audit configuration
func (al *AuditLogger) GetConfig() *AuditConfig {
	return al.config
}

// SetConfig updates the audit configuration
func (al *AuditLogger) SetConfig(config *AuditConfig) {
	al.mu.Lock()
	defer al.mu.Unlock()
	al.config = config
}