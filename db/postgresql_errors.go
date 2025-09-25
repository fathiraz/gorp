package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// PostgreSQLErrorHandler provides enhanced error handling for PostgreSQL operations
type PostgreSQLErrorHandler struct {
	retryConfig *RetryConfig
}

// RetryConfig holds retry configuration for database operations
type RetryConfig struct {
	MaxRetries      int
	InitialDelay    time.Duration
	MaxDelay        time.Duration
	BackoffFactor   float64
	RetryableErrors []string
}

// DefaultRetryConfig returns sensible defaults for retry configuration
func DefaultRetryConfig() *RetryConfig {
	return &RetryConfig{
		MaxRetries:    3,
		InitialDelay:  100 * time.Millisecond,
		MaxDelay:      5 * time.Second,
		BackoffFactor: 2.0,
		RetryableErrors: []string{
			"53300", // too_many_connections
			"40001", // serialization_failure
			"40P01", // deadlock_detected
			"25006", // read_only_sql_transaction
			"08000", // connection_exception
			"08003", // connection_does_not_exist
			"08006", // connection_failure
			"57P03", // cannot_connect_now
		},
	}
}

// NewPostgreSQLErrorHandler creates a new error handler
func NewPostgreSQLErrorHandler(config *RetryConfig) *PostgreSQLErrorHandler {
	if config == nil {
		config = DefaultRetryConfig()
	}
	return &PostgreSQLErrorHandler{retryConfig: config}
}

// PostgreSQLError represents an enhanced PostgreSQL error
type PostgreSQLError struct {
	*pgconn.PgError
	Operation   string
	Context     string
	Timestamp   time.Time
	Retryable   bool
	Severity    ErrorSeverity
	Category    ErrorCategory
}

// ErrorSeverity represents the severity level of an error
type ErrorSeverity int

const (
	ErrorSeverityInfo ErrorSeverity = iota
	ErrorSeverityWarning
	ErrorSeverityError
	ErrorSeverityCritical
	ErrorSeverityFatal
)

// ErrorCategory represents the category of an error
type ErrorCategory int

const (
	ErrorCategoryConnection ErrorCategory = iota
	ErrorCategorySyntax
	ErrorCategoryConstraint
	ErrorCategoryPermission
	ErrorCategoryResource
	ErrorCategoryTransaction
	ErrorCategoryData
	ErrorCategoryUnknown
)

// Error implements the error interface
func (pe *PostgreSQLError) Error() string {
	return fmt.Sprintf("[%s] %s: %s (SQLSTATE: %s, Operation: %s)",
		pe.Severity.String(), pe.Category.String(), pe.Message, pe.Code, pe.Operation)
}

// String returns string representation of ErrorSeverity
func (es ErrorSeverity) String() string {
	switch es {
	case ErrorSeverityInfo:
		return "INFO"
	case ErrorSeverityWarning:
		return "WARNING"
	case ErrorSeverityError:
		return "ERROR"
	case ErrorSeverityCritical:
		return "CRITICAL"
	case ErrorSeverityFatal:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

// String returns string representation of ErrorCategory
func (ec ErrorCategory) String() string {
	switch ec {
	case ErrorCategoryConnection:
		return "CONNECTION"
	case ErrorCategorySyntax:
		return "SYNTAX"
	case ErrorCategoryConstraint:
		return "CONSTRAINT"
	case ErrorCategoryPermission:
		return "PERMISSION"
	case ErrorCategoryResource:
		return "RESOURCE"
	case ErrorCategoryTransaction:
		return "TRANSACTION"
	case ErrorCategoryData:
		return "DATA"
	default:
		return "UNKNOWN"
	}
}

// WrapError wraps a PostgreSQL error with additional context
func (eh *PostgreSQLErrorHandler) WrapError(err error, operation, context string) error {
	if err == nil {
		return nil
	}

	// Handle pgx-specific errors
	var pgError *pgconn.PgError
	if errors.As(err, &pgError) {
		return &PostgreSQLError{
			PgError:   pgError,
			Operation: operation,
			Context:   context,
			Timestamp: time.Now(),
			Retryable: eh.isRetryable(pgError.Code),
			Severity:  eh.getSeverity(pgError.Code),
			Category:  eh.getCategory(pgError.Code),
		}
	}

	// Handle other pgx errors
	if errors.Is(err, pgx.ErrNoRows) {
		return &PostgreSQLError{
			PgError: &pgconn.PgError{
				Code:    "02000",
				Message: "no data found",
			},
			Operation: operation,
			Context:   context,
			Timestamp: time.Now(),
			Retryable: false,
			Severity:  ErrorSeverityInfo,
			Category:  ErrorCategoryData,
		}
	}

	// Handle context errors
	if errors.Is(err, context.DeadlineExceeded) {
		return &PostgreSQLError{
			PgError: &pgconn.PgError{
				Code:    "57014",
				Message: "query timeout",
			},
			Operation: operation,
			Context:   context,
			Timestamp: time.Now(),
			Retryable: true,
			Severity:  ErrorSeverityWarning,
			Category:  ErrorCategoryResource,
		}
	}

	if errors.Is(err, context.Canceled) {
		return &PostgreSQLError{
			PgError: &pgconn.PgError{
				Code:    "57014",
				Message: "query canceled",
			},
			Operation: operation,
			Context:   context,
			Timestamp: time.Now(),
			Retryable: false,
			Severity:  ErrorSeverityInfo,
			Category:  ErrorCategoryResource,
		}
	}

	// Return original error if not a PostgreSQL error
	return err
}

// isRetryable determines if an error is retryable based on SQLSTATE
func (eh *PostgreSQLErrorHandler) isRetryable(sqlstate string) bool {
	for _, retryableCode := range eh.retryConfig.RetryableErrors {
		if sqlstate == retryableCode {
			return true
		}
	}

	// Check for retryable error classes
	switch {
	case strings.HasPrefix(sqlstate, "08"): // Connection Exception
		return true
	case strings.HasPrefix(sqlstate, "53"): // Insufficient Resources
		return true
	case strings.HasPrefix(sqlstate, "57"): // Operator Intervention
		return sqlstate == "57P03" // cannot_connect_now
	case strings.HasPrefix(sqlstate, "40"): // Transaction Rollback
		return true
	}

	return false
}

// getSeverity determines error severity based on SQLSTATE
func (eh *PostgreSQLErrorHandler) getSeverity(sqlstate string) ErrorSeverity {
	switch {
	case strings.HasPrefix(sqlstate, "01"): // Warning
		return ErrorSeverityWarning
	case strings.HasPrefix(sqlstate, "02"): // No Data
		return ErrorSeverityInfo
	case strings.HasPrefix(sqlstate, "08"): // Connection Exception
		return ErrorSeverityCritical
	case strings.HasPrefix(sqlstate, "22"): // Data Exception
		return ErrorSeverityError
	case strings.HasPrefix(sqlstate, "23"): // Integrity Constraint Violation
		return ErrorSeverityError
	case strings.HasPrefix(sqlstate, "24"): // Invalid Cursor State
		return ErrorSeverityError
	case strings.HasPrefix(sqlstate, "25"): // Invalid Transaction State
		return ErrorSeverityError
	case strings.HasPrefix(sqlstate, "28"): // Invalid Authorization Specification
		return ErrorSeverityCritical
	case strings.HasPrefix(sqlstate, "42"): // Syntax Error or Access Rule Violation
		return ErrorSeverityError
	case strings.HasPrefix(sqlstate, "53"): // Insufficient Resources
		return ErrorSeverityCritical
	case strings.HasPrefix(sqlstate, "57"): // Operator Intervention
		return ErrorSeverityWarning
	case strings.HasPrefix(sqlstate, "58"): // System Error
		return ErrorSeverityFatal
	default:
		return ErrorSeverityError
	}
}

// getCategory determines error category based on SQLSTATE
func (eh *PostgreSQLErrorHandler) getCategory(sqlstate string) ErrorCategory {
	switch {
	case strings.HasPrefix(sqlstate, "08"): // Connection Exception
		return ErrorCategoryConnection
	case strings.HasPrefix(sqlstate, "22"): // Data Exception
		return ErrorCategoryData
	case strings.HasPrefix(sqlstate, "23"): // Integrity Constraint Violation
		return ErrorCategoryConstraint
	case strings.HasPrefix(sqlstate, "28"): // Invalid Authorization Specification
		return ErrorCategoryPermission
	case strings.HasPrefix(sqlstate, "40"): // Transaction Rollback
		return ErrorCategoryTransaction
	case strings.HasPrefix(sqlstate, "42"): // Syntax Error or Access Rule Violation
		return ErrorCategorySyntax
	case strings.HasPrefix(sqlstate, "53"): // Insufficient Resources
		return ErrorCategoryResource
	case strings.HasPrefix(sqlstate, "57"): // Operator Intervention
		return ErrorCategoryResource
	case strings.HasPrefix(sqlstate, "58"): // System Error
		return ErrorCategoryResource
	default:
		return ErrorCategoryUnknown
	}
}

// RetryOperation executes an operation with exponential backoff retry
func (eh *PostgreSQLErrorHandler) RetryOperation(ctx context.Context, operation func() error) error {
	var lastError error
	delay := eh.retryConfig.InitialDelay

	for attempt := 0; attempt <= eh.retryConfig.MaxRetries; attempt++ {
		if attempt > 0 {
			// Check if context is still valid before retry
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}

		lastError = operation()
		if lastError == nil {
			return nil
		}

		// Check if error is retryable
		var pgError *PostgreSQLError
		if errors.As(lastError, &pgError) && !pgError.Retryable {
			return lastError
		}

		if attempt < eh.retryConfig.MaxRetries {
			// Calculate next delay with exponential backoff
			delay = time.Duration(float64(delay) * eh.retryConfig.BackoffFactor)
			if delay > eh.retryConfig.MaxDelay {
				delay = eh.retryConfig.MaxDelay
			}
		}
	}

	return fmt.Errorf("operation failed after %d attempts: %w", eh.retryConfig.MaxRetries+1, lastError)
}

// IsDeadlock checks if the error is a deadlock
func IsDeadlock(err error) bool {
	var pgError *pgconn.PgError
	if errors.As(err, &pgError) {
		return pgError.Code == "40P01"
	}
	return false
}

// IsSerializationFailure checks if the error is a serialization failure
func IsSerializationFailure(err error) bool {
	var pgError *pgconn.PgError
	if errors.As(err, &pgError) {
		return pgError.Code == "40001"
	}
	return false
}

// IsUniqueViolation checks if the error is a unique constraint violation
func IsUniqueViolation(err error) bool {
	var pgError *pgconn.PgError
	if errors.As(err, &pgError) {
		return pgError.Code == "23505"
	}
	return false
}

// IsForeignKeyViolation checks if the error is a foreign key constraint violation
func IsForeignKeyViolation(err error) bool {
	var pgError *pgconn.PgError
	if errors.As(err, &pgError) {
		return pgError.Code == "23503"
	}
	return false
}

// IsConnectionError checks if the error is a connection-related error
func IsConnectionError(err error) bool {
	var pgError *pgconn.PgError
	if errors.As(err, &pgError) {
		return strings.HasPrefix(pgError.Code, "08")
	}
	return false
}

// ParseErrorDetails extracts detailed information from PostgreSQL error
func ParseErrorDetails(err error) map[string]string {
	details := make(map[string]string)

	var pgError *pgconn.PgError
	if errors.As(err, &pgError) {
		details["code"] = pgError.Code
		details["message"] = pgError.Message
		details["detail"] = pgError.Detail
		details["hint"] = pgError.Hint
		details["position"] = pgError.Position
		details["internal_position"] = pgError.InternalPosition
		details["internal_query"] = pgError.InternalQuery
		details["where"] = pgError.Where
		details["schema_name"] = pgError.SchemaName
		details["table_name"] = pgError.TableName
		details["column_name"] = pgError.ColumnName
		details["data_type_name"] = pgError.DataTypeName
		details["constraint_name"] = pgError.ConstraintName
		details["file"] = pgError.File
		details["line"] = pgError.Line
		details["routine"] = pgError.Routine
	}

	return details
}

// PostgreSQLHealthChecker provides comprehensive health checking for PostgreSQL
type PostgreSQLHealthChecker struct {
	pool         *pgxpool.Pool
	errorHandler *PostgreSQLErrorHandler
}

// NewPostgreSQLHealthChecker creates a new health checker
func NewPostgreSQLHealthChecker(pool *pgxpool.Pool) *PostgreSQLHealthChecker {
	return &PostgreSQLHealthChecker{
		pool:         pool,
		errorHandler: NewPostgreSQLErrorHandler(nil),
	}
}

// HealthStatus represents PostgreSQL health check results
type HealthStatus struct {
	Healthy           bool              `json:"healthy"`
	Timestamp         time.Time         `json:"timestamp"`
	ConnectionStats   ConnectionStats   `json:"connection_stats"`
	DatabaseVersion   string            `json:"database_version"`
	ServerUptime      time.Duration     `json:"server_uptime"`
	ActiveConnections int               `json:"active_connections"`
	IdleConnections   int               `json:"idle_connections"`
	Replication       ReplicationStatus `json:"replication"`
	LastError         string            `json:"last_error,omitempty"`
}

// ReplicationStatus represents PostgreSQL replication information
type ReplicationStatus struct {
	IsReplica         bool     `json:"is_replica"`
	ReplicationLag    *int64   `json:"replication_lag,omitempty"`
	ReplicaHosts      []string `json:"replica_hosts,omitempty"`
	LastWALReceive    *time.Time `json:"last_wal_receive,omitempty"`
	LastWALReplay     *time.Time `json:"last_wal_replay,omitempty"`
}

// CheckHealth performs comprehensive PostgreSQL health check
func (hc *PostgreSQLHealthChecker) CheckHealth(ctx context.Context) (*HealthStatus, error) {
	status := &HealthStatus{
		Timestamp: time.Now(),
	}

	// Basic connectivity test
	if err := hc.pool.Ping(ctx); err != nil {
		status.Healthy = false
		status.LastError = hc.errorHandler.WrapError(err, "ping", "health_check").Error()
		return status, err
	}

	// Get connection statistics
	poolStats := hc.pool.Stat()
	status.ConnectionStats = ConnectionStats{
		MaxOpenConnections: int(poolStats.MaxConns()),
		OpenConnections:    int(poolStats.TotalConns()),
		InUse:              int(poolStats.AcquiredConns()),
		Idle:               int(poolStats.IdleConns()),
	}
	status.ActiveConnections = status.ConnectionStats.InUse
	status.IdleConnections = status.ConnectionStats.Idle

	// Get database version
	var version string
	err := hc.pool.QueryRow(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		status.LastError = hc.errorHandler.WrapError(err, "version_check", "health_check").Error()
	} else {
		status.DatabaseVersion = version
	}

	// Get server uptime
	var uptimeSeconds int64
	err = hc.pool.QueryRow(ctx, "SELECT EXTRACT(EPOCH FROM (now() - pg_postmaster_start_time()))").Scan(&uptimeSeconds)
	if err == nil {
		status.ServerUptime = time.Duration(uptimeSeconds) * time.Second
	}

	// Check replication status
	status.Replication = hc.checkReplicationStatus(ctx)

	status.Healthy = status.LastError == ""
	return status, nil
}

// checkReplicationStatus checks PostgreSQL replication information
func (hc *PostgreSQLHealthChecker) checkReplicationStatus(ctx context.Context) ReplicationStatus {
	repl := ReplicationStatus{}

	// Check if this is a replica
	var isReplica bool
	err := hc.pool.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&isReplica)
	if err == nil {
		repl.IsReplica = isReplica
	}

	if isReplica {
		// Get replication lag for replica
		var lagBytes *int64
		err = hc.pool.QueryRow(ctx, `
			SELECT EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))::bigint
		`).Scan(&lagBytes)
		if err == nil {
			repl.ReplicationLag = lagBytes
		}

		// Get last WAL receive and replay times
		var lastReceive, lastReplay *time.Time
		err = hc.pool.QueryRow(ctx, `
			SELECT pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn()
		`).Scan(&lastReceive, &lastReplay)
		if err == nil {
			repl.LastWALReceive = lastReceive
			repl.LastWALReplay = lastReplay
		}
	} else {
		// Get replica information for primary
		rows, err := hc.pool.Query(ctx, `
			SELECT client_addr FROM pg_stat_replication WHERE state = 'streaming'
		`)
		if err == nil {
			defer rows.Close()

			var replicas []string
			for rows.Next() {
				var addr string
				if rows.Scan(&addr) == nil {
					replicas = append(replicas, addr)
				}
			}
			repl.ReplicaHosts = replicas
		}
	}

	return repl
}