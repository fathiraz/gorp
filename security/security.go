package security

import (
	"context"
	"time"
)

// SecurityConfig holds security configuration
type SecurityConfig struct {
	EnableSQLInjectionProtection bool
	EnableParameterSanitization  bool
	EnableAuditLogging          bool
	EnableSecurityScanning      bool
	TLSConfig                   *TLSConfig
	ValidationRules             []ValidationRule
	AuditConfig                 *AuditConfig
	ScannerConfig              *ScannerConfig
}

// TLSConfig holds TLS configuration for secure connections
type TLSConfig struct {
	Enabled               bool
	CertFile              string
	KeyFile               string
	CAFile                string
	InsecureSkipVerify    bool
	MinVersion            uint16
	MaxVersion            uint16
	CipherSuites          []uint16
	PreferServerCiphers   bool
	ServerName            string
}

// AuditConfig holds audit logging configuration
type AuditConfig struct {
	Enabled           bool
	LogSensitiveData  bool
	LogQueries        bool
	LogConnections    bool
	LogTransactions   bool
	MaxLogSize        int64
	LogRotation       bool
	RotationInterval  time.Duration
	RetentionPeriod   time.Duration
}

// ScannerConfig holds security scanner configuration
type ScannerConfig struct {
	Enabled               bool
	EnableSQLInjectionScan bool
	EnableCredentialScan   bool
	EnablePatternScan      bool
	CustomPatterns        []string
	ScanInterval          time.Duration
	AlertThreshold        int
}

// SecurityManager provides centralized security management
type SecurityManager[T any] struct {
	config           *SecurityConfig
	validator        *Validator[T]
	protector        *SQLInjectionProtector
	sanitizer        *ParameterSanitizer
	auditor          *AuditLogger
	scanner          *SecurityScanner
	connectionSecure *ConnectionSecurity
}

// SecureEntity represents an entity with security constraints
type SecureEntity interface {
	GetSecurityLevel() SecurityLevel
	GetValidationRules() []ValidationRule
	SanitizeForLogging() map[string]interface{}
}

// SecurityLevel defines security access levels
type SecurityLevel int

const (
	PublicLevel SecurityLevel = iota
	InternalLevel
	ConfidentialLevel
	RestrictedLevel
)

// SecurityContext holds security information for operations
type SecurityContext struct {
	UserID        string
	SessionID     string
	IPAddress     string
	UserAgent     string
	Permissions   []string
	SecurityLevel SecurityLevel
	AuditTrail    []AuditEvent
}

// NewSecurityManager creates a new security manager
func NewSecurityManager[T any](config *SecurityConfig) *SecurityManager[T] {
	if config == nil {
		config = DefaultSecurityConfig()
	}

	sm := &SecurityManager[T]{
		config: config,
	}

	if config.EnableSQLInjectionProtection {
		sm.protector = NewSQLInjectionProtector()
	}

	if config.EnableParameterSanitization {
		sm.sanitizer = NewParameterSanitizer()
	}

	if config.EnableAuditLogging {
		sm.auditor = NewAuditLogger(config.AuditConfig)
	}

	if config.EnableSecurityScanning {
		sm.scanner = NewSecurityScanner(config.ScannerConfig)
	}

	sm.validator = NewValidator[T](config.ValidationRules)
	sm.connectionSecure = NewConnectionSecurity(config.TLSConfig)

	return sm
}

// ValidateQuery validates a query for security issues
func (sm *SecurityManager[T]) ValidateQuery(ctx context.Context, query string, args []interface{}) error {
	if sm.protector != nil {
		if err := sm.protector.ValidateQuery(query, args); err != nil {
			if sm.auditor != nil {
				sm.auditor.LogSecurityEvent(ctx, SecurityEventSQLInjectionAttempt, query, err)
			}
			return err
		}
	}

	if sm.scanner != nil {
		if violations := sm.scanner.ScanQuery(query, args); len(violations) > 0 {
			if sm.auditor != nil {
				sm.auditor.LogSecurityViolations(ctx, violations)
			}
			return NewSecurityViolationError(violations)
		}
	}

	return nil
}

// SanitizeParameters sanitizes parameters for logging
func (sm *SecurityManager[T]) SanitizeParameters(params map[string]interface{}) map[string]interface{} {
	if sm.sanitizer != nil {
		return sm.sanitizer.SanitizeParameters(params)
	}
	return params
}

// ValidateEntity validates an entity according to security rules
func (sm *SecurityManager[T]) ValidateEntity(ctx context.Context, entity T) error {
	if sm.validator != nil {
		return sm.validator.Validate(ctx, entity)
	}
	return nil
}

// AuditOperation logs an operation for audit purposes
func (sm *SecurityManager[T]) AuditOperation(ctx context.Context, operation string, entity T, result interface{}) {
	if sm.auditor != nil {
		sm.auditor.LogOperation(ctx, operation, entity, result)
	}
}

// DefaultSecurityConfig returns default security configuration
func DefaultSecurityConfig() *SecurityConfig {
	return &SecurityConfig{
		EnableSQLInjectionProtection: true,
		EnableParameterSanitization:  true,
		EnableAuditLogging:          true,
		EnableSecurityScanning:      true,
		TLSConfig:                   DefaultTLSConfig(),
		ValidationRules:             []ValidationRule{},
		AuditConfig:                 DefaultAuditConfig(),
		ScannerConfig:              DefaultScannerConfig(),
	}
}

// DefaultTLSConfig returns default TLS configuration
func DefaultTLSConfig() *TLSConfig {
	return &TLSConfig{
		Enabled:             true,
		InsecureSkipVerify:  false,
		MinVersion:          0x0303, // TLS 1.2
		PreferServerCiphers: true,
	}
}

// DefaultAuditConfig returns default audit configuration
func DefaultAuditConfig() *AuditConfig {
	return &AuditConfig{
		Enabled:          true,
		LogSensitiveData: false,
		LogQueries:       true,
		LogConnections:   true,
		LogTransactions:  true,
		MaxLogSize:       100 * 1024 * 1024, // 100MB
		LogRotation:      true,
		RotationInterval: 24 * time.Hour,
		RetentionPeriod:  30 * 24 * time.Hour, // 30 days
	}
}

// DefaultScannerConfig returns default scanner configuration
func DefaultScannerConfig() *ScannerConfig {
	return &ScannerConfig{
		Enabled:               true,
		EnableSQLInjectionScan: true,
		EnableCredentialScan:   true,
		EnablePatternScan:      true,
		ScanInterval:          1 * time.Hour,
		AlertThreshold:        5,
	}
}