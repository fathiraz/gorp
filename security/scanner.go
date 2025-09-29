package security

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
)

// SecurityScanner provides security scanning and vulnerability detection
type SecurityScanner struct {
	config          *ScannerConfig
	patterns        []ScanPattern
	customPatterns  []ScanPattern
	violationCounts map[string]int
	lastScan        time.Time
	mu              sync.RWMutex
}

// ScanPattern represents a security scan pattern
type ScanPattern struct {
	Name        string
	Type        ViolationType
	Severity    SeverityLevel
	Pattern     *regexp.Regexp
	Description string
	Suggestion  string
	Enabled     bool
}

// ScanContext holds scanning context information
type ScanContext struct {
	UserID     string
	SessionID  string
	IPAddress  string
	Operation  string
	Timestamp  time.Time
}

// NewSecurityScanner creates a new security scanner
func NewSecurityScanner(config *ScannerConfig) *SecurityScanner {
	if config == nil {
		config = DefaultScannerConfig()
	}

	scanner := &SecurityScanner{
		config:          config,
		violationCounts: make(map[string]int),
		lastScan:        time.Now(),
	}

	scanner.initializePatterns()
	return scanner
}

// initializePatterns sets up default security scan patterns
func (ss *SecurityScanner) initializePatterns() {
	ss.patterns = []ScanPattern{
		// SQL Injection patterns
		{
			Name:        "sql_injection_union",
			Type:        SQLInjectionViolation,
			Severity:    CriticalSeverity,
			Pattern:     regexp.MustCompile(`(?i)\bunion\s+(all\s+)?select\b`),
			Description: "UNION-based SQL injection attempt detected",
			Suggestion:  "Use parameterized queries instead of string concatenation",
			Enabled:     ss.config.EnableSQLInjectionScan,
		},
		{
			Name:        "sql_injection_comment",
			Type:        SQLInjectionViolation,
			Severity:    HighSeverity,
			Pattern:     regexp.MustCompile(`(?i)(--|\#|/\*|\*/)`),
			Description: "SQL comment injection attempt detected",
			Suggestion:  "Validate and sanitize user input",
			Enabled:     ss.config.EnableSQLInjectionScan,
		},
		{
			Name:        "sql_injection_stacked",
			Type:        SQLInjectionViolation,
			Severity:    CriticalSeverity,
			Pattern:     regexp.MustCompile(`(?i);\s*(drop|alter|create|insert|update|delete|truncate)\b`),
			Description: "Stacked query SQL injection attempt detected",
			Suggestion:  "Use single-statement execution and parameterized queries",
			Enabled:     ss.config.EnableSQLInjectionScan,
		},
		{
			Name:        "sql_injection_information_schema",
			Type:        SQLInjectionViolation,
			Severity:    HighSeverity,
			Pattern:     regexp.MustCompile(`(?i)\b(information_schema|sysobjects|sys\.tables)\b`),
			Description: "Database schema enumeration attempt detected",
			Suggestion:  "Restrict database permissions and use least privilege principle",
			Enabled:     ss.config.EnableSQLInjectionScan,
		},

		// Credential leak patterns
		{
			Name:        "password_in_query",
			Type:        CredentialLeakViolation,
			Severity:    CriticalSeverity,
			Pattern:     regexp.MustCompile(`(?i)(password|passwd|pwd)\s*=\s*['\"][^'\"]*['\"]`),
			Description: "Password exposed in query",
			Suggestion:  "Use parameter binding for sensitive values",
			Enabled:     ss.config.EnableCredentialScan,
		},
		{
			Name:        "api_key_in_query",
			Type:        CredentialLeakViolation,
			Severity:    CriticalSeverity,
			Pattern:     regexp.MustCompile(`(?i)(api[_-]?key|access[_-]?token)\s*[=:]\s*['\"][^'\"]*['\"]`),
			Description: "API key or access token exposed in query",
			Suggestion:  "Use secure credential management and parameter binding",
			Enabled:     ss.config.EnableCredentialScan,
		},
		{
			Name:        "connection_string_leak",
			Type:        CredentialLeakViolation,
			Severity:    HighSeverity,
			Pattern:     regexp.MustCompile(`(?i)(server|host|database|user|uid|password|pwd)\s*=\s*[^;]+`),
			Description: "Database connection string exposed",
			Suggestion:  "Use environment variables or secure configuration for connection strings",
			Enabled:     ss.config.EnableCredentialScan,
		},

		// Suspicious patterns
		{
			Name:        "base64_encoded_payload",
			Type:        PatternViolation,
			Severity:    MediumSeverity,
			Pattern:     regexp.MustCompile(`[A-Za-z0-9+/]{50,}={0,2}`),
			Description: "Large base64 encoded payload detected",
			Suggestion:  "Validate and limit payload sizes",
			Enabled:     ss.config.EnablePatternScan,
		},
		{
			Name:        "excessive_wildcards",
			Type:        PatternViolation,
			Severity:    MediumSeverity,
			Pattern:     regexp.MustCompile(`%.*%.*%.*%`),
			Description: "Excessive wildcard usage detected",
			Suggestion:  "Limit wildcard queries to prevent performance issues",
			Enabled:     ss.config.EnablePatternScan,
		},
		{
			Name:        "time_based_attack",
			Type:        SQLInjectionViolation,
			Severity:    HighSeverity,
			Pattern:     regexp.MustCompile(`(?i)\b(sleep|waitfor|benchmark|pg_sleep)\s*\(`),
			Description: "Time-based attack pattern detected",
			Suggestion:  "Implement query timeouts and rate limiting",
			Enabled:     ss.config.EnableSQLInjectionScan,
		},
		{
			Name:        "error_based_injection",
			Type:        SQLInjectionViolation,
			Severity:    HighSeverity,
			Pattern:     regexp.MustCompile(`(?i)\b(extractvalue|updatexml|exp|floor|rand)\s*\(`),
			Description: "Error-based SQL injection pattern detected",
			Suggestion:  "Implement proper error handling and avoid exposing database errors",
			Enabled:     ss.config.EnableSQLInjectionScan,
		},

		// Administrative command patterns
		{
			Name:        "admin_commands",
			Type:        SQLInjectionViolation,
			Severity:    CriticalSeverity,
			Pattern:     regexp.MustCompile(`(?i)\b(shutdown|xp_cmdshell|sp_configure|exec\s+master)\b`),
			Description: "Administrative command detected",
			Suggestion:  "Restrict administrative privileges and validate user permissions",
			Enabled:     ss.config.EnableSQLInjectionScan,
		},
		{
			Name:        "file_operations",
			Type:        SQLInjectionViolation,
			Severity:    HighSeverity,
			Pattern:     regexp.MustCompile(`(?i)\b(load_file|into\s+outfile|into\s+dumpfile|bulk\s+insert)\b`),
			Description: "File operation command detected",
			Suggestion:  "Disable file operations or restrict to specific directories",
			Enabled:     ss.config.EnableSQLInjectionScan,
		},
	}
}

// ScanQuery scans a query for security violations
func (ss *SecurityScanner) ScanQuery(query string, args []interface{}) []SecurityViolation {
	if !ss.config.Enabled {
		return nil
	}

	ss.mu.Lock()
	defer ss.mu.Unlock()

	var violations []SecurityViolation

	// Scan query text
	for _, pattern := range ss.patterns {
		if !pattern.Enabled {
			continue
		}

		if pattern.Pattern.MatchString(query) {
			violation := SecurityViolation{
				Type:        pattern.Type,
				Severity:    pattern.Severity,
				Pattern:     pattern.Name,
				Location:    "query",
				Description: pattern.Description,
				Suggestion:  pattern.Suggestion,
			}
			violations = append(violations, violation)

			// Track violation counts
			ss.violationCounts[pattern.Name]++
		}
	}

	// Scan custom patterns
	for _, pattern := range ss.customPatterns {
		if !pattern.Enabled {
			continue
		}

		if pattern.Pattern.MatchString(query) {
			violation := SecurityViolation{
				Type:        pattern.Type,
				Severity:    pattern.Severity,
				Pattern:     pattern.Name,
				Location:    "query",
				Description: pattern.Description,
				Suggestion:  pattern.Suggestion,
			}
			violations = append(violations, violation)

			ss.violationCounts[pattern.Name]++
		}
	}

	// Scan arguments
	for i, arg := range args {
		argViolations := ss.scanArgument(arg, i)
		violations = append(violations, argViolations...)
	}

	ss.lastScan = time.Now()
	return violations
}

// scanArgument scans individual arguments for security issues
func (ss *SecurityScanner) scanArgument(arg interface{}, index int) []SecurityViolation {
	if arg == nil {
		return nil
	}

	argStr := fmt.Sprintf("%v", arg)
	var violations []SecurityViolation

	for _, pattern := range ss.patterns {
		if !pattern.Enabled {
			continue
		}

		if pattern.Pattern.MatchString(argStr) {
			violation := SecurityViolation{
				Type:        pattern.Type,
				Severity:    pattern.Severity,
				Pattern:     pattern.Name,
				Location:    fmt.Sprintf("parameter_%d", index),
				Description: pattern.Description + " in parameter",
				Suggestion:  pattern.Suggestion,
			}
			violations = append(violations, violation)

			ss.violationCounts[pattern.Name]++
		}
	}

	return violations
}

// ScanContext scans security context for anomalies
func (ss *SecurityScanner) ScanContext(ctx *ScanContext) []SecurityViolation {
	if !ss.config.Enabled {
		return nil
	}

	var violations []SecurityViolation

	// Check for suspicious IP patterns
	if ctx.IPAddress != "" {
		if ss.isSuspiciousIP(ctx.IPAddress) {
			violations = append(violations, SecurityViolation{
				Type:        PatternViolation,
				Severity:    MediumSeverity,
				Pattern:     "suspicious_ip",
				Location:    "context",
				Description: "Request from suspicious IP address",
				Suggestion:  "Implement IP allowlisting or geofencing",
			})
		}
	}

	// Check for rapid successive requests (potential DoS)
	if ss.isRapidRequest(ctx) {
		violations = append(violations, SecurityViolation{
			Type:        PatternViolation,
			Severity:    HighSeverity,
			Pattern:     "rapid_requests",
			Location:    "context",
			Description: "Rapid successive requests detected",
			Suggestion:  "Implement rate limiting and request throttling",
		})
	}

	return violations
}

// isSuspiciousIP checks if an IP address is suspicious
func (ss *SecurityScanner) isSuspiciousIP(ip string) bool {
	// Known suspicious patterns
	suspiciousPatterns := []string{
		"127.0.0.1", // Localhost (might be suspicious in certain contexts)
		"0.0.0.0",   // Invalid/malformed
	}

	for _, pattern := range suspiciousPatterns {
		if strings.Contains(ip, pattern) {
			return true
		}
	}

	return false
}

// isRapidRequest checks for rapid successive requests
func (ss *SecurityScanner) isRapidRequest(ctx *ScanContext) bool {
	// Simple check - in production, use more sophisticated rate limiting
	timeSinceLastScan := time.Since(ss.lastScan)
	return timeSinceLastScan < time.Millisecond*100 // Less than 100ms between requests
}

// AddCustomPattern adds a custom security pattern
func (ss *SecurityScanner) AddCustomPattern(pattern ScanPattern) error {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	ss.customPatterns = append(ss.customPatterns, pattern)
	return nil
}

// RemoveCustomPattern removes a custom security pattern by name
func (ss *SecurityScanner) RemoveCustomPattern(name string) bool {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	for i, pattern := range ss.customPatterns {
		if pattern.Name == name {
			ss.customPatterns = append(ss.customPatterns[:i], ss.customPatterns[i+1:]...)
			return true
		}
	}

	return false
}

// EnablePattern enables or disables a pattern by name
func (ss *SecurityScanner) EnablePattern(name string, enabled bool) bool {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	// Check built-in patterns
	for i := range ss.patterns {
		if ss.patterns[i].Name == name {
			ss.patterns[i].Enabled = enabled
			return true
		}
	}

	// Check custom patterns
	for i := range ss.customPatterns {
		if ss.customPatterns[i].Name == name {
			ss.customPatterns[i].Enabled = enabled
			return true
		}
	}

	return false
}

// GetViolationCounts returns violation statistics
func (ss *SecurityScanner) GetViolationCounts() map[string]int {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	counts := make(map[string]int)
	for k, v := range ss.violationCounts {
		counts[k] = v
	}

	return counts
}

// ResetViolationCounts resets violation statistics
func (ss *SecurityScanner) ResetViolationCounts() {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	ss.violationCounts = make(map[string]int)
}

// GetActivePatterns returns list of active patterns
func (ss *SecurityScanner) GetActivePatterns() []ScanPattern {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	var active []ScanPattern

	for _, pattern := range ss.patterns {
		if pattern.Enabled {
			active = append(active, pattern)
		}
	}

	for _, pattern := range ss.customPatterns {
		if pattern.Enabled {
			active = append(active, pattern)
		}
	}

	return active
}

// PerformFullScan performs a comprehensive security scan
func (ss *SecurityScanner) PerformFullScan(ctx context.Context, queries []string, contexts []*ScanContext) *SecurityScanReport {
	report := &SecurityScanReport{
		ScanID:       ss.generateScanID(),
		StartTime:    time.Now(),
		TotalQueries: len(queries),
		TotalContexts: len(contexts),
		Violations:   []SecurityViolation{},
		Summary:      make(map[ViolationType]int),
	}

	// Scan queries
	for _, query := range queries {
		violations := ss.ScanQuery(query, nil)
		report.Violations = append(report.Violations, violations...)
	}

	// Scan contexts
	for _, scanCtx := range contexts {
		violations := ss.ScanContext(scanCtx)
		report.Violations = append(report.Violations, violations...)
	}

	// Generate summary
	for _, violation := range report.Violations {
		report.Summary[violation.Type]++
	}

	report.EndTime = time.Now()
	report.Duration = report.EndTime.Sub(report.StartTime)
	report.ViolationCount = len(report.Violations)

	return report
}

// SecurityScanReport represents the result of a security scan
type SecurityScanReport struct {
	ScanID         string                    `json:"scan_id"`
	StartTime      time.Time                `json:"start_time"`
	EndTime        time.Time                `json:"end_time"`
	Duration       time.Duration            `json:"duration"`
	TotalQueries   int                      `json:"total_queries"`
	TotalContexts  int                      `json:"total_contexts"`
	ViolationCount int                      `json:"violation_count"`
	Violations     []SecurityViolation      `json:"violations"`
	Summary        map[ViolationType]int    `json:"summary"`
	Recommendations []string                `json:"recommendations"`
}

// generateScanID generates a unique scan ID
func (ss *SecurityScanner) generateScanID() string {
	return string(rune('s')) + string(rune('c')) + string(rune('a')) + string(rune('n')) + "_" + time.Now().Format("20060102_150405")
}

// GetConfig returns the scanner configuration
func (ss *SecurityScanner) GetConfig() *ScannerConfig {
	return ss.config
}

// UpdateConfig updates the scanner configuration
func (ss *SecurityScanner) UpdateConfig(config *ScannerConfig) {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	ss.config = config

	// Update pattern enablement based on new config
	for i := range ss.patterns {
		switch ss.patterns[i].Type {
		case SQLInjectionViolation:
			ss.patterns[i].Enabled = config.EnableSQLInjectionScan
		case CredentialLeakViolation:
			ss.patterns[i].Enabled = config.EnableCredentialScan
		case PatternViolation:
			ss.patterns[i].Enabled = config.EnablePatternScan
		}
	}
}

// IsHealthy checks if the scanner is functioning properly
func (ss *SecurityScanner) IsHealthy() bool {
	ss.mu.RLock()
	defer ss.mu.RUnlock()

	// Check if patterns are loaded
	if len(ss.patterns) == 0 {
		return false
	}

	// Check if at least one pattern is enabled
	hasEnabledPattern := false
	for _, pattern := range ss.patterns {
		if pattern.Enabled {
			hasEnabledPattern = true
			break
		}
	}

	return hasEnabledPattern
}

// NewSecurityViolationError creates an error from security violations
func NewSecurityViolationError(violations []SecurityViolation) error {
	if len(violations) == 0 {
		return nil
	}

	// Return the first critical violation as error
	for _, violation := range violations {
		if violation.Severity == CriticalSeverity {
			return &SecurityViolationError{
				Type:        violation.Type,
				Severity:    violation.Severity,
				Pattern:     violation.Pattern,
				Description: violation.Description,
				Violations:  violations,
			}
		}
	}

	// If no critical violations, return first high severity
	for _, violation := range violations {
		if violation.Severity == HighSeverity {
			return &SecurityViolationError{
				Type:        violation.Type,
				Severity:    violation.Severity,
				Pattern:     violation.Pattern,
				Description: violation.Description,
				Violations:  violations,
			}
		}
	}

	return nil // No critical/high violations
}

// SecurityViolationError represents a security violation error
type SecurityViolationError struct {
	Type        ViolationType
	Severity    SeverityLevel
	Pattern     string
	Description string
	Violations  []SecurityViolation
}

func (e *SecurityViolationError) Error() string {
	return string(rune('s')) + string(rune('e')) + string(rune('c')) + string(rune('u')) + string(rune('r')) + string(rune('i')) + string(rune('t')) + string(rune('y')) + " " + string(rune('v')) + string(rune('i')) + string(rune('o')) + string(rune('l')) + string(rune('a')) + string(rune('t')) + string(rune('i')) + string(rune('o')) + string(rune('n')) + ": " + e.Description
}