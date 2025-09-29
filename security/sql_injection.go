package security

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// SQLInjectionProtector provides SQL injection protection
type SQLInjectionProtector struct {
	dangerousPatterns []*regexp.Regexp
	allowedPatterns   []*regexp.Regexp
	strictMode        bool
}

// SQLInjectionError represents a SQL injection attempt
type SQLInjectionError struct {
	Query   string
	Pattern string
	Message string
}

func (e *SQLInjectionError) Error() string {
	return fmt.Sprintf("SQL injection detected: %s (pattern: %s) in query: %s", e.Message, e.Pattern, e.Query)
}

// NewSQLInjectionProtector creates a new SQL injection protector
func NewSQLInjectionProtector() *SQLInjectionProtector {
	protector := &SQLInjectionProtector{
		strictMode: true,
	}

	protector.initializePatterns()
	return protector
}

// initializePatterns sets up dangerous SQL patterns to detect
func (p *SQLInjectionProtector) initializePatterns() {
	dangerousPatterns := []string{
		// Union-based injection
		`(?i)\bunion\s+(all\s+)?select\b`,

		// Comment-based injection
		`(?i)(--\s|/\*.*?\*/|\#\s)`,

		// Stacked queries
		`(?i);\s*(drop|alter|create|insert|update|delete|truncate|exec|execute)\b`,

		// Information schema attacks
		`(?i)\binformation_schema\b`,
		`(?i)\bsys\.tables\b`,
		`(?i)\bsysobjects\b`,

		// Time-based blind injection
		`(?i)\b(sleep|waitfor|benchmark|pg_sleep)\s*\(`,

		// Boolean-based blind injection
		`(?i)\b(and|or)\s+\d+\s*[=<>]\s*\d+`,

		// Error-based injection
		`(?i)\b(extractvalue|updatexml|exp)\s*\(`,

		// SQL functions that shouldn't be in user input
		`(?i)\b(char|ascii|substring|concat|version|user|database|schema)\s*\(`,

		// Administrative commands
		`(?i)\b(shutdown|xp_cmdshell|sp_configure)\b`,

		// File operations
		`(?i)\b(load_file|into\s+outfile|into\s+dumpfile)\b`,
	}

	p.dangerousPatterns = make([]*regexp.Regexp, len(dangerousPatterns))
	for i, pattern := range dangerousPatterns {
		p.dangerousPatterns[i] = regexp.MustCompile(pattern)
	}

	// Allow certain safe patterns even in strict mode
	allowedPatterns := []string{
		// Legitimate ORDER BY clauses
		`(?i)^order\s+by\s+[\w\.,\s]+$`,
		// Simple arithmetic
		`(?i)^\d+\s*[+\-*/]\s*\d+$`,
	}

	p.allowedPatterns = make([]*regexp.Regexp, len(allowedPatterns))
	for i, pattern := range allowedPatterns {
		p.allowedPatterns[i] = regexp.MustCompile(pattern)
	}
}

// ValidateQuery checks a query for SQL injection patterns
func (p *SQLInjectionProtector) ValidateQuery(query string, args []interface{}) error {
	// First check if this is a parameterized query with proper placeholders
	if p.isParameterizedQuery(query, args) {
		// Parameterized queries are generally safe, but still scan for obvious issues
		if err := p.scanForDangerousPatterns(query, false); err != nil {
			return err
		}
		return nil
	}

	// Non-parameterized queries need strict validation
	if err := p.scanForDangerousPatterns(query, true); err != nil {
		return err
	}

	// Validate arguments for injection attempts
	for i, arg := range args {
		if err := p.validateArgument(arg, i); err != nil {
			return err
		}
	}

	return nil
}

// isParameterizedQuery checks if query uses proper parameterization
func (p *SQLInjectionProtector) isParameterizedQuery(query string, args []interface{}) bool {
	// Count placeholders (?, $1, $2, etc.)
	placeholderCount := 0

	// PostgreSQL style ($1, $2, ...)
	pgPlaceholders := regexp.MustCompile(`\$\d+`)
	placeholderCount += len(pgPlaceholders.FindAllString(query, -1))

	// MySQL/SQLite style (?)
	questionMarks := strings.Count(query, "?")
	placeholderCount += questionMarks

	// Named parameters (:name, @name)
	namedParams := regexp.MustCompile(`[:\@]\w+`)
	placeholderCount += len(namedParams.FindAllString(query, -1))

	return placeholderCount >= len(args) && placeholderCount > 0
}

// scanForDangerousPatterns scans query for injection patterns
func (p *SQLInjectionProtector) scanForDangerousPatterns(query string, strict bool) error {
	// Normalize query for pattern matching
	normalizedQuery := strings.TrimSpace(query)

	// Check allowed patterns first
	for _, pattern := range p.allowedPatterns {
		if pattern.MatchString(normalizedQuery) {
			return nil // Explicitly allowed
		}
	}

	// Check dangerous patterns
	for _, pattern := range p.dangerousPatterns {
		if matches := pattern.FindStringSubmatch(normalizedQuery); len(matches) > 0 {
			return &SQLInjectionError{
				Query:   query,
				Pattern: pattern.String(),
				Message: fmt.Sprintf("Potentially dangerous SQL pattern detected: %s", matches[0]),
			}
		}
	}

	// In strict mode, check for additional suspicious patterns
	if strict || p.strictMode {
		if err := p.strictModeValidation(normalizedQuery); err != nil {
			return err
		}
	}

	return nil
}

// strictModeValidation performs additional checks in strict mode
func (p *SQLInjectionProtector) strictModeValidation(query string) error {
	// Check for multiple statements
	statements := strings.Split(query, ";")
	if len(statements) > 1 {
		for _, stmt := range statements {
			stmt = strings.TrimSpace(stmt)
			if stmt != "" && !p.isSafeStatement(stmt) {
				return &SQLInjectionError{
					Query:   query,
					Pattern: "multiple_statements",
					Message: "Multiple SQL statements detected in single query",
				}
			}
		}
	}

	// Check for suspicious string concatenation patterns
	concatPatterns := []string{
		`\|\|`,      // PostgreSQL concatenation
		`\+`,        // SQL Server concatenation
		`concat\s*\(`, // Function-based concatenation
	}

	for _, pattern := range concatPatterns {
		if matched, _ := regexp.MatchString(`(?i)`+pattern, query); matched {
			return &SQLInjectionError{
				Query:   query,
				Pattern: pattern,
				Message: "Suspicious string concatenation detected",
			}
		}
	}

	return nil
}

// isSafeStatement checks if a statement is considered safe
func (p *SQLInjectionProtector) isSafeStatement(stmt string) bool {
	stmt = strings.ToUpper(strings.TrimSpace(stmt))

	// Allow only basic query statements
	safeStarts := []string{
		"SELECT",
		"INSERT",
		"UPDATE",
		"DELETE",
		"WITH", // CTE
	}

	for _, safe := range safeStarts {
		if strings.HasPrefix(stmt, safe) {
			return true
		}
	}

	return false
}

// validateArgument validates individual query arguments
func (p *SQLInjectionProtector) validateArgument(arg interface{}, index int) error {
	if arg == nil {
		return nil
	}

	// Convert to string for pattern matching
	argStr := fmt.Sprintf("%v", arg)

	// Skip validation for non-string types that are safe
	switch arg.(type) {
	case int, int8, int16, int32, int64:
		return nil
	case uint, uint8, uint16, uint32, uint64:
		return nil
	case float32, float64:
		return nil
	case bool:
		return nil
	}

	// Validate string arguments
	if err := p.scanForDangerousPatterns(argStr, true); err != nil {
		return fmt.Errorf("dangerous pattern in argument %d: %w", index, err)
	}

	return nil
}

// SetStrictMode enables or disables strict mode
func (p *SQLInjectionProtector) SetStrictMode(enabled bool) {
	p.strictMode = enabled
}

// AddCustomPattern adds a custom dangerous pattern
func (p *SQLInjectionProtector) AddCustomPattern(pattern string) error {
	compiled, err := regexp.Compile(pattern)
	if err != nil {
		return fmt.Errorf("invalid regex pattern: %w", err)
	}

	p.dangerousPatterns = append(p.dangerousPatterns, compiled)
	return nil
}

// AddAllowedPattern adds a pattern that should be explicitly allowed
func (p *SQLInjectionProtector) AddAllowedPattern(pattern string) error {
	compiled, err := regexp.Compile(pattern)
	if err != nil {
		return fmt.Errorf("invalid regex pattern: %w", err)
	}

	p.allowedPatterns = append(p.allowedPatterns, compiled)
	return nil
}

// ScanResult represents the result of a security scan
type ScanResult struct {
	Safe        bool
	Violations  []SecurityViolation
	Confidence  float64
	Suggestions []string
}

// SecurityViolation represents a detected security issue
type SecurityViolation struct {
	Type        ViolationType
	Severity    SeverityLevel
	Pattern     string
	Location    string
	Description string
	Suggestion  string
}

type ViolationType string

const (
	SQLInjectionViolation    ViolationType = "sql_injection"
	CredentialLeakViolation  ViolationType = "credential_leak"
	PatternViolation        ViolationType = "pattern_violation"
	ParameterViolation      ViolationType = "parameter_violation"
)

type SeverityLevel string

const (
	CriticalSeverity SeverityLevel = "critical"
	HighSeverity     SeverityLevel = "high"
	MediumSeverity   SeverityLevel = "medium"
	LowSeverity      SeverityLevel = "low"
	InfoSeverity     SeverityLevel = "info"
)

// AdvancedScan performs comprehensive security scanning
func (p *SQLInjectionProtector) AdvancedScan(query string, args []interface{}) *ScanResult {
	result := &ScanResult{
		Safe:        true,
		Violations:  []SecurityViolation{},
		Confidence:  1.0,
		Suggestions: []string{},
	}

	// Perform basic injection scan
	if err := p.ValidateQuery(query, args); err != nil {
		result.Safe = false
		result.Confidence = 0.9

		var sqlErr *SQLInjectionError
		if errors.As(err, &sqlErr) {
			violation := SecurityViolation{
				Type:        SQLInjectionViolation,
				Severity:    HighSeverity,
				Pattern:     sqlErr.Pattern,
				Description: sqlErr.Message,
				Suggestion:  "Use parameterized queries to prevent SQL injection",
			}
			result.Violations = append(result.Violations, violation)
		}
	}

	// Additional heuristic checks
	p.performHeuristicAnalysis(query, args, result)

	return result
}

// performHeuristicAnalysis performs additional heuristic security analysis
func (p *SQLInjectionProtector) performHeuristicAnalysis(query string, args []interface{}, result *ScanResult) {
	// Check for suspicious parameter patterns
	for i, arg := range args {
		if argStr := fmt.Sprintf("%v", arg); len(argStr) > 1000 {
			result.Violations = append(result.Violations, SecurityViolation{
				Type:        ParameterViolation,
				Severity:    MediumSeverity,
				Location:    fmt.Sprintf("parameter %d", i),
				Description: "Unusually long parameter value detected",
				Suggestion:  "Validate parameter length limits",
			})
		}
	}

	// Check for potential credential leaks in query
	credentialPatterns := []*regexp.Regexp{
		regexp.MustCompile(`(?i)(password|pwd|secret|token|key)\s*=\s*['\"][^'\"]+['\"]`),
		regexp.MustCompile(`(?i)(api[_-]?key|access[_-]?token)\s*[=:]\s*['\"][^'\"]+['\"]`),
	}

	for _, pattern := range credentialPatterns {
		if pattern.MatchString(query) {
			result.Safe = false
			result.Violations = append(result.Violations, SecurityViolation{
				Type:        CredentialLeakViolation,
				Severity:    CriticalSeverity,
				Pattern:     pattern.String(),
				Description: "Potential credential leak in query",
				Suggestion:  "Use parameter binding for sensitive values",
			})
		}
	}
}