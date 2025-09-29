package security

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// ParameterSanitizer provides parameter sanitization for logging and tracing
type ParameterSanitizer struct {
	sensitivePatterns []*regexp.Regexp
	replacementText   string
	customRules       []SanitizationRule
}

// SanitizationRule defines custom sanitization behavior
type SanitizationRule struct {
	Name        string
	Pattern     *regexp.Regexp
	Replacement string
	Enabled     bool
}

// SanitizationLevel defines how aggressive sanitization should be
type SanitizationLevel int

const (
	MinimalSanitization SanitizationLevel = iota
	StandardSanitization
	AggressiveSanitization
)

// NewParameterSanitizer creates a new parameter sanitizer
func NewParameterSanitizer() *ParameterSanitizer {
	sanitizer := &ParameterSanitizer{
		replacementText: "[REDACTED]",
	}

	sanitizer.initializeSensitivePatterns()
	sanitizer.initializeCustomRules()

	return sanitizer
}

// initializeSensitivePatterns sets up patterns for sensitive data detection
func (ps *ParameterSanitizer) initializeSensitivePatterns() {
	sensitivePatterns := []string{
		// Passwords and secrets
		`(?i)(password|passwd|pwd|secret|key|token)`,
		`(?i)(api[_-]?key|access[_-]?token|refresh[_-]?token)`,
		`(?i)(client[_-]?secret|app[_-]?secret)`,

		// Credit card numbers (basic pattern)
		`\b(?:\d{4}[-\s]?){3}\d{4}\b`,

		// Social Security Numbers
		`\b\d{3}-\d{2}-\d{4}\b`,

		// Email addresses (when used as sensitive identifiers)
		`\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b`,

		// Phone numbers
		`\b\+?1?[-.\s]?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}\b`,

		// IP addresses (for privacy)
		`\b(?:\d{1,3}\.){3}\d{1,3}\b`,

		// JWT tokens (basic pattern)
		`\beyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b`,

		// UUIDs (sometimes sensitive)
		`\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b`,

		// Database connection strings
		`(?i)(server|host|database|user|uid|password|pwd)\s*=\s*[^;]+`,
	}

	ps.sensitivePatterns = make([]*regexp.Regexp, len(sensitivePatterns))
	for i, pattern := range sensitivePatterns {
		ps.sensitivePatterns[i] = regexp.MustCompile(pattern)
	}
}

// initializeCustomRules sets up default custom sanitization rules
func (ps *ParameterSanitizer) initializeCustomRules() {
	ps.customRules = []SanitizationRule{
		{
			Name:        "credit_card_partial",
			Pattern:     regexp.MustCompile(`\b(\d{4})(\d{4,8})(\d{4})\b`),
			Replacement: "$1****$3",
			Enabled:     true,
		},
		{
			Name:        "email_partial",
			Pattern:     regexp.MustCompile(`\b([a-zA-Z0-9])[a-zA-Z0-9._%+-]*@([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b`),
			Replacement: "$1***@$2",
			Enabled:     true,
		},
		{
			Name:        "phone_partial",
			Pattern:     regexp.MustCompile(`\b(\+?1?[-.\s]?\(?[0-9]{3}\)?[-.\s]?)([0-9]{3})([-.\s]?[0-9]{4})\b`),
			Replacement: "$1***$3",
			Enabled:     true,
		},
	}
}

// SanitizeParameters sanitizes a map of parameters for safe logging
func (ps *ParameterSanitizer) SanitizeParameters(params map[string]interface{}) map[string]interface{} {
	if params == nil {
		return nil
	}

	sanitized := make(map[string]interface{})

	for key, value := range params {
		sanitized[key] = ps.sanitizeValue(key, value)
	}

	return sanitized
}

// SanitizeQuery sanitizes a SQL query for safe logging
func (ps *ParameterSanitizer) SanitizeQuery(query string) string {
	return ps.sanitizeString(query)
}

// SanitizeString sanitizes a string value
func (ps *ParameterSanitizer) SanitizeString(value string) string {
	return ps.sanitizeString(value)
}

// sanitizeValue sanitizes individual parameter values
func (ps *ParameterSanitizer) sanitizeValue(key string, value interface{}) interface{} {
	if value == nil {
		return nil
	}

	// Check if key itself indicates sensitive data
	if ps.isKeyNameSensitive(key) {
		return ps.replacementText
	}

	// Handle different value types
	switch v := value.(type) {
	case string:
		return ps.sanitizeString(v)
	case []byte:
		return ps.sanitizeString(string(v))
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		// Numbers are generally safe, but check if key suggests otherwise
		if ps.isKeyNameSensitive(key) {
			return "[REDACTED]"
		}
		return value
	case float32, float64:
		if ps.isKeyNameSensitive(key) {
			return "[REDACTED]"
		}
		return value
	case bool:
		return value
	case map[string]interface{}:
		return ps.SanitizeParameters(v)
	case []interface{}:
		return ps.sanitizeSlice(v)
	default:
		// For unknown types, convert to string and sanitize
		return ps.sanitizeString(fmt.Sprintf("%v", v))
	}
}

// sanitizeString performs string sanitization
func (ps *ParameterSanitizer) sanitizeString(value string) string {
	if value == "" {
		return value
	}

	sanitized := value

	// Apply custom rules first (for partial sanitization)
	for _, rule := range ps.customRules {
		if rule.Enabled && rule.Pattern != nil {
			sanitized = rule.Pattern.ReplaceAllString(sanitized, rule.Replacement)
		}
	}

	// Apply full redaction for completely sensitive patterns
	for _, pattern := range ps.sensitivePatterns {
		if pattern.MatchString(sanitized) {
			// For very sensitive data, replace entirely
			if ps.isCompleteSensitivePattern(pattern) {
				return ps.replacementText
			}
			// For less sensitive data, apply pattern-based replacement
			sanitized = pattern.ReplaceAllStringFunc(sanitized, func(match string) string {
				return ps.getReplacementForPattern(match, pattern)
			})
		}
	}

	return sanitized
}

// sanitizeSlice sanitizes slice values
func (ps *ParameterSanitizer) sanitizeSlice(slice []interface{}) []interface{} {
	sanitized := make([]interface{}, len(slice))
	for i, item := range slice {
		sanitized[i] = ps.sanitizeValue(fmt.Sprintf("item_%d", i), item)
	}
	return sanitized
}

// isKeyNameSensitive checks if a parameter key name indicates sensitive data
func (ps *ParameterSanitizer) isKeyNameSensitive(key string) bool {
	sensitiveKeys := []string{
		"password", "passwd", "pwd", "secret", "token", "key",
		"api_key", "apikey", "access_token", "refresh_token",
		"client_secret", "app_secret", "private_key",
		"ssn", "social_security", "credit_card", "card_number",
		"cvv", "cvc", "security_code", "pin",
	}

	keyLower := strings.ToLower(key)
	for _, sensitiveKey := range sensitiveKeys {
		if strings.Contains(keyLower, sensitiveKey) {
			return true
		}
	}

	return false
}

// isCompleteSensitivePattern determines if a pattern should result in complete redaction
func (ps *ParameterSanitizer) isCompleteSensitivePattern(pattern *regexp.Regexp) bool {
	patternStr := pattern.String()
	completeSensitive := []string{
		"password", "secret", "token", "key",
		"jwt", "connection",
	}

	for _, sensitive := range completeSensitive {
		if strings.Contains(strings.ToLower(patternStr), sensitive) {
			return true
		}
	}

	return false
}

// getReplacementForPattern gets appropriate replacement text for a pattern
func (ps *ParameterSanitizer) getReplacementForPattern(match string, pattern *regexp.Regexp) string {
	// For different pattern types, return different replacements
	patternStr := strings.ToLower(pattern.String())

	if strings.Contains(patternStr, "credit") {
		return "[CARD_REDACTED]"
	}
	if strings.Contains(patternStr, "email") {
		return "[EMAIL_REDACTED]"
	}
	if strings.Contains(patternStr, "phone") {
		return "[PHONE_REDACTED]"
	}
	if strings.Contains(patternStr, "ip") {
		return "[IP_REDACTED]"
	}
	if strings.Contains(patternStr, "uuid") {
		return "[UUID_REDACTED]"
	}

	return ps.replacementText
}

// SanitizeForJSON sanitizes data for JSON serialization
func (ps *ParameterSanitizer) SanitizeForJSON(data interface{}) ([]byte, error) {
	sanitized := ps.sanitizeValue("root", data)
	return json.Marshal(sanitized)
}

// SetReplacementText sets the default replacement text
func (ps *ParameterSanitizer) SetReplacementText(text string) {
	ps.replacementText = text
}

// AddCustomRule adds a custom sanitization rule
func (ps *ParameterSanitizer) AddCustomRule(rule SanitizationRule) {
	ps.customRules = append(ps.customRules, rule)
}

// EnableRule enables or disables a custom rule by name
func (ps *ParameterSanitizer) EnableRule(name string, enabled bool) {
	for i := range ps.customRules {
		if ps.customRules[i].Name == name {
			ps.customRules[i].Enabled = enabled
			break
		}
	}
}

// AddSensitivePattern adds a new sensitive pattern for detection
func (ps *ParameterSanitizer) AddSensitivePattern(pattern string) error {
	compiled, err := regexp.Compile(pattern)
	if err != nil {
		return fmt.Errorf("invalid regex pattern: %w", err)
	}

	ps.sensitivePatterns = append(ps.sensitivePatterns, compiled)
	return nil
}

// SanitizeLevel applies different levels of sanitization
func (ps *ParameterSanitizer) SanitizeWithLevel(params map[string]interface{}, level SanitizationLevel) map[string]interface{} {
	switch level {
	case MinimalSanitization:
		return ps.minimalSanitize(params)
	case StandardSanitization:
		return ps.SanitizeParameters(params)
	case AggressiveSanitization:
		return ps.aggressiveSanitize(params)
	default:
		return ps.SanitizeParameters(params)
	}
}

// minimalSanitize applies only essential sanitization
func (ps *ParameterSanitizer) minimalSanitize(params map[string]interface{}) map[string]interface{} {
	if params == nil {
		return nil
	}

	sanitized := make(map[string]interface{})
	essentialPatterns := []string{"password", "secret", "token", "key"}

	for key, value := range params {
		keyLower := strings.ToLower(key)
		shouldSanitize := false

		for _, pattern := range essentialPatterns {
			if strings.Contains(keyLower, pattern) {
				shouldSanitize = true
				break
			}
		}

		if shouldSanitize {
			sanitized[key] = ps.replacementText
		} else {
			sanitized[key] = value
		}
	}

	return sanitized
}

// aggressiveSanitize applies comprehensive sanitization
func (ps *ParameterSanitizer) aggressiveSanitize(params map[string]interface{}) map[string]interface{} {
	if params == nil {
		return nil
	}

	sanitized := make(map[string]interface{})

	for key, value := range params {
		// In aggressive mode, sanitize many more field types
		if ps.isAggressiveSensitive(key, value) {
			sanitized[key] = ps.replacementText
		} else {
			sanitized[key] = ps.sanitizeValue(key, value)
		}
	}

	return sanitized
}

// isAggressiveSensitive determines if field should be sanitized in aggressive mode
func (ps *ParameterSanitizer) isAggressiveSensitive(key string, value interface{}) bool {
	keyLower := strings.ToLower(key)

	// Aggressive patterns
	aggressivePatterns := []string{
		"id", "user", "email", "name", "address", "phone",
		"ip", "session", "cookie", "auth", "login",
	}

	for _, pattern := range aggressivePatterns {
		if strings.Contains(keyLower, pattern) {
			return true
		}
	}

	// Check value length - very long values might be sensitive
	if valueStr := fmt.Sprintf("%v", value); len(valueStr) > 100 {
		return true
	}

	return false
}