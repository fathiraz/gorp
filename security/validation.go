package security

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

// Validator provides generic input validation framework
type Validator[T any] struct {
	rules         []ValidationRule
	customRules   map[string]CustomValidationFunc
	errorMessages map[string]string
	failFast      bool
}

// ValidationRule defines a validation rule
type ValidationRule interface {
	Validate(ctx context.Context, field string, value interface{}) []ValidationError
	GetFieldName() string
	GetRuleName() string
}

// CustomValidationFunc defines custom validation function signature
type CustomValidationFunc func(ctx context.Context, value interface{}) error

// ValidationError represents a validation error
type ValidationError struct {
	Field       string
	Rule        string
	Value       interface{}
	Message     string
	Severity    SeverityLevel
	Code        string
	Suggestions []string
}

func (ve ValidationError) Error() string {
	return fmt.Sprintf("validation failed for field '%s': %s (rule: %s)", ve.Field, ve.Message, ve.Rule)
}

// ValidationResult contains validation results
type ValidationResult struct {
	Valid      bool
	Errors     []ValidationError
	Warnings   []ValidationError
	FieldCount int
	RuleCount  int
}

// Built-in validation rules

// RequiredRule validates that a field is not empty
type RequiredRule struct {
	FieldName string
}

func (r RequiredRule) Validate(ctx context.Context, field string, value interface{}) []ValidationError {
	if r.FieldName != "" && field != r.FieldName {
		return nil
	}

	if value == nil {
		return []ValidationError{{
			Field:    field,
			Rule:     "required",
			Value:    value,
			Message:  "field is required",
			Severity: HighSeverity,
			Code:     "REQUIRED_FIELD",
		}}
	}

	// Check for empty strings
	if str, ok := value.(string); ok && strings.TrimSpace(str) == "" {
		return []ValidationError{{
			Field:    field,
			Rule:     "required",
			Value:    value,
			Message:  "field cannot be empty",
			Severity: HighSeverity,
			Code:     "EMPTY_FIELD",
		}}
	}

	return nil
}

func (r RequiredRule) GetFieldName() string { return r.FieldName }
func (r RequiredRule) GetRuleName() string  { return "required" }

// LengthRule validates string length constraints
type LengthRule struct {
	FieldName string
	MinLength int
	MaxLength int
}

func (r LengthRule) Validate(ctx context.Context, field string, value interface{}) []ValidationError {
	if r.FieldName != "" && field != r.FieldName {
		return nil
	}

	str, ok := value.(string)
	if !ok {
		if value == nil {
			return nil // Let RequiredRule handle nil values
		}
		str = fmt.Sprintf("%v", value)
	}

	length := len(str)
	var errors []ValidationError

	if r.MinLength > 0 && length < r.MinLength {
		errors = append(errors, ValidationError{
			Field:    field,
			Rule:     "length",
			Value:    value,
			Message:  fmt.Sprintf("field must be at least %d characters long", r.MinLength),
			Severity: MediumSeverity,
			Code:     "MIN_LENGTH",
			Suggestions: []string{
				fmt.Sprintf("Current length: %d, required: %d", length, r.MinLength),
			},
		})
	}

	if r.MaxLength > 0 && length > r.MaxLength {
		errors = append(errors, ValidationError{
			Field:    field,
			Rule:     "length",
			Value:    value,
			Message:  fmt.Sprintf("field must be at most %d characters long", r.MaxLength),
			Severity: MediumSeverity,
			Code:     "MAX_LENGTH",
			Suggestions: []string{
				fmt.Sprintf("Current length: %d, maximum: %d", length, r.MaxLength),
			},
		})
	}

	return errors
}

func (r LengthRule) GetFieldName() string { return r.FieldName }
func (r LengthRule) GetRuleName() string  { return "length" }

// RegexRule validates field against regular expression
type RegexRule struct {
	FieldName string
	Pattern   *regexp.Regexp
	Message   string
}

func (r RegexRule) Validate(ctx context.Context, field string, value interface{}) []ValidationError {
	if r.FieldName != "" && field != r.FieldName {
		return nil
	}

	if value == nil {
		return nil
	}

	str := fmt.Sprintf("%v", value)
	if !r.Pattern.MatchString(str) {
		message := r.Message
		if message == "" {
			message = fmt.Sprintf("field does not match required pattern: %s", r.Pattern.String())
		}

		return []ValidationError{{
			Field:    field,
			Rule:     "regex",
			Value:    value,
			Message:  message,
			Severity: MediumSeverity,
			Code:     "PATTERN_MISMATCH",
			Suggestions: []string{
				fmt.Sprintf("Pattern: %s", r.Pattern.String()),
			},
		}}
	}

	return nil
}

func (r RegexRule) GetFieldName() string { return r.FieldName }
func (r RegexRule) GetRuleName() string  { return "regex" }

// RangeRule validates numeric ranges
type RangeRule struct {
	FieldName string
	MinValue  *float64
	MaxValue  *float64
}

func (r RangeRule) Validate(ctx context.Context, field string, value interface{}) []ValidationError {
	if r.FieldName != "" && field != r.FieldName {
		return nil
	}

	if value == nil {
		return nil
	}

	var numValue float64
	var err error

	switch v := value.(type) {
	case int:
		numValue = float64(v)
	case int8:
		numValue = float64(v)
	case int16:
		numValue = float64(v)
	case int32:
		numValue = float64(v)
	case int64:
		numValue = float64(v)
	case uint:
		numValue = float64(v)
	case uint8:
		numValue = float64(v)
	case uint16:
		numValue = float64(v)
	case uint32:
		numValue = float64(v)
	case uint64:
		numValue = float64(v)
	case float32:
		numValue = float64(v)
	case float64:
		numValue = v
	case string:
		numValue, err = strconv.ParseFloat(v, 64)
		if err != nil {
			return []ValidationError{{
				Field:    field,
				Rule:     "range",
				Value:    value,
				Message:  "field must be a valid number",
				Severity: MediumSeverity,
				Code:     "INVALID_NUMBER",
			}}
		}
	default:
		return []ValidationError{{
			Field:    field,
			Rule:     "range",
			Value:    value,
			Message:  "field must be a number",
			Severity: MediumSeverity,
			Code:     "NOT_A_NUMBER",
		}}
	}

	var errors []ValidationError

	if r.MinValue != nil && numValue < *r.MinValue {
		errors = append(errors, ValidationError{
			Field:    field,
			Rule:     "range",
			Value:    value,
			Message:  fmt.Sprintf("field must be at least %g", *r.MinValue),
			Severity: MediumSeverity,
			Code:     "MIN_VALUE",
		})
	}

	if r.MaxValue != nil && numValue > *r.MaxValue {
		errors = append(errors, ValidationError{
			Field:    field,
			Rule:     "range",
			Value:    value,
			Message:  fmt.Sprintf("field must be at most %g", *r.MaxValue),
			Severity: MediumSeverity,
			Code:     "MAX_VALUE",
		})
	}

	return errors
}

func (r RangeRule) GetFieldName() string { return r.FieldName }
func (r RangeRule) GetRuleName() string  { return "range" }

// EmailRule validates email format
type EmailRule struct {
	FieldName string
	Strict    bool
}

func (r EmailRule) Validate(ctx context.Context, field string, value interface{}) []ValidationError {
	if r.FieldName != "" && field != r.FieldName {
		return nil
	}

	if value == nil {
		return nil
	}

	str := fmt.Sprintf("%v", value)
	if str == "" {
		return nil
	}

	// Basic email pattern
	pattern := `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`
	if r.Strict {
		// More strict pattern
		pattern = `^[a-zA-Z0-9]([a-zA-Z0-9._+-]*[a-zA-Z0-9])?@[a-zA-Z0-9]([a-zA-Z0-9.-]*[a-zA-Z0-9])?\.[a-zA-Z]{2,}$`
	}

	regex := regexp.MustCompile(pattern)
	if !regex.MatchString(str) {
		return []ValidationError{{
			Field:    field,
			Rule:     "email",
			Value:    value,
			Message:  "field must be a valid email address",
			Severity: MediumSeverity,
			Code:     "INVALID_EMAIL",
			Suggestions: []string{
				"Email format: user@domain.com",
			},
		}}
	}

	return nil
}

func (r EmailRule) GetFieldName() string { return r.FieldName }
func (r EmailRule) GetRuleName() string  { return "email" }

// SecurityRule validates for security issues
type SecurityRule struct {
	FieldName           string
	PreventSQLInjection bool
	PreventXSS          bool
	PreventCodeInjection bool
	MaxLength           int
}

func (r SecurityRule) Validate(ctx context.Context, field string, value interface{}) []ValidationError {
	if r.FieldName != "" && field != r.FieldName {
		return nil
	}

	if value == nil {
		return nil
	}

	str := fmt.Sprintf("%v", value)
	if str == "" {
		return nil
	}

	var errors []ValidationError

	// Check length first
	if r.MaxLength > 0 && len(str) > r.MaxLength {
		errors = append(errors, ValidationError{
			Field:    field,
			Rule:     "security",
			Value:    value,
			Message:  fmt.Sprintf("field exceeds maximum security length of %d characters", r.MaxLength),
			Severity: HighSeverity,
			Code:     "SECURITY_LENGTH_EXCEEDED",
		})
	}

	// SQL Injection patterns
	if r.PreventSQLInjection {
		sqlPatterns := []*regexp.Regexp{
			regexp.MustCompile(`(?i)\b(union|select|insert|update|delete|drop|create|alter|exec|execute)\b`),
			regexp.MustCompile(`(?i)(--|/\*|\*/|;|\bor\b|\band\b).*?(=|<|>)`),
			regexp.MustCompile(`(?i)\b(information_schema|sysobjects|sys\.tables)\b`),
		}

		for _, pattern := range sqlPatterns {
			if pattern.MatchString(str) {
				errors = append(errors, ValidationError{
					Field:    field,
					Rule:     "security",
					Value:    value,
					Message:  "field contains potentially dangerous SQL patterns",
					Severity: CriticalSeverity,
					Code:     "SQL_INJECTION_PATTERN",
				})
				break
			}
		}
	}

	// XSS patterns
	if r.PreventXSS {
		xssPatterns := []*regexp.Regexp{
			regexp.MustCompile(`(?i)<script[^>]*>.*?</script>`),
			regexp.MustCompile(`(?i)javascript:`),
			regexp.MustCompile(`(?i)on\w+\s*=`),
			regexp.MustCompile(`(?i)<iframe[^>]*>`),
		}

		for _, pattern := range xssPatterns {
			if pattern.MatchString(str) {
				errors = append(errors, ValidationError{
					Field:    field,
					Rule:     "security",
					Value:    value,
					Message:  "field contains potentially dangerous XSS patterns",
					Severity: HighSeverity,
					Code:     "XSS_PATTERN",
				})
				break
			}
		}
	}

	// Code injection patterns
	if r.PreventCodeInjection {
		codePatterns := []*regexp.Regexp{
			regexp.MustCompile(`(?i)(eval|exec|system|shell_exec|passthru)\s*\(`),
			regexp.MustCompile(`(?i)(\$\{|\#\{|<%=)`),
			regexp.MustCompile(`(?i)(import|require|include)\s+`),
		}

		for _, pattern := range codePatterns {
			if pattern.MatchString(str) {
				errors = append(errors, ValidationError{
					Field:    field,
					Rule:     "security",
					Value:    value,
					Message:  "field contains potentially dangerous code injection patterns",
					Severity: CriticalSeverity,
					Code:     "CODE_INJECTION_PATTERN",
				})
				break
			}
		}
	}

	return errors
}

func (r SecurityRule) GetFieldName() string { return r.FieldName }
func (r SecurityRule) GetRuleName() string  { return "security" }

// NewValidator creates a new validator instance
func NewValidator[T any](rules []ValidationRule) *Validator[T] {
	return &Validator[T]{
		rules:         rules,
		customRules:   make(map[string]CustomValidationFunc),
		errorMessages: make(map[string]string),
		failFast:      false,
	}
}

// AddRule adds a validation rule
func (v *Validator[T]) AddRule(rule ValidationRule) {
	v.rules = append(v.rules, rule)
}

// AddCustomRule adds a custom validation function
func (v *Validator[T]) AddCustomRule(name string, fn CustomValidationFunc) {
	v.customRules[name] = fn
}

// SetErrorMessage sets a custom error message for a rule
func (v *Validator[T]) SetErrorMessage(rule string, message string) {
	v.errorMessages[rule] = message
}

// SetFailFast sets whether validation should stop at first error
func (v *Validator[T]) SetFailFast(failFast bool) {
	v.failFast = failFast
}

// Validate validates an entity according to all rules
func (v *Validator[T]) Validate(ctx context.Context, entity T) error {
	result := v.ValidateWithResult(ctx, entity)
	if !result.Valid {
		return NewValidationError(result.Errors)
	}
	return nil
}

// ValidateWithResult validates and returns detailed results
func (v *Validator[T]) ValidateWithResult(ctx context.Context, entity T) *ValidationResult {
	result := &ValidationResult{
		Valid:      true,
		Errors:     []ValidationError{},
		Warnings:   []ValidationError{},
		FieldCount: 0,
		RuleCount:  len(v.rules),
	}

	// Use reflection to get struct fields
	entityValue := reflect.ValueOf(entity)
	entityType := reflect.TypeOf(entity)

	// Handle pointer types
	if entityType.Kind() == reflect.Ptr {
		if entityValue.IsNil() {
			result.Valid = false
			result.Errors = append(result.Errors, ValidationError{
				Field:    "entity",
				Rule:     "required",
				Value:    entity,
				Message:  "entity cannot be nil",
				Severity: CriticalSeverity,
				Code:     "NIL_ENTITY",
			})
			return result
		}
		entityValue = entityValue.Elem()
		entityType = entityType.Elem()
	}

	if entityType.Kind() != reflect.Struct {
		result.Valid = false
		result.Errors = append(result.Errors, ValidationError{
			Field:    "entity",
			Rule:     "type",
			Value:    entity,
			Message:  "entity must be a struct",
			Severity: CriticalSeverity,
			Code:     "INVALID_TYPE",
		})
		return result
	}

	// Validate each field
	for i := 0; i < entityType.NumField(); i++ {
		field := entityType.Field(i)
		fieldValue := entityValue.Field(i)

		// Skip unexported fields
		if !fieldValue.CanInterface() {
			continue
		}

		result.FieldCount++
		fieldName := field.Name

		// Get field tag for validation directives
		tag := field.Tag.Get("validate")
		if tag != "" {
			if tagErrors := v.validateByTag(ctx, fieldName, fieldValue.Interface(), tag); len(tagErrors) > 0 {
				for _, err := range tagErrors {
					if err.Severity == CriticalSeverity || err.Severity == HighSeverity {
						result.Valid = false
						result.Errors = append(result.Errors, err)
					} else {
						result.Warnings = append(result.Warnings, err)
					}

					if v.failFast && !result.Valid {
						return result
					}
				}
			}
		}

		// Apply rules
		for _, rule := range v.rules {
			ruleErrors := rule.Validate(ctx, fieldName, fieldValue.Interface())
			for _, err := range ruleErrors {
				if err.Severity == CriticalSeverity || err.Severity == HighSeverity {
					result.Valid = false
					result.Errors = append(result.Errors, err)
				} else {
					result.Warnings = append(result.Warnings, err)
				}

				if v.failFast && !result.Valid {
					return result
				}
			}
		}
	}

	// Apply custom rules
	for name, fn := range v.customRules {
		if err := fn(ctx, entity); err != nil {
			result.Valid = false
			result.Errors = append(result.Errors, ValidationError{
				Field:    "entity",
				Rule:     name,
				Value:    entity,
				Message:  err.Error(),
				Severity: HighSeverity,
				Code:     "CUSTOM_RULE_FAILED",
			})

			if v.failFast {
				return result
			}
		}
	}

	return result
}

// validateByTag validates field based on struct tag
func (v *Validator[T]) validateByTag(ctx context.Context, fieldName string, value interface{}, tag string) []ValidationError {
	var errors []ValidationError

	// Parse tag directives
	directives := strings.Split(tag, ",")
	for _, directive := range directives {
		directive = strings.TrimSpace(directive)

		switch {
		case directive == "required":
			rule := RequiredRule{FieldName: fieldName}
			errors = append(errors, rule.Validate(ctx, fieldName, value)...)

		case strings.HasPrefix(directive, "min="):
			if minStr := strings.TrimPrefix(directive, "min="); minStr != "" {
				if min, err := strconv.ParseFloat(minStr, 64); err == nil {
					rule := RangeRule{FieldName: fieldName, MinValue: &min}
					errors = append(errors, rule.Validate(ctx, fieldName, value)...)
				}
			}

		case strings.HasPrefix(directive, "max="):
			if maxStr := strings.TrimPrefix(directive, "max="); maxStr != "" {
				if max, err := strconv.ParseFloat(maxStr, 64); err == nil {
					rule := RangeRule{FieldName: fieldName, MaxValue: &max}
					errors = append(errors, rule.Validate(ctx, fieldName, value)...)
				}
			}

		case directive == "email":
			rule := EmailRule{FieldName: fieldName}
			errors = append(errors, rule.Validate(ctx, fieldName, value)...)

		case directive == "security":
			rule := SecurityRule{
				FieldName:           fieldName,
				PreventSQLInjection: true,
				PreventXSS:          true,
				PreventCodeInjection: true,
				MaxLength:           10000,
			}
			errors = append(errors, rule.Validate(ctx, fieldName, value)...)
		}
	}

	return errors
}

// NewValidationError creates a validation error from multiple validation errors
func NewValidationError(errors []ValidationError) error {
	if len(errors) == 0 {
		return nil
	}

	messages := make([]string, len(errors))
	for i, err := range errors {
		messages[i] = err.Error()
	}

	return errors[0] // Return first error, or create a composite error
}

// Helper functions for creating common rules

// Required creates a required field rule
func Required(fieldName string) RequiredRule {
	return RequiredRule{FieldName: fieldName}
}

// Length creates a length validation rule
func Length(fieldName string, min, max int) LengthRule {
	return LengthRule{FieldName: fieldName, MinLength: min, MaxLength: max}
}

// Range creates a numeric range rule
func Range(fieldName string, min, max float64) RangeRule {
	return RangeRule{FieldName: fieldName, MinValue: &min, MaxValue: &max}
}

// Email creates an email validation rule
func Email(fieldName string) EmailRule {
	return EmailRule{FieldName: fieldName}
}

// Security creates a security validation rule
func Security(fieldName string) SecurityRule {
	return SecurityRule{
		FieldName:           fieldName,
		PreventSQLInjection: true,
		PreventXSS:          true,
		PreventCodeInjection: true,
		MaxLength:           10000,
	}
}