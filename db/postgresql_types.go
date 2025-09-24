package db

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
)

// PostgreSQL-specific type wrappers for native pgx integration

// JSONB represents a PostgreSQL JSONB column
type JSONB[T any] struct {
	Data  T
	Valid bool
}

// NewJSONB creates a new JSONB wrapper
func NewJSONB[T any](data T) JSONB[T] {
	return JSONB[T]{Data: data, Valid: true}
}

// Scan implements the pgx scanner interface for JSONB
func (j *JSONB[T]) Scan(value interface{}) error {
	if value == nil {
		j.Valid = false
		return nil
	}

	var data []byte
	switch v := value.(type) {
	case []byte:
		data = v
	case string:
		data = []byte(v)
	default:
		return fmt.Errorf("cannot scan %T into JSONB", value)
	}

	if err := json.Unmarshal(data, &j.Data); err != nil {
		j.Valid = false
		return err
	}

	j.Valid = true
	return nil
}

// Value implements the driver.Valuer interface for JSONB
func (j JSONB[T]) Value() (driver.Value, error) {
	if !j.Valid {
		return nil, nil
	}
	return json.Marshal(j.Data)
}

// PostgreSQLArray represents a PostgreSQL array column
type PostgreSQLArray[T any] struct {
	Elements []T
	Valid    bool
}

// NewPostgreSQLArray creates a new PostgreSQL array wrapper
func NewPostgreSQLArray[T any](elements []T) PostgreSQLArray[T] {
	return PostgreSQLArray[T]{Elements: elements, Valid: true}
}

// Scan implements the scanner interface for PostgreSQL arrays
func (a *PostgreSQLArray[T]) Scan(value interface{}) error {
	if value == nil {
		a.Valid = false
		return nil
	}

	switch v := value.(type) {
	case []byte:
		return a.parseArrayString(string(v))
	case string:
		return a.parseArrayString(v)
	default:
		return fmt.Errorf("cannot scan %T into PostgreSQLArray", value)
	}
}

// parseArrayString parses PostgreSQL array literal format
func (a *PostgreSQLArray[T]) parseArrayString(s string) error {
	if s == "" || s == "{}" {
		a.Elements = []T{}
		a.Valid = true
		return nil
	}

	// Remove outer braces
	s = strings.TrimPrefix(s, "{")
	s = strings.TrimSuffix(s, "}")

	if s == "" {
		a.Elements = []T{}
		a.Valid = true
		return nil
	}

	// Split by comma, handling quoted elements
	elements := parseArrayElements(s)
	a.Elements = make([]T, 0, len(elements))

	var zero T
	elemType := reflect.TypeOf(zero)

	for _, elem := range elements {
		if elem == "NULL" {
			var zeroVal T
			a.Elements = append(a.Elements, zeroVal)
			continue
		}

		// Convert string to appropriate type
		val, err := convertStringToType(elem, elemType)
		if err != nil {
			return err
		}

		if convertedVal, ok := val.(T); ok {
			a.Elements = append(a.Elements, convertedVal)
		} else {
			return fmt.Errorf("type conversion failed for element: %s", elem)
		}
	}

	a.Valid = true
	return nil
}

// Value implements the driver.Valuer interface for PostgreSQL arrays
func (a PostgreSQLArray[T]) Value() (driver.Value, error) {
	if !a.Valid {
		return nil, nil
	}

	if len(a.Elements) == 0 {
		return "{}", nil
	}

	elements := make([]string, len(a.Elements))
	for i, elem := range a.Elements {
		elemStr, err := formatArrayElement(elem)
		if err != nil {
			return nil, err
		}
		elements[i] = elemStr
	}

	return "{" + strings.Join(elements, ",") + "}", nil
}

// PostgreSQLUUID represents a PostgreSQL UUID
type PostgreSQLUUID struct {
	UUID  [16]byte
	Valid bool
}

// NewPostgreSQLUUID creates a new UUID wrapper
func NewPostgreSQLUUID(uuid [16]byte) PostgreSQLUUID {
	return PostgreSQLUUID{UUID: uuid, Valid: true}
}

// String returns the string representation of the UUID
func (u PostgreSQLUUID) String() string {
	if !u.Valid {
		return ""
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x",
		u.UUID[0:4], u.UUID[4:6], u.UUID[6:8], u.UUID[8:10], u.UUID[10:16])
}

// Scan implements the scanner interface for UUID
func (u *PostgreSQLUUID) Scan(value interface{}) error {
	if value == nil {
		u.Valid = false
		return nil
	}

	switch v := value.(type) {
	case string:
		return u.parseUUID(v)
	case []byte:
		return u.parseUUID(string(v))
	default:
		return fmt.Errorf("cannot scan %T into PostgreSQLUUID", value)
	}
}

// parseUUID parses a UUID string into the UUID array
func (u *PostgreSQLUUID) parseUUID(s string) error {
	// Remove hyphens
	s = strings.ReplaceAll(s, "-", "")
	if len(s) != 32 {
		return fmt.Errorf("invalid UUID length: %d", len(s))
	}

	for i := 0; i < 16; i++ {
		_, err := fmt.Sscanf(s[i*2:i*2+2], "%02x", &u.UUID[i])
		if err != nil {
			return err
		}
	}

	u.Valid = true
	return nil
}

// Value implements the driver.Valuer interface for UUID
func (u PostgreSQLUUID) Value() (driver.Value, error) {
	if !u.Valid {
		return nil, nil
	}
	return u.String(), nil
}

// PostgreSQLHStore represents a PostgreSQL HSTORE column
type PostgreSQLHStore struct {
	Map   map[string]*string
	Valid bool
}

// NewPostgreSQLHStore creates a new HSTORE wrapper
func NewPostgreSQLHStore(m map[string]*string) PostgreSQLHStore {
	return PostgreSQLHStore{Map: m, Valid: true}
}

// Scan implements the scanner interface for HSTORE
func (h *PostgreSQLHStore) Scan(value interface{}) error {
	if value == nil {
		h.Valid = false
		return nil
	}

	var s string
	switch v := value.(type) {
	case string:
		s = v
	case []byte:
		s = string(v)
	default:
		return fmt.Errorf("cannot scan %T into PostgreSQLHStore", value)
	}

	h.Map = parseHStore(s)
	h.Valid = true
	return nil
}

// Value implements the driver.Valuer interface for HSTORE
func (h PostgreSQLHStore) Value() (driver.Value, error) {
	if !h.Valid {
		return nil, nil
	}

	if len(h.Map) == 0 {
		return "", nil
	}

	parts := make([]string, 0, len(h.Map))
	for key, value := range h.Map {
		if value == nil {
			parts = append(parts, fmt.Sprintf(`"%s"=>NULL`, escapeHStoreKey(key)))
		} else {
			parts = append(parts, fmt.Sprintf(`"%s"=>"%s"`, escapeHStoreKey(key), escapeHStoreValue(*value)))
		}
	}

	return strings.Join(parts, ","), nil
}

// PostgreSQLTypeMapper provides type mapping utilities for PostgreSQL
type PostgreSQLTypeMapper struct {
	customTypes map[reflect.Type]pgtype.Codec
}

// NewPostgreSQLTypeMapper creates a new type mapper
func NewPostgreSQLTypeMapper() *PostgreSQLTypeMapper {
	return &PostgreSQLTypeMapper{
		customTypes: make(map[reflect.Type]pgtype.Codec),
	}
}

// RegisterType registers a custom type mapping
func (ptm *PostgreSQLTypeMapper) RegisterType(goType reflect.Type, codec pgtype.Codec) {
	ptm.customTypes[goType] = codec
}

// GetCodec returns the codec for a given type
func (ptm *PostgreSQLTypeMapper) GetCodec(goType reflect.Type) (pgtype.Codec, bool) {
	codec, exists := ptm.customTypes[goType]
	return codec, exists
}

// Helper functions

func parseArrayElements(s string) []string {
	var elements []string
	var current strings.Builder
	inQuotes := false
	escaped := false

	for _, r := range s {
		switch {
		case escaped:
			current.WriteRune(r)
			escaped = false
		case r == '\\':
			escaped = true
			current.WriteRune(r)
		case r == '"':
			inQuotes = !inQuotes
			current.WriteRune(r)
		case r == ',' && !inQuotes:
			elements = append(elements, strings.TrimSpace(current.String()))
			current.Reset()
		default:
			current.WriteRune(r)
		}
	}

	if current.Len() > 0 {
		elements = append(elements, strings.TrimSpace(current.String()))
	}

	return elements
}

func convertStringToType(s string, targetType reflect.Type) (interface{}, error) {
	// Remove quotes if present
	s = strings.Trim(s, `"`)

	switch targetType.Kind() {
	case reflect.String:
		return s, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		var result int64
		_, err := fmt.Sscanf(s, "%d", &result)
		return result, err
	case reflect.Float32, reflect.Float64:
		var result float64
		_, err := fmt.Sscanf(s, "%f", &result)
		return result, err
	case reflect.Bool:
		return s == "t" || s == "true", nil
	default:
		return s, nil
	}
}

func formatArrayElement(elem interface{}) (string, error) {
	switch v := elem.(type) {
	case string:
		// Escape quotes and backslashes
		escaped := strings.ReplaceAll(v, `\`, `\\`)
		escaped = strings.ReplaceAll(escaped, `"`, `\"`)
		return `"` + escaped + `"`, nil
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v), nil
	case float32, float64:
		return fmt.Sprintf("%g", v), nil
	case bool:
		if v {
			return "t", nil
		}
		return "f", nil
	default:
		return fmt.Sprintf(`"%v"`, v), nil
	}
}

func parseHStore(s string) map[string]*string {
	result := make(map[string]*string)
	if s == "" {
		return result
	}

	// Simple HSTORE parser - in production, you'd want a more robust parser
	pairs := strings.Split(s, ",")
	for _, pair := range pairs {
		parts := strings.SplitN(strings.TrimSpace(pair), "=>", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.Trim(strings.TrimSpace(parts[0]), `"`)
		valueStr := strings.TrimSpace(parts[1])

		if valueStr == "NULL" {
			result[key] = nil
		} else {
			value := strings.Trim(valueStr, `"`)
			result[key] = &value
		}
	}

	return result
}

func escapeHStoreKey(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), `"`, `\"`)
}

func escapeHStoreValue(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), `"`, `\"`)
}