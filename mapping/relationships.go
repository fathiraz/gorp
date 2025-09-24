package mapping

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
)

// Relationship types using type inference for compile-time safety
type RelationshipType int

const (
	OneToOneRelation RelationshipType = iota
	OneToManyRelation
	ManyToManyRelation
)

// OneToOne defines a one-to-one relationship between types T and R
type OneToOne[T, R Mappable] struct {
	SourceField string
	TargetField string
	ForeignKey  string
	Lazy        bool
}

// OneToMany defines a one-to-many relationship between types T and R
type OneToMany[T, R Mappable] struct {
	SourceField string
	TargetField string
	ForeignKey  string
	Lazy        bool
	OrderBy     string
}

// ManyToMany defines a many-to-many relationship between types T and R
type ManyToMany[T, R Mappable] struct {
	SourceField string
	TargetField string
	JoinTable   string
	SourceKey   string
	TargetKey   string
	Lazy        bool
	OrderBy     string
}

// RelationshipMapper manages relationships between types
type RelationshipMapper[T Mappable] struct {
	relationships map[string]interface{}
	ctx          context.Context
}

// NewRelationshipMapper creates a new relationship mapper
func NewRelationshipMapper[T Mappable](ctx context.Context) *RelationshipMapper[T] {
	return &RelationshipMapper[T]{
		relationships: make(map[string]interface{}),
		ctx:          ctx,
	}
}

// HasOne defines a one-to-one relationship
func (rm *RelationshipMapper[T]) HasOne(sourceField, targetField, foreignKey string, relType reflect.Type) *RelationshipMapper[T] {
	rel := map[string]interface{}{
		"type":        OneToOneRelation,
		"sourceField": sourceField,
		"targetField": targetField,
		"foreignKey":  foreignKey,
		"relType":     relType,
		"lazy":        true,
	}
	rm.relationships[sourceField] = rel
	return rm
}

// HasMany defines a one-to-many relationship
func (rm *RelationshipMapper[T]) HasMany(sourceField, targetField, foreignKey string, relType reflect.Type) *RelationshipMapper[T] {
	rel := map[string]interface{}{
		"type":        OneToManyRelation,
		"sourceField": sourceField,
		"targetField": targetField,
		"foreignKey":  foreignKey,
		"relType":     relType,
		"lazy":        true,
	}
	rm.relationships[sourceField] = rel
	return rm
}

// BelongsToMany defines a many-to-many relationship
func (rm *RelationshipMapper[T]) BelongsToMany(sourceField, targetField, joinTable, sourceKey, targetKey string, relType reflect.Type) *RelationshipMapper[T] {
	rel := map[string]interface{}{
		"type":        ManyToManyRelation,
		"sourceField": sourceField,
		"targetField": targetField,
		"joinTable":   joinTable,
		"sourceKey":   sourceKey,
		"targetKey":   targetKey,
		"relType":     relType,
		"lazy":        true,
	}
	rm.relationships[sourceField] = rel
	return rm
}

// GetRelationship returns a relationship by field name
func (rm *RelationshipMapper[T]) GetRelationship(fieldName string) (interface{}, bool) {
	rel, exists := rm.relationships[fieldName]
	return rel, exists
}

// GetRelationshipType returns the type of relationship
func (rm *RelationshipMapper[T]) GetRelationshipType(fieldName string) (RelationshipType, bool) {
	rel, exists := rm.relationships[fieldName]
	if !exists {
		return 0, false
	}

	if relMap, ok := rel.(map[string]interface{}); ok {
		if relType, ok := relMap["type"].(RelationshipType); ok {
			return relType, true
		}
	}

	return 0, false
}

// Complex type support with generic Scanner/Valuer interfaces

// JSONType wraps any type for JSON storage
type JSONType[T any] struct {
	Data  T
	Valid bool
}

// Scan implements the sql.Scanner interface for JSON types
func (j *JSONType[T]) Scan(value interface{}) error {
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
		return fmt.Errorf("cannot scan %T into JSONType", value)
	}

	if err := json.Unmarshal(data, &j.Data); err != nil {
		j.Valid = false
		return err
	}

	j.Valid = true
	return nil
}

// Value implements the driver.Valuer interface for JSON types
func (j JSONType[T]) Value() (driver.Value, error) {
	if !j.Valid {
		return nil, nil
	}

	return json.Marshal(j.Data)
}

// ArrayType wraps slice types for array storage (PostgreSQL)
type ArrayType[T any] struct {
	Data  []T
	Valid bool
}

// Scan implements the sql.Scanner interface for array types
func (a *ArrayType[T]) Scan(value interface{}) error {
	if value == nil {
		a.Valid = false
		return nil
	}

	// For PostgreSQL arrays, this would need database-specific parsing
	// This is a simplified implementation
	switch v := value.(type) {
	case []byte:
		return json.Unmarshal(v, &a.Data)
	case string:
		return json.Unmarshal([]byte(v), &a.Data)
	default:
		return fmt.Errorf("cannot scan %T into ArrayType", value)
	}
}

// Value implements the driver.Valuer interface for array types
func (a ArrayType[T]) Value() (driver.Value, error) {
	if !a.Valid {
		return nil, nil
	}

	// For PostgreSQL arrays, this would need database-specific formatting
	// This is a simplified JSON implementation
	return json.Marshal(a.Data)
}

// NullableType wraps any type to make it nullable
type NullableType[T any] struct {
	Data  T
	Valid bool
}

// Scan implements the sql.Scanner interface for nullable types
func (n *NullableType[T]) Scan(value interface{}) error {
	if value == nil {
		n.Valid = false
		return nil
	}

	// Use reflection to handle different types
	val := reflect.ValueOf(&n.Data).Elem()
	src := reflect.ValueOf(value)

	if !src.Type().ConvertibleTo(val.Type()) {
		return fmt.Errorf("cannot convert %T to %T", value, n.Data)
	}

	val.Set(src.Convert(val.Type()))
	n.Valid = true
	return nil
}

// Value implements the driver.Valuer interface for nullable types
func (n NullableType[T]) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}

	// Handle different types
	switch v := any(n.Data).(type) {
	case driver.Valuer:
		return v.Value()
	default:
		return n.Data, nil
	}
}

// TypeConstraints for complex type handling
type Scannable interface {
	Scan(value interface{}) error
}

type Valuable interface {
	Value() (driver.Value, error)
}

type ComplexType interface {
	Scannable
	Valuable
}

// ComplexTypeMapper handles custom type mapping
type ComplexTypeMapper struct {
	typeMap map[reflect.Type]ComplexType
}

// NewComplexTypeMapper creates a new complex type mapper
func NewComplexTypeMapper() *ComplexTypeMapper {
	return &ComplexTypeMapper{
		typeMap: make(map[reflect.Type]ComplexType),
	}
}

// RegisterType registers a custom type mapping
func (ctm *ComplexTypeMapper) RegisterType(goType reflect.Type, handler ComplexType) {
	ctm.typeMap[goType] = handler
}

// GetHandler returns a handler for the given type
func (ctm *ComplexTypeMapper) GetHandler(goType reflect.Type) (ComplexType, bool) {
	handler, exists := ctm.typeMap[goType]
	return handler, exists
}

// IsComplexType returns true if the type requires special handling
func (ctm *ComplexTypeMapper) IsComplexType(goType reflect.Type) bool {
	_, exists := ctm.typeMap[goType]
	if exists {
		return true
	}

	// Check for built-in complex types
	switch goType.Kind() {
	case reflect.Slice, reflect.Array:
		return goType.Elem().Kind() != reflect.Uint8 // not []byte
	case reflect.Map, reflect.Struct:
		// Don't treat time.Time as complex
		return goType != reflect.TypeOf((*driver.Valuer)(nil)).Elem()
	case reflect.Interface:
		return true
	default:
		return false
	}
}

// Global complex type mapper instance
var DefaultComplexTypeMapper = NewComplexTypeMapper()

// Convenience functions for creating complex types

// NewJSONType creates a new JSON type wrapper
func NewJSONType[T any](value T) JSONType[T] {
	return JSONType[T]{Data: value, Valid: true}
}

// NewArrayType creates a new array type wrapper
func NewArrayType[T any](value []T) ArrayType[T] {
	return ArrayType[T]{Data: value, Valid: true}
}

// NewNullableType creates a new nullable type wrapper
func NewNullableType[T any](value T) NullableType[T] {
	return NullableType[T]{Data: value, Valid: true}
}

// NewNullType creates a new null type wrapper
func NewNullType[T any]() NullableType[T] {
	var zero T
	return NullableType[T]{Data: zero, Valid: false}
}