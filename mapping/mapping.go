// Package mapping provides generic struct-to-table binding functionality
// using Go 1.24 generics to minimize reflection usage.
package mapping

import (
	"context"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// MappingError represents a mapping configuration error
type MappingError struct {
	Message string
}

func (e *MappingError) Error() string {
	return fmt.Sprintf("mapping error: %s", e.Message)
}

// NewMappingError creates a new mapping error
func NewMappingError(message string) *MappingError {
	return &MappingError{Message: message}
}

// Mappable defines types that can be mapped to database tables
type Mappable interface {
	~struct{}
}

// TableMapper defines the interface for struct-to-table mapping operations
type TableMapper[T Mappable] interface {
	// TableName returns the table name for type T
	TableName() string

	// ColumnMap returns the mapping between struct fields and database columns
	ColumnMap() map[string]string

	// PrimaryKey returns the primary key column name(s)
	PrimaryKey() []string

	// AutoIncrement returns whether the primary key is auto-incrementing
	AutoIncrement() bool

	// Validate validates the mapping configuration
	Validate() error

	// Schema returns the table schema definition
	Schema() SchemaDefinition
}

// ColumnMapper provides field-to-column binding with compile-time type safety
type ColumnMapper[T Mappable, F any] interface {
	// FieldName returns the struct field name
	FieldName() string

	// ColumnName returns the database column name
	ColumnName() string

	// FieldType returns the Go field type
	FieldType() reflect.Type

	// ColumnType returns the database column type
	ColumnType() string

	// IsNullable returns whether the column can be NULL
	IsNullable() bool

	// IsPrimaryKey returns whether this is a primary key field
	IsPrimaryKey() bool

	// IsAutoIncrement returns whether this field is auto-incrementing
	IsAutoIncrement() bool

	// HasDefault returns whether the column has a default value
	HasDefault() bool

	// DefaultValue returns the default value if any
	DefaultValue() interface{}

	// Validate validates the field mapping
	Validate() error
}

// SchemaDefinition represents a table schema
type SchemaDefinition struct {
	TableName string
	Columns   []ColumnDefinition
	Indexes   []IndexDefinition
	Constraints []ConstraintDefinition
}

// ColumnDefinition represents a column definition
type ColumnDefinition struct {
	Name         string
	Type         string
	Nullable     bool
	PrimaryKey   bool
	AutoIncrement bool
	Default      *string
	Unique       bool
	Length       *int
	Precision    *int
	Scale        *int
}

// IndexDefinition represents an index definition
type IndexDefinition struct {
	Name    string
	Columns []string
	Unique  bool
}

// ConstraintDefinition represents a constraint definition
type ConstraintDefinition struct {
	Name       string
	Type       string // CHECK, FOREIGN KEY, etc.
	Expression string
}

// DefaultTableMapper provides a default implementation of TableMapper
type DefaultTableMapper[T Mappable] struct {
	tableName     string
	columnMap     map[string]string
	primaryKey    []string
	autoIncrement bool
	schema        SchemaDefinition
	columns       []ColumnMapper[T, any]
}

// NewDefaultTableMapper creates a new DefaultTableMapper for type T
func NewDefaultTableMapper[T Mappable](tableName string) *DefaultTableMapper[T] {
	return &DefaultTableMapper[T]{
		tableName:     tableName,
		columnMap:     make(map[string]string),
		primaryKey:    []string{"id"},
		autoIncrement: true,
		schema: SchemaDefinition{
			TableName: tableName,
			Columns:   make([]ColumnDefinition, 0),
			Indexes:   make([]IndexDefinition, 0),
			Constraints: make([]ConstraintDefinition, 0),
		},
		columns: make([]ColumnMapper[T, any], 0),
	}
}

func (m *DefaultTableMapper[T]) TableName() string {
	return m.tableName
}

func (m *DefaultTableMapper[T]) ColumnMap() map[string]string {
	return m.columnMap
}

func (m *DefaultTableMapper[T]) PrimaryKey() []string {
	return m.primaryKey
}

func (m *DefaultTableMapper[T]) AutoIncrement() bool {
	return m.autoIncrement
}

func (m *DefaultTableMapper[T]) Validate() error {
	if m.tableName == "" {
		return NewMappingError("table name cannot be empty")
	}
	if len(m.primaryKey) == 0 {
		return NewMappingError("primary key cannot be empty")
	}
	return nil
}

func (m *DefaultTableMapper[T]) Schema() SchemaDefinition {
	return m.schema
}

// WithColumnMap sets the column mapping
func (m *DefaultTableMapper[T]) WithColumnMap(columnMap map[string]string) *DefaultTableMapper[T] {
	m.columnMap = columnMap
	return m
}

// WithPrimaryKey sets the primary key column(s)
func (m *DefaultTableMapper[T]) WithPrimaryKey(keys ...string) *DefaultTableMapper[T] {
	m.primaryKey = keys
	return m
}

// WithAutoIncrement sets whether the primary key is auto-incrementing
func (m *DefaultTableMapper[T]) WithAutoIncrement(auto bool) *DefaultTableMapper[T] {
	m.autoIncrement = auto
	return m
}

// WithSchema sets the schema definition
func (m *DefaultTableMapper[T]) WithSchema(schema SchemaDefinition) *DefaultTableMapper[T] {
	m.schema = schema
	return m
}

// AddColumn adds a column mapping
func (m *DefaultTableMapper[T]) AddColumn(column ColumnMapper[T, any]) *DefaultTableMapper[T] {
	m.columns = append(m.columns, column)
	return m
}

// MappingContext provides context for mapping operations
type MappingContext struct {
	ctx context.Context
}

// NewMappingContext creates a new MappingContext
func NewMappingContext(ctx context.Context) *MappingContext {
	return &MappingContext{ctx: ctx}
}

// Context returns the underlying context
func (mc *MappingContext) Context() context.Context {
	return mc.ctx
}

// StructInfo provides metadata about a struct type using minimal reflection
type StructInfo struct {
	Type   reflect.Type
	Fields []FieldInfo
}

// FieldInfo contains information about a struct field
type FieldInfo struct {
	Name         string
	Type         reflect.Type
	Tag          reflect.StructTag
	ColumnName   string
	IsPrimaryKey bool
}

// GetStructInfo extracts struct information for type T
func GetStructInfo[T any]() StructInfo {
	var zero T
	t := reflect.TypeOf(zero)

	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	info := StructInfo{
		Type:   t,
		Fields: make([]FieldInfo, 0, t.NumField()),
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		columnName := field.Tag.Get("db")
		if columnName == "" {
			columnName = field.Name
		}

		info.Fields = append(info.Fields, FieldInfo{
			Name:         field.Name,
			Type:         field.Type,
			Tag:          field.Tag,
			ColumnName:   columnName,
			IsPrimaryKey: field.Tag.Get("gorp") == "primarykey",
		})
	}

	return info
}

// DefaultColumnMapper provides a default implementation of ColumnMapper
type DefaultColumnMapper[T Mappable, F any] struct {
	fieldName     string
	columnName    string
	fieldType     reflect.Type
	columnType    string
	nullable      bool
	primaryKey    bool
	autoIncrement bool
	hasDefault    bool
	defaultValue  interface{}
}

// NewDefaultColumnMapper creates a new DefaultColumnMapper
func NewDefaultColumnMapper[T Mappable, F any](fieldName, columnName string) *DefaultColumnMapper[T, F] {
	var zero F
	fieldType := reflect.TypeOf(zero)

	return &DefaultColumnMapper[T, F]{
		fieldName:     fieldName,
		columnName:    columnName,
		fieldType:     fieldType,
		columnType:    inferColumnType(fieldType),
		nullable:      false,
		primaryKey:    false,
		autoIncrement: false,
		hasDefault:    false,
		defaultValue:  nil,
	}
}

func (c *DefaultColumnMapper[T, F]) FieldName() string {
	return c.fieldName
}

func (c *DefaultColumnMapper[T, F]) ColumnName() string {
	return c.columnName
}

func (c *DefaultColumnMapper[T, F]) FieldType() reflect.Type {
	return c.fieldType
}

func (c *DefaultColumnMapper[T, F]) ColumnType() string {
	return c.columnType
}

func (c *DefaultColumnMapper[T, F]) IsNullable() bool {
	return c.nullable
}

func (c *DefaultColumnMapper[T, F]) IsPrimaryKey() bool {
	return c.primaryKey
}

func (c *DefaultColumnMapper[T, F]) IsAutoIncrement() bool {
	return c.autoIncrement
}

func (c *DefaultColumnMapper[T, F]) HasDefault() bool {
	return c.hasDefault
}

func (c *DefaultColumnMapper[T, F]) DefaultValue() interface{} {
	return c.defaultValue
}

func (c *DefaultColumnMapper[T, F]) Validate() error {
	if c.fieldName == "" {
		return NewMappingError("field name cannot be empty")
	}
	if c.columnName == "" {
		return NewMappingError("column name cannot be empty")
	}
	return nil
}

// WithNullable sets whether the column is nullable
func (c *DefaultColumnMapper[T, F]) WithNullable(nullable bool) *DefaultColumnMapper[T, F] {
	c.nullable = nullable
	return c
}

// WithPrimaryKey sets whether this is a primary key field
func (c *DefaultColumnMapper[T, F]) WithPrimaryKey(primaryKey bool) *DefaultColumnMapper[T, F] {
	c.primaryKey = primaryKey
	return c
}

// WithAutoIncrement sets whether this field is auto-incrementing
func (c *DefaultColumnMapper[T, F]) WithAutoIncrement(autoIncrement bool) *DefaultColumnMapper[T, F] {
	c.autoIncrement = autoIncrement
	return c
}

// WithDefault sets the default value
func (c *DefaultColumnMapper[T, F]) WithDefault(value interface{}) *DefaultColumnMapper[T, F] {
	c.hasDefault = true
	c.defaultValue = value
	return c
}

// WithColumnType sets the database column type
func (c *DefaultColumnMapper[T, F]) WithColumnType(columnType string) *DefaultColumnMapper[T, F] {
	c.columnType = columnType
	return c
}

// inferColumnType infers the database column type from Go type
func inferColumnType(t reflect.Type) string {
	switch t.Kind() {
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32:
		return "INTEGER"
	case reflect.Int64:
		return "BIGINT"
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return "INTEGER"
	case reflect.Uint64:
		return "BIGINT"
	case reflect.Float32:
		return "REAL"
	case reflect.Float64:
		return "DOUBLE PRECISION"
	case reflect.String:
		return "VARCHAR(255)"
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return "BYTEA" // byte slice
		}
		return "TEXT" // JSON array or similar
	case reflect.Struct:
		if t == reflect.TypeOf(time.Time{}) {
			return "TIMESTAMP"
		}
		return "JSONB" // struct as JSON
	case reflect.Ptr:
		return inferColumnType(t.Elem())
	case reflect.Interface:
		return "JSONB"
	default:
		return "TEXT"
	}
}

// Tag parsing utilities
type FieldTag struct {
	ColumnName    string
	PrimaryKey    bool
	AutoIncrement bool
	NotNull       bool
	Unique        bool
	Index         bool
	Default       *string
	Type          string
	Size          int
	Precision     int
	Scale         int
	Ignore        bool
}

// ParseFieldTag parses struct field tags compatible with sqlx and pgx
func ParseFieldTag(tag reflect.StructTag) FieldTag {
	result := FieldTag{}

	// Parse db tag (sqlx/pgx compatible)
	if dbTag := tag.Get("db"); dbTag != "" {
		if dbTag == "-" {
			result.Ignore = true
			return result
		}
		result.ColumnName = dbTag
	}

	// Parse gorp tag for additional metadata
	if gorpTag := tag.Get("gorp"); gorpTag != "" {
		parts := strings.Split(gorpTag, ",")
		for _, part := range parts {
			part = strings.TrimSpace(part)
			switch {
			case part == "primarykey":
				result.PrimaryKey = true
			case part == "autoincrement":
				result.AutoIncrement = true
			case part == "notnull":
				result.NotNull = true
			case part == "unique":
				result.Unique = true
			case part == "index":
				result.Index = true
			case strings.HasPrefix(part, "size:"):
				if size := strings.TrimPrefix(part, "size:"); size != "" {
					fmt.Sscanf(size, "%d", &result.Size)
				}
			case strings.HasPrefix(part, "type:"):
				result.Type = strings.TrimPrefix(part, "type:")
			case strings.HasPrefix(part, "default:"):
				defaultVal := strings.TrimPrefix(part, "default:")
				result.Default = &defaultVal
			}
		}
	}

	return result
}

// CreateColumnMapperFromField creates a ColumnMapper from struct field info
func CreateColumnMapperFromField[T Mappable](field reflect.StructField) ColumnMapper[T, any] {
	tag := ParseFieldTag(field.Tag)

	columnName := tag.ColumnName
	if columnName == "" {
		columnName = field.Name
	}

	mapper := &DefaultColumnMapper[T, any]{
		fieldName:     field.Name,
		columnName:    columnName,
		fieldType:     field.Type,
		columnType:    tag.Type,
		nullable:      !tag.NotNull,
		primaryKey:    tag.PrimaryKey,
		autoIncrement: tag.AutoIncrement,
		hasDefault:    tag.Default != nil,
		defaultValue:  nil,
	}

	if mapper.columnType == "" {
		mapper.columnType = inferColumnType(field.Type)
	}

	if tag.Default != nil {
		mapper.defaultValue = *tag.Default
	}

	return mapper
}