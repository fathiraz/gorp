// Package mapping provides generic struct-to-table binding functionality
// using Go 1.24 generics to minimize reflection usage.
package mapping

import (
	"context"
	"reflect"
)

// TableMapper defines the interface for struct-to-table mapping operations
type TableMapper[T any] interface {
	// TableName returns the table name for type T
	TableName() string

	// ColumnMap returns the mapping between struct fields and database columns
	ColumnMap() map[string]string

	// PrimaryKey returns the primary key column name(s)
	PrimaryKey() []string

	// AutoIncrement returns whether the primary key is auto-incrementing
	AutoIncrement() bool
}

// DefaultTableMapper provides a default implementation of TableMapper
type DefaultTableMapper[T any] struct {
	tableName     string
	columnMap     map[string]string
	primaryKey    []string
	autoIncrement bool
}

// NewDefaultTableMapper creates a new DefaultTableMapper for type T
func NewDefaultTableMapper[T any](tableName string) *DefaultTableMapper[T] {
	return &DefaultTableMapper[T]{
		tableName:     tableName,
		columnMap:     make(map[string]string),
		primaryKey:    []string{"id"},
		autoIncrement: true,
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