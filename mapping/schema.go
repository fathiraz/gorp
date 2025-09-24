package mapping

import (
	"context"
	"fmt"
	"reflect"
	"strings"
)

// SchemaBuilder provides generic schema definition helpers
type SchemaBuilder[T Mappable] struct {
	mapper TableMapper[T]
	ctx    context.Context
}

// NewSchemaBuilder creates a new SchemaBuilder for type T
func NewSchemaBuilder[T Mappable](ctx context.Context) *SchemaBuilder[T] {
	var zero T
	tableName := getTableName(reflect.TypeOf(zero))
	mapper := NewDefaultTableMapper[T](tableName)

	return &SchemaBuilder[T]{
		mapper: mapper,
		ctx:    ctx,
	}
}

// WithTableMapper sets a custom table mapper
func (sb *SchemaBuilder[T]) WithTableMapper(mapper TableMapper[T]) *SchemaBuilder[T] {
	sb.mapper = mapper
	return sb
}

// CreateTable generates CREATE TABLE DDL for type T
func (sb *SchemaBuilder[T]) CreateTable() (string, error) {
	structInfo := GetStructInfo[T]()

	var columns []string
	var primaryKeys []string

	for _, field := range structInfo.Fields {
		tag := ParseFieldTag(field.Tag)
		if tag.Ignore {
			continue
		}

		columnName := tag.ColumnName
		if columnName == "" {
			columnName = field.Name
		}

		// Convert FieldInfo to reflect.StructField
		structField := reflect.StructField{
			Name: field.Name,
			Type: field.Type,
			Tag:  field.Tag,
		}

		columnDef := sb.buildColumnDefinition(structField, tag)
		columns = append(columns, columnDef)

		if tag.PrimaryKey {
			primaryKeys = append(primaryKeys, columnName)
		}
	}

	// Add primary key constraint if any
	if len(primaryKeys) > 0 {
		pkConstraint := fmt.Sprintf("PRIMARY KEY (%s)", strings.Join(primaryKeys, ", "))
		columns = append(columns, pkConstraint)
	}

	tableName := sb.mapper.TableName()
	ddl := fmt.Sprintf("CREATE TABLE %s (\n    %s\n)", tableName, strings.Join(columns, ",\n    "))

	return ddl, nil
}

// AlterTable generates ALTER TABLE DDL for type T
func (sb *SchemaBuilder[T]) AlterTable() *AlterTableBuilder[T] {
	return &AlterTableBuilder[T]{
		schemaBuilder: sb,
		operations:    make([]string, 0),
	}
}

// DropTable generates DROP TABLE DDL for type T
func (sb *SchemaBuilder[T]) DropTable(ifExists bool) string {
	tableName := sb.mapper.TableName()
	if ifExists {
		return fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	}
	return fmt.Sprintf("DROP TABLE %s", tableName)
}

// buildColumnDefinition builds a column definition string
func (sb *SchemaBuilder[T]) buildColumnDefinition(field reflect.StructField, tag FieldTag) string {
	columnName := tag.ColumnName
	if columnName == "" {
		columnName = field.Name
	}

	columnType := tag.Type
	if columnType == "" {
		columnType = inferColumnType(field.Type)
	}

	// Handle size specification
	if tag.Size > 0 {
		switch {
		case strings.Contains(columnType, "VARCHAR"):
			columnType = fmt.Sprintf("VARCHAR(%d)", tag.Size)
		case strings.Contains(columnType, "CHAR"):
			columnType = fmt.Sprintf("CHAR(%d)", tag.Size)
		}
	}

	var parts []string
	parts = append(parts, columnName, columnType)

	if tag.NotNull {
		parts = append(parts, "NOT NULL")
	}

	if tag.Unique {
		parts = append(parts, "UNIQUE")
	}

	if tag.AutoIncrement {
		parts = append(parts, "AUTO_INCREMENT")
	}

	if tag.Default != nil {
		parts = append(parts, fmt.Sprintf("DEFAULT %s", *tag.Default))
	}

	return strings.Join(parts, " ")
}

// AlterTableBuilder provides fluent interface for ALTER TABLE operations
type AlterTableBuilder[T Mappable] struct {
	schemaBuilder *SchemaBuilder[T]
	operations    []string
}

// AddColumn adds a column to the table
func (atb *AlterTableBuilder[T]) AddColumn(columnName, columnType string) *AlterTableBuilder[T] {
	op := fmt.Sprintf("ADD COLUMN %s %s", columnName, columnType)
	atb.operations = append(atb.operations, op)
	return atb
}

// DropColumn drops a column from the table
func (atb *AlterTableBuilder[T]) DropColumn(columnName string) *AlterTableBuilder[T] {
	op := fmt.Sprintf("DROP COLUMN %s", columnName)
	atb.operations = append(atb.operations, op)
	return atb
}

// ModifyColumn modifies a column definition
func (atb *AlterTableBuilder[T]) ModifyColumn(columnName, columnType string) *AlterTableBuilder[T] {
	op := fmt.Sprintf("MODIFY COLUMN %s %s", columnName, columnType)
	atb.operations = append(atb.operations, op)
	return atb
}

// RenameColumn renames a column
func (atb *AlterTableBuilder[T]) RenameColumn(oldName, newName string) *AlterTableBuilder[T] {
	op := fmt.Sprintf("RENAME COLUMN %s TO %s", oldName, newName)
	atb.operations = append(atb.operations, op)
	return atb
}

// AddIndex adds an index
func (atb *AlterTableBuilder[T]) AddIndex(indexName string, columns ...string) *AlterTableBuilder[T] {
	op := fmt.Sprintf("ADD INDEX %s (%s)", indexName, strings.Join(columns, ", "))
	atb.operations = append(atb.operations, op)
	return atb
}

// DropIndex drops an index
func (atb *AlterTableBuilder[T]) DropIndex(indexName string) *AlterTableBuilder[T] {
	op := fmt.Sprintf("DROP INDEX %s", indexName)
	atb.operations = append(atb.operations, op)
	return atb
}

// Build generates the ALTER TABLE DDL
func (atb *AlterTableBuilder[T]) Build() string {
	if len(atb.operations) == 0 {
		return ""
	}

	tableName := atb.schemaBuilder.mapper.TableName()
	return fmt.Sprintf("ALTER TABLE %s %s", tableName, strings.Join(atb.operations, ", "))
}

// getTableName infers table name from struct type
func getTableName(t reflect.Type) string {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	// Convert CamelCase to snake_case
	name := t.Name()
	var result strings.Builder

	for i, r := range name {
		if i > 0 && isUpper(r) {
			result.WriteRune('_')
		}
		result.WriteRune(toLower(r))
	}

	return result.String()
}

func isUpper(r rune) bool {
	return r >= 'A' && r <= 'Z'
}

func toLower(r rune) rune {
	if r >= 'A' && r <= 'Z' {
		return r + 32
	}
	return r
}

// CreateTable is a convenience function for creating table DDL
func CreateTable[T Mappable](ctx context.Context) (string, error) {
	builder := NewSchemaBuilder[T](ctx)
	return builder.CreateTable()
}

// AlterTable is a convenience function for altering table DDL
func AlterTable[T Mappable](ctx context.Context) *AlterTableBuilder[T] {
	builder := NewSchemaBuilder[T](ctx)
	return builder.AlterTable()
}

// DropTable is a convenience function for dropping table DDL
func DropTable[T Mappable](ctx context.Context, ifExists bool) string {
	builder := NewSchemaBuilder[T](ctx)
	return builder.DropTable(ifExists)
}