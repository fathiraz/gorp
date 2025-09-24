// Package dialect provides database-specific SQL generators and optimizations
package dialect

import (
	"context"
	"fmt"
	"strings"
)

// Dialect defines the interface for database-specific SQL generation
type Dialect interface {
	// Name returns the dialect name
	Name() string

	// QuoteIdentifier quotes a database identifier (table name, column name, etc.)
	QuoteIdentifier(identifier string) string

	// Placeholder returns the placeholder string for parameter substitution
	Placeholder(index int) string

	// LimitClause generates a LIMIT clause for the dialect
	LimitClause(limit, offset int) string

	// CreateTableSQL generates CREATE TABLE SQL for the given table definition
	CreateTableSQL(table string, columns []ColumnDefinition) string

	// InsertSQL generates an INSERT SQL statement
	InsertSQL(table string, columns []string, returning bool) string

	// UpdateSQL generates an UPDATE SQL statement
	UpdateSQL(table string, columns []string, whereColumns []string) string

	// DeleteSQL generates a DELETE SQL statement
	DeleteSQL(table string, whereColumns []string) string

	// SupportsReturning returns true if the dialect supports RETURNING clause
	SupportsReturning() bool

	// SupportsBatchInsert returns true if the dialect supports batch inserts
	SupportsBatchInsert() bool

	// BatchInsertSQL generates SQL for batch insert operations
	BatchInsertSQL(table string, columns []string, batchSize int) string
}

// ColumnDefinition defines a database column
type ColumnDefinition struct {
	Name         string
	Type         string
	NotNull      bool
	PrimaryKey   bool
	AutoIncrement bool
	Default      *string
	Unique       bool
}

// BaseDialect provides common functionality for all dialects
type BaseDialect struct {
	name string
}

// NewBaseDialect creates a new BaseDialect
func NewBaseDialect(name string) *BaseDialect {
	return &BaseDialect{name: name}
}

func (d *BaseDialect) Name() string {
	return d.name
}

func (d *BaseDialect) QuoteIdentifier(identifier string) string {
	return fmt.Sprintf(`"%s"`, identifier)
}

func (d *BaseDialect) Placeholder(index int) string {
	return "?"
}

func (d *BaseDialect) LimitClause(limit, offset int) string {
	if limit > 0 && offset > 0 {
		return fmt.Sprintf(" LIMIT %d OFFSET %d", limit, offset)
	} else if limit > 0 {
		return fmt.Sprintf(" LIMIT %d", limit)
	}
	return ""
}

func (d *BaseDialect) SupportsReturning() bool {
	return false
}

func (d *BaseDialect) SupportsBatchInsert() bool {
	return true
}

func (d *BaseDialect) InsertSQL(table string, columns []string, returning bool) string {
	var sql strings.Builder
	sql.WriteString("INSERT INTO ")
	sql.WriteString(d.QuoteIdentifier(table))
	sql.WriteString(" (")

	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = d.QuoteIdentifier(col)
	}
	sql.WriteString(strings.Join(quotedColumns, ", "))
	sql.WriteString(") VALUES (")

	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = d.Placeholder(i + 1)
	}
	sql.WriteString(strings.Join(placeholders, ", "))
	sql.WriteString(")")

	return sql.String()
}

func (d *BaseDialect) UpdateSQL(table string, columns []string, whereColumns []string) string {
	var sql strings.Builder
	sql.WriteString("UPDATE ")
	sql.WriteString(d.QuoteIdentifier(table))
	sql.WriteString(" SET ")

	setPairs := make([]string, len(columns))
	for i, col := range columns {
		setPairs[i] = d.QuoteIdentifier(col) + " = " + d.Placeholder(i+1)
	}
	sql.WriteString(strings.Join(setPairs, ", "))

	if len(whereColumns) > 0 {
		sql.WriteString(" WHERE ")
		wherePairs := make([]string, len(whereColumns))
		for i, col := range whereColumns {
			wherePairs[i] = d.QuoteIdentifier(col) + " = " + d.Placeholder(len(columns)+i+1)
		}
		sql.WriteString(strings.Join(wherePairs, " AND "))
	}

	return sql.String()
}

func (d *BaseDialect) DeleteSQL(table string, whereColumns []string) string {
	var sql strings.Builder
	sql.WriteString("DELETE FROM ")
	sql.WriteString(d.QuoteIdentifier(table))

	if len(whereColumns) > 0 {
		sql.WriteString(" WHERE ")
		wherePairs := make([]string, len(whereColumns))
		for i, col := range whereColumns {
			wherePairs[i] = d.QuoteIdentifier(col) + " = " + d.Placeholder(i+1)
		}
		sql.WriteString(strings.Join(wherePairs, " AND "))
	}

	return sql.String()
}

func (d *BaseDialect) CreateTableSQL(table string, columns []ColumnDefinition) string {
	var sql strings.Builder
	sql.WriteString("CREATE TABLE ")
	sql.WriteString(d.QuoteIdentifier(table))
	sql.WriteString(" (")

	columnDefs := make([]string, len(columns))
	for i, col := range columns {
		columnDefs[i] = d.formatColumnDefinition(col)
	}
	sql.WriteString(strings.Join(columnDefs, ", "))
	sql.WriteString(")")

	return sql.String()
}

func (d *BaseDialect) formatColumnDefinition(col ColumnDefinition) string {
	var def strings.Builder
	def.WriteString(d.QuoteIdentifier(col.Name))
	def.WriteString(" ")
	def.WriteString(col.Type)

	if col.NotNull {
		def.WriteString(" NOT NULL")
	}

	if col.PrimaryKey {
		def.WriteString(" PRIMARY KEY")
	}

	if col.AutoIncrement {
		def.WriteString(" AUTO_INCREMENT")
	}

	if col.Default != nil {
		def.WriteString(" DEFAULT ")
		def.WriteString(*col.Default)
	}

	if col.Unique && !col.PrimaryKey {
		def.WriteString(" UNIQUE")
	}

	return def.String()
}

func (d *BaseDialect) BatchInsertSQL(table string, columns []string, batchSize int) string {
	var sql strings.Builder
	sql.WriteString("INSERT INTO ")
	sql.WriteString(d.QuoteIdentifier(table))
	sql.WriteString(" (")

	quotedColumns := make([]string, len(columns))
	for i, col := range columns {
		quotedColumns[i] = d.QuoteIdentifier(col)
	}
	sql.WriteString(strings.Join(quotedColumns, ", "))
	sql.WriteString(") VALUES ")

	valueSets := make([]string, batchSize)
	for i := 0; i < batchSize; i++ {
		placeholders := make([]string, len(columns))
		for j := range placeholders {
			placeholders[j] = d.Placeholder(i*len(columns) + j + 1)
		}
		valueSets[i] = "(" + strings.Join(placeholders, ", ") + ")"
	}
	sql.WriteString(strings.Join(valueSets, ", "))

	return sql.String()
}

// DialectRegistry manages available dialects
type DialectRegistry struct {
	dialects map[string]Dialect
}

// NewDialectRegistry creates a new dialect registry
func NewDialectRegistry() *DialectRegistry {
	return &DialectRegistry{
		dialects: make(map[string]Dialect),
	}
}

// Register registers a dialect
func (r *DialectRegistry) Register(name string, dialect Dialect) {
	r.dialects[name] = dialect
}

// Get retrieves a dialect by name
func (r *DialectRegistry) Get(name string) (Dialect, bool) {
	dialect, exists := r.dialects[name]
	return dialect, exists
}

// List returns all registered dialect names
func (r *DialectRegistry) List() []string {
	names := make([]string, 0, len(r.dialects))
	for name := range r.dialects {
		names = append(names, name)
	}
	return names
}

// DefaultRegistry is the default dialect registry
var DefaultRegistry = NewDialectRegistry()

// DialectContext provides context for dialect operations
type DialectContext struct {
	ctx     context.Context
	dialect Dialect
}

// NewDialectContext creates a new dialect context
func NewDialectContext(ctx context.Context, dialect Dialect) *DialectContext {
	return &DialectContext{
		ctx:     ctx,
		dialect: dialect,
	}
}

// Context returns the underlying context
func (dc *DialectContext) Context() context.Context {
	return dc.ctx
}

// Dialect returns the dialect
func (dc *DialectContext) Dialect() Dialect {
	return dc.dialect
}