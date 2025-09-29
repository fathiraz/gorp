// Package query provides fluent, type-safe query builders using Go 1.24 generics
package query

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/fathiraz/gorp/mapping"
)

// Queryable defines types that can be queried
type Queryable interface {
	mapping.Mappable
}

// QueryBuilder provides a generic interface for all query builders
type QueryBuilder[T Queryable] interface {
	// Build constructs the SQL query string and arguments
	Build() (string, []interface{}, error)

	// Context returns the query context
	Context() context.Context

	// WithContext sets a new context
	WithContext(ctx context.Context) QueryBuilder[T]

	// Validate validates the query configuration
	Validate() error
}

// BaseQueryBuilder provides common functionality for all query builders
type BaseQueryBuilder[T Queryable] struct {
	ctx         context.Context
	mapper      mapping.TableMapper[T]
	args        []interface{}
	hints       []string
	parameters  map[string]interface{}
}

// NewBaseQueryBuilder creates a new BaseQueryBuilder for type T
func NewBaseQueryBuilder[T Queryable](ctx context.Context, mapper mapping.TableMapper[T]) *BaseQueryBuilder[T] {
	return &BaseQueryBuilder[T]{
		ctx:        ctx,
		mapper:     mapper,
		args:       make([]interface{}, 0),
		hints:      make([]string, 0),
		parameters: make(map[string]interface{}),
	}
}

func (bqb *BaseQueryBuilder[T]) Context() context.Context {
	return bqb.ctx
}

func (bqb *BaseQueryBuilder[T]) WithHint(hint string) *BaseQueryBuilder[T] {
	bqb.hints = append(bqb.hints, hint)
	return bqb
}

func (bqb *BaseQueryBuilder[T]) WithParameter(name string, value interface{}) *BaseQueryBuilder[T] {
	bqb.parameters[name] = value
	return bqb
}

func (bqb *BaseQueryBuilder[T]) TableName() string {
	return bqb.mapper.TableName()
}

// Supporting types for query building

// JoinType represents different types of SQL joins
type JoinType string

const (
	InnerJoin JoinType = "INNER JOIN"
	LeftJoin  JoinType = "LEFT JOIN"
	RightJoin JoinType = "RIGHT JOIN"
	FullJoin  JoinType = "FULL JOIN"
)

// JoinClause represents a JOIN clause in a SQL query
type JoinClause struct {
	Type      JoinType
	Table     string
	Alias     string
	Condition string
	Args      []interface{}
}

// OrderDirection represents sort direction
type OrderDirection string

const (
	ASC  OrderDirection = "ASC"
	DESC OrderDirection = "DESC"
)

// Condition represents a WHERE/HAVING condition
type Condition struct {
	Expression string
	Args       []interface{}
	Operator   string // AND, OR
}

// SubQuery represents a subquery
type SubQuery[T Queryable] struct {
	Builder *SelectQueryBuilder[T]
	Alias   string
}

// SelectQueryBuilder provides a fluent interface for SELECT queries
type SelectQueryBuilder[T Queryable] struct {
	*BaseQueryBuilder[T]
	selectFields  []string
	joins         []JoinClause
	whereClause   []string
	groupByClause []string
	havingClause  []string
	orderByClause []string
	limitValue    *int
	offsetValue   *int
	distinct      bool
	subQueries    map[string]*SelectQueryBuilder[T]
}

// NewSelectQueryBuilder creates a new SelectQueryBuilder for type T
func NewSelectQueryBuilder[T Queryable](ctx context.Context, mapper mapping.TableMapper[T]) *SelectQueryBuilder[T] {
	return &SelectQueryBuilder[T]{
		BaseQueryBuilder: NewBaseQueryBuilder(ctx, mapper),
		selectFields:     make([]string, 0),
		joins:            make([]JoinClause, 0),
		whereClause:      make([]string, 0),
		groupByClause:    make([]string, 0),
		havingClause:     make([]string, 0),
		orderByClause:    make([]string, 0),
		distinct:         false,
		subQueries:       make(map[string]*SelectQueryBuilder[T]),
	}
}

// Select specifies the columns to select
func (sqb *SelectQueryBuilder[T]) Select(fields ...string) *SelectQueryBuilder[T] {
	sqb.selectFields = append(sqb.selectFields, fields...)
	return sqb
}

// SelectDistinct enables DISTINCT for the query
func (sqb *SelectQueryBuilder[T]) SelectDistinct(fields ...string) *SelectQueryBuilder[T] {
	sqb.distinct = true
	sqb.selectFields = append(sqb.selectFields, fields...)
	return sqb
}

// Where adds a WHERE condition
func (sqb *SelectQueryBuilder[T]) Where(condition string, args ...interface{}) *SelectQueryBuilder[T] {
	sqb.whereClause = append(sqb.whereClause, condition)
	sqb.args = append(sqb.args, args...)
	return sqb
}

// WhereEq adds a WHERE condition with equality
func (sqb *SelectQueryBuilder[T]) WhereEq(column string, value interface{}) *SelectQueryBuilder[T] {
	return sqb.Where(column+" = ?", value)
}

// WhereIn adds a WHERE IN condition
func (sqb *SelectQueryBuilder[T]) WhereIn(column string, values ...interface{}) *SelectQueryBuilder[T] {
	if len(values) == 0 {
		return sqb
	}

	placeholders := make([]string, len(values))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	condition := fmt.Sprintf("%s IN (%s)", column, strings.Join(placeholders, ", "))
	return sqb.Where(condition, values...)
}

// Join adds an INNER JOIN clause
func (sqb *SelectQueryBuilder[T]) Join(table, condition string, args ...interface{}) *SelectQueryBuilder[T] {
	return sqb.JoinWithType(InnerJoin, table, "", condition, args...)
}

// LeftJoin adds a LEFT JOIN clause
func (sqb *SelectQueryBuilder[T]) LeftJoin(table, condition string, args ...interface{}) *SelectQueryBuilder[T] {
	return sqb.JoinWithType(LeftJoin, table, "", condition, args...)
}

// RightJoin adds a RIGHT JOIN clause
func (sqb *SelectQueryBuilder[T]) RightJoin(table, condition string, args ...interface{}) *SelectQueryBuilder[T] {
	return sqb.JoinWithType(RightJoin, table, "", condition, args...)
}

// FullJoin adds a FULL JOIN clause
func (sqb *SelectQueryBuilder[T]) FullJoin(table, condition string, args ...interface{}) *SelectQueryBuilder[T] {
	return sqb.JoinWithType(FullJoin, table, "", condition, args...)
}

// JoinWithType adds a JOIN clause with specified type
func (sqb *SelectQueryBuilder[T]) JoinWithType(joinType JoinType, table, alias, condition string, args ...interface{}) *SelectQueryBuilder[T] {
	join := JoinClause{
		Type:      joinType,
		Table:     table,
		Alias:     alias,
		Condition: condition,
		Args:      args,
	}
	sqb.joins = append(sqb.joins, join)
	sqb.args = append(sqb.args, args...)
	return sqb
}

// GroupBy adds a GROUP BY clause
func (sqb *SelectQueryBuilder[T]) GroupBy(columns ...string) *SelectQueryBuilder[T] {
	sqb.groupByClause = append(sqb.groupByClause, columns...)
	return sqb
}

// Having adds a HAVING clause
func (sqb *SelectQueryBuilder[T]) Having(condition string, args ...interface{}) *SelectQueryBuilder[T] {
	sqb.havingClause = append(sqb.havingClause, condition)
	sqb.args = append(sqb.args, args...)
	return sqb
}

// OrderBy adds an ORDER BY clause
func (sqb *SelectQueryBuilder[T]) OrderBy(column string, direction ...OrderDirection) *SelectQueryBuilder[T] {
	orderBy := column
	if len(direction) > 0 {
		orderBy += " " + string(direction[0])
	}
	sqb.orderByClause = append(sqb.orderByClause, orderBy)
	return sqb
}

// Limit sets the LIMIT clause
func (sqb *SelectQueryBuilder[T]) Limit(limit int) *SelectQueryBuilder[T] {
	sqb.limitValue = &limit
	return sqb
}

// Offset sets the OFFSET clause
func (sqb *SelectQueryBuilder[T]) Offset(offset int) *SelectQueryBuilder[T] {
	sqb.offsetValue = &offset
	return sqb
}

// Paginate sets both LIMIT and OFFSET for pagination
func (sqb *SelectQueryBuilder[T]) Paginate(limit, offset int) *SelectQueryBuilder[T] {
	sqb.limitValue = &limit
	sqb.offsetValue = &offset
	return sqb
}

// WithContext sets a new context
func (sqb *SelectQueryBuilder[T]) WithContext(ctx context.Context) QueryBuilder[T] {
	sqb.ctx = ctx
	return sqb
}

// SubQuery adds a subquery with an alias
func (sqb *SelectQueryBuilder[T]) SubQuery(alias string, subQuery *SelectQueryBuilder[T]) *SelectQueryBuilder[T] {
	sqb.subQueries[alias] = subQuery
	return sqb
}

// Build constructs the SQL query string
func (sqb *SelectQueryBuilder[T]) Build() (string, []interface{}, error) {
	if err := sqb.Validate(); err != nil {
		return "", nil, err
	}

	var query strings.Builder
	var allArgs []interface{}

	// SELECT clause
	query.WriteString("SELECT")
	if sqb.distinct {
		query.WriteString(" DISTINCT")
	}

	if len(sqb.selectFields) > 0 {
		query.WriteString(" ")
		query.WriteString(strings.Join(sqb.selectFields, ", "))
	} else {
		query.WriteString(" *")
	}

	// FROM clause
	query.WriteString(" FROM ")
	query.WriteString(sqb.TableName())

	// JOIN clauses
	for _, join := range sqb.joins {
		query.WriteString(" ")
		query.WriteString(string(join.Type))
		query.WriteString(" ")
		query.WriteString(join.Table)
		if join.Alias != "" {
			query.WriteString(" AS ")
			query.WriteString(join.Alias)
		}
		query.WriteString(" ON ")
		query.WriteString(join.Condition)
		allArgs = append(allArgs, join.Args...)
	}

	// WHERE clause
	if len(sqb.whereClause) > 0 {
		query.WriteString(" WHERE ")
		query.WriteString(strings.Join(sqb.whereClause, " AND "))
		allArgs = append(allArgs, sqb.args...)
	}

	// GROUP BY clause
	if len(sqb.groupByClause) > 0 {
		query.WriteString(" GROUP BY ")
		query.WriteString(strings.Join(sqb.groupByClause, ", "))
	}

	// HAVING clause
	if len(sqb.havingClause) > 0 {
		query.WriteString(" HAVING ")
		query.WriteString(strings.Join(sqb.havingClause, " AND "))
	}

	// ORDER BY clause
	if len(sqb.orderByClause) > 0 {
		query.WriteString(" ORDER BY ")
		query.WriteString(strings.Join(sqb.orderByClause, ", "))
	}

	// LIMIT clause
	if sqb.limitValue != nil {
		query.WriteString(fmt.Sprintf(" LIMIT %d", *sqb.limitValue))
	}

	// OFFSET clause
	if sqb.offsetValue != nil {
		query.WriteString(fmt.Sprintf(" OFFSET %d", *sqb.offsetValue))
	}

	return query.String(), allArgs, nil
}

// Validate validates the query configuration
func (sqb *SelectQueryBuilder[T]) Validate() error {
	if sqb.mapper == nil {
		return fmt.Errorf("table mapper is required")
	}
	return nil
}

// InsertQueryBuilder provides a fluent interface for building INSERT queries
type InsertQueryBuilder[T Queryable] struct {
	*BaseQueryBuilder[T]
	columns      []string
	values       [][]interface{}
	onConflict   string
	returning    []string
	batchInsert  bool
}

// NewInsertQueryBuilder creates a new InsertQueryBuilder for type T
func NewInsertQueryBuilder[T Queryable](ctx context.Context, mapper mapping.TableMapper[T]) *InsertQueryBuilder[T] {
	return &InsertQueryBuilder[T]{
		BaseQueryBuilder: NewBaseQueryBuilder(ctx, mapper),
		columns:          make([]string, 0),
		values:           make([][]interface{}, 0),
		returning:        make([]string, 0),
		batchInsert:      false,
	}
}

// Columns specifies the columns to insert into
func (iqb *InsertQueryBuilder[T]) Columns(columns ...string) *InsertQueryBuilder[T] {
	iqb.columns = columns
	return iqb
}

// Values adds values to insert (single row)
func (iqb *InsertQueryBuilder[T]) Values(values ...interface{}) *InsertQueryBuilder[T] {
	iqb.values = append(iqb.values, values)
	return iqb
}

// BatchValues adds multiple rows of values for batch insert
func (iqb *InsertQueryBuilder[T]) BatchValues(valueRows [][]interface{}) *InsertQueryBuilder[T] {
	iqb.values = append(iqb.values, valueRows...)
	iqb.batchInsert = true
	return iqb
}

// OnConflict adds ON CONFLICT clause (PostgreSQL)
func (iqb *InsertQueryBuilder[T]) OnConflict(action string) *InsertQueryBuilder[T] {
	iqb.onConflict = action
	return iqb
}

// Returning adds RETURNING clause
func (iqb *InsertQueryBuilder[T]) Returning(columns ...string) *InsertQueryBuilder[T] {
	iqb.returning = columns
	return iqb
}

// WithContext sets a new context
func (iqb *InsertQueryBuilder[T]) WithContext(ctx context.Context) QueryBuilder[T] {
	iqb.ctx = ctx
	return iqb
}

// Build constructs the INSERT SQL query string
func (iqb *InsertQueryBuilder[T]) Build() (string, []interface{}, error) {
	if err := iqb.Validate(); err != nil {
		return "", nil, err
	}

	var query strings.Builder
	var allArgs []interface{}

	query.WriteString("INSERT INTO ")
	query.WriteString(iqb.TableName())

	if len(iqb.columns) > 0 {
		query.WriteString(" (")
		query.WriteString(strings.Join(iqb.columns, ", "))
		query.WriteString(")")
	}

	if len(iqb.values) > 0 {
		query.WriteString(" VALUES ")

		valueSets := make([]string, len(iqb.values))
		for i, valueRow := range iqb.values {
			placeholders := make([]string, len(valueRow))
			for j := range placeholders {
				placeholders[j] = "?"
			}
			valueSets[i] = "(" + strings.Join(placeholders, ", ") + ")"
			allArgs = append(allArgs, valueRow...)
		}
		query.WriteString(strings.Join(valueSets, ", "))
	}

	// ON CONFLICT clause (PostgreSQL)
	if iqb.onConflict != "" {
		query.WriteString(" ON CONFLICT ")
		query.WriteString(iqb.onConflict)
	}

	// RETURNING clause
	if len(iqb.returning) > 0 {
		query.WriteString(" RETURNING ")
		query.WriteString(strings.Join(iqb.returning, ", "))
	}

	return query.String(), allArgs, nil
}

// Validate validates the query configuration
func (iqb *InsertQueryBuilder[T]) Validate() error {
	if iqb.mapper == nil {
		return fmt.Errorf("table mapper is required")
	}
	if len(iqb.values) == 0 {
		return fmt.Errorf("no values specified for insert")
	}
	if len(iqb.columns) > 0 && len(iqb.values) > 0 && len(iqb.columns) != len(iqb.values[0]) {
		return fmt.Errorf("column count doesn't match value count")
	}
	return nil
}

// UpdateQueryBuilder provides a fluent interface for building UPDATE queries
type UpdateQueryBuilder[T Queryable] struct {
	*BaseQueryBuilder[T]
	setClause   map[string]interface{}
	whereClause []string
	joins       []JoinClause
	returning   []string
}

// NewUpdateQueryBuilder creates a new UpdateQueryBuilder for type T
func NewUpdateQueryBuilder[T Queryable](ctx context.Context, mapper mapping.TableMapper[T]) *UpdateQueryBuilder[T] {
	return &UpdateQueryBuilder[T]{
		BaseQueryBuilder: NewBaseQueryBuilder(ctx, mapper),
		setClause:        make(map[string]interface{}),
		whereClause:      make([]string, 0),
		joins:            make([]JoinClause, 0),
		returning:        make([]string, 0),
	}
}

// Set adds a SET clause
func (uqb *UpdateQueryBuilder[T]) Set(column string, value interface{}) *UpdateQueryBuilder[T] {
	uqb.setClause[column] = value
	return uqb
}

// SetMany adds multiple SET clauses
func (uqb *UpdateQueryBuilder[T]) SetMany(values map[string]interface{}) *UpdateQueryBuilder[T] {
	for column, value := range values {
		uqb.setClause[column] = value
	}
	return uqb
}

// Where adds a WHERE condition
func (uqb *UpdateQueryBuilder[T]) Where(condition string, args ...interface{}) *UpdateQueryBuilder[T] {
	uqb.whereClause = append(uqb.whereClause, condition)
	uqb.args = append(uqb.args, args...)
	return uqb
}

// WhereEq adds a WHERE condition with equality
func (uqb *UpdateQueryBuilder[T]) WhereEq(column string, value interface{}) *UpdateQueryBuilder[T] {
	return uqb.Where(column+" = ?", value)
}

// Join adds an INNER JOIN clause
func (uqb *UpdateQueryBuilder[T]) Join(table, condition string, args ...interface{}) *UpdateQueryBuilder[T] {
	return uqb.JoinWithType(InnerJoin, table, "", condition, args...)
}

// LeftJoin adds a LEFT JOIN clause
func (uqb *UpdateQueryBuilder[T]) LeftJoin(table, condition string, args ...interface{}) *UpdateQueryBuilder[T] {
	return uqb.JoinWithType(LeftJoin, table, "", condition, args...)
}

// JoinWithType adds a JOIN clause with specified type
func (uqb *UpdateQueryBuilder[T]) JoinWithType(joinType JoinType, table, alias, condition string, args ...interface{}) *UpdateQueryBuilder[T] {
	join := JoinClause{
		Type:      joinType,
		Table:     table,
		Alias:     alias,
		Condition: condition,
		Args:      args,
	}
	uqb.joins = append(uqb.joins, join)
	return uqb
}

// Returning adds RETURNING clause
func (uqb *UpdateQueryBuilder[T]) Returning(columns ...string) *UpdateQueryBuilder[T] {
	uqb.returning = columns
	return uqb
}

// WithContext sets a new context
func (uqb *UpdateQueryBuilder[T]) WithContext(ctx context.Context) QueryBuilder[T] {
	uqb.ctx = ctx
	return uqb
}

// Build constructs the UPDATE SQL query string
func (uqb *UpdateQueryBuilder[T]) Build() (string, []interface{}, error) {
	if err := uqb.Validate(); err != nil {
		return "", nil, err
	}

	var query strings.Builder
	var allArgs []interface{}

	query.WriteString("UPDATE ")
	query.WriteString(uqb.TableName())

	// JOIN clauses (for UPDATE with JOINs)
	for _, join := range uqb.joins {
		query.WriteString(" ")
		query.WriteString(string(join.Type))
		query.WriteString(" ")
		query.WriteString(join.Table)
		if join.Alias != "" {
			query.WriteString(" AS ")
			query.WriteString(join.Alias)
		}
		query.WriteString(" ON ")
		query.WriteString(join.Condition)
		allArgs = append(allArgs, join.Args...)
	}

	query.WriteString(" SET ")

	setPairs := make([]string, 0, len(uqb.setClause))
	for column, value := range uqb.setClause {
		setPairs = append(setPairs, column+" = ?")
		allArgs = append(allArgs, value)
	}
	query.WriteString(strings.Join(setPairs, ", "))

	// WHERE clause
	if len(uqb.whereClause) > 0 {
		query.WriteString(" WHERE ")
		query.WriteString(strings.Join(uqb.whereClause, " AND "))
		allArgs = append(allArgs, uqb.args...)
	}

	// RETURNING clause
	if len(uqb.returning) > 0 {
		query.WriteString(" RETURNING ")
		query.WriteString(strings.Join(uqb.returning, ", "))
	}

	return query.String(), allArgs, nil
}

// Validate validates the query configuration
func (uqb *UpdateQueryBuilder[T]) Validate() error {
	if uqb.mapper == nil {
		return fmt.Errorf("table mapper is required")
	}
	if len(uqb.setClause) == 0 {
		return fmt.Errorf("no SET clauses specified for update")
	}
	return nil
}

// DeleteQueryBuilder provides a fluent interface for building DELETE queries
type DeleteQueryBuilder[T Queryable] struct {
	*BaseQueryBuilder[T]
	whereClause []string
	joins       []JoinClause
	returning   []string
}

// NewDeleteQueryBuilder creates a new DeleteQueryBuilder for type T
func NewDeleteQueryBuilder[T Queryable](ctx context.Context, mapper mapping.TableMapper[T]) *DeleteQueryBuilder[T] {
	return &DeleteQueryBuilder[T]{
		BaseQueryBuilder: NewBaseQueryBuilder(ctx, mapper),
		whereClause:      make([]string, 0),
		joins:            make([]JoinClause, 0),
		returning:        make([]string, 0),
	}
}

// Where adds a WHERE condition
func (dqb *DeleteQueryBuilder[T]) Where(condition string, args ...interface{}) *DeleteQueryBuilder[T] {
	dqb.whereClause = append(dqb.whereClause, condition)
	dqb.args = append(dqb.args, args...)
	return dqb
}

// WhereEq adds a WHERE condition with equality
func (dqb *DeleteQueryBuilder[T]) WhereEq(column string, value interface{}) *DeleteQueryBuilder[T] {
	return dqb.Where(column+" = ?", value)
}

// WhereIn adds a WHERE IN condition
func (dqb *DeleteQueryBuilder[T]) WhereIn(column string, values ...interface{}) *DeleteQueryBuilder[T] {
	if len(values) == 0 {
		return dqb
	}

	placeholders := make([]string, len(values))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	condition := fmt.Sprintf("%s IN (%s)", column, strings.Join(placeholders, ", "))
	return dqb.Where(condition, values...)
}

// Join adds an INNER JOIN clause (for DELETE with JOIN)
func (dqb *DeleteQueryBuilder[T]) Join(table, condition string, args ...interface{}) *DeleteQueryBuilder[T] {
	return dqb.JoinWithType(InnerJoin, table, "", condition, args...)
}

// LeftJoin adds a LEFT JOIN clause
func (dqb *DeleteQueryBuilder[T]) LeftJoin(table, condition string, args ...interface{}) *DeleteQueryBuilder[T] {
	return dqb.JoinWithType(LeftJoin, table, "", condition, args...)
}

// JoinWithType adds a JOIN clause with specified type
func (dqb *DeleteQueryBuilder[T]) JoinWithType(joinType JoinType, table, alias, condition string, args ...interface{}) *DeleteQueryBuilder[T] {
	join := JoinClause{
		Type:      joinType,
		Table:     table,
		Alias:     alias,
		Condition: condition,
		Args:      args,
	}
	dqb.joins = append(dqb.joins, join)
	return dqb
}

// Returning adds RETURNING clause
func (dqb *DeleteQueryBuilder[T]) Returning(columns ...string) *DeleteQueryBuilder[T] {
	dqb.returning = columns
	return dqb
}

// WithContext sets a new context
func (dqb *DeleteQueryBuilder[T]) WithContext(ctx context.Context) QueryBuilder[T] {
	dqb.ctx = ctx
	return dqb
}

// Build constructs the DELETE SQL query string
func (dqb *DeleteQueryBuilder[T]) Build() (string, []interface{}, error) {
	if err := dqb.Validate(); err != nil {
		return "", nil, err
	}

	var query strings.Builder
	var allArgs []interface{}

	query.WriteString("DELETE FROM ")
	query.WriteString(dqb.TableName())

	// JOIN clauses (for DELETE with JOINs)
	for _, join := range dqb.joins {
		query.WriteString(" ")
		query.WriteString(string(join.Type))
		query.WriteString(" ")
		query.WriteString(join.Table)
		if join.Alias != "" {
			query.WriteString(" AS ")
			query.WriteString(join.Alias)
		}
		query.WriteString(" ON ")
		query.WriteString(join.Condition)
		allArgs = append(allArgs, join.Args...)
	}

	// WHERE clause
	if len(dqb.whereClause) > 0 {
		query.WriteString(" WHERE ")
		query.WriteString(strings.Join(dqb.whereClause, " AND "))
		allArgs = append(allArgs, dqb.args...)
	}

	// RETURNING clause
	if len(dqb.returning) > 0 {
		query.WriteString(" RETURNING ")
		query.WriteString(strings.Join(dqb.returning, ", "))
	}

	return query.String(), allArgs, nil
}

// Validate validates the query configuration
func (dqb *DeleteQueryBuilder[T]) Validate() error {
	if dqb.mapper == nil {
		return fmt.Errorf("table mapper is required")
	}
	return nil
}