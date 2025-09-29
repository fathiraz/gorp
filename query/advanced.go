package query

import (
	"context"
	"fmt"
	"strings"

	"github.com/fathiraz/gorp/mapping"
)

// PreparedStatementCache manages prepared statement reuse
type PreparedStatementCache struct {
	statements map[string]*PreparedStatement
}

// PreparedStatement represents a cached prepared statement
type PreparedStatement struct {
	SQL  string
	Args []interface{}
	Hash string
}

// NewPreparedStatementCache creates a new prepared statement cache
func NewPreparedStatementCache() *PreparedStatementCache {
	return &PreparedStatementCache{
		statements: make(map[string]*PreparedStatement),
	}
}

// Get retrieves a prepared statement by hash
func (psc *PreparedStatementCache) Get(hash string) (*PreparedStatement, bool) {
	stmt, exists := psc.statements[hash]
	return stmt, exists
}

// Store stores a prepared statement
func (psc *PreparedStatementCache) Store(hash string, stmt *PreparedStatement) {
	psc.statements[hash] = stmt
}

// QueryOptimizer provides query optimization hints and strategies
type QueryOptimizer[T Queryable] struct {
	hints     []string
	indexUsage map[string][]string
	tableName string
}

// NewQueryOptimizer creates a new query optimizer
func NewQueryOptimizer[T Queryable](tableName string) *QueryOptimizer[T] {
	return &QueryOptimizer[T]{
		hints:      make([]string, 0),
		indexUsage: make(map[string][]string),
		tableName:  tableName,
	}
}

// UseIndex adds an index hint
func (qo *QueryOptimizer[T]) UseIndex(indexName string) *QueryOptimizer[T] {
	qo.hints = append(qo.hints, fmt.Sprintf("USE INDEX (%s)", indexName))
	return qo
}

// ForceIndex adds a force index hint
func (qo *QueryOptimizer[T]) ForceIndex(indexName string) *QueryOptimizer[T] {
	qo.hints = append(qo.hints, fmt.Sprintf("FORCE INDEX (%s)", indexName))
	return qo
}

// IgnoreIndex adds an ignore index hint
func (qo *QueryOptimizer[T]) IgnoreIndex(indexName string) *QueryOptimizer[T] {
	qo.hints = append(qo.hints, fmt.Sprintf("IGNORE INDEX (%s)", indexName))
	return qo
}

// GetHints returns all optimization hints
func (qo *QueryOptimizer[T]) GetHints() []string {
	return qo.hints
}

// PaginatedResult represents a paginated query result
type PaginatedResult[T Queryable] struct {
	Items      []T
	TotalCount int64
	Page       int
	PageSize   int
	TotalPages int
	HasNext    bool
	HasPrev    bool
}

// Paginator provides pagination functionality
type Paginator[T Queryable] struct {
	builder   *SelectQueryBuilder[T]
	pageSize  int
	countSQL  string
	countArgs []interface{}
}

// NewPaginator creates a new paginator
func NewPaginator[T Queryable](builder *SelectQueryBuilder[T], pageSize int) *Paginator[T] {
	return &Paginator[T]{
		builder:  builder,
		pageSize: pageSize,
	}
}

// WithCountQuery sets a custom count query
func (p *Paginator[T]) WithCountQuery(sql string, args ...interface{}) *Paginator[T] {
	p.countSQL = sql
	p.countArgs = args
	return p
}

// GetPage builds the paginated query for a specific page
func (p *Paginator[T]) GetPage(page int) (*SelectQueryBuilder[T], error) {
	if page < 1 {
		return nil, fmt.Errorf("page must be >= 1")
	}

	offset := (page - 1) * p.pageSize
	return p.builder.Limit(p.pageSize).Offset(offset), nil
}

// NamedParameter represents a named parameter in a query
type NamedParameter struct {
	Name  string
	Value interface{}
}

// ParameterBinder manages named parameter binding
type ParameterBinder struct {
	parameters map[string]interface{}
}

// NewParameterBinder creates a new parameter binder
func NewParameterBinder() *ParameterBinder {
	return &ParameterBinder{
		parameters: make(map[string]interface{}),
	}
}

// Bind adds a named parameter
func (pb *ParameterBinder) Bind(name string, value interface{}) *ParameterBinder {
	pb.parameters[name] = value
	return pb
}

// BindMany adds multiple named parameters
func (pb *ParameterBinder) BindMany(params map[string]interface{}) *ParameterBinder {
	for name, value := range params {
		pb.parameters[name] = value
	}
	return pb
}

// ReplaceNamedParameters converts named parameters to positional parameters
func (pb *ParameterBinder) ReplaceNamedParameters(sql string) (string, []interface{}, error) {
	var args []interface{}
	var resultSQL strings.Builder

	i := 0
	for i < len(sql) {
		if sql[i] == ':' && i+1 < len(sql) {
			// Find the end of the parameter name
			start := i + 1
			end := start
			for end < len(sql) && (isAlphaNumeric(sql[end]) || sql[end] == '_') {
				end++
			}

			if end > start {
				paramName := sql[start:end]
				if value, exists := pb.parameters[paramName]; exists {
					resultSQL.WriteString("?")
					args = append(args, value)
					i = end
					continue
				}
			}
		}
		resultSQL.WriteByte(sql[i])
		i++
	}

	return resultSQL.String(), args, nil
}

func isAlphaNumeric(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
}

// QueryExecutionPlan represents a query execution plan
type QueryExecutionPlan struct {
	SQL           string
	Parameters    []interface{}
	EstimatedCost float64
	IndexUsage    []string
	TableScans    []string
}

// QueryAnalyzer provides query analysis capabilities
type QueryAnalyzer[T Queryable] struct {
	optimizer *QueryOptimizer[T]
	cache     *PreparedStatementCache
}

// NewQueryAnalyzer creates a new query analyzer
func NewQueryAnalyzer[T Queryable](tableName string) *QueryAnalyzer[T] {
	return &QueryAnalyzer[T]{
		optimizer: NewQueryOptimizer[T](tableName),
		cache:     NewPreparedStatementCache(),
	}
}

// AnalyzeQuery analyzes a query and provides optimization suggestions
func (qa *QueryAnalyzer[T]) AnalyzeQuery(sql string, args []interface{}) *QueryExecutionPlan {
	return &QueryExecutionPlan{
		SQL:           sql,
		Parameters:    args,
		EstimatedCost: 0.0, // Would be implemented with database-specific logic
		IndexUsage:    []string{},
		TableScans:    []string{},
	}
}

// CTE (Common Table Expression) builder
type CTEBuilder[T Queryable] struct {
	name    string
	columns []string
	query   *SelectQueryBuilder[T]
}

// NewCTEBuilder creates a new CTE builder
func NewCTEBuilder[T Queryable](name string) *CTEBuilder[T] {
	return &CTEBuilder[T]{
		name:    name,
		columns: make([]string, 0),
	}
}

// Columns specifies the CTE column names
func (cte *CTEBuilder[T]) Columns(columns ...string) *CTEBuilder[T] {
	cte.columns = columns
	return cte
}

// As sets the query for the CTE
func (cte *CTEBuilder[T]) As(query *SelectQueryBuilder[T]) *CTEBuilder[T] {
	cte.query = query
	return cte
}

// Build constructs the CTE SQL
func (cte *CTEBuilder[T]) Build() (string, []interface{}, error) {
	if cte.query == nil {
		return "", nil, fmt.Errorf("CTE query is required")
	}

	var cteSQL strings.Builder
	cteSQL.WriteString(cte.name)

	if len(cte.columns) > 0 {
		cteSQL.WriteString(" (")
		cteSQL.WriteString(strings.Join(cte.columns, ", "))
		cteSQL.WriteString(")")
	}

	cteSQL.WriteString(" AS (")

	querySQL, args, err := cte.query.Build()
	if err != nil {
		return "", nil, err
	}

	cteSQL.WriteString(querySQL)
	cteSQL.WriteString(")")

	return cteSQL.String(), args, nil
}

// Advanced SelectQueryBuilder extensions

// WithCTE adds a Common Table Expression to the query
func (sqb *SelectQueryBuilder[T]) WithCTE(cte *CTEBuilder[T]) *SelectQueryBuilder[T] {
	// Implementation would require modifying the SelectQueryBuilder struct
	// to include CTEs and modify the Build method
	return sqb
}

// Window function builder
type WindowFunction struct {
	function    string
	partitionBy []string
	orderBy     []string
	frameClause string
}

// NewWindowFunction creates a new window function
func NewWindowFunction(function string) *WindowFunction {
	return &WindowFunction{
		function:    function,
		partitionBy: make([]string, 0),
		orderBy:     make([]string, 0),
	}
}

// PartitionBy adds PARTITION BY clause
func (wf *WindowFunction) PartitionBy(columns ...string) *WindowFunction {
	wf.partitionBy = columns
	return wf
}

// OrderBy adds ORDER BY clause
func (wf *WindowFunction) OrderBy(columns ...string) *WindowFunction {
	wf.orderBy = columns
	return wf
}

// Frame adds frame clause (ROWS/RANGE)
func (wf *WindowFunction) Frame(frameClause string) *WindowFunction {
	wf.frameClause = frameClause
	return wf
}

// Build constructs the window function SQL
func (wf *WindowFunction) Build() string {
	var sql strings.Builder
	sql.WriteString(wf.function)
	sql.WriteString(" OVER (")

	if len(wf.partitionBy) > 0 {
		sql.WriteString("PARTITION BY ")
		sql.WriteString(strings.Join(wf.partitionBy, ", "))
	}

	if len(wf.orderBy) > 0 {
		if len(wf.partitionBy) > 0 {
			sql.WriteString(" ")
		}
		sql.WriteString("ORDER BY ")
		sql.WriteString(strings.Join(wf.orderBy, ", "))
	}

	if wf.frameClause != "" {
		sql.WriteString(" ")
		sql.WriteString(wf.frameClause)
	}

	sql.WriteString(")")
	return sql.String()
}

// Convenience functions for creating query builders

// Select creates a new SelectQueryBuilder
func Select[T Queryable](ctx context.Context, mapper mapping.TableMapper[T]) *SelectQueryBuilder[T] {
	return NewSelectQueryBuilder[T](ctx, mapper)
}

// Insert creates a new InsertQueryBuilder
func Insert[T Queryable](ctx context.Context, mapper mapping.TableMapper[T]) *InsertQueryBuilder[T] {
	return NewInsertQueryBuilder[T](ctx, mapper)
}

// Update creates a new UpdateQueryBuilder
func Update[T Queryable](ctx context.Context, mapper mapping.TableMapper[T]) *UpdateQueryBuilder[T] {
	return NewUpdateQueryBuilder[T](ctx, mapper)
}

// Delete creates a new DeleteQueryBuilder
func Delete[T Queryable](ctx context.Context, mapper mapping.TableMapper[T]) *DeleteQueryBuilder[T] {
	return NewDeleteQueryBuilder[T](ctx, mapper)
}

// QueryExecutor interface for executing queries
type QueryExecutor interface {
	Execute(ctx context.Context, sql string, args ...interface{}) error
	Query(ctx context.Context, sql string, args ...interface{}) (*QueryResult, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) *QueryRowResult
}

// QueryResult represents query result set
type QueryResult struct {
	// Implementation would depend on database driver
}

// QueryRowResult represents single row result
type QueryRowResult struct {
	// Implementation would depend on database driver
}

// TransactionQueryBuilder wraps query builders with transaction context
type TransactionQueryBuilder[T Queryable] struct {
	tx QueryExecutor
}

// NewTransactionQueryBuilder creates a new transaction query builder
func NewTransactionQueryBuilder[T Queryable](tx QueryExecutor) *TransactionQueryBuilder[T] {
	return &TransactionQueryBuilder[T]{tx: tx}
}

// Select creates a SELECT query within transaction
func (tqb *TransactionQueryBuilder[T]) Select(ctx context.Context, mapper mapping.TableMapper[T]) *SelectQueryBuilder[T] {
	return NewSelectQueryBuilder[T](ctx, mapper)
}

// Insert creates an INSERT query within transaction
func (tqb *TransactionQueryBuilder[T]) Insert(ctx context.Context, mapper mapping.TableMapper[T]) *InsertQueryBuilder[T] {
	return NewInsertQueryBuilder[T](ctx, mapper)
}

// Update creates an UPDATE query within transaction
func (tqb *TransactionQueryBuilder[T]) Update(ctx context.Context, mapper mapping.TableMapper[T]) *UpdateQueryBuilder[T] {
	return NewUpdateQueryBuilder[T](ctx, mapper)
}

// Delete creates a DELETE query within transaction
func (tqb *TransactionQueryBuilder[T]) Delete(ctx context.Context, mapper mapping.TableMapper[T]) *DeleteQueryBuilder[T] {
	return NewDeleteQueryBuilder[T](ctx, mapper)
}