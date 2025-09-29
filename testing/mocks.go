// Package testing provides comprehensive testing utilities for GORP enhancements
// with MockExecutor interfaces, Docker integration, and property-based testing
package testing

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/jmoiron/sqlx"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/fathiraz/gorp/db"
	"github.com/fathiraz/gorp/mapping"
	"github.com/fathiraz/gorp/query"
	"github.com/fathiraz/gorp/dialect"
	"github.com/fathiraz/gorp/transaction"
	"github.com/fathiraz/gorp/instrumentation"
)

// MockExecutor provides a comprehensive mock for database execution operations
type MockExecutor struct {
	mock.Mock
}

// Core database operations
func (m *MockExecutor) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	mockArgs := make([]interface{}, len(args)+2)
	mockArgs[0] = ctx
	mockArgs[1] = query
	copy(mockArgs[2:], args)
	ret := m.Called(mockArgs...)
	return ret.Get(0).(sql.Result), ret.Error(1)
}

func (m *MockExecutor) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	mockArgs := make([]interface{}, len(args)+2)
	mockArgs[0] = ctx
	mockArgs[1] = query
	copy(mockArgs[2:], args)
	ret := m.Called(mockArgs...)
	return ret.Get(0).(*sql.Rows), ret.Error(1)
}

func (m *MockExecutor) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	mockArgs := make([]interface{}, len(args)+2)
	mockArgs[0] = ctx
	mockArgs[1] = query
	copy(mockArgs[2:], args)
	ret := m.Called(mockArgs...)
	return ret.Get(0).(*sql.Row)
}

func (m *MockExecutor) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	ret := m.Called(ctx, query)
	return ret.Get(0).(*sql.Stmt), ret.Error(1)
}

// MockConnection provides mock database connection functionality
type MockConnection struct {
	mock.Mock
}

func (m *MockConnection) Type() db.ConnectionType {
	ret := m.Called()
	return ret.Get(0).(db.ConnectionType)
}

func (m *MockConnection) Role() db.ConnectionRole {
	ret := m.Called()
	return ret.Get(0).(db.ConnectionRole)
}

func (m *MockConnection) IsHealthy(ctx context.Context) bool {
	ret := m.Called(ctx)
	return ret.Bool(0)
}

func (m *MockConnection) Close() error {
	ret := m.Called()
	return ret.Error(0)
}

func (m *MockConnection) Begin(ctx context.Context) (db.Transaction, error) {
	ret := m.Called(ctx)
	return ret.Get(0).(db.Transaction), ret.Error(1)
}

func (m *MockConnection) Ping(ctx context.Context) error {
	ret := m.Called(ctx)
	return ret.Error(0)
}

func (m *MockConnection) Stats() sql.DBStats {
	ret := m.Called()
	return ret.Get(0).(sql.DBStats)
}

// PostgreSQL-specific mock connection
func (m *MockConnection) PgxPool() *pgxpool.Pool {
	ret := m.Called()
	if ret.Get(0) == nil {
		return nil
	}
	return ret.Get(0).(*pgxpool.Pool)
}

// sqlx-based connection for MySQL, SQLite, SQL Server
func (m *MockConnection) SqlxDB() *sqlx.DB {
	ret := m.Called()
	if ret.Get(0) == nil {
		return nil
	}
	return ret.Get(0).(*sqlx.DB)
}

// Raw SQL connection
func (m *MockConnection) SQLDB() *sql.DB {
	ret := m.Called()
	if ret.Get(0) == nil {
		return nil
	}
	return ret.Get(0).(*sql.DB)
}

// MockTransaction provides mock transaction functionality
type MockTransaction struct {
	mock.Mock
}

func (m *MockTransaction) Commit() error {
	ret := m.Called()
	return ret.Error(0)
}

func (m *MockTransaction) Rollback() error {
	ret := m.Called()
	return ret.Error(0)
}

func (m *MockTransaction) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	mockArgs := make([]interface{}, len(args)+2)
	mockArgs[0] = ctx
	mockArgs[1] = query
	copy(mockArgs[2:], args)
	ret := m.Called(mockArgs...)
	return ret.Get(0).(sql.Result), ret.Error(1)
}

func (m *MockTransaction) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	mockArgs := make([]interface{}, len(args)+2)
	mockArgs[0] = ctx
	mockArgs[1] = query
	copy(mockArgs[2:], args)
	ret := m.Called(mockArgs...)
	return ret.Get(0).(*sql.Rows), ret.Error(1)
}

func (m *MockTransaction) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	mockArgs := make([]interface{}, len(args)+2)
	mockArgs[0] = ctx
	mockArgs[1] = query
	copy(mockArgs[2:], args)
	ret := m.Called(mockArgs...)
	return ret.Get(0).(*sql.Row)
}

func (m *MockTransaction) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	ret := m.Called(ctx, query)
	return ret.Get(0).(*sql.Stmt), ret.Error(1)
}

func (m *MockTransaction) Context() context.Context {
	ret := m.Called()
	return ret.Get(0).(context.Context)
}

// MockTableMapper provides mock mapping functionality
type MockTableMapper[T mapping.Mappable] struct {
	mock.Mock
}

func (m *MockTableMapper[T]) TableName() string {
	ret := m.Called()
	return ret.String(0)
}

func (m *MockTableMapper[T]) ColumnMap() map[string]string {
	ret := m.Called()
	return ret.Get(0).(map[string]string)
}

func (m *MockTableMapper[T]) PrimaryKey() []string {
	ret := m.Called()
	return ret.Get(0).([]string)
}

func (m *MockTableMapper[T]) Indexes() []mapping.IndexDefinition {
	ret := m.Called()
	return ret.Get(0).([]mapping.IndexDefinition)
}

func (m *MockTableMapper[T]) ToRow(entity T) (map[string]interface{}, error) {
	ret := m.Called(entity)
	return ret.Get(0).(map[string]interface{}), ret.Error(1)
}

func (m *MockTableMapper[T]) FromRow(row map[string]interface{}) (T, error) {
	ret := m.Called(row)
	return ret.Get(0).(T), ret.Error(1)
}

func (m *MockTableMapper[T]) Schema() *mapping.TableSchema {
	ret := m.Called()
	return ret.Get(0).(*mapping.TableSchema)
}

// MockQueryBuilder provides mock query building functionality
type MockQueryBuilder[T mapping.Mappable] struct {
	mock.Mock
}

func (m *MockQueryBuilder[T]) Build() (string, []interface{}, error) {
	ret := m.Called()
	return ret.String(0), ret.Get(1).([]interface{}), ret.Error(2)
}

func (m *MockQueryBuilder[T]) Context() context.Context {
	ret := m.Called()
	return ret.Get(0).(context.Context)
}

func (m *MockQueryBuilder[T]) WithContext(ctx context.Context) query.QueryBuilder[T] {
	ret := m.Called(ctx)
	return ret.Get(0).(query.QueryBuilder[T])
}

func (m *MockQueryBuilder[T]) Validate() error {
	ret := m.Called()
	return ret.Error(0)
}

// MockDialect provides mock dialect functionality
type MockDialect struct {
	mock.Mock
}

func (m *MockDialect) QuerySuffix() string {
	ret := m.Called()
	return ret.String(0)
}

func (m *MockDialect) CreateTableSuffix() string {
	ret := m.Called()
	return ret.String(0)
}

func (m *MockDialect) DropTableSuffix() string {
	ret := m.Called()
	return ret.String(0)
}

func (m *MockDialect) TruncateClause() string {
	ret := m.Called()
	return ret.String(0)
}

func (m *MockDialect) CreateIndexSuffix() string {
	ret := m.Called()
	return ret.String(0)
}

func (m *MockDialect) DropIndexSuffix() string {
	ret := m.Called()
	return ret.String(0)
}

func (m *MockDialect) AutoIncrStr() string {
	ret := m.Called()
	return ret.String(0)
}

func (m *MockDialect) BinaryKeyword() string {
	ret := m.Called()
	return ret.String(0)
}

func (m *MockDialect) IfSchemaNotExists(command, schema string) string {
	ret := m.Called(command, schema)
	return ret.String(0)
}

func (m *MockDialect) IfTableExists(command, schema, table string) string {
	ret := m.Called(command, schema, table)
	return ret.String(0)
}

func (m *MockDialect) IfTableNotExists(command, schema, table string) string {
	ret := m.Called(command, schema, table)
	return ret.String(0)
}

// MockResult provides mock sql.Result functionality
type MockResult struct {
	mock.Mock
}

func (m *MockResult) LastInsertId() (int64, error) {
	ret := m.Called()
	return ret.Get(0).(int64), ret.Error(1)
}

func (m *MockResult) RowsAffected() (int64, error) {
	ret := m.Called()
	return ret.Get(0).(int64), ret.Error(1)
}

// MockRows provides mock sql.Rows functionality
type MockRows struct {
	mock.Mock
	data     []map[string]interface{}
	current  int
	columns  []string
	closed   bool
}

func NewMockRows(columns []string, data []map[string]interface{}) *MockRows {
	return &MockRows{
		columns: columns,
		data:    data,
		current: -1,
	}
}

func (m *MockRows) Columns() ([]string, error) {
	if m.closed {
		return nil, sql.ErrConnDone
	}
	return m.columns, nil
}

func (m *MockRows) Close() error {
	m.closed = true
	ret := m.Called()
	return ret.Error(0)
}

func (m *MockRows) Next() bool {
	if m.closed {
		return false
	}
	m.current++
	return m.current < len(m.data)
}

func (m *MockRows) Scan(dest ...interface{}) error {
	if m.closed || m.current < 0 || m.current >= len(m.data) {
		return sql.ErrNoRows
	}

	row := m.data[m.current]
	for i, col := range m.columns {
		if i < len(dest) {
			if ptr, ok := dest[i].(*interface{}); ok {
				*ptr = row[col]
			} else if ptr, ok := dest[i].(*string); ok {
				if val, exists := row[col]; exists {
					if s, ok := val.(string); ok {
						*ptr = s
					}
				}
			} else if ptr, ok := dest[i].(*int64); ok {
				if val, exists := row[col]; exists {
					if i, ok := val.(int64); ok {
						*ptr = i
					}
				}
			}
		}
	}

	ret := m.Called(dest)
	return ret.Error(0)
}

func (m *MockRows) Err() error {
	ret := m.Called()
	return ret.Error(0)
}

// MockInstrumentation provides mock instrumentation functionality
type MockInstrumentation struct {
	mock.Mock
}

func (m *MockInstrumentation) RecordQuery(ctx context.Context, query string, duration time.Duration, err error) {
	m.Called(ctx, query, duration, err)
}

func (m *MockInstrumentation) RecordTransaction(ctx context.Context, operation string, duration time.Duration, err error) {
	m.Called(ctx, operation, duration, err)
}

func (m *MockInstrumentation) IncrementCounter(name string, value int64, labels map[string]string) {
	m.Called(name, value, labels)
}

func (m *MockInstrumentation) RecordHistogram(name string, value float64, labels map[string]string) {
	m.Called(name, value, labels)
}

func (m *MockInstrumentation) SetGauge(name string, value float64, labels map[string]string) {
	m.Called(name, value, labels)
}

// Mock factory functions for easy test setup
func NewMockExecutor() *MockExecutor {
	return &MockExecutor{}
}

func NewMockConnection() *MockConnection {
	return &MockConnection{}
}

func NewMockTransaction() *MockTransaction {
	return &MockTransaction{}
}

func NewMockTableMapper[T mapping.Mappable]() *MockTableMapper[T] {
	return &MockTableMapper[T]{}
}

func NewMockQueryBuilder[T mapping.Mappable]() *MockQueryBuilder[T] {
	return &MockQueryBuilder[T]{}
}

func NewMockDialect() *MockDialect {
	return &MockDialect{}
}

func NewMockResult(lastInsertId, rowsAffected int64) *MockResult {
	result := &MockResult{}
	result.On("LastInsertId").Return(lastInsertId, nil)
	result.On("RowsAffected").Return(rowsAffected, nil)
	return result
}

func NewMockInstrumentation() *MockInstrumentation {
	return &MockInstrumentation{}
}