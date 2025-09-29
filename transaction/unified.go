package transaction

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/trace"

	"github.com/fathiraz/gorp/db"
)

// UnifiedTransaction provides a unified interface for different transaction backends
type UnifiedTransaction struct {
	backend TransactionBackend
	connType db.ConnectionType
	span    trace.Span
}

// TransactionBackend interface for different transaction implementations
type TransactionBackend interface {
	Query(ctx context.Context, query string, args ...interface{}) (UnifiedRows, error)
	QueryRow(ctx context.Context, query string, args ...interface{}) UnifiedRow
	Exec(ctx context.Context, query string, args ...interface{}) (UnifiedResult, error)
	Commit() error
	Rollback() error
}

// UnifiedRows provides a unified interface for query results
type UnifiedRows interface {
	Next() bool
	Scan(dest ...interface{}) error
	Close() error
	Err() error
	Columns() ([]string, error)
}

// UnifiedRow provides a unified interface for single row results
type UnifiedRow interface {
	Scan(dest ...interface{}) error
}

// UnifiedResult provides a unified interface for execution results
type UnifiedResult interface {
	LastInsertId() (int64, error)
	RowsAffected() (int64, error)
}

// NewUnifiedTransaction creates a unified transaction wrapper
func NewUnifiedTransaction(tx db.Transaction, connType db.ConnectionType, span trace.Span) *UnifiedTransaction {
	var backend TransactionBackend

	switch connType {
	case db.PostgreSQLConnectionType:
		if pgxTx, ok := tx.(*db.PostgreSQLTransaction); ok {
			backend = &PgxTransactionBackend{tx: pgxTx}
		}
	case db.MySQLConnectionType:
		if sqlxTx, ok := tx.(*db.MySQLTransaction); ok {
			backend = &SqlxTransactionBackend{tx: sqlxTx}
		}
	case db.SQLiteConnectionType:
		if sqlxTx, ok := tx.(*db.SQLiteTransaction); ok {
			backend = &SqlxTransactionBackend{tx: sqlxTx}
		}
	case db.SQLServerConnectionType:
		if sqlxTx, ok := tx.(*db.SQLServerTransaction); ok {
			backend = &SqlxTransactionBackend{tx: sqlxTx}
		}
	}

	if backend == nil {
		backend = &GenericTransactionBackend{tx: tx}
	}

	return &UnifiedTransaction{
		backend:  backend,
		connType: connType,
		span:     span,
	}
}

// Query executes a query that returns rows
func (ut *UnifiedTransaction) Query(ctx context.Context, query string, args ...interface{}) (UnifiedRows, error) {
	if ut.span != nil {
		_, span := ut.span.TracerProvider().Tracer("transaction").Start(ctx, "transaction.query")
		defer span.End()
		ctx = trace.ContextWithSpan(ctx, span)
	}

	return ut.backend.Query(ctx, query, args...)
}

// QueryRow executes a query that returns at most one row
func (ut *UnifiedTransaction) QueryRow(ctx context.Context, query string, args ...interface{}) UnifiedRow {
	if ut.span != nil {
		_, span := ut.span.TracerProvider().Tracer("transaction").Start(ctx, "transaction.query_row")
		defer span.End()
		ctx = trace.ContextWithSpan(ctx, span)
	}

	return ut.backend.QueryRow(ctx, query, args...)
}

// Exec executes a query without returning rows
func (ut *UnifiedTransaction) Exec(ctx context.Context, query string, args ...interface{}) (UnifiedResult, error) {
	if ut.span != nil {
		_, span := ut.span.TracerProvider().Tracer("transaction").Start(ctx, "transaction.exec")
		defer span.End()
		ctx = trace.ContextWithSpan(ctx, span)
	}

	return ut.backend.Exec(ctx, query, args...)
}

// Commit commits the transaction
func (ut *UnifiedTransaction) Commit() error {
	return ut.backend.Commit()
}

// Rollback rolls back the transaction
func (ut *UnifiedTransaction) Rollback() error {
	return ut.backend.Rollback()
}

// GetConnectionType returns the underlying connection type
func (ut *UnifiedTransaction) GetConnectionType() db.ConnectionType {
	return ut.connType
}

// PgxTransactionBackend implements TransactionBackend for pgx
type PgxTransactionBackend struct {
	tx *db.PostgreSQLTransaction
}

func (ptb *PgxTransactionBackend) Query(ctx context.Context, query string, args ...interface{}) (UnifiedRows, error) {
	rows, err := ptb.tx.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &PgxRowsWrapper{rows: rows}, nil
}

func (ptb *PgxTransactionBackend) QueryRow(ctx context.Context, query string, args ...interface{}) UnifiedRow {
	row := ptb.tx.QueryRow(ctx, query, args...)
	return &PgxRowWrapper{row: row}
}

func (ptb *PgxTransactionBackend) Exec(ctx context.Context, query string, args ...interface{}) (UnifiedResult, error) {
	result, err := ptb.tx.Exec(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &PgxResultWrapper{result: result}, nil
}

func (ptb *PgxTransactionBackend) Commit() error {
	return ptb.tx.Commit()
}

func (ptb *PgxTransactionBackend) Rollback() error {
	return ptb.tx.Rollback()
}

// SqlxTransactionBackend implements TransactionBackend for sqlx
type SqlxTransactionBackend struct {
	tx interface{} // Could be MySQLTransaction, SQLiteTransaction, or SQLServerTransaction
}

func (stb *SqlxTransactionBackend) Query(ctx context.Context, query string, args ...interface{}) (UnifiedRows, error) {
	// Type assertion to get the correct transaction type
	switch tx := stb.tx.(type) {
	case *db.MySQLTransaction:
		rows, err := tx.Query(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		return &SqlxRowsWrapper{rows: rows}, nil
	case *db.SQLiteTransaction:
		rows, err := tx.Query(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		return &SqlxRowsWrapper{rows: rows}, nil
	case *db.SQLServerTransaction:
		rows, err := tx.Query(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		return &SqlxRowsWrapper{rows: rows}, nil
	default:
		return nil, fmt.Errorf("unsupported transaction type: %T", tx)
	}
}

func (stb *SqlxTransactionBackend) QueryRow(ctx context.Context, query string, args ...interface{}) UnifiedRow {
	switch tx := stb.tx.(type) {
	case *db.MySQLTransaction:
		row := tx.QueryRow(ctx, query, args...)
		return &SqlxRowWrapper{row: row}
	case *db.SQLiteTransaction:
		row := tx.QueryRow(ctx, query, args...)
		return &SqlxRowWrapper{row: row}
	case *db.SQLServerTransaction:
		row := tx.QueryRow(ctx, query, args...)
		return &SqlxRowWrapper{row: row}
	default:
		return &ErrorRowWrapper{err: fmt.Errorf("unsupported transaction type: %T", tx)}
	}
}

func (stb *SqlxTransactionBackend) Exec(ctx context.Context, query string, args ...interface{}) (UnifiedResult, error) {
	switch tx := stb.tx.(type) {
	case *db.MySQLTransaction:
		result, err := tx.Exec(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		return &SqlxResultWrapper{result: result}, nil
	case *db.SQLiteTransaction:
		result, err := tx.Exec(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		return &SqlxResultWrapper{result: result}, nil
	case *db.SQLServerTransaction:
		result, err := tx.Exec(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		return &SqlxResultWrapper{result: result}, nil
	default:
		return nil, fmt.Errorf("unsupported transaction type: %T", tx)
	}
}

func (stb *SqlxTransactionBackend) Commit() error {
	switch tx := stb.tx.(type) {
	case *db.MySQLTransaction:
		return tx.Commit()
	case *db.SQLiteTransaction:
		return tx.Commit()
	case *db.SQLServerTransaction:
		return tx.Commit()
	default:
		return fmt.Errorf("unsupported transaction type: %T", tx)
	}
}

func (stb *SqlxTransactionBackend) Rollback() error {
	switch tx := stb.tx.(type) {
	case *db.MySQLTransaction:
		return tx.Rollback()
	case *db.SQLiteTransaction:
		return tx.Rollback()
	case *db.SQLServerTransaction:
		return tx.Rollback()
	default:
		return fmt.Errorf("unsupported transaction type: %T", tx)
	}
}

// GenericTransactionBackend implements TransactionBackend for generic db.Transaction
type GenericTransactionBackend struct {
	tx db.Transaction
}

func (gtb *GenericTransactionBackend) Query(ctx context.Context, query string, args ...interface{}) (UnifiedRows, error) {
	rows, err := gtb.tx.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &GenericRowsWrapper{rows: rows}, nil
}

func (gtb *GenericTransactionBackend) QueryRow(ctx context.Context, query string, args ...interface{}) UnifiedRow {
	row := gtb.tx.QueryRow(ctx, query, args...)
	return &GenericRowWrapper{row: row}
}

func (gtb *GenericTransactionBackend) Exec(ctx context.Context, query string, args ...interface{}) (UnifiedResult, error) {
	result, err := gtb.tx.Exec(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &GenericResultWrapper{result: result}, nil
}

func (gtb *GenericTransactionBackend) Commit() error {
	return gtb.tx.Commit()
}

func (gtb *GenericTransactionBackend) Rollback() error {
	return gtb.tx.Rollback()
}

// Wrapper implementations

// PgxRowsWrapper wraps pgx rows
type PgxRowsWrapper struct {
	rows db.Rows
}

func (prw *PgxRowsWrapper) Next() bool             { return prw.rows.Next() }
func (prw *PgxRowsWrapper) Scan(dest ...interface{}) error { return prw.rows.Scan(dest...) }
func (prw *PgxRowsWrapper) Close() error           { return prw.rows.Close() }
func (prw *PgxRowsWrapper) Err() error             { return prw.rows.Err() }
func (prw *PgxRowsWrapper) Columns() ([]string, error) { return prw.rows.Columns() }

// PgxRowWrapper wraps pgx row
type PgxRowWrapper struct {
	row db.Row
}

func (prw *PgxRowWrapper) Scan(dest ...interface{}) error { return prw.row.Scan(dest...) }

// PgxResultWrapper wraps pgx result
type PgxResultWrapper struct {
	result db.Result
}

func (prw *PgxResultWrapper) LastInsertId() (int64, error) { return prw.result.LastInsertId() }
func (prw *PgxResultWrapper) RowsAffected() (int64, error) { return prw.result.RowsAffected() }

// SqlxRowsWrapper wraps sqlx rows
type SqlxRowsWrapper struct {
	rows db.Rows
}

func (srw *SqlxRowsWrapper) Next() bool             { return srw.rows.Next() }
func (srw *SqlxRowsWrapper) Scan(dest ...interface{}) error { return srw.rows.Scan(dest...) }
func (srw *SqlxRowsWrapper) Close() error           { return srw.rows.Close() }
func (srw *SqlxRowsWrapper) Err() error             { return srw.rows.Err() }
func (srw *SqlxRowsWrapper) Columns() ([]string, error) { return srw.rows.Columns() }

// SqlxRowWrapper wraps sqlx row
type SqlxRowWrapper struct {
	row db.Row
}

func (srw *SqlxRowWrapper) Scan(dest ...interface{}) error { return srw.row.Scan(dest...) }

// SqlxResultWrapper wraps sqlx result
type SqlxResultWrapper struct {
	result db.Result
}

func (srw *SqlxResultWrapper) LastInsertId() (int64, error) { return srw.result.LastInsertId() }
func (srw *SqlxResultWrapper) RowsAffected() (int64, error) { return srw.result.RowsAffected() }

// GenericRowsWrapper wraps generic db.Rows
type GenericRowsWrapper struct {
	rows db.Rows
}

func (grw *GenericRowsWrapper) Next() bool             { return grw.rows.Next() }
func (grw *GenericRowsWrapper) Scan(dest ...interface{}) error { return grw.rows.Scan(dest...) }
func (grw *GenericRowsWrapper) Close() error           { return grw.rows.Close() }
func (grw *GenericRowsWrapper) Err() error             { return grw.rows.Err() }
func (grw *GenericRowsWrapper) Columns() ([]string, error) { return grw.rows.Columns() }

// GenericRowWrapper wraps generic db.Row
type GenericRowWrapper struct {
	row db.Row
}

func (grw *GenericRowWrapper) Scan(dest ...interface{}) error { return grw.row.Scan(dest...) }

// GenericResultWrapper wraps generic db.Result
type GenericResultWrapper struct {
	result db.Result
}

func (grw *GenericResultWrapper) LastInsertId() (int64, error) { return grw.result.LastInsertId() }
func (grw *GenericResultWrapper) RowsAffected() (int64, error) { return grw.result.RowsAffected() }

// ErrorRowWrapper provides error handling for row operations
type ErrorRowWrapper struct {
	err error
}

func (erw *ErrorRowWrapper) Scan(dest ...interface{}) error { return erw.err }