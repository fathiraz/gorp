// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package gorp

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
)

// TypedSelector provides type-safe select operations
type TypedSelector[T any] struct {
	dbmap *DbMap
	exec  SqlExecutor
	hooks *HookMap[T]
	ctx   context.Context
}

// NewTypedSelector creates a new TypedSelector for type T
func NewTypedSelector[T any](ctx context.Context, dbmap *DbMap, exec SqlExecutor) *TypedSelector[T] {
	return &TypedSelector[T]{
		dbmap: dbmap,
		exec:  exec,
		hooks: NewHookMap[T](),
		ctx:   ctx,
	}
}

// WithHooks sets the hooks for this selector
func (s *TypedSelector[T]) WithHooks(hooks *HookMap[T]) *TypedSelector[T] {
	s.hooks = hooks
	return s
}

// SelectOne executes a type-safe select for a single row
func (s *TypedSelector[T]) SelectOne(query string, args ...interface{}) (*T, error) {
	var result T
	err := SelectOne(s.dbmap, s.exec, &result, query, args...)
	if err != nil {
		return nil, err
	}

	// Execute after select hooks
	if s.hooks != nil {
		if err := s.hooks.ExecuteHooks(s.ctx, AfterSelect, s.exec); err != nil {
			return nil, &HookError{
				Point:  AfterSelect,
				Entity: result,
				Err:    err,
			}
		}
	}

	return &result, nil
}

// Select executes a type-safe select for multiple rows
func (s *TypedSelector[T]) Select(query string, args ...interface{}) ([]T, error) {
	var result T
	list, err := hookedselect(s.dbmap, s.exec, &result, query, args...)
	if err != nil {
		return nil, err
	}

	typedList := make([]T, len(list))
	for i, v := range list {
		typedList[i] = *v.(*T)
	}

	return typedList, nil
}

// SelectInt executes the given query, which should be a SELECT statement for a single
// integer column, and returns the value of the first row returned.  If no rows are
// found, zero is returned.
func SelectInt(e SqlExecutor, query string, args ...interface{}) (int64, error) {
	var h int64
	err := selectVal(e, &h, query, args...)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}
	return h, nil
}

// SelectNullInt executes the given query, which should be a SELECT statement for a single
// integer column, and returns the value of the first row returned.  If no rows are
// found, the empty sql.NullInt64 value is returned.
func SelectNullInt(e SqlExecutor, query string, args ...interface{}) (sql.NullInt64, error) {
	var h sql.NullInt64
	err := selectVal(e, &h, query, args...)
	if err != nil && err != sql.ErrNoRows {
		return h, err
	}
	return h, nil
}

// SelectFloat executes the given query, which should be a SELECT statement for a single
// float column, and returns the value of the first row returned. If no rows are
// found, zero is returned.
func SelectFloat(e SqlExecutor, query string, args ...interface{}) (float64, error) {
	var h float64
	err := selectVal(e, &h, query, args...)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}
	return h, nil
}

// SelectNullFloat executes the given query, which should be a SELECT statement for a single
// float column, and returns the value of the first row returned. If no rows are
// found, the empty sql.NullFloat64 value is returned.
func SelectNullFloat(e SqlExecutor, query string, args ...interface{}) (sql.NullFloat64, error) {
	var h sql.NullFloat64
	err := selectVal(e, &h, query, args...)
	if err != nil && err != sql.ErrNoRows {
		return h, err
	}
	return h, nil
}

// SelectStr executes the given query, which should be a SELECT statement for a single
// char/varchar column, and returns the value of the first row returned.  If no rows are
// found, an empty string is returned.
func SelectStr(e SqlExecutor, query string, args ...interface{}) (string, error) {
	var h string
	err := selectVal(e, &h, query, args...)
	if err != nil && err != sql.ErrNoRows {
		return "", err
	}
	return h, nil
}

// SelectNullStr executes the given query, which should be a SELECT statement for a single
// char/varchar column, and returns the value of the first row returned.  If no rows are
// found, the empty sql.NullString is returned.
func SelectNullStr(e SqlExecutor, query string, args ...interface{}) (sql.NullString, error) {
	var h sql.NullString
	err := selectVal(e, &h, query, args...)
	if err != nil && err != sql.ErrNoRows {
		return h, err
	}
	return h, nil
}

// SelectBool executes the given query, which should be a SELECT statement for a single
// boolean column, and returns the value of the first row returned.  If no rows are
// found, false is returned.
func SelectBool(e SqlExecutor, query string, args ...interface{}) (bool, error) {
	var h bool
	err := selectVal(e, &h, query, args...)
	if err != nil && err != sql.ErrNoRows {
		return false, err
	}
	return h, nil
}

// SelectNullBool executes the given query, which should be a SELECT statement for a single
// boolean column, and returns the value of the first row returned.  If no rows are
// found, the empty sql.NullBool is returned.
func SelectNullBool(e SqlExecutor, query string, args ...interface{}) (sql.NullBool, error) {
	var h sql.NullBool
	err := selectVal(e, &h, query, args...)
	if err != nil && err != sql.ErrNoRows {
		return h, err
	}
	return h, nil
}

// SelectOne executes the given query (which should be a SELECT statement)
// and binds the result to holder, which must be a pointer.
//
// # If no row is found, an error (sql.ErrNoRows specifically) will be returned
//
// If more than one row is found, an error will be returned.
func SelectOne(m *DbMap, e SqlExecutor, holder interface{}, query string, args ...interface{}) error {
	t := reflect.TypeOf(holder)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	} else {
		return fmt.Errorf("gorp: SelectOne holder must be a pointer, but got: %T", holder)
	}

	// Handle pointer to pointer
	isptr := false
	if t.Kind() == reflect.Ptr {
		isptr = true
		t = t.Elem()
	}

	if t.Kind() == reflect.Struct {
		var nonFatalErr error

		list, err := hookedselect(m, e, holder, query, args...)
		if err != nil {
			if !NonFatalError(err) {
				return err
			}
			nonFatalErr = err
		}

		dest := reflect.ValueOf(holder)
		if isptr {
			dest = dest.Elem()
		}

		if list != nil && len(list) > 0 {
			// Check for multiple rows
			if len(list) > 1 {
				return fmt.Errorf("gorp: multiple rows returned for: %s - %v", query, args)
			}

			// Initialize if nil
			if dest.IsNil() {
				dest.Set(reflect.New(t))
			}

			// Only one row found
			src := reflect.ValueOf(list[0])
			dest.Elem().Set(src.Elem())
		} else {
			// No rows found, return a proper error
			return sql.ErrNoRows
		}

		return nonFatalErr
	}

	return selectVal(e, holder, query, args...)
}

// selectVal executes the given query (which should be a SELECT statement)
// and binds the result to holder
func selectVal(e SqlExecutor, holder interface{}, query string, args ...interface{}) error {
	if len(args) == 1 {
		switch m := e.(type) {
		case *DbMap:
			query, args = maybeExpandNamedQuery(m, query, args)
		case *Transaction:
			query, args = maybeExpandNamedQuery(m.dbmap, query, args)
		}
	}
	rows, err := e.Query(query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}

	return rows.Scan(holder)
}

// hookedselect executes the given query (which should be a SELECT statement)
// and returns the results
func hookedselect(m *DbMap, exec SqlExecutor, i interface{}, query string,
	args ...interface{}) ([]interface{}, error) {

	var nonFatalErr error

	list, err := rawselect(m, exec, i, query, args...)
	if err != nil {
		if !NonFatalError(err) {
			return nil, err
		}
		nonFatalErr = err
	}

	// Execute after select hooks
	if len(list) > 0 {
		for _, v := range list {
			if v, ok := v.(HasPostGet); ok {
				ctx := context.Background()
				if execCtx, ok := exec.(hasContext); ok {
					ctx = execCtx.Context()
				}
				if err := v.PostGet(ctx, exec); err != nil {
					return nil, &HookError{
						Point:  AfterSelect,
						Entity: v,
						Err:    err,
					}
				}
			}
		}
	}

	return list, nonFatalErr
}

// rawselect executes the given query (which should be a SELECT statement)
// and returns the results
func rawselect(m *DbMap, exec SqlExecutor, i interface{}, query string,
	args ...interface{}) ([]interface{}, error) {
	var (
		appendToSlice   = false // Write results to i directly?
		intoStruct      = true  // Selecting into a struct?
		pointerElements = true  // Are the slice elements pointers (vs values)?
	)

	var nonFatalErr error

	tableName := ""
	var dynObj DynamicTable
	isDynamic := false
	if dynObj, isDynamic = i.(DynamicTable); isDynamic {
		tableName = dynObj.TableName()
	}

	// get type for i, verifying it's a supported destination
	t, err := toType(i)
	if err != nil {
		var err2 error
		if t, err2 = toSliceType(i); t == nil {
			if err2 != nil {
				return nil, err2
			}
			return nil, err
		}
		pointerElements = t.Kind() == reflect.Ptr
		if pointerElements {
			t = t.Elem()
		}
		appendToSlice = true
		intoStruct = t.Kind() == reflect.Struct
	}

	// If the caller supplied a single struct/map argument, assume a "named
	// parameter" query.  Extract the named arguments from the struct/map, create
	// the flat arg slice, and rewrite the query to use the dialect's placeholder.
	if len(args) == 1 {
		query, args = maybeExpandNamedQuery(m, query, args)
	}

	// Run the query
	rows, err := exec.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Fetch the column names as returned from db
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	if !intoStruct && len(cols) > 1 {
		return nil, fmt.Errorf("gorp: select into non-struct slice requires 1 column, got %d", len(cols))
	}

	var colToFieldIndex [][]int
	if intoStruct {
		colToFieldIndex, err = columnToFieldIndex(m, t, tableName, cols)
		if err != nil {
			if !NonFatalError(err) {
				return nil, err
			}
			nonFatalErr = err
		}
	}

	conv := m.TypeConverter

	// Add results to one of these two slices.
	var (
		list       = make([]interface{}, 0)
		sliceValue = reflect.Indirect(reflect.ValueOf(i))
	)

	for {
		if !rows.Next() {
			// if error occured return rawselect
			if rows.Err() != nil {
				return nil, rows.Err()
			}
			// time to exit from outer "for" loop
			break
		}
		v := reflect.New(t)

		if isDynamic {
			v.Interface().(DynamicTable).SetTableName(tableName)
		}

		dest := make([]interface{}, len(cols))

		custScan := make([]CustomScanner, 0)

		for x := range cols {
			f := v.Elem()
			if intoStruct {
				index := colToFieldIndex[x]
				if index == nil {
					// this field is not present in the struct, so create a dummy
					// value for rows.Scan to scan into
					var dummy dummyField
					dest[x] = &dummy
					continue
				}
				f = f.FieldByIndex(index)
			}
			target := f.Addr().Interface()
			if conv != nil {
				scanner, ok := conv.FromDb(target)
				if ok {
					target = scanner.Holder
					custScan = append(custScan, scanner)
				}
			}
			dest[x] = target
		}

		err = rows.Scan(dest...)
		if err != nil {
			return nil, err
		}

		for _, c := range custScan {
			err = c.Bind()
			if err != nil {
				return nil, err
			}
		}

		if appendToSlice {
			if !pointerElements {
				v = v.Elem()
			}
			sliceValue.Set(reflect.Append(sliceValue, v))
		} else {
			list = append(list, v.Interface())
		}
	}

	if appendToSlice && sliceValue.IsNil() {
		sliceValue.Set(reflect.MakeSlice(sliceValue.Type(), 0, 0))
	}

	return list, nonFatalErr
}

// hasContext is an interface that is implemented by the context.Context and
// context.Value objects
type hasContext interface {
	Context() context.Context
}
