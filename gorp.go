// Copyright 2012 James Cooper. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package gorp

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

// SqlTyper is a type that returns its database type.  Most of the
// time, the type can just use "database/sql/driver".Valuer; but when
// it returns nil for its empty value, it needs to implement SqlTyper
// to have its column type detected properly during table creation.
type SqlTyper interface {
	SqlType() driver.Value
}

// legacySqlTyper prevents breaking clients who depended on the previous
// SqlTyper interface
type legacySqlTyper interface {
	SqlType() driver.Valuer
}

// for fields that exists in DB table, but not exists in struct
type dummyField struct{}

// Scan implements the Scanner interface.
func (nt *dummyField) Scan(value interface{}) error {
	return nil
}

var zeroVal reflect.Value
var versFieldConst = "[gorp_ver_field]"

// The TypeConverter interface provides a way to map a value of one
// type to another type when persisting to, or loading from, a database.
//
// Example use cases: Implement type converter to convert bool types to "y"/"n" strings,
// or serialize a struct member as a JSON blob.
type TypeConverter interface {
	// ToDb converts val to another type. Called before INSERT/UPDATE operations
	ToDb(val interface{}) (interface{}, error)

	// FromDb returns a CustomScanner appropriate for this type. This will be used
	// to hold values returned from SELECT queries.
	//
	// In particular the CustomScanner returned should implement a Binder
	// function appropriate for the Go type you wish to convert the db value to
	//
	// If bool==false, then no custom scanner will be used for this field.
	FromDb(target interface{}) (CustomScanner, bool)
}

// SqlExecutor exposes gorp operations that can be run from Pre/Post
// hooks.  This hides whether the current operation that triggered the
// hook is in a transaction.
type SqlExecutor interface {
	WithContext(ctx context.Context) SqlExecutor

	// Legacy gorp methods
	Get(i interface{}, keys ...interface{}) (interface{}, error)
	Insert(list ...interface{}) error
	Update(list ...interface{}) (int64, error)
	Delete(list ...interface{}) (int64, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
	Select(i interface{}, query string, args ...interface{}) ([]interface{}, error)
	SelectInt(query string, args ...interface{}) (int64, error)
	SelectNullInt(query string, args ...interface{}) (sql.NullInt64, error)
	SelectFloat(query string, args ...interface{}) (float64, error)
	SelectNullFloat(query string, args ...interface{}) (sql.NullFloat64, error)
	SelectStr(query string, args ...interface{}) (string, error)
	SelectNullStr(query string, args ...interface{}) (sql.NullString, error)
	SelectBool(query string, args ...interface{}) (bool, error)
	SelectNullBool(query string, args ...interface{}) (sql.NullBool, error)
	SelectOne(holder interface{}, query string, args ...interface{}) error

	// Standard sql query methods
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row

	// sqlx enhanced methods
	Queryx(query string, args ...interface{}) (*sqlx.Rows, error)
	QueryRowx(query string, args ...interface{}) *sqlx.Row
	MustExec(query string, args ...interface{}) sql.Result

	// sqlx Get/Select methods
	Getx(dest interface{}, query string, args ...interface{}) error
	Selectx(dest interface{}, query string, args ...interface{}) error

	// sqlx named query methods
	NamedExec(query string, arg interface{}) (sql.Result, error)
	NamedQuery(query string, arg interface{}) (*sqlx.Rows, error)

	// sqlx prepared statements
	PreparexNamed(query string) (*sqlx.NamedStmt, error)
}

// DynamicTable allows the users of gorp to dynamically
// use different database table names during runtime
// while sharing the same golang struct for in-memory data
type DynamicTable interface {
	TableName() string
	SetTableName(string)
}

// Compile-time check that DbMap and Transaction implement the SqlExecutor
// interface.
var _, _ SqlExecutor = &DbMap{}, &Transaction{}

// argValue returns the underlying value for driver.Valuer types, or the original value if not a Valuer.
// This is used to properly handle nil values and custom database types.
func argValue(a interface{}) interface{} {
	v, ok := a.(driver.Valuer)
	if !ok {
		return a
	}
	vV := reflect.ValueOf(v)
	if vV.Kind() == reflect.Ptr && vV.IsNil() {
		return nil
	}
	ret, err := v.Value()
	if err != nil {
		return a
	}
	return ret
}

// argsString formats a slice of arguments into a string for logging purposes.
// It handles special formatting for strings and provides positional references.
func argsString(args ...interface{}) string {
	var margs string
	for i, a := range args {
		v := argValue(a)
		switch v.(type) {
		case string:
			v = fmt.Sprintf("%q", v)
		default:
			v = fmt.Sprintf("%v", v)
		}
		margs += fmt.Sprintf("%d:%s", i+1, v)
		if i+1 < len(args) {
			margs += " "
		}
	}
	return margs
}

// Calls the Exec function on the executor, but attempts to expand any eligible named
// query arguments first.
func maybeExpandNamedQueryAndExec(e SqlExecutor, query string, args ...interface{}) (sql.Result, error) {
	dbMap := extractDbMap(e)

	if len(args) == 1 {
		query, args = maybeExpandNamedQuery(dbMap, query, args)
	}

	return exec(e, query, args...)
}

// extractDbMap retrieves the underlying DbMap from either a DbMap or Transaction executor.
func extractDbMap(e SqlExecutor) *DbMap {
	switch m := e.(type) {
	case *DbMap:
		return m
	case *Transaction:
		return m.dbmap
	}
	return nil
}

// executor exposes both sql.DB/sql.Tx and sqlx.DB/sqlx.Tx functions so that it can be used
// on internal functions that need to be agnostic to the underlying object.
type executor interface {
	// Standard sql methods
	Exec(query string, args ...interface{}) (sql.Result, error)
	Prepare(query string) (*sql.Stmt, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Query(query string, args ...interface{}) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)

	// sqlx variants
	MustExec(query string, args ...interface{}) sql.Result
	Preparex(query string) (*sqlx.Stmt, error)
	QueryRowx(query string, args ...interface{}) *sqlx.Row
	Queryx(query string, args ...interface{}) (*sqlx.Rows, error)
	PreparexContext(ctx context.Context, query string) (*sqlx.Stmt, error)
	QueryRowxContext(ctx context.Context, query string, args ...interface{}) *sqlx.Row
	QueryxContext(ctx context.Context, query string, args ...interface{}) (*sqlx.Rows, error)

	// sqlx Get/Select methods
	Get(dest interface{}, query string, args ...interface{}) error
	Select(dest interface{}, query string, args ...interface{}) error

	// sqlx named query methods
	NamedExec(query string, arg interface{}) (sql.Result, error)
	NamedQuery(query string, arg interface{}) (*sqlx.Rows, error)
	PrepareNamed(query string) (*sqlx.NamedStmt, error)
}

func extractExecutorAndContext(e SqlExecutor) (executor, context.Context) {
	switch m := e.(type) {
	case *DbMap:
		return m.Db, m.ctx
	case *Transaction:
		return m.tx, m.ctx
	}
	return nil, nil
}

// maybeExpandNamedQuery checks the given arg to see if it's eligible to be used
// as input to a named query.  If so, it rewrites the query to use
// dialect-dependent bindvars and instantiates the corresponding slice of
// parameters by extracting data from the map / struct.
// If not, returns the input values unchanged.
func maybeExpandNamedQuery(m *DbMap, query string, args []interface{}) (string, []interface{}) {
	var (
		arg    = args[0]
		argval = reflect.ValueOf(arg)
	)
	if argval.Kind() == reflect.Ptr {
		argval = argval.Elem()
	}

	if argval.Kind() == reflect.Map && argval.Type().Key().Kind() == reflect.String {
		return expandNamedQuery(m, query, func(key string) reflect.Value {
			return argval.MapIndex(reflect.ValueOf(key))
		})
	}
	if argval.Kind() != reflect.Struct {
		return query, args
	}
	if _, ok := arg.(time.Time); ok {
		// time.Time is driver.Value
		return query, args
	}
	if _, ok := arg.(driver.Valuer); ok {
		// driver.Valuer will be converted to driver.Value.
		return query, args
	}

	return expandNamedQuery(m, query, argval.FieldByName)
}

var keyRegexp = regexp.MustCompile(`:[[:word:]]+`)

// expandNamedQuery accepts a query with placeholders of the form ":key", and a
// single arg of Kind Struct or Map[string].  It returns the query with the
// dialect's placeholders, and a slice of args ready for positional insertion
// into the query.
func expandNamedQuery(m *DbMap, query string, keyGetter func(key string) reflect.Value) (string, []interface{}) {
	var (
		n    int
		args []interface{}
	)
	return keyRegexp.ReplaceAllStringFunc(query, func(key string) string {
		val := keyGetter(key[1:])
		if !val.IsValid() {
			return key
		}
		args = append(args, val.Interface())
		newVar := m.Dialect.BindVar(n)
		n++
		return newVar
	}), args
}

// columnToFieldIndex returns a slice of index paths mapping database columns to struct fields.
// It handles nested structs and embedded fields.
func columnToFieldIndex(m *DbMap, t reflect.Type, name string, cols []string) ([][]int, error) {
	colToFieldIndex := make([][]int, len(cols))

	// check if type t is a mapped table - if so we'll
	// check the table for column aliasing below
	tableMapped := false
	table := tableOrNil(m, t, name)
	if table != nil {
		tableMapped = true
	}

	// Loop over column names and find field in i to bind to
	// based on column name. all returned columns must match
	// a field in the i struct
	missingColNames := []string{}
	for x := range cols {
		colName := strings.ToLower(cols[x])
		field, found := t.FieldByNameFunc(func(fieldName string) bool {
			field, _ := t.FieldByName(fieldName)
			cArguments := strings.Split(field.Tag.Get("db"), ",")
			fieldName = cArguments[0]

			if fieldName == "-" {
				return false
			} else if fieldName == "" {
				fieldName = field.Name
			}
			if tableMapped {
				colMap := table.colMapOrNil(fieldName)
				if colMap != nil {
					fieldName = colMap.ColumnName
				}
			}
			return colName == strings.ToLower(fieldName)
		})
		if found {
			colToFieldIndex[x] = field.Index
		}
		if colToFieldIndex[x] == nil {
			missingColNames = append(missingColNames, colName)
		}
	}
	if len(missingColNames) > 0 {
		return colToFieldIndex, &NoFieldInTypeError{
			TypeName:        t.Name(),
			MissingColNames: missingColNames,
		}
	}
	return colToFieldIndex, nil
}

// fieldByName finds a field by name in a struct value, supporting embedded fields.
// Returns nil if the field is not found.
func fieldByName(val reflect.Value, fieldName string) *reflect.Value {
	// try to find field by exact match
	f := val.FieldByName(fieldName)

	if f != zeroVal {
		return &f
	}

	// try to find by case insensitive match - only the Postgres driver
	// seems to require this - in the case where columns are aliased in the sql
	fieldNameL := strings.ToLower(fieldName)
	fieldCount := val.NumField()
	t := val.Type()
	for i := 0; i < fieldCount; i++ {
		sf := t.Field(i)
		if strings.ToLower(sf.Name) == fieldNameL {
			f := val.Field(i)
			return &f
		}
	}

	return nil
}

// toSliceType returns the element type of the given object, if the object is a
// "*[]*Element" or "*[]Element". If not, returns nil.
// err is returned if the user was trying to pass a pointer-to-slice but failed.
func toSliceType(i interface{}) (reflect.Type, error) {
	t := reflect.TypeOf(i)
	if t.Kind() != reflect.Ptr {
		// If it's a slice, return a more helpful error message
		if t.Kind() == reflect.Slice {
			return nil, fmt.Errorf("gorp: cannot SELECT into a non-pointer slice: %v", t)
		}
		return nil, nil
	}
	if t = t.Elem(); t.Kind() != reflect.Slice {
		return nil, nil
	}
	return t.Elem(), nil
}

func toType(i interface{}) (reflect.Type, error) {
	t := reflect.TypeOf(i)

	// If a Pointer to a type, follow
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("gorp: cannot SELECT into this type: %v", reflect.TypeOf(i))
	}
	return t, nil
}

type foundTable struct {
	table   *TableMap
	dynName *string
}

// tableFor returns the TableMap and table name for a given struct type.
// It handles both static and dynamic table names.
func tableFor(m *DbMap, t reflect.Type, i interface{}) (*foundTable, error) {
	if dyn, isDynamic := i.(DynamicTable); isDynamic {
		tableName := dyn.TableName()
		table, err := m.DynamicTableFor(tableName, true)
		if err != nil {
			return nil, err
		}
		return &foundTable{
			table:   table,
			dynName: &tableName,
		}, nil
	}
	table, err := m.TableFor(t, true)
	if err != nil {
		return nil, err
	}
	return &foundTable{table: table}, nil
}

// get retrieves a single record from the database and loads it into the specified struct.
// It returns an error if the record is not found or if there's an issue with the query.
func get(m *DbMap, exec SqlExecutor, i interface{}, keys ...interface{}) (interface{}, error) {
	t, err := toType(i)
	if err != nil {
		return nil, err
	}

	foundTable, err := tableFor(m, t, i)
	if err != nil {
		return nil, err
	}
	table := foundTable.table

	plan := table.bindGet()

	v := reflect.New(t)
	if foundTable.dynName != nil {
		retDyn := v.Interface().(DynamicTable)
		retDyn.SetTableName(*foundTable.dynName)
	}

	dest := make([]interface{}, len(plan.argFields))

	conv := m.TypeConverter
	custScan := make([]CustomScanner, 0)

	for x, fieldName := range plan.argFields {
		f := v.Elem().FieldByName(fieldName)
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

	row := exec.QueryRow(plan.query, keys...)
	err = row.Scan(dest...)
	if err != nil {
		if err == sql.ErrNoRows {
			err = nil
		}
		return nil, err
	}

	for _, c := range custScan {
		err = c.Bind()
		if err != nil {
			return nil, err
		}
	}

	if v, ok := v.Interface().(HasPostGet); ok {
		err := v.PostGet(m.ctx, exec)
		if err != nil {
			return nil, err
		}
	}

	return v.Interface(), nil
}

// delete removes one or more records from the database.
// It executes pre/post delete hooks and returns the number of affected rows.
func delete(m *DbMap, exec SqlExecutor, list ...interface{}) (int64, error) {
	count := int64(0)
	for _, ptr := range list {
		table, elem, err := m.tableForPointer(ptr, true)
		if err != nil {
			return -1, err
		}

		eval := elem.Addr().Interface()
		if v, ok := eval.(HasPreDelete); ok {
			err = v.PreDelete(m.ctx, exec)
			if err != nil {
				return -1, err
			}
		}

		bi, err := table.bindDelete(elem)
		if err != nil {
			return -1, err
		}

		res, err := exec.Exec(bi.query, bi.args...)
		if err != nil {
			return -1, err
		}
		rows, err := res.RowsAffected()
		if err != nil {
			return -1, err
		}

		if rows == 0 && bi.existingVersion > 0 {
			return lockError(m, exec, table.TableName,
				bi.existingVersion, elem, bi.keys...)
		}

		count += rows

		if v, ok := eval.(HasPostDelete); ok {
			err := v.PostDelete(m.ctx, exec)
			if err != nil {
				return -1, err
			}
		}
	}

	return count, nil
}

// update modifies one or more records in the database.
// It executes pre/post update hooks and returns the number of affected rows.
func update(m *DbMap, exec SqlExecutor, colFilter ColumnFilter, list ...interface{}) (int64, error) {
	count := int64(0)
	for _, ptr := range list {
		table, elem, err := m.tableForPointer(ptr, true)
		if err != nil {
			return -1, err
		}

		eval := elem.Addr().Interface()
		if v, ok := eval.(HasPreUpdate); ok {
			err = v.PreUpdate(m.ctx, exec)
			if err != nil {
				return -1, err
			}
		}

		bi, err := table.bindUpdate(elem, colFilter)
		if err != nil {
			return -1, err
		}

		res, err := exec.Exec(bi.query, bi.args...)
		if err != nil {
			return -1, err
		}

		rows, err := res.RowsAffected()
		if err != nil {
			return -1, err
		}

		if rows == 0 && bi.existingVersion > 0 {
			return lockError(m, exec, table.TableName,
				bi.existingVersion, elem, bi.keys...)
		}

		if bi.versField != "" {
			elem.FieldByName(bi.versField).SetInt(bi.existingVersion + 1)
		}

		count += rows

		if v, ok := eval.(HasPostUpdate); ok {
			err = v.PostUpdate(m.ctx, exec)
			if err != nil {
				return -1, err
			}
		}
	}
	return count, nil
}

// insert adds one or more records to the database.
// It executes pre/post insert hooks and handles auto-incrementing primary keys.
func insert(m *DbMap, exec SqlExecutor, list ...interface{}) error {
	for _, ptr := range list {
		table, elem, err := m.tableForPointer(ptr, false)
		if err != nil {
			return err
		}

		eval := elem.Addr().Interface()
		if v, ok := eval.(HasPreInsert); ok {
			err := v.PreInsert(m.ctx, exec)
			if err != nil {
				return err
			}
		}

		bi, err := table.bindInsert(elem)
		if err != nil {
			return err
		}

		if bi.autoIncrIdx > -1 {
			f := elem.FieldByName(bi.autoIncrFieldName)
			switch inserter := m.Dialect.(type) {
			case IntegerAutoIncrInserter:
				id, err := inserter.InsertAutoIncr(exec, bi.query, bi.args...)
				if err != nil {
					return err
				}
				k := f.Kind()
				if (k == reflect.Int) || (k == reflect.Int16) || (k == reflect.Int32) || (k == reflect.Int64) {
					f.SetInt(id)
				} else if (k == reflect.Uint) || (k == reflect.Uint16) || (k == reflect.Uint32) || (k == reflect.Uint64) {
					f.SetUint(uint64(id))
				} else {
					return fmt.Errorf("gorp: cannot set autoincrement value on non-Int field. SQL=%s  autoIncrIdx=%d autoIncrFieldName=%s", bi.query, bi.autoIncrIdx, bi.autoIncrFieldName)
				}
			case TargetedAutoIncrInserter:
				err := inserter.InsertAutoIncrToTarget(exec, bi.query, f.Addr().Interface(), bi.args...)
				if err != nil {
					return err
				}
			case TargetQueryInserter:
				var idQuery = table.ColMap(bi.autoIncrFieldName).GeneratedIdQuery
				if idQuery == "" {
					return fmt.Errorf("gorp: cannot set %s value if its ColumnMap.GeneratedIdQuery is empty", bi.autoIncrFieldName)
				}
				err := inserter.InsertQueryToTarget(exec, bi.query, idQuery, f.Addr().Interface(), bi.args...)
				if err != nil {
					return err
				}
			default:
				return fmt.Errorf("gorp: cannot use autoincrement fields on dialects that do not implement an autoincrementing interface")
			}
		} else {
			_, err := exec.Exec(bi.query, bi.args...)
			if err != nil {
				return err
			}
		}

		if v, ok := eval.(HasPostInsert); ok {
			err := v.PostInsert(m.ctx, exec)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// exec executes a SQL query and returns the result.
func exec(e SqlExecutor, query string, args ...interface{}) (sql.Result, error) {
	executor, ctx := extractExecutorAndContext(e)

	if ctx != nil {
		return executor.ExecContext(ctx, query, args...)
	}

	return executor.Exec(query, args...)
}
