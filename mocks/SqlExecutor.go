// Code generated by mockery v2.49.0. DO NOT EDIT.

package mocks

import (
	context "context"

	gorp "github.com/go-gorp/gorp/v3"
	mock "github.com/stretchr/testify/mock"

	sql "database/sql"

	sqlx "github.com/jmoiron/sqlx"
)

// SqlExecutor is an autogenerated mock type for the SqlExecutor type
type SqlExecutor struct {
	mock.Mock
}

// Delete provides a mock function with given fields: list
func (_m *SqlExecutor) Delete(list ...interface{}) (int64, error) {
	var _ca []interface{}
	_ca = append(_ca, list...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for Delete")
	}

	var r0 int64
	var r1 error
	if rf, ok := ret.Get(0).(func(...interface{}) (int64, error)); ok {
		return rf(list...)
	}
	if rf, ok := ret.Get(0).(func(...interface{}) int64); ok {
		r0 = rf(list...)
	} else {
		r0 = ret.Get(0).(int64)
	}

	if rf, ok := ret.Get(1).(func(...interface{}) error); ok {
		r1 = rf(list...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Exec provides a mock function with given fields: query, args
func (_m *SqlExecutor) Exec(query string, args ...interface{}) (sql.Result, error) {
	var _ca []interface{}
	_ca = append(_ca, query)
	_ca = append(_ca, args...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for Exec")
	}

	var r0 sql.Result
	var r1 error
	if rf, ok := ret.Get(0).(func(string, ...interface{}) (sql.Result, error)); ok {
		return rf(query, args...)
	}
	if rf, ok := ret.Get(0).(func(string, ...interface{}) sql.Result); ok {
		r0 = rf(query, args...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(sql.Result)
		}
	}

	if rf, ok := ret.Get(1).(func(string, ...interface{}) error); ok {
		r1 = rf(query, args...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Get provides a mock function with given fields: i, keys
func (_m *SqlExecutor) Get(i interface{}, keys ...interface{}) (interface{}, error) {
	var _ca []interface{}
	_ca = append(_ca, i)
	_ca = append(_ca, keys...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for Get")
	}

	var r0 interface{}
	var r1 error
	if rf, ok := ret.Get(0).(func(interface{}, ...interface{}) (interface{}, error)); ok {
		return rf(i, keys...)
	}
	if rf, ok := ret.Get(0).(func(interface{}, ...interface{}) interface{}); ok {
		r0 = rf(i, keys...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(interface{})
		}
	}

	if rf, ok := ret.Get(1).(func(interface{}, ...interface{}) error); ok {
		r1 = rf(i, keys...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Getx provides a mock function with given fields: dest, query, args
func (_m *SqlExecutor) Getx(dest interface{}, query string, args ...interface{}) error {
	var _ca []interface{}
	_ca = append(_ca, dest, query)
	_ca = append(_ca, args...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for Getx")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}, string, ...interface{}) error); ok {
		r0 = rf(dest, query, args...)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Insert provides a mock function with given fields: list
func (_m *SqlExecutor) Insert(list ...interface{}) error {
	var _ca []interface{}
	_ca = append(_ca, list...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for Insert")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(...interface{}) error); ok {
		r0 = rf(list...)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MustExec provides a mock function with given fields: query, args
func (_m *SqlExecutor) MustExec(query string, args ...interface{}) sql.Result {
	var _ca []interface{}
	_ca = append(_ca, query)
	_ca = append(_ca, args...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for MustExec")
	}

	var r0 sql.Result
	if rf, ok := ret.Get(0).(func(string, ...interface{}) sql.Result); ok {
		r0 = rf(query, args...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(sql.Result)
		}
	}

	return r0
}

// NamedExec provides a mock function with given fields: query, arg
func (_m *SqlExecutor) NamedExec(query string, arg interface{}) (sql.Result, error) {
	ret := _m.Called(query, arg)

	if len(ret) == 0 {
		panic("no return value specified for NamedExec")
	}

	var r0 sql.Result
	var r1 error
	if rf, ok := ret.Get(0).(func(string, interface{}) (sql.Result, error)); ok {
		return rf(query, arg)
	}
	if rf, ok := ret.Get(0).(func(string, interface{}) sql.Result); ok {
		r0 = rf(query, arg)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(sql.Result)
		}
	}

	if rf, ok := ret.Get(1).(func(string, interface{}) error); ok {
		r1 = rf(query, arg)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NamedQuery provides a mock function with given fields: query, arg
func (_m *SqlExecutor) NamedQuery(query string, arg interface{}) (*sqlx.Rows, error) {
	ret := _m.Called(query, arg)

	if len(ret) == 0 {
		panic("no return value specified for NamedQuery")
	}

	var r0 *sqlx.Rows
	var r1 error
	if rf, ok := ret.Get(0).(func(string, interface{}) (*sqlx.Rows, error)); ok {
		return rf(query, arg)
	}
	if rf, ok := ret.Get(0).(func(string, interface{}) *sqlx.Rows); ok {
		r0 = rf(query, arg)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*sqlx.Rows)
		}
	}

	if rf, ok := ret.Get(1).(func(string, interface{}) error); ok {
		r1 = rf(query, arg)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// PreparexNamed provides a mock function with given fields: query
func (_m *SqlExecutor) PreparexNamed(query string) (*sqlx.NamedStmt, error) {
	ret := _m.Called(query)

	if len(ret) == 0 {
		panic("no return value specified for PreparexNamed")
	}

	var r0 *sqlx.NamedStmt
	var r1 error
	if rf, ok := ret.Get(0).(func(string) (*sqlx.NamedStmt, error)); ok {
		return rf(query)
	}
	if rf, ok := ret.Get(0).(func(string) *sqlx.NamedStmt); ok {
		r0 = rf(query)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*sqlx.NamedStmt)
		}
	}

	if rf, ok := ret.Get(1).(func(string) error); ok {
		r1 = rf(query)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Query provides a mock function with given fields: query, args
func (_m *SqlExecutor) Query(query string, args ...interface{}) (*sql.Rows, error) {
	var _ca []interface{}
	_ca = append(_ca, query)
	_ca = append(_ca, args...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for Query")
	}

	var r0 *sql.Rows
	var r1 error
	if rf, ok := ret.Get(0).(func(string, ...interface{}) (*sql.Rows, error)); ok {
		return rf(query, args...)
	}
	if rf, ok := ret.Get(0).(func(string, ...interface{}) *sql.Rows); ok {
		r0 = rf(query, args...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*sql.Rows)
		}
	}

	if rf, ok := ret.Get(1).(func(string, ...interface{}) error); ok {
		r1 = rf(query, args...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// QueryRow provides a mock function with given fields: query, args
func (_m *SqlExecutor) QueryRow(query string, args ...interface{}) *sql.Row {
	var _ca []interface{}
	_ca = append(_ca, query)
	_ca = append(_ca, args...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for QueryRow")
	}

	var r0 *sql.Row
	if rf, ok := ret.Get(0).(func(string, ...interface{}) *sql.Row); ok {
		r0 = rf(query, args...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*sql.Row)
		}
	}

	return r0
}

// QueryRowx provides a mock function with given fields: query, args
func (_m *SqlExecutor) QueryRowx(query string, args ...interface{}) *sqlx.Row {
	var _ca []interface{}
	_ca = append(_ca, query)
	_ca = append(_ca, args...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for QueryRowx")
	}

	var r0 *sqlx.Row
	if rf, ok := ret.Get(0).(func(string, ...interface{}) *sqlx.Row); ok {
		r0 = rf(query, args...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*sqlx.Row)
		}
	}

	return r0
}

// Queryx provides a mock function with given fields: query, args
func (_m *SqlExecutor) Queryx(query string, args ...interface{}) (*sqlx.Rows, error) {
	var _ca []interface{}
	_ca = append(_ca, query)
	_ca = append(_ca, args...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for Queryx")
	}

	var r0 *sqlx.Rows
	var r1 error
	if rf, ok := ret.Get(0).(func(string, ...interface{}) (*sqlx.Rows, error)); ok {
		return rf(query, args...)
	}
	if rf, ok := ret.Get(0).(func(string, ...interface{}) *sqlx.Rows); ok {
		r0 = rf(query, args...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*sqlx.Rows)
		}
	}

	if rf, ok := ret.Get(1).(func(string, ...interface{}) error); ok {
		r1 = rf(query, args...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Select provides a mock function with given fields: i, query, args
func (_m *SqlExecutor) Select(i interface{}, query string, args ...interface{}) ([]interface{}, error) {
	var _ca []interface{}
	_ca = append(_ca, i, query)
	_ca = append(_ca, args...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for Select")
	}

	var r0 []interface{}
	var r1 error
	if rf, ok := ret.Get(0).(func(interface{}, string, ...interface{}) ([]interface{}, error)); ok {
		return rf(i, query, args...)
	}
	if rf, ok := ret.Get(0).(func(interface{}, string, ...interface{}) []interface{}); ok {
		r0 = rf(i, query, args...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]interface{})
		}
	}

	if rf, ok := ret.Get(1).(func(interface{}, string, ...interface{}) error); ok {
		r1 = rf(i, query, args...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// SelectBool provides a mock function with given fields: query, args
func (_m *SqlExecutor) SelectBool(query string, args ...interface{}) (bool, error) {
	var _ca []interface{}
	_ca = append(_ca, query)
	_ca = append(_ca, args...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for SelectBool")
	}

	var r0 bool
	var r1 error
	if rf, ok := ret.Get(0).(func(string, ...interface{}) (bool, error)); ok {
		return rf(query, args...)
	}
	if rf, ok := ret.Get(0).(func(string, ...interface{}) bool); ok {
		r0 = rf(query, args...)
	} else {
		r0 = ret.Get(0).(bool)
	}

	if rf, ok := ret.Get(1).(func(string, ...interface{}) error); ok {
		r1 = rf(query, args...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// SelectFloat provides a mock function with given fields: query, args
func (_m *SqlExecutor) SelectFloat(query string, args ...interface{}) (float64, error) {
	var _ca []interface{}
	_ca = append(_ca, query)
	_ca = append(_ca, args...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for SelectFloat")
	}

	var r0 float64
	var r1 error
	if rf, ok := ret.Get(0).(func(string, ...interface{}) (float64, error)); ok {
		return rf(query, args...)
	}
	if rf, ok := ret.Get(0).(func(string, ...interface{}) float64); ok {
		r0 = rf(query, args...)
	} else {
		r0 = ret.Get(0).(float64)
	}

	if rf, ok := ret.Get(1).(func(string, ...interface{}) error); ok {
		r1 = rf(query, args...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// SelectInt provides a mock function with given fields: query, args
func (_m *SqlExecutor) SelectInt(query string, args ...interface{}) (int64, error) {
	var _ca []interface{}
	_ca = append(_ca, query)
	_ca = append(_ca, args...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for SelectInt")
	}

	var r0 int64
	var r1 error
	if rf, ok := ret.Get(0).(func(string, ...interface{}) (int64, error)); ok {
		return rf(query, args...)
	}
	if rf, ok := ret.Get(0).(func(string, ...interface{}) int64); ok {
		r0 = rf(query, args...)
	} else {
		r0 = ret.Get(0).(int64)
	}

	if rf, ok := ret.Get(1).(func(string, ...interface{}) error); ok {
		r1 = rf(query, args...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// SelectNullBool provides a mock function with given fields: query, args
func (_m *SqlExecutor) SelectNullBool(query string, args ...interface{}) (sql.NullBool, error) {
	var _ca []interface{}
	_ca = append(_ca, query)
	_ca = append(_ca, args...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for SelectNullBool")
	}

	var r0 sql.NullBool
	var r1 error
	if rf, ok := ret.Get(0).(func(string, ...interface{}) (sql.NullBool, error)); ok {
		return rf(query, args...)
	}
	if rf, ok := ret.Get(0).(func(string, ...interface{}) sql.NullBool); ok {
		r0 = rf(query, args...)
	} else {
		r0 = ret.Get(0).(sql.NullBool)
	}

	if rf, ok := ret.Get(1).(func(string, ...interface{}) error); ok {
		r1 = rf(query, args...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// SelectNullFloat provides a mock function with given fields: query, args
func (_m *SqlExecutor) SelectNullFloat(query string, args ...interface{}) (sql.NullFloat64, error) {
	var _ca []interface{}
	_ca = append(_ca, query)
	_ca = append(_ca, args...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for SelectNullFloat")
	}

	var r0 sql.NullFloat64
	var r1 error
	if rf, ok := ret.Get(0).(func(string, ...interface{}) (sql.NullFloat64, error)); ok {
		return rf(query, args...)
	}
	if rf, ok := ret.Get(0).(func(string, ...interface{}) sql.NullFloat64); ok {
		r0 = rf(query, args...)
	} else {
		r0 = ret.Get(0).(sql.NullFloat64)
	}

	if rf, ok := ret.Get(1).(func(string, ...interface{}) error); ok {
		r1 = rf(query, args...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// SelectNullInt provides a mock function with given fields: query, args
func (_m *SqlExecutor) SelectNullInt(query string, args ...interface{}) (sql.NullInt64, error) {
	var _ca []interface{}
	_ca = append(_ca, query)
	_ca = append(_ca, args...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for SelectNullInt")
	}

	var r0 sql.NullInt64
	var r1 error
	if rf, ok := ret.Get(0).(func(string, ...interface{}) (sql.NullInt64, error)); ok {
		return rf(query, args...)
	}
	if rf, ok := ret.Get(0).(func(string, ...interface{}) sql.NullInt64); ok {
		r0 = rf(query, args...)
	} else {
		r0 = ret.Get(0).(sql.NullInt64)
	}

	if rf, ok := ret.Get(1).(func(string, ...interface{}) error); ok {
		r1 = rf(query, args...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// SelectNullStr provides a mock function with given fields: query, args
func (_m *SqlExecutor) SelectNullStr(query string, args ...interface{}) (sql.NullString, error) {
	var _ca []interface{}
	_ca = append(_ca, query)
	_ca = append(_ca, args...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for SelectNullStr")
	}

	var r0 sql.NullString
	var r1 error
	if rf, ok := ret.Get(0).(func(string, ...interface{}) (sql.NullString, error)); ok {
		return rf(query, args...)
	}
	if rf, ok := ret.Get(0).(func(string, ...interface{}) sql.NullString); ok {
		r0 = rf(query, args...)
	} else {
		r0 = ret.Get(0).(sql.NullString)
	}

	if rf, ok := ret.Get(1).(func(string, ...interface{}) error); ok {
		r1 = rf(query, args...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// SelectOne provides a mock function with given fields: holder, query, args
func (_m *SqlExecutor) SelectOne(holder interface{}, query string, args ...interface{}) error {
	var _ca []interface{}
	_ca = append(_ca, holder, query)
	_ca = append(_ca, args...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for SelectOne")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}, string, ...interface{}) error); ok {
		r0 = rf(holder, query, args...)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SelectStr provides a mock function with given fields: query, args
func (_m *SqlExecutor) SelectStr(query string, args ...interface{}) (string, error) {
	var _ca []interface{}
	_ca = append(_ca, query)
	_ca = append(_ca, args...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for SelectStr")
	}

	var r0 string
	var r1 error
	if rf, ok := ret.Get(0).(func(string, ...interface{}) (string, error)); ok {
		return rf(query, args...)
	}
	if rf, ok := ret.Get(0).(func(string, ...interface{}) string); ok {
		r0 = rf(query, args...)
	} else {
		r0 = ret.Get(0).(string)
	}

	if rf, ok := ret.Get(1).(func(string, ...interface{}) error); ok {
		r1 = rf(query, args...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Selectx provides a mock function with given fields: dest, query, args
func (_m *SqlExecutor) Selectx(dest interface{}, query string, args ...interface{}) error {
	var _ca []interface{}
	_ca = append(_ca, dest, query)
	_ca = append(_ca, args...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for Selectx")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(interface{}, string, ...interface{}) error); ok {
		r0 = rf(dest, query, args...)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Update provides a mock function with given fields: list
func (_m *SqlExecutor) Update(list ...interface{}) (int64, error) {
	var _ca []interface{}
	_ca = append(_ca, list...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for Update")
	}

	var r0 int64
	var r1 error
	if rf, ok := ret.Get(0).(func(...interface{}) (int64, error)); ok {
		return rf(list...)
	}
	if rf, ok := ret.Get(0).(func(...interface{}) int64); ok {
		r0 = rf(list...)
	} else {
		r0 = ret.Get(0).(int64)
	}

	if rf, ok := ret.Get(1).(func(...interface{}) error); ok {
		r1 = rf(list...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// WithContext provides a mock function with given fields: ctx
func (_m *SqlExecutor) WithContext(ctx context.Context) gorp.SqlExecutor {
	ret := _m.Called(ctx)

	if len(ret) == 0 {
		panic("no return value specified for WithContext")
	}

	var r0 gorp.SqlExecutor
	if rf, ok := ret.Get(0).(func(context.Context) gorp.SqlExecutor); ok {
		r0 = rf(ctx)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(gorp.SqlExecutor)
		}
	}

	return r0
}

// NewSqlExecutor creates a new instance of SqlExecutor. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewSqlExecutor(t interface {
	mock.TestingT
	Cleanup(func())
}) *SqlExecutor {
	mock := &SqlExecutor{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
