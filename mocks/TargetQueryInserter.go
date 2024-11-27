// Code generated by mockery v2.49.0. DO NOT EDIT.

package mocks

import (
	gorp "github.com/go-gorp/gorp/v3"
	mock "github.com/stretchr/testify/mock"
)

// TargetQueryInserter is an autogenerated mock type for the TargetQueryInserter type
type TargetQueryInserter struct {
	mock.Mock
}

// InsertQueryToTarget provides a mock function with given fields: exec, insertSql, idSql, target, params
func (_m *TargetQueryInserter) InsertQueryToTarget(exec gorp.SqlExecutor, insertSql string, idSql string, target interface{}, params ...interface{}) error {
	var _ca []interface{}
	_ca = append(_ca, exec, insertSql, idSql, target)
	_ca = append(_ca, params...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for InsertQueryToTarget")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(gorp.SqlExecutor, string, string, interface{}, ...interface{}) error); ok {
		r0 = rf(exec, insertSql, idSql, target, params...)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// NewTargetQueryInserter creates a new instance of TargetQueryInserter. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewTargetQueryInserter(t interface {
	mock.TestingT
	Cleanup(func())
}) *TargetQueryInserter {
	mock := &TargetQueryInserter{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}