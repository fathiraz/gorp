// Code generated by mockery v2.49.0. DO NOT EDIT.

package mocks

import (
	context "context"

	gorp "github.com/go-gorp/gorp/v3"
	mock "github.com/stretchr/testify/mock"
)

// HasPostInsert is an autogenerated mock type for the HasPostInsert type
type HasPostInsert struct {
	mock.Mock
}

// PostInsert provides a mock function with given fields: _a0, _a1
func (_m *HasPostInsert) PostInsert(_a0 context.Context, _a1 gorp.SqlExecutor) error {
	ret := _m.Called(_a0, _a1)

	if len(ret) == 0 {
		panic("no return value specified for PostInsert")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, gorp.SqlExecutor) error); ok {
		r0 = rf(_a0, _a1)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// NewHasPostInsert creates a new instance of HasPostInsert. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewHasPostInsert(t interface {
	mock.TestingT
	Cleanup(func())
}) *HasPostInsert {
	mock := &HasPostInsert{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
