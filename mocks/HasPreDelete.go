// Code generated by mockery v2.49.0. DO NOT EDIT.

package mocks

import (
	context "context"

	gorp "github.com/go-gorp/gorp/v3"
	mock "github.com/stretchr/testify/mock"
)

// HasPreDelete is an autogenerated mock type for the HasPreDelete type
type HasPreDelete struct {
	mock.Mock
}

// PreDelete provides a mock function with given fields: _a0, _a1
func (_m *HasPreDelete) PreDelete(_a0 context.Context, _a1 gorp.SqlExecutor) error {
	ret := _m.Called(_a0, _a1)

	if len(ret) == 0 {
		panic("no return value specified for PreDelete")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, gorp.SqlExecutor) error); ok {
		r0 = rf(_a0, _a1)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// NewHasPreDelete creates a new instance of HasPreDelete. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewHasPreDelete(t interface {
	mock.TestingT
	Cleanup(func())
}) *HasPreDelete {
	mock := &HasPreDelete{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}